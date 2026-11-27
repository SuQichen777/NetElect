package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader    = 2
)

const (
	ElectionTimeoutMin = 150 // ms
	ElectionTimeoutMax = 300 // ms
	HeartbeatInterval  = 50  // ms
)

type Raft struct {
	mu    sync.Mutex
	peers []*Peer
	me    int
	dead  int32

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a dummy entry; real entries start at index 1

	// Volatile state
	state         int
	leaderId      int
	lastHeartbeat time.Time
	commitIndex   int // highest log index known to be committed
	lastApplied   int // highest log index applied to state machine

	// Leader-only volatile state (reinitialized after each election)
	nextIndex  []int // for each peer: next log index to send
	matchIndex []int // for each peer: highest log index known replicated

	// ReadIndex state
	pendingReads []*readRequest // pending read requests waiting to be batched
	readNotify   chan struct{}  // signal that wakes the batching goroutine

	// Apply state
	kvStore       map[string]string   // KV state machine
	pendingWrites map[int]chan string // log index → channel waiting for apply result
	applyNotify   chan struct{}       // signals apply loop when commitIndex advances

	// NetElect state (latency-aware leader switching)
	ne netelectState

	// Election message counters for experiment metrics.
	// Incremented atomically; accessible via GetElectionMsgCount().
	electionMsgsSent int64
	electionMsgsRecv int64

	// Fault injection: sleep before responding to Ping (makes peers see high RTT to this node).
	// Controlled by SetPingDelay(); 0 = no delay.
	faultPingDelayMs int64

	// Fault injection: if > 0, the node steps down this long after each time it
	// becomes leader (recurring forced failover). Set via WithLeaderStepdown.
	leaderStepdownAfter time.Duration
}

type readRequest struct {
	done chan bool // signals completion; true means success
}

// Make creates a new Raft instance.
// Optional RaftOptions (e.g. WithNetElect) enable extensions.
func Make(peers []*Peer, me int, opts ...RaftOption) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = StateFollower
	rf.votedFor = -1
	rf.leaderId = -1
	rf.lastHeartbeat = time.Now()
	rf.readNotify = make(chan struct{}, 1)
	// dummy entry at index 0 so real entries start at index 1
	rf.log = []LogEntry{{Term: 0, Index: 0}}
	rf.kvStore = make(map[string]string)
	rf.pendingWrites = make(map[int]chan string)
	rf.applyNotify = make(chan struct{}, 1)

	// Apply options
	for _, opt := range opts {
		opt(rf)
	}

	go rf.ticker()
	go rf.batchReadIndexLoop()
	go rf.applyLoop()

	// If netelect is enabled, start the latency monitor and the penalty-score broadcast
	if rf.netelectEnabled() {
		go rf.latencyMonitorLoop()
		go rf.penaltyBroadcastLoop()
	}

	return rf
}

// WithLeaderStepdown makes the node step down `d` after every time it becomes
// leader — a recurring forced failover used by the failure experiment (each
// elected leader crashes after `d`, driving repeated re-elections). 0 disables.
func WithLeaderStepdown(d time.Duration) RaftOption {
	return func(rf *Raft) {
		rf.leaderStepdownAfter = d
	}
}

// scheduleLeaderStepdown arms a one-shot timer for the leadership won at `term`.
// When it fires it steps down only if the node is still leader for that same term,
// so a stale timer from a past leadership never trips a later one. Called under rf.mu.
func (rf *Raft) scheduleLeaderStepdown(term int) {
	if rf.leaderStepdownAfter <= 0 {
		return
	}
	go func() {
		time.Sleep(rf.leaderStepdownAfter)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		stale := rf.state != StateLeader || rf.currentTerm != term
		rf.mu.Unlock()
		if stale {
			return
		}
		stepped := rf.StepDown()
		fmt.Printf("fault_stepdown: node=%d term=%d stepped=%v ts=%d\n",
			rf.me, term, stepped, time.Now().UnixMilli())
	}()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// GetState returns the current term and whether this node is the Leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

// Kill stops the Raft instance
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// StepDown forces the current leader to become a follower, triggering re-election.
// Returns true if this node was the leader and stepped down; false if already a follower/candidate.
// lastHeartbeat is set to now so this node does not immediately re-campaign.
func (rf *Raft) StepDown() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return false
	}
	rf.state = StateFollower
	rf.lastHeartbeat = time.Now()
	rf.leaderId = -1
	return true
}

// GetElectionMsgCount returns cumulative RequestVote messages sent and received.
// Used for experiment metrics — call at shutdown and log the result.
func (rf *Raft) GetElectionMsgCount() (sent, recv int64) {
	return atomic.LoadInt64(&rf.electionMsgsSent), atomic.LoadInt64(&rf.electionMsgsRecv)
}

// RequestVote RPC Handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	atomic.AddInt64(&rf.electionMsgsRecv, 1)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Petition special case: the proposed leader asks the current leader to step down
	if args.IsPetition {
		if rf.state == StateLeader {
			fmt.Printf("[petition_stepdown] node=%d term=%d forced_by=%d\n",
				rf.me, rf.currentTerm, args.CandidateId)
			rf.state = StateFollower
			rf.leaderId = -1
			rf.clearPetitionState()
			// No vote is cast; the leader simply steps down
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.clearPetitionState()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	// candidate's log must be at least as up-to-date as ours
	candidateUpToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
		rf.state = StateFollower
	}
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.clearPetitionState()
	}
	rf.state = StateFollower
	rf.leaderId = args.LeaderId
	rf.lastHeartbeat = time.Now()

	// 1. Log consistency check at PrevLogIndex/PrevLogTerm
	if args.PrevLogIndex > rf.lastLogIndex() {
		// our log is too short — tell leader where it ends
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// term mismatch — find the first index of the conflicting term
		// so the leader can skip the whole conflicting term in one step
		ct := rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && rf.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = ci
		return
	}

	// 2. Append entries, truncating any conflicts
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx >= len(rf.log) || rf.log[idx].Term != entry.Term {
			rf.log = append(rf.log[:idx], args.Entries[i:]...)
			break
		}
	}

	// 3. Advance commitIndex to match leader
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		select {
		case rf.applyNotify <- struct{}{}:
		default:
		}
	}

	reply.Success = true
}

// Ping RPC Handler
func (rf *Raft) Ping(args *PingArgs, reply *PingReply) {
	if ms := atomic.LoadInt64(&rf.faultPingDelayMs); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	reply.NodeId = rf.me
}

// SetPingDelay configures an artificial delay injected before each Ping response.
// Other nodes measure RTT to this node via Ping, so this simulates high inbound RTT.
// Pass 0 to disable. Thread-safe; can be called after Make().
func (rf *Raft) SetPingDelay(ms int64) {
	atomic.StoreInt64(&rf.faultPingDelayMs, ms)
}

// ClientRead RPC Handler
// Semantics:
//   - Leader: goes through the batched ReadIndex path
//   - Follower/Candidate: returns Success=false + LeaderId; the client retries
func (rf *Raft) ClientRead(args *ClientReadArgs, reply *ClientReadReply) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		leaderID := rf.leaderId
		rf.mu.Unlock()
		reply.Success = false
		if leaderID >= 0 {
			reply.LeaderId = fmt.Sprintf("%d", leaderID)
		}
		return
	}

	// 1. Create the request context
	req := &readRequest{
		done: make(chan bool, 1),
	}
	rf.pendingReads = append(rf.pendingReads, req)

	// 2. Try to wake the batching goroutine
	select {
	case rf.readNotify <- struct{}{}:
	default:
	}
	rf.mu.Unlock()

	// 3. Wait for the result
	success := <-req.done

	if success {
		rf.mu.Lock()
		reply.Result = rf.kvStore[args.Payload] // Payload is the key
		rf.mu.Unlock()
		reply.Success = true
	} else {
		reply.Success = false
		rf.mu.Lock()
		reply.LeaderId = fmt.Sprintf("%d", rf.leaderId)
		rf.mu.Unlock()
	}
}

// ClientWrite RPC Handler — appends a command to the leader's log and waits for it to be committed and applied.
func (rf *Raft) ClientWrite(args *ClientWriteArgs, reply *ClientWriteReply) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		leaderID := rf.leaderId
		rf.mu.Unlock()
		reply.Success = false
		if leaderID >= 0 {
			reply.LeaderId = fmt.Sprintf("%d", leaderID)
		}
		return
	}

	idx := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Index:   idx,
		Command: KVCommand{Op: args.Op, Key: args.Key, Value: args.Value},
	})
	rf.matchIndex[rf.me] = idx

	ch := make(chan string, 1)
	rf.pendingWrites[idx] = ch
	rf.mu.Unlock()

	// trigger replication immediately rather than waiting for next heartbeat tick
	go rf.sendHeartbeats()

	select {
	case <-ch:
		reply.Success = true
	case <-time.After(time.Duration(ElectionTimeoutMax) * time.Millisecond):
		rf.mu.Lock()
		delete(rf.pendingWrites, idx)
		rf.mu.Unlock()
		reply.Success = false
	}
}

// batchReadIndexLoop processes read requests in batches
func (rf *Raft) batchReadIndexLoop() {
	for !rf.killed() {
		// Wait for requests to arrive
		<-rf.readNotify

		rf.mu.Lock()
		if len(rf.pendingReads) == 0 {
			rf.mu.Unlock()
			continue
		}
		// Take all pending requests as one batch
		batch := rf.pendingReads
		rf.pendingReads = nil
		term := rf.currentTerm
		isLeader := rf.state == StateLeader
		rf.mu.Unlock()

		if !isLeader {
			// Fail fast if no longer the Leader
			for _, req := range batch {
				req.done <- false
			}
			continue
		}

		// Run one heartbeat round to confirm leadership
		success := rf.confirmLeadership(term)

		// Deliver the results
		for _, req := range batch {
			req.done <- success
		}
	}
}

// confirmLeadership runs one heartbeat round to confirm this node is still the Leader
func (rf *Raft) confirmLeadership(term int) bool {
	rf.mu.Lock()
	me := rf.me
	if rf.state != StateLeader || rf.currentTerm != term {
		rf.mu.Unlock()
		return false
	}
	peers := make([]*Peer, len(rf.peers))
	copy(peers, rf.peers)
	rf.mu.Unlock()

	needed := len(peers)/2 + 1
	acks := 1
	ackCh := make(chan struct{}, len(peers))

	for _, p := range peers {
		if p.ID == me {
			continue
		}
		go func(peer *Peer) {
			args := AppendEntriesArgs{Term: term, LeaderId: me}
			var reply AppendEntriesReply
			if peer.Transport.Call("Raft.AppendEntries", &args, &reply) && reply.Success {
				ackCh <- struct{}{}
			}
		}(p)
	}

	timer := time.NewTimer(time.Duration(ElectionTimeoutMin) * time.Millisecond)
	defer timer.Stop()

	for acks < needed {
		select {
		case <-ackCh:
			acks++
			rf.mu.Lock()
			stillLeader := rf.state == StateLeader && rf.currentTerm == term
			rf.mu.Unlock()
			if !stillLeader {
				return false
			}
		case <-timer.C:
			return false
		}
	}
	return true
}

func (rf *Raft) startElection() {
	// netelect: check whether this node is eligible to start an election
	if !rf.isEligibleForElection() {
		return
	}

	rf.state = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// detection time: from the last heartbeat-bearing event to becoming candidate
	// (single-clock, so no cross-node skew). Captured before lastHeartbeat is reset.
	detectMs := float64(time.Since(rf.lastHeartbeat)) / float64(time.Millisecond)
	electionStart := time.Now()
	rf.lastHeartbeat = electionStart
	rf.clearPetitionState()
	term := rf.currentTerm
	fmt.Printf("[election_start] node=%d term=%d ts=%d detect_ms=%.1f\n",
		rf.me, term, electionStart.UnixMilli(), detectMs)

	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	votesReceived := 1 // vote for self

	for _, peer := range rf.peers {
		if peer.ID == rf.me {
			continue
		}

		go func(p *Peer) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply

			atomic.AddInt64(&rf.electionMsgsSent, 1)
			if ok := p.Transport.Call("Raft.RequestVote", &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != StateCandidate || rf.currentTerm != term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = StateFollower
					rf.votedFor = -1
					rf.clearPetitionState()
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > len(rf.peers)/2 {
						rf.state = StateLeader
						rf.leaderId = rf.me
						fmt.Printf("[leader_elected] node=%d term=%d ts=%d\n", rf.me, term, time.Now().UnixMilli())
						rf.scheduleLeaderStepdown(term)
						// initialize leader state
						n := len(rf.peers)
						rf.nextIndex = make([]int, n)
						rf.matchIndex = make([]int, n)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.lastLogIndex() + 1
						}
						rf.matchIndex[rf.me] = rf.lastLogIndex()
						go rf.sendHeartbeats()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	commitIndex := rf.commitIndex

	type peerTask struct {
		peer     *Peer
		prevIdx  int
		prevTerm int
		entries  []LogEntry
	}
	tasks := make([]peerTask, 0, len(rf.peers))
	for _, p := range rf.peers {
		if p.ID == me {
			continue
		}
		ni := rf.nextIndex[p.ID]
		prevIdx := ni - 1
		prevTerm := rf.log[prevIdx].Term
		var entries []LogEntry
		if ni <= rf.lastLogIndex() {
			entries = make([]LogEntry, len(rf.log[ni:]))
			copy(entries, rf.log[ni:])
		}
		tasks = append(tasks, peerTask{p, prevIdx, prevTerm, entries})
	}
	rf.mu.Unlock()

	for _, t := range tasks {
		go func(task peerTask) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				PrevLogIndex: task.prevIdx,
				PrevLogTerm:  task.prevTerm,
				Entries:      task.entries,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply
			if !task.peer.Transport.Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = StateFollower
				rf.votedFor = -1
				rf.clearPetitionState()
				return
			}
			if rf.state != StateLeader || rf.currentTerm != term {
				return
			}

			if reply.Success {
				newMatch := task.prevIdx + len(task.entries)
				if newMatch > rf.matchIndex[task.peer.ID] {
					rf.matchIndex[task.peer.ID] = newMatch
					rf.nextIndex[task.peer.ID] = newMatch + 1
				}
				rf.maybeAdvanceCommitIndex()
			} else {
				// back up nextIndex using conflict hint
				newNext := reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					// find last entry in our log with ConflictTerm; if found,
					// we can skip past it rather than going all the way back
					for i := len(rf.log) - 1; i >= 1; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							newNext = i + 1
							break
						}
					}
				}
				if newNext < 1 {
					newNext = 1
				}
				rf.nextIndex[task.peer.ID] = newNext
			}
		}(t)
	}
}

// applyLoop watches commitIndex and applies newly committed entries to the KV state machine.
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		<-rf.applyNotify

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			result := ""
			if cmd, ok := entry.Command.(KVCommand); ok {
				switch cmd.Op {
				case KVPut:
					rf.kvStore[cmd.Key] = cmd.Value
					result = "ok"
				case KVDelete:
					delete(rf.kvStore, cmd.Key)
					result = "ok"
				}
			}
			if ch, ok := rf.pendingWrites[rf.lastApplied]; ok {
				ch <- result
				delete(rf.pendingWrites, rf.lastApplied)
			}
		}
		rf.mu.Unlock()
	}
}

// maybeAdvanceCommitIndex advances commitIndex to the highest N where
// a majority of peers have matchIndex >= N and log[N].Term == currentTerm.
func (rf *Raft) maybeAdvanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1
		for _, p := range rf.peers {
			if p.ID != rf.me && rf.matchIndex[p.ID] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			select {
			case rf.applyNotify <- struct{}{}:
			default:
			}
			break
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == StateLeader {
			rf.sendHeartbeats()
			time.Sleep(HeartbeatInterval * time.Millisecond)
		} else {
			// Follower / Candidate
			rf.mu.Lock()
			timeoutMs := rf.computeElectionTimeoutMs()
			rf.mu.Unlock()
			start := time.Now()

			for {
				if rf.killed() {
					return
				}

				rf.mu.Lock()
				if rf.state == StateLeader {
					rf.mu.Unlock()
					break
				}
				if rf.lastHeartbeat.After(start) {
					start = rf.lastHeartbeat
					timeoutMs = rf.computeElectionTimeoutMs()
				}

				if time.Since(start) > time.Duration(timeoutMs*float64(time.Millisecond)) {
					rf.startElection()
					start = time.Now()
					timeoutMs = rf.computeElectionTimeoutMs()
				}
				rf.mu.Unlock()

				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}
