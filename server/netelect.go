package server

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Failure-detection modes, i.e. how the election timeout is estimated.
// The mode is selected by NetElectConfig.FDMode.
const (
	FDModeRaft     = "raft"     // Standard Raft: uniform random timeout in [150, 300) ms
	FDModeJacobson = "Jacobson" // Jacobson-style RTO estimate over RTT samples
	FDModeCI       = "CI"       // Upper bound of a confidence interval over RTT samples
)

// NetElectConfig holds the runtime parameters of netelect (latency-aware, proactive leader switching).
// It corresponds to the penalty + petition + fdMode protocol of frugal-leader-election.
type NetElectConfig struct {
	Enabled            bool    // Master switch for netelect
	LatencyThresholdMs float64 // Minimum current-leader penalty (ms) for a follower to consider petitioning
	MonitorFreqHz      int     // Rate (Hz) of RTT pings and penalty-score broadcasts
	PenaltyWeight      float64 // Weight w applied to the above-threshold excess in the penalty formula
	PenaltyThresholdMs float64 // Threshold T in the penalty formula (ms)

	EligibleLeaders        []int // Nodes eligible to become leader (empty = all)
	InitialEligibleLeaders []int // Nodes eligible to win the very first election (empty = all)

	// Failure-detection mode, which determines how the election timeout is computed; default "raft".
	// "raft": standard Raft, uniform random timeout in [150, 300) ms.
	// "Jacobson": Jacobson-style RTO srtt/2 + 4*rttvar (capped at 800 ms).
	// "CI": upper bound of a confidence interval over the RTT samples.
	FDMode                    string
	ConfidenceLevel           float64 // Confidence level for CI mode (default 0.95)
	HeartbeatIntervalMarginMs float64 // Safety margin added to the computed timeout (ms, default 50)
	SafetyMarginLowerBoundMs  float64 // Lower bound of the rank-based delay (ms, default 0)
	SafetyMarginStepSizeMs    float64 // Per-rank step of the rank-based delay (ms, default 20)
	RttWindowSize             int     // Size of the per-peer raw RTT sliding window (default 32)

	// Petition only if penalty(proposed) < penalty(current_leader) - PetitionPenaltyMarginMs.
	// This hysteresis prevents leadership churn between nodes whose penalties are close.
	// Default 0: the proposed leader must be strictly better than the current leader.
	PetitionPenaltyMarginMs float64

	// Determines how the new election is triggered once a petition is accepted:
	//   false (default) = only force the current leader to step down, then rely on election
	//     timeouts (the rank-based delay makes the proposed leader likely, but not certain, to win).
	//   true = the proposed leader (the receiver of a majority of petitions) starts a new
	//     election right after forcing the stepdown (currentTerm++, broadcast RequestVote),
	//     without waiting for a timeout, so it almost always wins. Used to compare the two failover paths.
	PetitionDirectElection bool
}

// DefaultNetElectConfig returns the default configuration (with netelect enabled).
func DefaultNetElectConfig() NetElectConfig {
	return NetElectConfig{
		Enabled:                   true,
		LatencyThresholdMs:        50.0,
		MonitorFreqHz:             10,
		PenaltyWeight:             1.0,
		PenaltyThresholdMs:        100.0,
		FDMode:                    FDModeRaft,
		ConfidenceLevel:           0.95,
		HeartbeatIntervalMarginMs: 50.0,
		SafetyMarginLowerBoundMs:  0,
		SafetyMarginStepSizeMs:    20.0,
		RttWindowSize:             32,
		PetitionPenaltyMarginMs:   0,
		PetitionDirectElection:    false,
	}
}

// netelectState holds the netelect runtime state; it lives in the Raft struct as rf.ne.
// It is protected by Raft.mu (the same lock that guards the rest of the Raft state).
type netelectState struct {
	cfg NetElectConfig

	// Penalty score of every node: nodeID -> score (ms)
	penaltyScores map[int]float64

	// RTT to each peer (ms): nodeID -> rtt (EWMA-smoothed; used by the penalty and petition logic)
	peerLatencies map[int]float64

	// Raw RTT sliding window to each peer (ms), used by the Jacobson/CI estimators
	peerRttSamples map[int][]float64

	// Petition state (used only when this node is the proposed leader).
	// petitioners: senderID -> that sender's reported latency to the current leader
	petitioners map[int]float64

	// Majority threshold: (N+1)/2, where N = len(peers)
	majorityCount int
}

// RaftOption is an optional argument to Make.
type RaftOption func(*Raft)

// WithNetElect injects the netelect configuration. Whether the netelect protocol actually runs
// (the ping/penalty/petition background goroutines, the Petition/PenaltyScore RPC handlers and the
// dynamic election timeout) is controlled by cfg.Enabled. With cfg.Enabled=false but InitialEligibleLeaders
// set, the option only pins the leader of the first election (see isEligibleForElection) and starts no
// netelect machinery. Note: do not force cfg.Enabled=true here; that would silently turn a plain-Raft
// baseline that only wants to pin the first leader into a full netelect run.
func WithNetElect(cfg NetElectConfig) RaftOption {
	return func(rf *Raft) {
		if cfg.MonitorFreqHz <= 0 {
			cfg.MonitorFreqHz = 10
		}
		if cfg.LatencyThresholdMs <= 0 {
			cfg.LatencyThresholdMs = 50.0
		}
		if cfg.PenaltyWeight <= 0 {
			cfg.PenaltyWeight = 1.0
		}
		if cfg.PenaltyThresholdMs <= 0 {
			cfg.PenaltyThresholdMs = 100.0
		}
		switch cfg.FDMode {
		case "":
			cfg.FDMode = FDModeRaft
		case FDModeRaft, FDModeJacobson, FDModeCI:
			// ok
		default:
			cfg.FDMode = FDModeRaft
		}
		if cfg.ConfidenceLevel <= 0 || cfg.ConfidenceLevel >= 1 {
			cfg.ConfidenceLevel = 0.95
		}
		if cfg.HeartbeatIntervalMarginMs < 0 {
			cfg.HeartbeatIntervalMarginMs = 0
		}
		if cfg.SafetyMarginLowerBoundMs < 0 {
			cfg.SafetyMarginLowerBoundMs = 0
		}
		if cfg.SafetyMarginStepSizeMs < 0 {
			cfg.SafetyMarginStepSizeMs = 0
		}
		if cfg.RttWindowSize <= 0 {
			cfg.RttWindowSize = 32
		}
		if cfg.PetitionPenaltyMarginMs < 0 {
			cfg.PetitionPenaltyMarginMs = 0
		}
		rf.ne.cfg = cfg
		rf.ne.penaltyScores = make(map[int]float64)
		rf.ne.peerLatencies = make(map[int]float64)
		rf.ne.peerRttSamples = make(map[int][]float64)
		rf.ne.petitioners = make(map[int]float64)
		rf.ne.majorityCount = (len(rf.peers) + 1) / 2
	}
}

// netelectEnabled reports whether netelect is enabled (safe to call without holding the lock).
func (rf *Raft) netelectEnabled() bool {
	return rf.ne.cfg.Enabled
}

// ---------- Latency Monitor ----------

// latencyMonitorLoop periodically pings all peers and updates peerLatencies.
// The interval between monitor rounds is 1/MonitorFreqHz seconds.
func (rf *Raft) latencyMonitorLoop() {
	interval := time.Second / time.Duration(rf.ne.cfg.MonitorFreqHz)
	for !rf.killed() {
		rf.measurePeerLatencies()
		time.Sleep(interval)
	}
}

// measurePeerLatencies pings all peers concurrently and measures the RTT to each.
func (rf *Raft) measurePeerLatencies() {
	rf.mu.Lock()
	me := rf.me
	peers := make([]*Peer, len(rf.peers))
	copy(peers, rf.peers)
	rf.mu.Unlock()

	type result struct {
		id  int
		rtt float64 // ms; -1 means unreachable
	}

	var wg sync.WaitGroup
	results := make(chan result, len(peers))

	for _, p := range peers {
		if p.ID == me {
			continue
		}
		wg.Add(1)
		go func(peer *Peer) {
			defer wg.Done()
			start := time.Now()
			args := PingArgs{}
			var reply PingReply
			if peer.Transport.Call("Raft.Ping", &args, &reply) {
				rtt := float64(time.Since(start).Microseconds()) / 1000.0 // convert to ms
				results <- result{id: peer.ID, rtt: rtt}
			} else {
				results <- result{id: peer.ID, rtt: -1}
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	rf.mu.Lock()
	for r := range results {
		if r.rtt >= 0 {
			// Smooth the RTT with an EWMA (exponentially weighted moving average)
			if old, ok := rf.ne.peerLatencies[r.id]; ok {
				rf.ne.peerLatencies[r.id] = old*0.8 + r.rtt*0.2
			} else {
				rf.ne.peerLatencies[r.id] = r.rtt
			}
			// Also maintain the raw RTT sliding window for the Jacobson/CI estimators
			rf.appendRttSampleLocked(r.id, r.rtt)
		}
		// If the ping failed, leave the previous value unchanged
	}
	rf.mu.Unlock()
}

// appendRttSampleLocked appends an RTT sample to the given peer's sliding window.
// The caller must hold rf.mu.
func (rf *Raft) appendRttSampleLocked(peerID int, rtt float64) {
	if rf.ne.peerRttSamples == nil {
		return
	}
	cap := rf.ne.cfg.RttWindowSize
	if cap <= 0 {
		cap = 32
	}
	samples := rf.ne.peerRttSamples[peerID]
	if len(samples) >= cap {
		samples = samples[len(samples)-cap+1:]
	}
	samples = append(samples, rtt)
	rf.ne.peerRttSamples[peerID] = samples
}

// getLatencyToPeer returns the (EWMA) latency to the given peer in ms, or -1 if there is no data.
// The caller must hold rf.mu.
func (rf *Raft) getLatencyToPeer(peerID int) float64 {
	if rtt, ok := rf.ne.peerLatencies[peerID]; ok {
		return rtt
	}
	return -1
}

// ---------- Penalty Score ----------

// computePenaltyScore computes this node's penalty score:
// the mean of (L_ij + w * max(0, L_ij - T)) over all peers j.
// The caller must hold rf.mu.
func (rf *Raft) computePenaltyScore() float64 {
	w := rf.ne.cfg.PenaltyWeight
	T := rf.ne.cfg.PenaltyThresholdMs
	n := len(rf.peers)
	if n <= 1 {
		return 0
	}

	sum := 0.0
	count := 0
	for _, p := range rf.peers {
		if p.ID == rf.me {
			continue
		}
		lat := rf.getLatencyToPeer(p.ID)
		if lat < 0 {
			continue // skip peers with no data
		}
		penalty := lat + w*math.Max(0, lat-T)
		sum += penalty
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// penaltyBroadcastLoop periodically computes and broadcasts the penalty score, and checks whether to petition.
func (rf *Raft) penaltyBroadcastLoop() {
	interval := time.Second / time.Duration(rf.ne.cfg.MonitorFreqHz)
	for !rf.killed() {
		rf.broadcastPenaltyAndCheckPetition()
		time.Sleep(interval)
	}
}

// broadcastPenaltyAndCheckPetition runs one iteration: compute score -> broadcast -> check petition.
func (rf *Raft) broadcastPenaltyAndCheckPetition() {
	rf.mu.Lock()
	if rf.state == StateLeader {
		// The leader only broadcasts its score; it never petitions
		score := rf.computePenaltyScore()
		rf.ne.penaltyScores[rf.me] = score
		term := rf.currentTerm
		me := rf.me
		peers := make([]*Peer, len(rf.peers))
		copy(peers, rf.peers)
		rf.mu.Unlock()

		rf.doBroadcastPenalty(peers, me, term, score)
		return
	}

	// Follower/Candidate: broadcast the score and check whether to petition
	score := rf.computePenaltyScore()
	rf.ne.penaltyScores[rf.me] = score
	term := rf.currentTerm
	me := rf.me
	leaderID := rf.leaderId
	latToLeader := rf.getLatencyToPeer(leaderID)
	threshold := rf.ne.cfg.LatencyThresholdMs
	margin := rf.ne.cfg.PetitionPenaltyMarginMs
	leaderPenalty, leaderPenaltyKnown := rf.ne.penaltyScores[leaderID]
	peers := make([]*Peer, len(rf.peers))
	copy(peers, rf.peers)

	// Pick the node with the lowest penalty score (excluding the current leader) as the proposed leader
	proposedLeader := -1
	bestScore := math.MaxFloat64
	for nodeID, ps := range rf.ne.penaltyScores {
		if nodeID == leaderID {
			continue
		}
		if ps < bestScore {
			bestScore = ps
			proposedLeader = nodeID
		}
	}
	rf.mu.Unlock()

	// Broadcast the penalty score
	rf.doBroadcastPenalty(peers, me, term, score)

	// Petition trigger: use the current leader's penalty, a global signal that reflects the leader's
	// actual network position, rather than this follower's local RTT to the leader. Reason: with
	// inbound_ping=0, a follower's RTT to the leader always equals its own outbound delay (whoever
	// leads), so an RTT threshold is "blind" for near nodes, which never notice that a far leader is
	// slow. With the leader's broadcast penalty, near and far nodes judge consistently. Both must hold:
	//   1) slowLeader: leaderPenalty > LatencyThresholdMs -- the current leader is slow in absolute terms
	//      (this also acts as a noise floor: no spurious trigger when penalties are close to 0).
	//   2) betterCandidate: bestScore < leaderPenalty - margin -- a clearly better candidate exists
	//      (this suppresses near<->near churn between nodes with similar penalties).
	// latToLeader must still be > 0: PetitionArgs must carry an RTT to the leader for the receiver's check.
	slowLeader := leaderPenaltyKnown && leaderPenalty > threshold
	betterCandidate := leaderPenaltyKnown && bestScore < leaderPenalty-margin
	if leaderID >= 0 && latToLeader > 0 && proposedLeader >= 0 && slowLeader && betterCandidate {
		rf.sendPetition(peers, term, me, leaderID, proposedLeader, latToLeader)
	}
}

// doBroadcastPenalty sends the penalty score to all peers.
func (rf *Raft) doBroadcastPenalty(peers []*Peer, me, term int, score float64) {
	args := PenaltyScoreArgs{
		Term:         term,
		NodeId:       me,
		PenaltyScore: score,
	}
	for _, p := range peers {
		if p.ID == me {
			continue
		}
		go func(peer *Peer) {
			var reply PenaltyScoreReply
			peer.Transport.Call("Raft.PenaltyScore", &args, &reply)
		}(p)
	}
}

// ---------- PenaltyScore RPC Handler ----------

// PenaltyScore is the RPC handler that records a penalty score broadcast by another node.
func (rf *Raft) PenaltyScore(args *PenaltyScoreArgs, reply *PenaltyScoreReply) {
	if !rf.netelectEnabled() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ne.penaltyScores[args.NodeId] = args.PenaltyScore
}

// ---------- Petition ----------

// sendPetition sends a petition to the proposed leader.
func (rf *Raft) sendPetition(peers []*Peer, term, me, currentLeader, proposedLeader int, latToLeader float64) {
	fmt.Printf("[petition_send] node=%d term=%d leader=%d proposed=%d rtt_to_leader=%.1fms\n",
		me, term, currentLeader, proposedLeader, latToLeader)
	args := PetitionArgs{
		Term:             term,
		CurrentLeaderId:  currentLeader,
		ProposedLeaderId: proposedLeader,
		LatencyToLeader:  latToLeader,
		SenderId:         me,
	}
	for _, p := range peers {
		if p.ID == proposedLeader {
			go func(peer *Peer) {
				var reply PetitionReply
				peer.Transport.Call("Raft.Petition", &args, &reply)
			}(p)
			return
		}
	}
}

// Petition is the RPC handler through which the proposed leader receives petitions.
func (rf *Raft) Petition(args *PetitionArgs, reply *PetitionReply) {
	if !rf.netelectEnabled() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Validate: the term must match, the current leader must match, and the proposed leader must be this node
	if args.Term != rf.currentTerm {
		return
	}
	if args.CurrentLeaderId != rf.leaderId {
		return
	}
	if args.ProposedLeaderId != rf.me {
		return
	}

	// Deduplicate: each sender is counted only once
	if _, exists := rf.ne.petitioners[args.SenderId]; exists {
		// Update the reported latency without counting the sender again
		rf.ne.petitioners[args.SenderId] = args.LatencyToLeader
		return
	}
	rf.ne.petitioners[args.SenderId] = args.LatencyToLeader

	// Check for a majority: majority - 1 petitioners suffice, since this node (the proposed leader) counts itself
	needed := rf.ne.majorityCount - 1
	fmt.Printf("[petition_recv] node=%d term=%d from=%d rtt_reported=%.1fms count=%d needed=%d\n",
		rf.me, rf.currentTerm, args.SenderId, args.LatencyToLeader, len(rf.ne.petitioners), needed)
	if len(rf.ne.petitioners) < needed {
		return
	}

	// Validate: my RTT to each petitioner must be <= the latency that petitioner reported to the current leader.
	// This ensures the proposed leader really is closer to the petitioners than the current leader is.
	for senderID, reportedLat := range rf.ne.petitioners {
		myLatToSender := rf.getLatencyToPeer(senderID)
		if myLatToSender < 0 {
			continue // no data; skip
		}
		if myLatToSender > reportedLat {
			// The proposed leader is farther from this follower than the current leader is; reject
			reply.Accepted = false
			return
		}
	}

	// Majority check passed: send a petition RequestVote to the current leader
	reply.Accepted = true
	term := rf.currentTerm
	me := rf.me
	leaderID := rf.leaderId
	directElection := rf.ne.cfg.PetitionDirectElection
	peers := make([]*Peer, len(rf.peers))
	copy(peers, rf.peers)
	fmt.Printf("[petition_accept] node=%d term=%d petitioners=%d force_stepdown_leader=%d direct=%t\n",
		me, term, len(rf.ne.petitioners), leaderID, directElection)
	rf.mu.Unlock()

	// Force the current leader to step down (done in both modes).
	rf.sendPetitionRequestVote(peers, term, me, leaderID)

	rf.mu.Lock()
	// Note: the lock is released by the deferred Unlock.
	//
	// Direct-election mode: the proposed leader (this node, the receiver of a majority of petitions)
	// does not wait for an election timeout; it increments the term and starts a new election at once,
	// so it almost always wins. This removes the race in which, after the leader steps down, another
	// node times out first and wins. Only do this if this node is still a Follower and no other RPC has
	// advanced the term, to avoid clashing with an election in progress or a higher term. startElection
	// requires rf.mu (held here); if this node is eligible, it increments the term and calls clearPetitionState.
	if directElection && rf.state == StateFollower && rf.currentTerm == term {
		fmt.Printf("[petition_direct_election] node=%d term=%d\n", rf.me, rf.currentTerm)
		rf.startElection()
	}
}

// sendPetitionRequestVote sends a petition RequestVote to the current leader, asking it to step down.
func (rf *Raft) sendPetitionRequestVote(peers []*Peer, term, me, leaderID int) {
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: me,
		IsPetition:  true,
	}
	for _, p := range peers {
		if p.ID == leaderID {
			var reply RequestVoteReply
			go func(peer *Peer) {
				atomic.AddInt64(&rf.electionMsgsSent, 1)
				peer.Transport.Call("Raft.RequestVote", &args, &reply)
			}(p)
			return
		}
	}
}

// clearPetitionState clears the petition state.
// The caller must hold rf.mu.
func (rf *Raft) clearPetitionState() {
	if rf.ne.petitioners != nil {
		rf.ne.petitioners = make(map[int]float64)
	}
}

// ---------- Eligible Leaders ----------

// isEligibleForElection reports whether this node is allowed to start an election.
// The caller must hold rf.mu.
func (rf *Raft) isEligibleForElection() bool {
	// InitialEligibleLeaders applies even when full netelect is disabled —
	// it lets experiments pin which node wins the very first election.
	if rf.currentTerm == 0 && rf.leaderId < 0 {
		if len(rf.ne.cfg.InitialEligibleLeaders) > 0 {
			return containsInt(rf.ne.cfg.InitialEligibleLeaders, rf.me)
		}
	}

	if !rf.netelectEnabled() {
		return true
	}

	// Later elections are restricted to EligibleLeaders (if set)
	if len(rf.ne.cfg.EligibleLeaders) > 0 {
		return containsInt(rf.ne.cfg.EligibleLeaders, rf.me)
	}

	return true
}

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// ---------- Failure Detection Estimators ----------
//
// These correspond to meanRtt / rttVariance / jacobsonEst / rttConfidenceInterval in
// frugal-leader-election/lib/tcp_stat_manager.cpp. All inputs and outputs are in milliseconds.

// maxJacobsonRTOMs is the upper bound (ms) on the Jacobson RTO estimate, matching max_RTO in frugal.
const maxJacobsonRTOMs = 800.0

// rttMean returns the sample mean, or 0 for an empty sample.
func rttMean(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	return sum / float64(len(samples))
}

// rttStdDev returns the unbiased sample standard deviation (denominator n-1), or 0 for fewer than 2 samples.
func rttStdDev(samples []float64) float64 {
	if len(samples) < 2 {
		return 0
	}
	mean := rttMean(samples)
	accum := 0.0
	for _, v := range samples {
		d := v - mean
		accum += d * d
	}
	return math.Sqrt(accum / float64(len(samples)-1))
}

// jacobsonRTO returns the Jacobson RTO estimate (ms).
// As in frugal: rto = srtt/2 + 4*rttvar, capped at max_RTO.
// Here rttvar is the sample standard deviation (frugal uses meanRttVar, since its rttvar comes from the kernel's
// tcpi_rttvar; we only have active Ping RTT samples, so the standard deviation is the closest substitute).
func jacobsonRTO(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	srtt := rttMean(samples)
	rttvar := rttStdDev(samples)
	rto := srtt/2 + 4*rttvar
	if rto > maxJacobsonRTOMs {
		return maxJacobsonRTOMs
	}
	return rto
}

// ciZScore returns the z-score for the confidence level, rounded down to the nearest tabulated level; levels below 0.90 default to 95%.
func ciZScore(level float64) float64 {
	switch {
	case level >= 0.999:
		return 3.291
	case level >= 0.995:
		return 2.807
	case level >= 0.99:
		return 2.576
	case level >= 0.95:
		return 1.96
	case level >= 0.90:
		return 1.645
	default:
		return 1.96
	}
}

// ciUpperBound returns mean + z*stddev (the confidence-interval upper bound, ms). With fewer than 2 samples it falls back to the mean.
func ciUpperBound(samples []float64, level float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	if len(samples) < 2 {
		return rttMean(samples)
	}
	return rttMean(samples) + ciZScore(level)*rttStdDev(samples)
}

// myPenaltyRankLocked returns this node's position in the ascending order of penaltyScores (0 = best).
// Returns 0 if this node's own score is unknown. The caller must hold rf.mu.
func (rf *Raft) myPenaltyRankLocked() int {
	myScore, ok := rf.ne.penaltyScores[rf.me]
	if !ok {
		return 0
	}
	rank := 0
	for nodeID, s := range rf.ne.penaltyScores {
		if nodeID == rf.me {
			continue
		}
		if s < myScore {
			rank++
		}
	}
	return rank
}

// computeSafetyDelayMs computes the rank-based delay (ms) from this node's penalty rank.
// The delay is uniform in [lower + step*rank, lower + step*(rank+1)).
// The caller must hold rf.mu.
func (rf *Raft) computeSafetyDelayMs() float64 {
	lower := rf.ne.cfg.SafetyMarginLowerBoundMs
	step := rf.ne.cfg.SafetyMarginStepSizeMs
	if step <= 0 {
		return lower
	}
	rank := rf.myPenaltyRankLocked()
	base := lower + step*float64(rank)
	// rand.Float64 is in [0,1), so the result is in [base, base+step)
	return base + step*rand.Float64()
}

// computeElectionTimeoutMs returns the election timeout (ms) for this round, computed from fdMode
// and the RTT samples to the current leader. It falls back to the standard Raft random timeout
// [ElectionTimeoutMin, ElectionTimeoutMax) if netelect is disabled, fdMode=raft, or RTT data is lacking.
// The caller must hold rf.mu.
func (rf *Raft) computeElectionTimeoutMs() float64 {
	raftTO := float64(ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin))
	if !rf.netelectEnabled() || rf.ne.cfg.FDMode == FDModeRaft || rf.ne.cfg.FDMode == "" {
		return raftTO
	}
	leaderID := rf.leaderId
	if leaderID < 0 || leaderID == rf.me {
		return raftTO
	}
	samples := rf.ne.peerRttSamples[leaderID]
	if len(samples) < 2 {
		return raftTO
	}
	var base float64
	switch rf.ne.cfg.FDMode {
	case FDModeJacobson:
		base = jacobsonRTO(samples)
	case FDModeCI:
		// As in frugal: timeout = upperbound/2 + margin + delay_ms (the non-double_time branch)
		base = ciUpperBound(samples, rf.ne.cfg.ConfidenceLevel) / 2
	default:
		return raftTO
	}
	if base <= 0 {
		return raftTO
	}
	delay := rf.computeSafetyDelayMs()
	return base + rf.ne.cfg.HeartbeatIntervalMarginMs + delay
}
