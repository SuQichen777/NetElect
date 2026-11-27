package server

// RequestVoteArgs is sent by a Candidate to request votes.
// Follows Figure 2 of the Raft paper.
type RequestVoteArgs struct {
	Term         int  // candidate's current term
	CandidateId  int  // ID of the candidate requesting the vote
	LastLogIndex int  // index of the candidate's last log entry (for the up-to-date check)
	LastLogTerm  int  // term of the candidate's last log entry (for the up-to-date check)
	IsPetition   bool // true = petition-triggered request (sent only to the current leader, asking it to step down)
}

// RequestVoteReply is the response to RequestVote.
type RequestVoteReply struct {
	Term        int  // current term, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// AppendEntriesArgs is sent by the Leader for log replication and heartbeats.
// An empty Entries slice makes it a heartbeat.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's ID, so followers can redirect clients
	PrevLogIndex int        // index of the log entry immediately preceding the new ones
	PrevLogTerm  int        // term of the entry at PrevLogIndex
	Entries      []LogEntry // log entries to store (empty for a heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply is the response to AppendEntries.
type AppendEntriesReply struct {
	Term    int  // current term, for the leader to update itself
	Success bool // true if the follower contained an entry matching PrevLogIndex and PrevLogTerm
	// Conflict hint: helps leader jump back quickly on rejection
	ConflictTerm  int // term of the conflicting entry (-1 if follower log is too short)
	ConflictIndex int // first index of ConflictTerm (or follower's last index + 1 if too short)
}

// LogEntry is a single entry in the replicated log.
// Entries are appended by the leader and applied in index order by applyLoop.
type LogEntry struct {
	Term    int         // term in which the leader received the entry
	Command interface{} // state-machine command (a KVCommand, see kv.go)
	Index   int         // position of the entry in the log
}

// ClientReadArgs is a read-only request from a client to a server.
// Used for ReadIndex (linearizable read) throughput tests.
type ClientReadArgs struct {
	CommandId string // globally unique request ID (for statistics and tracing)
	Payload   string // request body (a key, or the empty string)
	Forwarded bool   // reserved: marks a request a follower forwarded to the leader on a client's behalf, to prevent forwarding loops (currently unused; followers redirect instead)
}

// ClientReadReply is the response to ClientRead.
type ClientReadReply struct {
	LeaderId string // when not the leader, the known leader ID for the client to retry (redirect mode)
	Success  bool   // whether the request was served
	Result   string // read result (the key's value; empty string if absent)
}

// ClientWriteArgs is a write request (Put / Delete) from a client to a server.
type ClientWriteArgs struct {
	CommandId string // globally unique request ID
	Op        KVOp   // KVPut or KVDelete
	Key       string
	Value     string // used only by Put
}

// ClientWriteReply is the response to ClientWrite.
type ClientWriteReply struct {
	Success  bool
	LeaderId string // set when not the leader, for the client to retry
}

// PingArgs is a lightweight request used to probe a node's RTT.
type PingArgs struct{}

// PingReply is the response to Ping.
type PingReply struct {
	NodeId int
}

// --- NetElect RPC types ---

// PenaltyScoreArgs carries the penalty score each node broadcasts periodically.
type PenaltyScoreArgs struct {
	Term         int     // sender's current term
	NodeId       int     // sender's node ID
	PenaltyScore float64 // computed penalty score (ms)
}

// PenaltyScoreReply is the response to PenaltyScore (acknowledgement only).
type PenaltyScoreReply struct{}

// PetitionArgs is a petition sent by a follower to the proposed new leader.
type PetitionArgs struct {
	Term             int     // sender's current term
	CurrentLeaderId  int     // node ID of the current leader
	ProposedLeaderId int     // node ID of the proposed new leader
	LatencyToLeader  float64 // sender's latency to the current leader (ms)
	SenderId         int     // sender's node ID
}

// PetitionReply is the response to Petition.
type PetitionReply struct {
	Accepted bool // whether the petition was accepted for processing
}
