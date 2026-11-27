package tcpnet

import "raft-leader-election/server"

type RaftAdapter struct {
	Raft *server.Raft
}

func (a *RaftAdapter) RequestVote(args *server.RequestVoteArgs) *server.RequestVoteReply {
	var reply server.RequestVoteReply
	a.Raft.RequestVote(args, &reply)
	return &reply
}

func (a *RaftAdapter) AppendEntries(args *server.AppendEntriesArgs) *server.AppendEntriesReply {
	var reply server.AppendEntriesReply
	a.Raft.AppendEntries(args, &reply)
	return &reply
}

func (a *RaftAdapter) Ping(args *server.PingArgs) *server.PingReply {
	var reply server.PingReply
	a.Raft.Ping(args, &reply)
	return &reply
}

func (a *RaftAdapter) ClientRead(args *server.ClientReadArgs) *server.ClientReadReply {
	var reply server.ClientReadReply
	a.Raft.ClientRead(args, &reply)
	return &reply
}

func (a *RaftAdapter) PenaltyScore(args *server.PenaltyScoreArgs) *server.PenaltyScoreReply {
	var reply server.PenaltyScoreReply
	a.Raft.PenaltyScore(args, &reply)
	return &reply
}

func (a *RaftAdapter) Petition(args *server.PetitionArgs) *server.PetitionReply {
	var reply server.PetitionReply
	a.Raft.Petition(args, &reply)
	return &reply
}

func (a *RaftAdapter) ClientWrite(args *server.ClientWriteArgs) *server.ClientWriteReply {
	var reply server.ClientWriteReply
	a.Raft.ClientWrite(args, &reply)
	return &reply
}
