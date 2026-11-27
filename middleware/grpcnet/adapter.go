package grpcnet

import (
	"context"

	"raft-leader-election/server"
)

type RaftAdapter struct {
	Raft *server.Raft
}

func (a *RaftAdapter) RequestVote(ctx context.Context, args *server.RequestVoteArgs) (*server.RequestVoteReply, error) {
	var reply server.RequestVoteReply
	a.Raft.RequestVote(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) AppendEntries(ctx context.Context, args *server.AppendEntriesArgs) (*server.AppendEntriesReply, error) {
	var reply server.AppendEntriesReply
	a.Raft.AppendEntries(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) Ping(ctx context.Context, args *server.PingArgs) (*server.PingReply, error) {
	var reply server.PingReply
	a.Raft.Ping(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) ClientRead(ctx context.Context, args *server.ClientReadArgs) (*server.ClientReadReply, error) {
	var reply server.ClientReadReply
	a.Raft.ClientRead(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) ClientWrite(ctx context.Context, args *server.ClientWriteArgs) (*server.ClientWriteReply, error) {
	var reply server.ClientWriteReply
	a.Raft.ClientWrite(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) PenaltyScore(ctx context.Context, args *server.PenaltyScoreArgs) (*server.PenaltyScoreReply, error) {
	var reply server.PenaltyScoreReply
	a.Raft.PenaltyScore(args, &reply)
	return &reply, nil
}

func (a *RaftAdapter) Petition(ctx context.Context, args *server.PetitionArgs) (*server.PetitionReply, error) {
	var reply server.PetitionReply
	a.Raft.Petition(args, &reply)
	return &reply, nil
}
