package grpcnet

import (
	"context"
	"time"

	"raft-leader-election/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	client  RaftServiceClient
	timeout time.Duration
}

func NewTransport(conn grpc.ClientConnInterface, timeout time.Duration) *Transport {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	return &Transport{
		client:  NewRaftServiceClient(conn),
		timeout: timeout,
	}
}

func (t *Transport) Call(method string, args interface{}, reply interface{}) bool {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	switch method {
	case "Raft.RequestVote":
		a, okA := args.(*server.RequestVoteArgs)
		r, okR := reply.(*server.RequestVoteReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.RequestVote(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.AppendEntries":
		a, okA := args.(*server.AppendEntriesArgs)
		r, okR := reply.(*server.AppendEntriesReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.AppendEntries(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.Ping":
		a, okA := args.(*server.PingArgs)
		r, okR := reply.(*server.PingReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.Ping(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.ClientRead":
		a, okA := args.(*server.ClientReadArgs)
		r, okR := reply.(*server.ClientReadReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.ClientRead(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.ClientWrite":
		a, okA := args.(*server.ClientWriteArgs)
		r, okR := reply.(*server.ClientWriteReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.ClientWrite(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.PenaltyScore":
		a, okA := args.(*server.PenaltyScoreArgs)
		r, okR := reply.(*server.PenaltyScoreReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.PenaltyScore(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	case "Raft.Petition":
		a, okA := args.(*server.PetitionArgs)
		r, okR := reply.(*server.PetitionReply)
		if !okA || !okR {
			return false
		}
		out, err := t.client.Petition(ctx, a, grpc.CallContentSubtype(codecName))
		if err != nil {
			return false
		}
		*r = *out
		return true
	default:
		return false
	}
}

func Dial(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName)),
	)
}
