package grpcnet

import (
	"net"

	"raft-leader-election/server"

	"google.golang.org/grpc"
)

type Node struct {
	Addr   string
	Lis    net.Listener
	Server *grpc.Server
}

func StartNode(rf *server.Raft, addr string) (*Node, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return StartNodeWithListener(rf, lis)
}

func StartNodeWithListener(rf *server.Raft, lis net.Listener) (*Node, error) {
	s := grpc.NewServer()
	RegisterRaftServiceServer(s, &RaftAdapter{Raft: rf})
	n := &Node{Addr: lis.Addr().String(), Lis: lis, Server: s}
	go func() {
		_ = s.Serve(lis)
	}()
	return n, nil
}

func (n *Node) Stop() {
	if n.Server != nil {
		n.Server.Stop()
	}
	if n.Lis != nil {
		_ = n.Lis.Close()
	}
}
