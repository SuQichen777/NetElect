package tcpnet

import (
	"net"
	"raft-leader-election/server"
	"testing"
	"time"
)

func TestDialTransportRequestVote(t *testing.T) {
	rf := server.Make([]*server.Peer{{ID: 0}}, 0)
	defer rf.Kill()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	node, err := StartNodeWithListener(rf, lis)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	defer node.Stop()

	dt := NewDialTransport(node.Addr, 200*time.Millisecond)
	defer dt.Close()

	args := server.RequestVoteArgs{Term: 1, CandidateId: 1}
	var reply server.RequestVoteReply
	if !dt.Call("Raft.RequestVote", &args, &reply) {
		t.Fatalf("request vote call failed")
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted")
	}
	if reply.Term != 1 {
		t.Fatalf("unexpected term: %d", reply.Term)
	}
}

func TestDialTransportClientReadRedirect(t *testing.T) {
	rf := server.Make([]*server.Peer{{ID: 0}}, 0)
	defer rf.Kill()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	node, err := StartNodeWithListener(rf, lis)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	defer node.Stop()

	dt := NewDialTransport(node.Addr, 200*time.Millisecond)
	defer dt.Close()

	args := server.ClientReadArgs{CommandId: "1", Payload: "x"}
	var reply server.ClientReadReply
	if !dt.Call("Raft.ClientRead", &args, &reply) {
		t.Fatalf("client read call failed")
	}
	if reply.Success {
		t.Fatalf("expected follower redirect")
	}
}
