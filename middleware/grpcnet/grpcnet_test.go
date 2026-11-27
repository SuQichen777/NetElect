package grpcnet_test

import (
	"net"
	"raft-leader-election/middleware/grpcnet"
	"raft-leader-election/server"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCTransportInitialElection(t *testing.T) {
	const n = 3

	lis := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lis[i] = l
		addrs[i] = l.Addr().String()
	}

	conns := make([][]*grpc.ClientConn, n)
	for i := 0; i < n; i++ {
		conns[i] = make([]*grpc.ClientConn, n)
		for j := 0; j < n; j++ {
			cc, err := grpc.Dial(
				addrs[j],
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.CallContentSubtype("gob")),
			)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			conns[i][j] = cc
		}
	}

	nodes := make([]*grpcnet.Node, n)
	rafts := make([]*server.Raft, n)

	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			tr := grpcnet.NewTransport(conns[i][j], 250*time.Millisecond)
			peers[j] = &server.Peer{ID: j, Transport: tr}
		}
		rafts[i] = server.Make(peers, i)
		node, err := grpcnet.StartNodeWithListener(rafts[i], lis[i])
		if err != nil {
			t.Fatalf("start node: %v", err)
		}
		nodes[i] = node
	}

	defer func() {
		for i := 0; i < n; i++ {
			if rafts[i] != nil {
				rafts[i].Kill()
			}
			if nodes[i] != nil {
				nodes[i].Stop()
			}
			for j := 0; j < n; j++ {
				if conns[i][j] != nil {
					_ = conns[i][j].Close()
				}
			}
		}
	}()

	leader := waitOneLeader(t, rafts, 3*time.Second)
	if leader < 0 || leader >= n {
		t.Fatalf("invalid leader: %d", leader)
	}
}

func waitOneLeader(t *testing.T, rafts []*server.Raft, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1
		leaders := 0
		for i, rf := range rafts {
			_, isLeader := rf.GetState()
			if isLeader {
				leaders++
				leader = i
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no single leader elected")
	return -1
}
