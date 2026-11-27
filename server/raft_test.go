package server_test

import (
	"fmt"
	"raft-leader-election/middleware"
	"raft-leader-election/server"
	"strconv"
	"testing"
	"time"
)

// TestInitialElection tests the most basic election behavior:
// start a cluster and check that a Leader is elected within a reasonable time.
func TestInitialElection(t *testing.T) {
	// 1. Create a 3-node cluster
	_, rafts := makeCluster(t, 3)
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// 2. Check that a Leader has been elected
	// Allow ample time (here 1 second), since the election timeout is 150-300 ms
	fmt.Println("Waiting for leader election...")
	time.Sleep(1 * time.Second)

	leaderCount := 0
	for i, rf := range rafts {
		term, isLeader := rf.GetState()
		if isLeader {
			fmt.Printf("Node %d is Leader at term %d\n", i, term)
			leaderCount++
		}
	}

	if leaderCount == 0 {
		t.Fatalf("expected a leader to be elected, but got none")
	}
	if leaderCount > 1 {
		t.Fatalf("expected exactly one leader, but got %d", leaderCount)
	}
}

// TestReElection tests re-election after the Leader crashes
func TestReElection(t *testing.T) {
	net, rafts := makeCluster(t, 3)
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Wait for the first Leader
	time.Sleep(1 * time.Second)
	leaderId := checkOneLeader(t, rafts)
	fmt.Printf("Initial leader is %d\n", leaderId)

	// Simulate a Leader crash:
	// 1. Disconnect it from the network
	net.Enable(strconv.Itoa(leaderId), false)
	// 2. Stop the Raft instance (simulating a crash)
	rafts[leaderId].Kill()
	fmt.Printf("Leader %d crashed\n", leaderId)

	// Wait for re-election
	time.Sleep(1 * time.Second)

	// Check for a new Leader (excluding the old Leader)
	newLeaderCount := 0
	for i, rf := range rafts {
		if i == leaderId {
			continue
		}
		_, isLeader := rf.GetState()
		if isLeader {
			fmt.Printf("Node %d is new Leader\n", i)
			newLeaderCount++
		}
	}

	if newLeaderCount == 0 {
		t.Fatalf("expected a new leader to be elected after old leader crashed")
	}
	if newLeaderCount > 1 {
		t.Fatalf("expected exactly one new leader, but got %d", newLeaderCount)
	}
}

// TestReadIndex tests the ReadIndex (linearizable read) path
func TestReadIndex(t *testing.T) {
	net, rafts := makeCluster(t, 3)
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// 1. Wait for a Leader to be elected
	time.Sleep(1 * time.Second)
	leaderId := checkOneLeader(t, rafts)
	fmt.Printf("Initial leader is %d\n", leaderId)

	// 2. Send a ReadIndex request to the Leader
	// Expected: success
	args := server.ClientReadArgs{
		CommandId: "1",
		Payload:   "read",
	}
	var reply server.ClientReadReply
	rafts[leaderId].ClientRead(&args, &reply)

	if !reply.Success {
		t.Fatalf("expected ReadIndex to succeed on leader, but failed")
	}
	fmt.Println("ReadIndex success on leader")

	// 3. Send a ReadIndex request to a Follower
	// Expected: failure with LeaderId set, so the client retries on its own (client-side redirect design)
	followerId := (leaderId + 1) % 3
	var replyFollower server.ClientReadReply
	rafts[followerId].ClientRead(&args, &replyFollower)

	if replyFollower.Success {
		t.Fatalf("expected ReadIndex on follower to fail with redirect, but got success")
	}
	if replyFollower.LeaderId != strconv.Itoa(leaderId) {
		t.Fatalf("expected follower to return LeaderId=%d, got %q", leaderId, replyFollower.LeaderId)
	}
	fmt.Println("ReadIndex on follower correctly returned redirect to leader")

	// 4. Simulate loss of the majority: disable all Followers
	followerA := (leaderId + 1) % 3
	followerB := (leaderId + 2) % 3
	fmt.Printf("Disabling followers %d and %d\n", followerA, followerB)
	net.Enable(strconv.Itoa(followerA), false)
	net.Enable(strconv.Itoa(followerB), false)

	// Note: the call must be made concurrently, because confirmLeadership blocks waiting for heartbeat results
	// and our MockNetwork calls are synchronous.
	// Inside Raft.ClientRead, however, confirmLeadership is called from the batch loop,
	// and ClientRead waits on the done channel.

	// We expect this call to time out (confirmLeadership has its own timeout logic)
	// or to fail because the peers cannot be reached
	doneCh := make(chan bool)
	go func() {
		var replyPartitioned server.ClientReadReply
		rafts[leaderId].ClientRead(&args, &replyPartitioned)
		doneCh <- replyPartitioned.Success
	}()

	select {
	case success := <-doneCh:
		if success {
			t.Fatalf("expected ReadIndex to fail when quorum is unavailable")
		}
		fmt.Println("ReadIndex correctly failed when quorum is unavailable")
	case <-time.After(2 * time.Second):
		// If the confirmLeadership timeout works correctly, we should never wait this long.
		// Note: confirmLeadership times out after ElectionTimeoutMin (150 ms),
		// so 2 s is ample.
		t.Fatalf("ReadIndex request timed out (hung)")
	}
}

// --- Helpers ---

func makeCluster(t *testing.T, n int) (*middleware.MockNetwork, []*server.Raft) {
	net := middleware.MakeMockNetwork()
	rafts := make([]*server.Raft, n)

	// First build the Peers configuration for every server.
	// Note: in server i's Peers list, the j-th Peer must point to server j.
	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}

		// Start the Raft instance
		rafts[i] = server.Make(peers, i)

		// Register it with the network
		net.Register(strconv.Itoa(i), rafts[i])
	}

	return net, rafts
}

func checkOneLeader(t *testing.T, rafts []*server.Raft) int {
	for i, rf := range rafts {
		_, isLeader := rf.GetState()
		if isLeader {
			return i
		}
	}
	t.Fatalf("expected a leader, but got none")
	return -1
}
