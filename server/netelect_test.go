package server_test

import (
	"fmt"
	"raft-leader-election/middleware"
	"raft-leader-election/server"
	"strconv"
	"testing"
	"time"
)

// TestNetElectEligibleLeaders tests the leader-eligibility constraints:
// with InitialEligibleLeaders=[2], only node 2 may win the first election.
func TestNetElectEligibleLeaders(t *testing.T) {
	net := middleware.MakeMockNetwork()
	n := 3
	rafts := make([]*server.Raft, n)

	// Only node 2 may become the initial leader
	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}
		rafts[i] = server.Make(peers, i, server.WithNetElect(server.NetElectConfig{
			Enabled:                true,
			LatencyThresholdMs:     50,
			MonitorFreqHz:          10,
			PenaltyWeight:          1.0,
			PenaltyThresholdMs:     100,
			EligibleLeaders:        []int{0, 1, 2},
			InitialEligibleLeaders: []int{2},
		}))
		net.Register(strconv.Itoa(i), rafts[i])
	}
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Wait for the election to settle
	time.Sleep(2 * time.Second)

	leaderCount := 0
	leaderID := -1
	for i, rf := range rafts {
		_, isLeader := rf.GetState()
		if isLeader {
			leaderID = i
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
	if leaderID != 2 {
		t.Fatalf("expected node 2 to be initial leader (initialEligibleLeaders=[2]), got node %d", leaderID)
	}
	fmt.Printf("Initial leader is node %d (as expected from initialEligibleLeaders)\n", leaderID)
}

// TestNetElectPenaltyScoreBroadcast runs penalty-score broadcasting and checks that the cluster stays stable with one leader.
func TestNetElectPenaltyScoreBroadcast(t *testing.T) {
	net := middleware.MakeMockNetwork()
	n := 3
	rafts := make([]*server.Raft, n)

	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}
		rafts[i] = server.Make(peers, i, server.WithNetElect(server.NetElectConfig{
			Enabled:            true,
			LatencyThresholdMs: 9999, // high threshold, so no petition is triggered
			MonitorFreqHz:      10,
			PenaltyWeight:      1.0,
			PenaltyThresholdMs: 100,
		}))
		net.Register(strconv.Itoa(i), rafts[i])
	}
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Wait for the election and a few rounds of penalty broadcasts
	time.Sleep(2 * time.Second)

	leaderCount := 0
	for i, rf := range rafts {
		_, isLeader := rf.GetState()
		if isLeader {
			leaderCount++
			fmt.Printf("Leader is node %d\n", i)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
	fmt.Println("Penalty score broadcast test passed: cluster stable with netelect enabled")
}

// TestNetElectPetitionRequestVote tests that a petition RequestVote makes the leader step down.
func TestNetElectPetitionRequestVote(t *testing.T) {
	net := middleware.MakeMockNetwork()
	n := 3
	rafts := make([]*server.Raft, n)

	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}
		rafts[i] = server.Make(peers, i, server.WithNetElect(server.NetElectConfig{
			Enabled:            true,
			LatencyThresholdMs: 9999,
			MonitorFreqHz:      10,
			PenaltyWeight:      1.0,
			PenaltyThresholdMs: 100,
		}))
		net.Register(strconv.Itoa(i), rafts[i])
	}
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Wait for the election to settle
	time.Sleep(1 * time.Second)
	leaderID := -1
	for i, rf := range rafts {
		_, isLeader := rf.GetState()
		if isLeader {
			leaderID = i
		}
	}
	if leaderID < 0 {
		t.Fatal("no leader elected")
	}
	fmt.Printf("Leader is node %d, sending petition RequestVote to make it step down\n", leaderID)

	// Simulate a petition RequestVote sent directly to the leader
	args := server.RequestVoteArgs{
		Term:        1, // assume term 1 (the petition path does not check the term)
		CandidateId: (leaderID + 1) % n,
		IsPetition:  true,
	}
	var reply server.RequestVoteReply
	rafts[leaderID].RequestVote(&args, &reply)

	// The leader should have stepped down
	_, isLeader := rafts[leaderID].GetState()
	if isLeader {
		t.Fatal("leader should have stepped down after receiving petition RequestVote")
	}
	fmt.Printf("Node %d stepped down as expected after petition RequestVote\n", leaderID)

	// Wait for a new election
	time.Sleep(1 * time.Second)
	newLeaderCount := 0
	for i, rf := range rafts {
		_, isLeader := rf.GetState()
		if isLeader {
			newLeaderCount++
			fmt.Printf("New leader is node %d\n", i)
		}
	}
	if newLeaderCount != 1 {
		t.Fatalf("expected 1 new leader after petition, got %d", newLeaderCount)
	}
}

// TestNetElectPetitionDirectElection verifies that, with PetitionDirectElection=true, the proposed leader
// that receives a majority of petitions wins by itself right after forcing the current leader down, without relying on a timeout.
func TestNetElectPetitionDirectElection(t *testing.T) {
	net := middleware.MakeMockNetwork()
	n := 3
	rafts := make([]*server.Raft, n)

	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}
		rafts[i] = server.Make(peers, i, server.WithNetElect(server.NetElectConfig{
			Enabled:                true,
			LatencyThresholdMs:     9999, // high threshold disables spontaneous petitions; the test injects one manually
			MonitorFreqHz:          10,
			PenaltyWeight:          1.0,
			PenaltyThresholdMs:     100,
			PetitionDirectElection: true,
		}))
		net.Register(strconv.Itoa(i), rafts[i])
	}
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Wait for the election to settle
	time.Sleep(1 * time.Second)
	leaderID, term := -1, 0
	for i, rf := range rafts {
		tm, isLeader := rf.GetState()
		if isLeader {
			leaderID, term = i, tm
		}
	}
	if leaderID < 0 {
		t.Fatal("no leader elected")
	}

	// proposed = the node after the leader; petitioner = the third node.
	proposed := (leaderID + 1) % n
	petitioner := (leaderID + 2) % n
	fmt.Printf("Leader=%d term=%d; injecting petition: proposed=%d petitioner=%d\n",
		leaderID, term, proposed, petitioner)

	// Inject a petition into the proposed leader (with n=3, needed=1, so a single petition triggers acceptance).
	// LatencyToLeader is set very high so that the proposed leader's RTT-advantage check passes.
	args := server.PetitionArgs{
		Term:             term,
		CurrentLeaderId:  leaderID,
		ProposedLeaderId: proposed,
		LatencyToLeader:  9999,
		SenderId:         petitioner,
	}
	var reply server.PetitionReply
	rafts[proposed].Petition(&args, &reply)
	if !reply.Accepted {
		t.Fatalf("petition should have been accepted by proposed leader %d", proposed)
	}

	// Direct-election mode: the proposed leader should win by itself shortly (without waiting for another node to time out).
	newLeader := -1
	for attempt := 0; attempt < 40; attempt++ {
		time.Sleep(50 * time.Millisecond)
		count, who := 0, -1
		for i, rf := range rafts {
			if _, isLeader := rf.GetState(); isLeader {
				count, who = count+1, i
			}
		}
		if count == 1 {
			newLeader = who
			break
		}
	}
	if newLeader != proposed {
		t.Fatalf("direct election: expected proposed leader %d to win, got %d", proposed, newLeader)
	}
	fmt.Printf("Direct election succeeded: proposed leader %d won\n", newLeader)
}

// TestFDEstimators checks the numerical behavior of the Jacobson / CI estimators.
func TestFDEstimators(t *testing.T) {
	// Samples with a mean of 10 ms and a small standard deviation.
	samples := []float64{9, 10, 11, 10, 10, 9, 11, 10}
	mean := server.RttMeanForTest(samples)
	std := server.RttStdDevForTest(samples)
	if mean < 9.5 || mean > 10.5 {
		t.Fatalf("unexpected mean: %v", mean)
	}
	if std <= 0 || std > 2 {
		t.Fatalf("unexpected stddev: %v", std)
	}

	rto := server.JacobsonRTOForTest(samples)
	// rto = srtt/2 + 4*rttvar; it must be > 0 and below the cap (800 ms)
	if rto <= 0 || rto >= 800 {
		t.Fatalf("unexpected jacobson rto: %v", rto)
	}
	expected := mean/2 + 4*std
	if diff := rto - expected; diff < -1e-6 || diff > 1e-6 {
		t.Fatalf("jacobson formula mismatch: got %v want %v", rto, expected)
	}

	// Cap check: build a high-variance sample and make sure the RTO is clamped to the cap
	wide := []float64{1, 2, 3, 400, 500, 600}
	if got := server.JacobsonRTOForTest(wide); got != 800 {
		t.Fatalf("expected cap at 800ms, got %v", got)
	}

	// With fewer than 2 samples the CI upper bound falls back to the mean
	if got := server.CIUpperBoundForTest([]float64{5}, 0.95); got != 5 {
		t.Fatalf("single-sample CI should be mean, got %v", got)
	}
	// For ordinary samples the CI upper bound should exceed the mean
	upper := server.CIUpperBoundForTest(samples, 0.95)
	if upper <= mean {
		t.Fatalf("CI upper should exceed mean, got %v mean %v", upper, mean)
	}
}

// TestNetElectJacobsonMode verifies that the cluster elects a leader in Jacobson mode.
func TestNetElectJacobsonMode(t *testing.T) {
	runFDModeCluster(t, server.FDModeJacobson)
}

// TestNetElectCIMode verifies that the cluster elects a leader in CI mode.
func TestNetElectCIMode(t *testing.T) {
	runFDModeCluster(t, server.FDModeCI)
}

func runFDModeCluster(t *testing.T, mode string) {
	t.Helper()
	net := middleware.MakeMockNetwork()
	n := 3
	rafts := make([]*server.Raft, n)
	for i := 0; i < n; i++ {
		peers := make([]*server.Peer, n)
		for j := 0; j < n; j++ {
			peers[j] = &server.Peer{
				ID:        j,
				Transport: net.ConnectFromTo(strconv.Itoa(i), strconv.Itoa(j)),
			}
		}
		rafts[i] = server.Make(peers, i, server.WithNetElect(server.NetElectConfig{
			Enabled:                   true,
			LatencyThresholdMs:        9999, // never triggers a petition
			MonitorFreqHz:             20,
			PenaltyWeight:             1.0,
			PenaltyThresholdMs:        100,
			FDMode:                    mode,
			ConfidenceLevel:           0.95,
			HeartbeatIntervalMarginMs: 50,
			SafetyMarginLowerBoundMs:  0,
			SafetyMarginStepSizeMs:    10,
			RttWindowSize:             16,
		}))
		net.Register(strconv.Itoa(i), rafts[i])
	}
	defer func() {
		for _, rf := range rafts {
			rf.Kill()
		}
	}()

	// Let RTT samples accumulate and the election settle
	time.Sleep(2 * time.Second)

	leaderCount := 0
	for _, rf := range rafts {
		if _, isLeader := rf.GetState(); isLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("[%s] expected exactly 1 leader, got %d", mode, leaderCount)
	}
	fmt.Printf("fdMode=%s converged with 1 leader\n", mode)
}
