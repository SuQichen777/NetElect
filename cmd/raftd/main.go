package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"raft-leader-election/config"
	"raft-leader-election/middleware/grpcnet"
	"raft-leader-election/middleware/tcpnet"
	"raft-leader-election/server"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		configPath = flag.String("config", "", "cluster config JSON path")
		transport  = flag.String("transport", config.TransportGRPC, "transport: grpc|tcp")
		idStr      = flag.String("id", "", "node id: 0..N-1")
		listenStr  = flag.String("listen", "", "listen address, e.g. 0.0.0.0:7000")
		peersStr   = flag.String("peers", "", "comma-separated peer addrs, indexed by id, e.g. host0:7000,host1:7000,...")
		rpcTO      = flag.Duration("rpc-timeout", 250*time.Millisecond, "per-rpc timeout")
	)
	flag.Parse()

	id, err := parseID(*idStr)
	if *idStr != "" && err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	var (
		listenAddr string
		addrs      []string
		usedTO     time.Duration
		fault      *config.FaultInjection
	)
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
		if *idStr == "" {
			fmt.Fprintln(os.Stderr, "missing required flag: -id when using -config")
			os.Exit(2)
		}
		nodeCfg, err := cfg.NodeByID(id)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
		timeout, err := cfg.Timeout()
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
		*transport = cfg.Transport
		listenAddr = nodeCfg.Listen
		addrs = cfg.PeerAddrs()
		usedTO = timeout
		fault = nodeCfg.Fault
	} else {
		if *idStr == "" || *listenStr == "" || *peersStr == "" {
			fmt.Fprintln(os.Stderr, "missing required flags: -id -listen -peers")
			os.Exit(2)
		}
		addrs = splitAddrs(*peersStr)
		if len(addrs) == 0 {
			fmt.Fprintln(os.Stderr, "invalid -peers")
			os.Exit(2)
		}
		listenAddr = *listenStr
		usedTO = *rpcTO
	}
	if id < 0 || id >= len(addrs) {
		fmt.Fprintln(os.Stderr, "invalid -id: out of range for -peers")
		os.Exit(2)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	peers := make([]*server.Peer, len(addrs))
	delayTransports := make([]*server.DelayTransport, len(addrs))
	for i := range addrs {
		var inner server.Transport
		switch *transport {
		case config.TransportGRPC:
			inner = grpcnet.NewDialTransport(addrs[i], usedTO)
		case config.TransportTCP:
			inner = tcpnet.NewDialTransport(addrs[i], usedTO)
		default:
			fmt.Fprintln(os.Stderr, "invalid -transport: expected grpc or tcp")
			os.Exit(2)
		}
		dt := &server.DelayTransport{Inner: inner}
		delayTransports[i] = dt
		peers[i] = &server.Peer{ID: i, Transport: dt}
	}

	var raftOpts []server.RaftOption
	if *configPath != "" {
		cfg, _ := config.Load(*configPath)
		ne := cfg.NetElect
		// Pass WithNetElect when fully enabled OR when InitialEligibleLeaders is set
		// (so the first-election pin works even on plain-raft configs).
		if ne != nil && (ne.Enabled || len(ne.InitialEligibleLeaders) > 0) {
			raftOpts = append(raftOpts, server.WithNetElect(server.NetElectConfig{
				Enabled:                   ne.Enabled,
				LatencyThresholdMs:        ne.LatencyThresholdMs,
				MonitorFreqHz:             ne.MonitorFreqHz,
				PenaltyWeight:             ne.PenaltyWeight,
				PenaltyThresholdMs:        ne.PenaltyThresholdMs,
				EligibleLeaders:           ne.EligibleLeaders,
				InitialEligibleLeaders:    ne.InitialEligibleLeaders,
				FDMode:                    ne.FDMode,
				ConfidenceLevel:           ne.ConfidenceLevel,
				HeartbeatIntervalMarginMs: ne.HeartbeatIntervalMarginMs,
				SafetyMarginLowerBoundMs:  ne.SafetyMarginLowerBoundMs,
				SafetyMarginStepSizeMs:    ne.SafetyMarginStepSizeMs,
				RttWindowSize:             ne.RttWindowSize,
				PetitionPenaltyMarginMs:   ne.PetitionPenaltyMarginMs,
				PetitionDirectElection:    ne.PetitionDirectElection,
			}))
		}
	}

	// Recurring forced failover: every elected leader steps down after N seconds.
	// Must be a Make option (the server arms the timer on each election win).
	if fault != nil && fault.LeaderStepdownAfterS > 0 {
		raftOpts = append(raftOpts, server.WithLeaderStepdown(
			time.Duration(fault.LeaderStepdownAfterS*float64(time.Second))))
	}

	rf := server.Make(peers, id, raftOpts...)

	// Apply fault injection from config.
	if fault != nil {
		// Resolve the actual delay values: random draw if *MaxMs > 0, else fixed.
		resolveDelay := func(fixed, minMs, maxMs int) int {
			if maxMs > 0 {
				span := maxMs - minMs
				if span <= 0 {
					return minMs
				}
				return minMs + rand.Intn(span+1)
			}
			return fixed
		}
		outboundMs := resolveDelay(fault.OutboundDelayMs, fault.OutboundDelayMinMs, fault.OutboundDelayMaxMs)
		inboundMs := resolveDelay(fault.InboundPingDelayMs, fault.InboundPingDelayMinMs, fault.InboundPingDelayMaxMs)
		isRandom := fault.OutboundDelayMaxMs > 0 || fault.InboundPingDelayMaxMs > 0

		// activateDelays sets the outbound and inbound-Ping delays on all peer transports.
		activateDelays := func() {
			for _, dt := range delayTransports {
				dt.SetDelay(int64(outboundMs))
			}
			rf.SetPingDelay(int64(inboundMs))
			fmt.Printf("fault_latency_active: node=%d outbound_delay=%dms inbound_ping_delay=%dms random=%v\n",
				id, outboundMs, inboundMs, isRandom)
		}

		if outboundMs > 0 || inboundMs > 0 {
			if fault.DelayStartAfterS <= 0 {
				activateDelays()
			} else {
				go func() {
					time.Sleep(time.Duration(fault.DelayStartAfterS * float64(time.Second)))
					activateDelays()
				}()
			}
		}

		if fault.StepdownAfterS > 0 {
			go func() {
				time.Sleep(time.Duration(fault.StepdownAfterS * float64(time.Second)))
				stepped := rf.StepDown()
				fmt.Printf("fault_stepdown: node=%d stepped=%v ts=%d\n", id, stepped, time.Now().UnixMilli())
			}()
		}
	}

	var stop func()
	switch *transport {
	case config.TransportGRPC:
		node, err := grpcnet.StartNodeWithListener(rf, lis)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		stop = node.Stop
	case config.TransportTCP:
		node, err := tcpnet.StartNodeWithListener(rf, lis)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		stop = node.Stop
	default:
		fmt.Fprintln(os.Stderr, "invalid -transport: expected grpc or tcp")
		os.Exit(2)
	}

	fmt.Printf("raftd up: id=%d transport=%s listen=%s peers=%s\n",
		id, *transport, listenAddr, strings.Join(addrs, ","))

	// Leadership changes are logged event-driven by the server itself
	// ([leader_elected] in startElection), which never misses a transition even
	// under heavy churn — unlike a periodic poller that the mutex can starve.

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	rf.Kill()
	stop()

	// Print election message counts for experiment metrics.
	sent, recv := rf.GetElectionMsgCount()
	fmt.Printf("election_msgs: node=%d sent=%d recv=%d\n", id, sent, recv)

	for _, dt := range delayTransports {
		if dt != nil {
			_ = dt.Close()
		}
	}
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func parseID(s string) (int, error) {
	var id int
	_, err := fmt.Sscanf(s, "%d", &id)
	if err != nil {
		return 0, fmt.Errorf("invalid -id: %q", s)
	}
	return id, nil
}
