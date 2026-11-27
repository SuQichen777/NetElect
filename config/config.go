package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	TransportGRPC = "grpc"
	TransportTCP  = "tcp"
)

type File struct {
	Transport  string          `json:"transport"`
	RPCTimeout string          `json:"rpc_timeout"`
	Nodes      []Node          `json:"nodes"`
	Benchmark  BenchmarkConfig `json:"benchmark"`
	NetElect   *NetElectConfig `json:"netelect,omitempty"`
}

// NetElectConfig configures latency-aware leader switching (the petition protocol) and
// the RTT-sample-based failure-detection modes (Jacobson/CI).
type NetElectConfig struct {
	Enabled                bool    `json:"enabled"`
	LatencyThresholdMs     float64 `json:"latency_threshold_ms"`     // minimum leader penalty (ms) before followers consider petitioning, default 50
	MonitorFreqHz          int     `json:"monitor_freq_hz"`          // ping and penalty-broadcast frequency (Hz), default 10
	PenaltyWeight          float64 `json:"penalty_weight"`           // weight w of the over-threshold penalty term, default 1.0
	PenaltyThresholdMs     float64 `json:"penalty_threshold_ms"`     // threshold T in the penalty formula (ms), default 100
	EligibleLeaders        []int   `json:"eligible_leaders"`         // node IDs allowed to become leader (empty = all)
	InitialEligibleLeaders []int   `json:"initial_eligible_leaders"` // node IDs allowed to become leader in the first election (empty = all)

	// Failure-detection mode (frugal's fdMode):
	// "raft" (default) = uniform random 150-300ms; "Jacobson" = RTO estimate; "CI" = confidence-interval upper bound
	FDMode                    string  `json:"fd_mode,omitempty"`
	ConfidenceLevel           float64 `json:"confidence_level,omitempty"`             // confidence level for CI mode, default 0.95
	HeartbeatIntervalMarginMs float64 `json:"heartbeat_interval_margin_ms,omitempty"` // extra safety margin added to the computed timeout (ms), default 50
	SafetyMarginLowerBoundMs  float64 `json:"safety_margin_lower_bound_ms,omitempty"` // floor of the rank-based delay (ms), default 0
	SafetyMarginStepSizeMs    float64 `json:"safety_margin_step_size_ms,omitempty"`   // per-rank delay step (ms), default 20
	RttWindowSize             int     `json:"rtt_window_size,omitempty"`              // RTT sample window size per peer, default 32

	// Petition only when the nominee's penalty is lower than the current leader's by at least this margin,
	// to avoid churning leadership between nodes with similar penalties. Default 0 (strictly lower suffices).
	PetitionPenaltyMarginMs float64 `json:"petition_penalty_margin_ms,omitempty"`

	// How the new election is triggered once a petition is accepted:
	//   false (default) = only force the current leader to step down; election timeouts decide the new leader
	//     (rank-biased: the proposed leader is likely, but not guaranteed, to win).
	//   true = the proposed leader starts an election immediately and almost always wins it.
	// Used to compare how the two failover paths affect which node ends up leading.
	PetitionDirectElection bool `json:"petition_direct_election,omitempty"`
}

type Node struct {
	ID     int             `json:"id"`
	Listen string          `json:"listen"`
	Fault  *FaultInjection `json:"fault,omitempty"`
}

// FaultInjection describes a node's preconfigured fault-injection parameters.
// Read when raftd starts and fixed for the whole run (apart from when the delay activates).
//
// OutboundDelayMs:    extra sleep (ms) before every outbound RPC from this node,
//
//	simulating high outbound RTT (affects outbound AppendEntries/Ping).
//
// InboundPingDelayMs: extra sleep (ms) before this node replies to Raft.Ping,
//
//	raising the RTT other nodes measure to it, which can trigger a netelect petition.
//
// DelayStartAfterS:   seconds after server start before the delay activates (0 = immediately).
//
//	Combined with startup_settle_s, e.g. settle=5 and delay_start=10
//	inject the delay 5s into the benchmark, so the time series shows before and after.
//
// StepdownAfterS:     wait N seconds after start, then force the leader to step down once (0 = disabled; one-shot).
// LeaderStepdownAfterS: force a stepdown N seconds after every election this node wins (0 = disabled).
//
//	Unlike StepdownAfterS this is a recurring fault: every leader steps down N seconds
//	after winning, causing repeated failover; used by the failure-recovery experiment.
type FaultInjection struct {
	// Fixed delay (used when *MaxMs == 0).
	OutboundDelayMs    int `json:"outbound_delay_ms"`
	InboundPingDelayMs int `json:"inbound_ping_delay_ms"`

	// Random-delay mode: if *MaxMs > 0, each node independently draws
	// a uniform random value in [*MinMs, *MaxMs] at startup, simulating
	// heterogeneous WAN latency across the cluster.
	OutboundDelayMinMs    int `json:"outbound_delay_min_ms"`
	OutboundDelayMaxMs    int `json:"outbound_delay_max_ms"`
	InboundPingDelayMinMs int `json:"inbound_ping_delay_min_ms"`
	InboundPingDelayMaxMs int `json:"inbound_ping_delay_max_ms"`

	DelayStartAfterS     float64 `json:"delay_start_after_s"`     // 0 = activate at startup
	StepdownAfterS       float64 `json:"stepdown_after_s"`        // 0 = disabled (one-shot)
	LeaderStepdownAfterS float64 `json:"leader_stepdown_after_s"` // 0 = disabled (recurring per leadership)
}

type BenchmarkConfig struct {
	Addrs     []string `json:"addrs"`
	Workers   int      `json:"workers"`
	Duration  string   `json:"duration"`
	Timeout   string   `json:"timeout"`
	SampleCap int      `json:"sample_cap"`
	Mode      string   `json:"mode"`       // "read" | "write" | "mixed"; default "read"
	ReadRatio float64  `json:"read_ratio"` // fraction of reads in mixed mode; default 0.8
	CSVOutput string   `json:"csv_output"` // path to append CSV results; empty = no file output
}

func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg File
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Transport == "" {
		cfg.Transport = TransportGRPC
	}
	if cfg.RPCTimeout == "" {
		cfg.RPCTimeout = "250ms"
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *File) Validate() error {
	switch c.Transport {
	case TransportGRPC, TransportTCP:
	default:
		return fmt.Errorf("invalid transport: %q", c.Transport)
	}

	seen := make(map[int]struct{}, len(c.Nodes))
	maxID := -1
	for _, n := range c.Nodes {
		if n.Listen == "" {
			return fmt.Errorf("node %d missing listen address", n.ID)
		}
		if n.ID < 0 {
			return fmt.Errorf("invalid node id: %d", n.ID)
		}
		if _, ok := seen[n.ID]; ok {
			return fmt.Errorf("duplicate node id: %d", n.ID)
		}
		seen[n.ID] = struct{}{}
		if n.ID > maxID {
			maxID = n.ID
		}
	}
	if maxID >= 0 && maxID != len(c.Nodes)-1 {
		return fmt.Errorf("node ids must be contiguous from 0 to %d", len(c.Nodes)-1)
	}
	return nil
}

func (c *File) Timeout() (time.Duration, error) {
	return time.ParseDuration(c.RPCTimeout)
}

func (c *File) NodeByID(id int) (Node, error) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return Node{}, fmt.Errorf("node id %d not found in config", id)
}

func (c *File) PeerAddrs() []string {
	out := make([]string, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.ID >= 0 && n.ID < len(c.Nodes) {
			out[n.ID] = n.Listen
		}
	}
	return out
}

func (c *File) BenchmarkAddrs() []string {
	if len(c.Benchmark.Addrs) > 0 {
		cp := make([]string, len(c.Benchmark.Addrs))
		copy(cp, c.Benchmark.Addrs)
		return cp
	}
	return c.PeerAddrs()
}
