# NetElect

A Raft implementation in Go (leader election, full log replication, a KV state machine, linearizable reads and writes) extended with **NetElect**, a latency-aware leader-switching protocol, plus the experiment harness and raw data used to evaluate it.

NetElect ports the penalty-score / petition protocol and the failure-detection modes (`fdMode`) of [frugal-leader-election](#differences-from-frugal-leader-election) onto a standard Raft core with pluggable transports.

## Contents

- [Overview](#overview)
- [Repository layout](#repository-layout)
- [Build and test](#build-and-test)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Experiments](#experiments)
- [Experiment data](#experiment-data)
- [API reference](#api-reference)
- [Differences from frugal-leader-election](#differences-from-frugal-leader-election)
- [Limitations](#limitations)

## Overview

### Raft core

- Leader election via `RequestVote` with the log up-to-date check
- Log replication via `AppendEntries` with ConflictTerm/ConflictIndex fast backup and commitIndex advancement
- KV state machine (`KVPut` / `KVDelete`) applied by a dedicated `applyLoop` goroutine
- **Linearizable reads** (`ClientRead`): ReadIndex-style; one quorum-confirmation round is shared by a batch of pending reads
- **Linearizable writes** (`ClientWrite`): committed through the log and applied before the call returns
- Three interchangeable transports behind the `server.Transport` interface: in-process mock (with fault injection), gRPC (gob encoding, no protobuf), and a custom TCP protocol with request multiplexing

### NetElect extension (opt-in)

Enabled with `server.WithNetElect(NetElectConfig{...})`, or the `netelect` block in the JSON config.

1. **RTT monitoring.** Every node pings all peers at `monitor_freq_hz` and keeps a per-peer EWMA plus a raw RTT sample window.
2. **Penalty score.** Each node computes `penalty = mean over peers of (L + w·max(0, L − T))` and broadcasts it via `Raft.PenaltyScore`.
3. **Petition.** A follower petitions when both (a) the current leader's broadcast penalty exceeds `latency_threshold_ms`, and (b) the lowest-penalty peer is below `leaderPenalty − petition_penalty_margin_ms`. It sends `Raft.Petition` to that peer.
4. **Acceptance.** The proposed leader accepts once a majority of followers have petitioned it *and* its own RTT to each petitioner is no worse than what that petitioner reports to the current leader.
5. **Stepdown.** The proposed leader sends `Raft.RequestVote{IsPetition: true}` to the current leader, which steps down without voting. With `petition_direct_election = false` (default) the next leader is decided by the normal election-timeout race; with `true` the proposed leader immediately starts its own election and almost always wins.

**Failure-detection modes** (`fd_mode`) decide how each election timeout is drawn:

| Mode | Election timeout |
|---|---|
| `raft` (default) | uniform random in [150, 300) ms; also the fallback when no RTT data is available |
| `Jacobson` | `RTO = srtt/2 + 4·stddev` over the leader's RTT window (capped at 800 ms), plus `heartbeat_interval_margin_ms` and a rank-based delay |
| `CI` | confidence-interval upper bound `mean + z·stddev` (z from `confidence_level`), halved, plus the margin and the rank-based delay |

The rank-based delay is uniform in `[lower + step·rank, lower + step·(rank+1))`, where `rank` is the node's position in the ascending order of penalty scores, so nodes with better network positions time out first.

See [DESIGN.md](DESIGN.md) for the full design, data flows and interface contracts.

## Repository layout

```
server/        Raft core (raft.go), NetElect (netelect.go), KV state machine (kv.go),
               RPC types (rpc.go), Transport interface, DelayTransport for fault injection
middleware/    Transports: mock_net.go (in-process), grpcnet/, tcpnet/
client/        Benchmark client (leader hint + redirect retry), stats, time-series collection
config/        JSON config schema and validation; cluster configs for every experiment
cmd/           Entry points: raftd, client, bench, grpcbench, tcpbench
scripts/       Experiment sweeps (bash + JSON settings) and the analysis script
results/       Raw experiment data, plots and the generated analysis report
DESIGN.md      Design document
EXPERIMENT.md  Experiment write-ups (goals, setup, mechanism, results, conclusions)
```

## Build and test

Requires Go 1.25+.

```bash
go build ./...
go test ./...
```

Individual suites:

```bash
go test ./server/                                                   # Raft core (mock network)
go test ./server/ -run TestNetElect                                 # NetElect penalty + petition
go test ./server/ -run 'TestFDEstimators|TestNetElect(Jacobson|CI)Mode'  # failure-detection modes
go test ./middleware/grpcnet/ ./middleware/tcpnet/                  # transports
```

Binaries:

```bash
go build -o bin/raftd  ./cmd/raftd    # node server
go build -o bin/client ./cmd/client   # benchmark client
go build -o bin/bench  ./cmd/bench    # in-process mock benchmark
```

Always pass an explicit `-o` for the client: the default output name `client` collides with the `client/` package directory.

## Quick start

Start a local 3-node cluster, one terminal per node:

```bash
./bin/raftd -config config/example.cluster.json -id 0
./bin/raftd -config config/example.cluster.json -id 1
./bin/raftd -config config/example.cluster.json -id 2
```

Run a benchmark against it:

```bash
./bin/client -config config/example.cluster.json                                  # read-only (default)
./bin/client -config config/example.cluster.json -mode write                      # write-only
./bin/client -config config/example.cluster.json -mode mixed -read-ratio 0.8      # 80% reads / 20% writes
```

Nodes can also be started from flags alone:

```bash
./bin/raftd -id 0 -listen 127.0.0.1:7000 -peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 -transport tcp
```

The client prints total/success/failure counts, QPS and p50/p95/p99 latency per operation type. `-csv FILE` writes a summary row; `-timeseries-csv FILE -timeseries-interval 1s` writes per-interval QPS and latency.

## Configuration

Clusters are described by a JSON file (see [config/example.cluster.json](config/example.cluster.json)).

| Field | Description |
|---|---|
| `transport` | `"grpc"` or `"tcp"` |
| `rpc_timeout` | per-RPC timeout, e.g. `"250ms"` |
| `nodes[]` | `{ id, listen }`, IDs contiguous from 0 |
| `nodes[].fault` | optional per-node fault injection (below) |
| `benchmark` | `workers`, `duration`, `timeout`, `sample_cap`, `mode` (`read` / `write` / `mixed`), `read_ratio` (default 0.8) |
| `netelect` | optional NetElect settings (below) |

### Fault injection (`nodes[].fault`)

| Key | Effect |
|---|---|
| `outbound_delay_ms` | sleep before every outbound RPC the node sends (heartbeat, RequestVote, Ping) |
| `inbound_ping_delay_ms` | sleep before replying to `Raft.Ping` only |
| `*_min_ms` / `*_max_ms` | if `*_max_ms > 0`, the node draws a uniform value from the range at startup (heterogeneous WAN) |
| `delay_start_after_s` | delays activate this many seconds after start (0 = immediately) |
| `stepdown_after_s` | force the current leader to step down once, this many seconds after start |
| `leader_stepdown_after_s` | recurring: every time the node wins an election it steps down this many seconds later |

Since RTT is measured by active Ping, `RTT(a→b) = a.outbound_delay + b.inbound_ping_delay`.

### NetElect (`netelect`)

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | master switch |
| `latency_threshold_ms` | 50 | minimum leader penalty before followers consider petitioning |
| `monitor_freq_hz` | 10 | ping and penalty-broadcast rate |
| `penalty_weight` | 1.0 | `w` in the penalty formula |
| `penalty_threshold_ms` | 100 | `T` in the penalty formula |
| `eligible_leaders` | `[]` | node IDs allowed to start elections (empty = all) |
| `initial_eligible_leaders` | `[]` | same, for the first election only |
| `fd_mode` | `"raft"` | `raft` / `Jacobson` / `CI` |
| `confidence_level` | 0.95 | CI mode only |
| `heartbeat_interval_margin_ms` | 50 | safety margin added to the computed timeout |
| `safety_margin_lower_bound_ms` | 0 | floor of the rank-based delay |
| `safety_margin_step_size_ms` | 20 | rank-based delay step per rank |
| `rtt_window_size` | 32 | raw RTT samples kept per peer |
| `petition_penalty_margin_ms` | 0 | petition only if `leaderPenalty − bestCandidatePenalty > margin` (anti-churn hysteresis) |
| `petition_direct_election` | `false` | `true` = the proposed leader starts its own election right after forcing the stepdown |

## Experiments

All experiments ran on 5 CloudLab nodes (Clemson cluster) over gRPC, with one benchmark client per node (128 workers each). Each sweep runs every variant for 20 mixed-workload runs (80% reads) and 20 read-only runs.

**Variants** (one cluster config each, `config/cluster.<variant>[.<scenario>].json`):

| Variant | Description |
|---|---|
| `raft` | plain Raft, NetElect disabled |
| `netelect.raft` | NetElect with `fd_mode = raft` |
| `netelect.jacobson` | NetElect with `fd_mode = Jacobson` |
| `netelect.ci` | NetElect with `fd_mode = CI` |

**Scenarios:**

| Scenario | Script | Configs | What it tests |
|---|---|---|---|
| Baseline | `scripts/run_benchmark_sweep.sh` | `cluster.<variant>.json` | NetElect overhead with no faults |
| WAN latency | `scripts/run_wan_latency_sweep.sh` | `cluster.<variant>.wan.json` | Tiered WAN: far nodes 0,1 outbound 30–40 ms, near nodes 2,3,4 outbound 10–20 ms, activated 5 s after start; node 0 leads first. Does leadership migrate to a near node? |
| WAN latency, direct election | `scripts/run_wan_latency_petition_direct_election.sh` | `cluster.<variant>.wan.pde.json` | Same as above with `petition_direct_election = true` |
| Leader crash | `scripts/run_failure_sweep.sh` | `cluster.<variant>.failure.json` | Every elected leader is forced down 7 s after winning (`leader_stepdown_after_s = 7`, recurring failover) |

`scripts/run_wan_latency_petition_compare.sh` runs both WAN arms back to back; `scripts/run_all_fault_sweeps.sh` runs the failure and WAN sweeps in sequence.

**Running a sweep.** Each script reads its settings (SSH user, node hostnames, run counts, settle times) from the JSON file of the same name in `scripts/`. It copies the cluster config to every node over SSH, restarts `raftd`, runs the clients, and collects client CSVs and server logs into `results/<group>/{mixed,read}/<run>/`. Each node needs the `raftd` and `client` binaries at `~/raft/bin/` (see DESIGN.md for the multi-node setup). `jq` is required locally.

**Analysis.** `scripts/analyze_sweep.py` is configured by `scripts/analyze_sweep.json`: a list of datasets, each naming its groups and the run range to analyze.

```bash
python3 scripts/analyze_sweep.py      # writes results/report.md and results/plots/*.png
```

It reports trimmed-mean throughput and latency (best and worst run dropped), run-to-run stability, and, from the server logs, leadership metrics: migration success rate and time, petition rate and petitioners, accepted petitions, election message counts, and which nodes win elections. The write-ups in [EXPERIMENT.md](EXPERIMENT.md) are based on these outputs.

## Experiment data

`results/` holds the raw data of every sweep (about 70 MB):

```
results/<group>/{mixed,read}/<run>/
    <host>.csv        client summary (QPS, latency percentiles, error counts)
    <host>.ts.csv     client time series (1 s intervals)
    raftd-<id>.log    server log (elections, role changes, petitions, fault activation)
    exp_info.json     run metadata (group label, workload mode, fault settings)
results/report.md     analysis report generated by analyze_sweep.py
results/plots/        figures generated by analyze_sweep.py
```

Group names are `<variant>[-<scenario>]`, e.g. `netelect-jacobson-wan`. The run ranges used for the reported numbers are those listed in `scripts/analyze_sweep.json`; other run directories are kept as collected. The `netelect-CI*` groups contain runs of the CI variant, which is not part of the current analysis configuration.

## API reference

**RPC methods** (identical across all transports; types in `server/rpc.go`):

- Core: `Raft.RequestVote`, `Raft.AppendEntries`, `Raft.Ping`, `Raft.ClientRead`, `Raft.ClientWrite`
- NetElect: `Raft.PenaltyScore`, `Raft.Petition`

**Go API:**

- `server.Make(peers, me, opts...) *server.Raft`
- `(*server.Raft).GetState() (term int, isLeader bool)`
- `(*server.Raft).Kill()`
- `server.WithNetElect(cfg server.NetElectConfig) server.RaftOption`

**Binaries:**

| Binary | Description |
|---|---|
| `cmd/raftd` | node process (config- or flag-driven); emits structured log lines used by the analysis |
| `cmd/client` | config-driven benchmark client (`read` / `write` / `mixed`) |
| `cmd/bench` | in-process benchmark on the mock network |
| `cmd/grpcbench`, `cmd/tcpbench` | earlier standalone benchmark entry points |

## Differences from frugal-leader-election

The NetElect protocol itself (penalty formula, petition flow, eligibility, Jacobson RTO with the 800 ms cap, CI z-score table) follows frugal-leader-election. The remaining differences:

- **RTT source.** frugal reads the kernel's `tcpi_rtt` / `tcpi_rttvar` via netlink (passive, about once per second). This implementation probes actively with a `Raft.Ping` RPC at `monitor_freq_hz` (10 Hz by default).
- **RTT variance.** frugal uses the mean of kernel-reported `tcpi_rttvar` samples; this implementation uses the sample standard deviation of the raw RTT window. The values are close but not identical.
- **No timeout doubling.** frugal doubles the next timeout after an expired election; here a fresh value is drawn instead.
- **Log replication.** frugal's log replication is tied to its UDP transport; this implementation has full Raft log replication over pluggable transports.
- **Transport.** frugal uses UDP; this implementation offers mock, gRPC and TCP transports.

## Limitations

- No persistence: all state is in memory; a restarted node must be re-seeded by the leader.
- No log compaction or snapshotting: the log grows without bound.
- No membership changes: the cluster size is fixed at startup.
- `ClientRead` does not take a `context.Context`, so client-side cancellation does not reach the server.
- Under high one-way delay and rapid term churn, heartbeats can become stale in flight and be rejected without refreshing the follower's election timer, which can sustain an election storm. Longer election timeouts and moderate load mitigate this.
