# NetElect Design Document (Kept in Sync with the Implementation)

This document is derived from the current code in this repository and serves as the single source of truth for its architecture and interfaces. Its goals are to:

- Explain the system's module boundaries, key data flows, and interface contracts
- Describe the scope of what the current implementation supports
- Provide clear entry points for future extensions (snapshots, membership changes, persistence, etc.)

> **Status.** Two points below describe intended rather than actual behaviour, and some operational details in §9–§10 are out of date. See [KNOWN_ISSUES.md](KNOWN_ISSUES.md):
> - Petition acceptance (§4.6): a follower can petition itself, and that petition counts toward the quorum ([C1](KNOWN_ISSUES.md#c1-self-petitions-count-toward-the-petition-quorum)).
> - Linearizable / ReadIndex reads: no read index is recorded or awaited, so reads right after a leader change can be stale ([C2](KNOWN_ISSUES.md#c2-reads-are-not-linearizable-after-a-leader-change)).
> - §9–§10: log collection, `exp_info.json` contents, run numbering, `run_sweep.sh`, hostnames and several smaller details ([D3](KNOWN_ISSUES.md#d3-designmd)).

## 1. Scope and Design Goals

### 1.1 Goals

- Implement the complete Raft core: leader election (`RequestVote`) + log replication (`AppendEntries` with the consistency check and ConflictTerm/ConflictIndex fast backup) + application to a KV state machine (`applyLoop`)
- Provide ReadIndex-style confirmation for linearizable reads: `ClientRead` returns only after the leader has confirmed its leadership with a majority
- Provide linearizable writes: `ClientWrite` goes through the log commit path and returns once the entry has been applied
- Decouple the algorithm from the network: a single `server.Transport` abstraction covers three interchangeable transports (mock / gRPC / TCP)
- Support the optional NetElect extension: RTT-based penalty-score broadcast + petition-driven proactive leader switching, plus pluggable failure detection (dynamic election timeout)
- Provide reproducible test and benchmark entry points: mock-network integration tests, transport-layer tests, and real-network benchmarks

### 1.2 Non-goals

- No persistence (currentTerm/votedFor/log are in-memory only and are lost when a node restarts)
- No membership changes (joint consensus), leader leases, or other more advanced Raft features
- No log compaction or snapshotting (the log only grows)
- `ClientRead` does not take a `context.Context`, so client-side cancellation cannot propagate to the server

## 2. System Structure and Module Breakdown

### 2.1 Directory Layout (by Responsibility)

- `server/`: Raft core state machine, RPC handling, log replication/commit/apply, KV state machine, ReadIndex, NetElect extension
  - `raft.go`: state machine core, all core RPC handlers, `ticker()`, `batchReadIndexLoop()`, `applyLoop()`
  - `rpc.go`: all RPC argument/reply structs
  - `kv.go`: KV command types (`KVOp`, `KVCommand`)
  - `transport.go`: `Transport` interface definition
  - `netelect.go`: NetElect extension (penalty/petition/FDMode)
  - `delay_transport.go`: `DelayTransport`, the outbound-delay wrapper used for fault injection (§4.7)
- `middleware/`: transport implementations (MockNetwork, gRPC with a gob codec, custom TCP protocol)
- `client/`: benchmark client (leaderHint + redirect retry) and statistics (QPS/percentiles)
- `config/`: JSON config loading and validation (transport/rpc_timeout/nodes/benchmark/netelect)
- `cmd/`: executable entry points (`raftd`, `client`, `grpcbench`, `tcpbench`, `bench`)

### 2.2 Boundaries and Dependency Direction

- `server` depends only on the `server.Transport` abstraction, never on a concrete `middleware` implementation
- `middleware` depends on `server`'s RPC types and adapts the `*server.Raft` object
- `client` depends only on `server.Transport`, the types in `server/rpc.go`, and the shared method names
- `cmd` does the wiring: it reads nodes and the transport choice from `config`, creates the `Transport`s, and starts the corresponding server

## 3. Core Runtime Model (Concurrency and State Management)

### 3.1 Raft Node Lifecycle

Each node creates a `*server.Raft` via `server.Make(peers, me, opts...)`. Key properties:

- All shared state is protected by a single `sync.Mutex` (`rf.mu`)
- Three background goroutines are always started:
  - `ticker()`: triggers timeout-driven elections on Followers/Candidates; sends periodic heartbeats + log replication on the Leader
  - `batchReadIndexLoop()`: batches `ClientRead` requests and runs one `confirmLeadership()` round per batch
  - `applyLoop()`: watches for `commitIndex > lastApplied`, applies committed entries to the KV state machine, and notifies waiting `ClientWrite` callers
- When NetElect is enabled, two more are started:
  - `latencyMonitorLoop()`: periodically probes RTT with `Ping` and maintains an EWMA plus an RTT sample window per peer
  - `penaltyBroadcastLoop()`: computes and broadcasts the penalty score; a follower petitions when the conditions are met

### 3.2 Key State and Invariants

Raft state (see `server/raft.go`):

**Election**
- `currentTerm`: monotonically increasing; on observing a higher term, a node must adopt it and revert to Follower
- `votedFor`: at most one candidate per term (`-1` means no vote cast)
- `state`: `Follower / Candidate / Leader`
- `leaderId`: the leader whose heartbeat was most recently accepted (`-1` if unknown)
- `lastHeartbeat`: time of the most recently accepted heartbeat or granted vote, used for the election-timeout check (also reset when the node starts an election or is forced to step down)

**Log replication**
- `log []LogEntry`: real entries start at index 1; index 0 holds a dummy sentinel entry (term = 0)
- `commitIndex`: highest log index known to be committed on a majority
- `lastApplied`: highest log index applied to the state machine
- `nextIndex[]` (Leader only): for each peer, the index of the next log entry to send
- `matchIndex[]` (Leader only): for each peer, the highest log index known to be replicated there

**State machine and write waiters**
- `kvStore map[string]string`: KV state machine data
- `pendingWrites map[int]chan string`: log index -> channel; `ClientWrite` blocks on it until the entry is applied
- `applyNotify chan struct{}`: buffered (capacity 1) wake-up signal, sent non-blockingly whenever `AppendEntries` or `maybeAdvanceCommitIndex` advances `commitIndex`

Implementation constraints:
- Every read or write of the fields above must hold `rf.mu`
- RPC handlers must handle term updates while avoiding blocking I/O inside the critical section (all outgoing RPCs are sent from separate goroutines)
- Each `pendingWrites` channel has capacity 1, so `applyLoop` can deliver the result without blocking even though it does so while holding `rf.mu`

## 4. Protocols and Data Flows

### 4.1 Unified RPC Method Names (Identical Across Transports)

All transports use the following method names:

- Core: `Raft.RequestVote`, `Raft.AppendEntries`, `Raft.Ping`, `Raft.ClientRead`, `Raft.ClientWrite`
- NetElect: `Raft.PenaltyScore`, `Raft.Petition`

The RPC argument/reply structs are defined in `server/rpc.go` and shared by `server`, `client`, and `middleware`.

### 4.2 Leader Election Data Flow (Standard Raft)

**Trigger**

- A Follower/Candidate enters `startElection()` when `ticker()` sees `time.Since(lastHeartbeat) > electionTimeout`
- Election timeout:
  - By default (FDMode = `raft`), uniform random in `[150, 300)` ms
  - With NetElect enabled, it may instead be a dynamic Jacobson/CI estimate (see §4.6)

**Voting**

1. Candidate (if it passes the NetElect eligibility check, §4.6): `currentTerm++`, `votedFor = me`, then sends `RequestVote` to all peers in parallel
2. Peer: if `args.Term` is higher, it adopts that term and reverts to Follower; within the same term, if it has not voted or has already voted for this candidate, and the candidate's log is at least as up-to-date as its own (`LastLogTerm > rf.lastLogTerm()`, or equal terms and `LastLogIndex >= rf.lastLogIndex()`), it replies `VoteGranted = true`
3. Candidate: on receiving a majority of votes it becomes Leader, reinitializes `nextIndex`/`matchIndex`, and immediately sends a round of heartbeats

### 4.3 Log Replication and Commit Data Flow (AppendEntries)

**Leader send**

1. Every heartbeat interval (`HeartbeatInterval` = 50 ms), or right after a `ClientWrite`, the Leader calls `sendHeartbeats()`, which sends `AppendEntries` to all peers in parallel
2. Each message carries `PrevLogIndex`, `PrevLogTerm`, `Entries` (the log suffix starting at `nextIndex[peer]`), and `LeaderCommit`

**Follower accept (three phases)**

1. **Consistency check**: if `PrevLogIndex > len(log)-1` or `log[PrevLogIndex].Term != PrevLogTerm`, reply `Success = false` with `ConflictTerm`/`ConflictIndex` so the leader can back up quickly
2. **Append**: compare entries one by one starting at `PrevLogIndex+1`; on the first conflict, truncate and append the rest; matching entries are kept
3. **Advance commitIndex**: if `LeaderCommit > commitIndex`, set `commitIndex = min(LeaderCommit, lastLogIndex)` and wake `applyLoop` via `applyNotify`

**Leader backup (fast backup)**

- Success: update `matchIndex[peer]` and `nextIndex[peer]`, then call `maybeAdvanceCommitIndex()`
- Failure: use `ConflictTerm`/`ConflictIndex` to move `nextIndex[peer]` back (rather than decrementing by one), reducing the number of round trips

**Commit rule (`maybeAdvanceCommitIndex`)**

Scan downward from lastLogIndex to find the largest N such that:
1. A majority of nodes (counting the leader itself) have `matchIndex >= N`
2. `log[N].Term == currentTerm` (the leader-completeness constraint, which prevents directly committing entries from earlier terms)

If such an N exists, set `commitIndex = N` and signal `applyNotify`.

### 4.4 KV State Machine Apply Data Flow (applyLoop)

`applyLoop()` waits on `applyNotify`. On each wake-up it acquires `rf.mu` and, while `lastApplied < commitIndex`:
1. Increments `lastApplied` and takes the entry at that index
2. Executes its `LogEntry.Command.(KVCommand)` (`Put` -> write to kvStore; `Delete` -> remove the key)
3. If `pendingWrites[index]` exists, sends the apply result on it and deletes the entry

The whole loop runs while holding `rf.mu`.

`ClientWrite` blocking path:
1. A non-Leader immediately returns `Success = false` plus `LeaderId` (when known)
2. The Leader appends the log entry and registers `pendingWrites[logIdx]` (a capacity-1 channel)
3. It releases `rf.mu` and triggers `go rf.sendHeartbeats()` to start replication
4. It blocks on `pendingWrites[logIdx]`, with a timeout of `ElectionTimeoutMax` (300 ms)
5. Once `applyLoop` applies the entry and signals, `ClientWrite` returns `Success = true`
6. If the wait times out, it removes `pendingWrites[logIdx]` and returns `Success = false` (without a `LeaderId`). There is no explicit check for a term change: if the leader is deposed, the pending write simply times out

### 4.5 Linearizable Read Data Flow (ReadIndex-Style Confirmation)

Goal: before answering a read, confirm that the current leader still holds a majority, so that a leader that has lost its majority does not keep serving reads.

Flow:

1. A non-Leader receiving `ClientRead` immediately returns `Success = false` plus `LeaderId` (the client then redirects and retries)
2. Leader: appends the request to `pendingReads` and wakes `batchReadIndexLoop()` via `readNotify`
3. `batchReadIndexLoop()`: on each wake-up, takes all current `pendingReads` as one batch and calls `confirmLeadership(term)` once
4. `confirmLeadership(term)`: sends a heartbeat (an empty `AppendEntries`) to all peers in parallel; confirmation succeeds if a majority of `Success = true` ACKs (counting the leader itself) arrive within `ElectionTimeoutMin` (150 ms)
5. Batch completion: on success, each request reads its value from `kvStore` and returns it; on failure, it returns `Success = false` plus `LeaderId`

Client-side design (redirect model):
- Maintains a `leaderHint`, initially -1 (pick a node at random)
- On `Success = false` with a `LeaderId`, updates the hint and retries immediately (no sleep)
- On RPC failure, clears the hint and backs off exponentially with jitter
- Usually converges on the leader after at most one redirect and stays there until the next re-election

### 4.6 NetElect Extension Data Flow (Penalty + Petition + Dynamic Timeout)

NetElect is an optional extension, enabled via `server.WithNetElect(NetElectConfig{...})`.

**RTT probing**

- Every node periodically sends `Raft.Ping` to all peers (at `monitor_freq_hz`)
- Two kinds of RTT data are kept per peer: an EWMA (used for the penalty score and petition decisions) and a window of raw RTT samples (used by the Jacobson/CI timeout estimators)

**Penalty broadcast**

- Every node periodically computes its penalty score as the mean, over all peers with RTT data, of `L + w·max(0, L − T)`, where `L` is the EWMA RTT to that peer, `w` is `penalty_weight`, and `T` is `penalty_threshold_ms`
- It broadcasts the score to all peers via `Raft.PenaltyScore`, giving every node a global view of penalty scores

**Petition-driven proactive leader switching**

1. In each penalty-broadcast round, a follower evaluates the current leader's *broadcast* penalty score (not its own RTT to the leader). It petitions only if both (a) the leader's penalty exceeds `latency_threshold_ms` (the leader is slow in absolute terms; this also acts as a noise floor), and (b) the node with the lowest penalty score other than the current leader has a penalty below `leaderPenalty − petition_penalty_margin_ms` (a meaningfully better candidate exists; the margin is anti-churn hysteresis). That lowest-penalty node becomes the proposed leader. The follower must also have an RTT measurement to the leader, which it includes in the petition. Using the leader's broadcast penalty matters when `inbound_ping_delay_ms = 0`: a follower's RTT to the leader then equals its own outbound delay regardless of which node leads, so a per-follower RTT threshold would never fire on well-connected (near) nodes.
2. The follower sends `Raft.Petition` to the proposed leader
3. The proposed leader counts a petition only if its term and current-leader ID match its own view and it names this node; each sender is counted once. Once it has `(N+1)/2 − 1` distinct petitioners (integer division; 2 in a 5-node cluster), it verifies that its own RTT to each petitioner (where known) is no worse than the RTT that petitioner reported to the current leader. If the check passes, it sends `RequestVote{IsPetition: true}` to the current leader, which steps down to Follower without casting a vote
4. What follows depends on `petition_direct_election`: when `false` (default), a normal election follows and whichever node times out first wins (under Jacobson/CI the rank-based delay favors low-penalty nodes, but nothing guarantees that the proposed leader wins); when `true`, the proposed leader immediately calls `startElection()` itself (provided it is still a Follower in the same term) and almost always wins

**Eligibility**

- `eligible_leaders`: steady-state whitelist of nodes allowed to start an election (empty = all)
- `initial_eligible_leaders`: whitelist applied only to the very first election (term 0, no known leader). It is honored even when `enabled` is false, which lets the plain-Raft baselines pin the first leader

**Dynamic election timeout (FDMode)**

- `raft`: falls back to the standard uniform random timeout in `[150, 300)` ms (no rank-based delay)
- `Jacobson`: from the RTT sample window to the current leader, `RTO = srtt/2 + 4·stddev` (capped at 800 ms), plus `heartbeat_interval_margin_ms` and the rank-based delay
- `CI`: half of the confidence-interval upper bound `mean + z·stddev` (z from `confidence_level`), plus `heartbeat_interval_margin_ms` and the rank-based delay
- Rank-based delay (Jacobson/CI only): uniform in `[lower + step·rank, lower + step·(rank+1))`, where `rank` is the node's position in ascending penalty order (`lower` = `safety_margin_lower_bound_ms`, `step` = `safety_margin_step_size_ms`). Nodes with better network conditions therefore tend to time out, and start an election, first
- Jacobson and CI fall back to the `raft` timeout when the leader is unknown or fewer than two RTT samples to it are available

### 4.7 Fault Injection (for Experiments)

To reproduce experimental scenarios, `server` exposes two kinds of injectable faults. They are configured via `nodes[].fault` in the config (`config.FaultInjection`) and wired up by `cmd/raftd` around `server.Make`:

**Delay injection (two independent channels)**

- **Outbound delay `outbound_delay_ms`**: the node's per-peer transports are wrapped in `DelayTransport` (`server/delay_transport.go`), which calls `time.Sleep` before **every** outbound RPC the node initiates (heartbeats, RequestVote, and Ping are all affected).
- **Ping reply delay `inbound_ping_delay_ms`**: `Raft.SetPingDelay()` (internally `faultPingDelayMs`) sleeps only before replying to `Raft.Ping`; AppendEntries is unaffected.
- **RTT model**: RTT is measured by active `Raft.Ping`, so `RTT(a→b) = a.outbound_delay + b.inbound_ping_delay`. Placing the delay on the outbound side (sender) versus the inbound-Ping side (responder) changes the directionality of the measured RTT, and is the key degree of freedom in designing the WAN experiments.
- **Random vs. fixed**: if `*_max_ms > 0`, each node draws one fixed value uniformly from `[*_min_ms, *_max_ms]` at startup (modeling a heterogeneous WAN); otherwise the fixed `*_ms` value is used.
- **Activation time `delay_start_after_s`**: when > 0, the delays activate only this many seconds after startup (0 = immediately). This lets a node first become a stable leader at zero delay and then degrade, giving a clean "steady state -> degradation -> switch" timeline.

**Forced stepdown**

- **`stepdown_after_s`** (one-shot): when > 0, `Raft.StepDown()` is called once, this many seconds after startup. If the node is currently the leader, it reverts to Follower, clears `leaderId`, and resets its own `lastHeartbeat` (so it does not immediately re-campaign), triggering a new election; on a non-leader it is a no-op (returns false). Used to inject a single "leader failure" event.
- **`leader_stepdown_after_s`** (recurring): via `server.WithLeaderStepdown`, every time the node wins an election it schedules a stepdown this many seconds later. The timer is term-guarded, so a stale timer never deposes a later leadership. Used by the failure experiment to drive repeated failovers within a single run.

**Experiment observability**

- `Raft.GetElectionMsgCount() (sent, recv int64)`: cumulative RequestVote send/receive counters (atomic); `cmd/raftd` prints an `election_msgs` line on SIGTERM.
- Leadership and petition events are logged by the server as they happen: `[election_start]` (including `detect_ms`, the time from the last heartbeat to becoming candidate), `[leader_elected]`, `[petition_send]`, `[petition_recv]`, `[petition_accept]`, `[petition_stepdown]`, and `[petition_direct_election]`. `cmd/raftd` additionally prints `raftd up` at startup and `fault_latency_active` when delays activate; a `fault_stepdown` line is printed on each forced stepdown (by `cmd/raftd` for the one-shot variant, by the server for the recurring one). There is no periodic state poller.
- `cmd/client` supports `-timeseries-csv` + `-timeseries-interval`, emitting per-interval QPS/latency rows to show behavior before and after an injected fault.

**Known interaction: election storms under high latency**

`AppendEntries` (`raft.go`) rejects a heartbeat with `args.Term < currentTerm` by returning immediately, **without refreshing `lastHeartbeat`**. When per-hop delay is large and terms churn quickly, a heartbeat the leader sent in term T can become stale in flight because an election started elsewhere (the receiver has already moved to T+1). The rejected heartbeat then fails to suppress elections, followers keep timing out, and the result is a self-sustaining election storm. Mitigations: lengthen the election timeout (`heartbeat_interval_margin_ms` for Jacobson/CI, or the `ElectionTimeoutMin/Max` constants for standard Raft), limit concurrent load, and use `delay_start_after_s` so the leader stabilizes before the network degrades.

## 5. Interface Specification (Developer View)

### 5.1 `server.Transport` Contract

The only requirement `server` places on a transport is:

- `Call(method, args, reply) bool`: a synchronous call; `true` means "the network call completed and a reply was successfully decoded", `false` means "network failure / timeout / unreachable"
- Application-level success is expressed in the `reply` fields, not in the return value of `Call`

### 5.2 gRPC and TCP Adaptation Constraints

- Both transports route by the same method names
- Both use gob for encoding (no protobuf dependency)
- The TCP transport multiplexes concurrent requests over a single connection using a request ID

### 5.3 Configuration Specification (JSON)

The config files under `config/` contain:

- `transport`: `"grpc"` or `"tcp"` (default `"grpc"`)
- `rpc_timeout`: e.g. `"250ms"` (the default)
- `nodes`: `{id, listen}` entries whose IDs are contiguous from 0, each with an optional `fault` block (see §4.7)
- `benchmark`: `workers`, `duration`, `timeout`, `sample_cap`, `mode` (`"read"` / `"write"` / `"mixed"`), `read_ratio` (fraction of reads in mixed mode, default 0.8), `csv_output`
- `netelect` (optional): the master switch and all parameters (thresholds, frequency, FDMode, safety margins, eligible leaders, `petition_penalty_margin_ms`, `petition_direct_election`, etc.)

## 6. Extensibility and Evolution

### 6.1 Log Compaction and Snapshots

- Add a snapshot trigger in `applyLoop` (e.g. when the log exceeds a size threshold)
- Add an `InstallSnapshot` RPC so lagging nodes can catch up quickly
- Requires persisting snapshot files along with `snapshotIndex`/`snapshotTerm`

### 6.2 Persistence

- Flush `currentTerm`, `votedFor`, and `log` to disk after every update (a WAL or a key-value store)
- Restore the persisted state in `Make()` so that a restarted node recovers its election state correctly

### 6.3 ReadIndex and Client Improvements

- Give `batchReadIndexLoop` a time window (e.g. a fixed 1 ms flush), a maximum batch size, and backpressure
- Make `ClientRead` accept a `context.Context` and propagate client cancellation to the server-side wait

### 6.4 Observability and Engineering

- Add exportable metrics (election count, leader tenure, petition count, read batch size, p95/p99)
- Add event tracing and reproducible fault scripts (MockNetwork already provides basic fault injection)

## 7. Testing and Validation Strategy

- `server/raft_test.go`: MockNetwork-based integration tests for initial election, re-election, and ReadIndex (`TestInitialElection`, `TestReElection`, `TestReadIndex`); there are currently no dedicated tests for log replication or `ClientWrite`, which are exercised by the benchmark client's `write`/`mixed` modes
- `server/netelect_test.go`: NetElect tests covering eligibility, penalty broadcast, petition (including `petition_direct_election`), and the FDMode estimators (Jacobson/CI)
- `middleware/grpcnet/*_test.go`, `middleware/tcpnet/*_test.go`: transport-layer functional tests
- `cmd/bench`: in-process benchmark for throughput and latency distribution
- `cmd/grpcbench`, `cmd/tcpbench`: real-network benchmarks to check consistency and performance characteristics across transport implementations

## 8. Known Limitations (Implementation Level)

- `ClientRead` does not accept a `context.Context`, so the server cannot observe client cancellation
- No log compaction/snapshots: the log only grows, and a long-running cluster will eventually exhaust memory
- No persistence: currentTerm/votedFor/log are in-memory only; a restarted node starts from empty state and the leader must re-replicate the entire log
- No membership changes: the cluster size is fixed at startup; joint consensus is not supported

## 9. CloudLab Multi-Node Experiment Guide

This section is the operations manual for benchmarking on a 5-node CloudLab cluster, covering the gRPC vs. TCP transport comparison and sweeps over several concurrency configurations.

### 9.0 Installing Go and Setting Up the Repository
```bash
GO_VER=1.22.5
curl -LO https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go${GO_VER}.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

go version
```
Note that `go.mod` declares `go 1.25.5`. With an older toolchain such as 1.22.5, the `go` command automatically downloads the required toolchain on first build (the default `GOTOOLCHAIN=auto`), which needs network access; alternatively, set `GO_VER=1.25.5` above.

Once Go is installed, create a dedicated experiment directory `~/raft` on each node and clone the code:
```bash
mkdir -p ~/raft
cd ~/raft
git clone git@github.com:SuQichen777/NetElect.git
```

### 9.1 Node Roles and Numbering

Five nodes (in the same CloudLab experiment, mutually reachable), all accessed with the same login user `Jiaxi`:

| Raft ID | Hostname | Role            |
|---------|----------|-----------------|
| 0       | 0 - addr | Server + Client |
| 1       | 1 - addr | Server + Client |
| 2       | 2 - addr | Server + Client |
| 3       | 3 - addr | Server + Client |
| 4       | 4 - addr | Server + Client |

Every node runs both `raftd` (the server) and `client` (the benchmark).

### 9.2 Config File (gRPC)

```bash
mkdir -p ~/raft/config
cat > ~/raft/config/cluster.grpc.json <<'EOF'
{
  "transport": "grpc",
  "rpc_timeout": "250ms",
  "nodes": [
    { "id": 0, "listen": "clnode126.clemson.cloudlab.us:7000" },
    { "id": 1, "listen": "clnode111.clemson.cloudlab.us:7000" },
    { "id": 2, "listen": "clnode122.clemson.cloudlab.us:7000" },
    { "id": 3, "listen": "clnode121.clemson.cloudlab.us:7000" },
    { "id": 4, "listen": "clnode101.clemson.cloudlab.us:7000" }
  ],
  "benchmark": {
    "workers": 128,
    "duration": "30s",
    "timeout": "250ms",
    "sample_cap": 200000,
    "mode": "read",
    "csv_output": "results.csv"
  },
  "netelect": {
    "enabled": false,
    "latency_threshold_ms": 50,
    "monitor_freq_hz": 10,
    "penalty_weight": 1.0,
    "penalty_threshold_ms": 100,
    "eligible_leaders": [],
    "initial_eligible_leaders": [],
    "fd_mode": "raft",
    "confidence_level": 0.95,
    "heartbeat_interval_margin_ms": 50,
    "safety_margin_lower_bound_ms": 0,
    "safety_margin_step_size_ms": 20,
    "rtt_window_size": 32
  }
}
EOF
```

For the TCP runs, create `~/raft/config/cluster.tcp.json` the same way, with `"transport": "tcp"`.

### 9.3 Building the Binaries on Each Node

From your workstation, SSH into each node and run the following commands from `~/raft` (the directory the repository was cloned into in §9.0):

```bash
mkdir -p ~/raft/bin
# Enter the cloned repository
cd NetElect

# Build
go build -o ~/raft/bin/raftd  ./cmd/raftd
go build -o ~/raft/bin/client ./cmd/client
```

Repeat these steps on each of the 5 nodes.

### 9.4 Starting the Servers (One Server Window per Node)
At this point, the file layout on each node should look like this:

```bash
~/raft/
├── bin/
│   ├── raftd
│   ├── client
├── config/
│   ├── cluster.grpc.json
│   ├── cluster.tcp.json
├── NetElect/

```
Open 5 SSH terminals and start the server with the matching ID in each window:

| Node                           | Command                                                             |
|-------------------------------|---------------------------------------------------------------------|
| `ssh name@0-addr` | `cd ~/raft && ./bin/raftd -config config/cluster.grpc.json -id 0`  |
| `ssh name@1-addr` | `cd ~/raft && ./bin/raftd -config config/cluster.grpc.json -id 1`  |
| `ssh name@2-addr` | `cd ~/raft && ./bin/raftd -config config/cluster.grpc.json -id 2`  |
| `ssh name@3-addr` | `cd ~/raft && ./bin/raftd -config config/cluster.grpc.json -id 3`  |
| `ssh name@4-addr` | `cd ~/raft && ./bin/raftd -config config/cluster.grpc.json -id 4`  |

Once all 5 windows are running, the cluster elects a leader within about 1–2 seconds. **Leave these 5 windows open in the foreground.**

To switch to TCP, press `Ctrl+C` in each server window and restart it with `cluster.tcp.json` in place of `cluster.grpc.json`.

### 9.5 Running the Client on All 5 Nodes Simultaneously

Open 5 more SSH terminals (client windows). For a single test run:

```bash
cd ~/raft && ./bin/client -config config/cluster.grpc.json -workers 128 -duration 30s
```
Command-line flags override the defaults in the config file.

Example output:

```
transport=grpc addrs=... workers=128 duration=30s timeout=250ms mode=read
[read] total=182054 success=181998 failed=56 qps=18205.40
[read] p50=5.8ms p95=11.2ms p99=18.7ms samples=182054
```

For mixed mode, use the config defaults or override them with flags:

```bash
./bin/client -config config/cluster.grpc.json -mode mixed -read-ratio 0.8
```

### 9.6 Running Tests in Bulk with Scripts

All scripts below are run from your local workstation and control the remote nodes over SSH. They require `jq` (`brew install jq`).

---

#### `scripts/restart_all_servers.sh` — Restart all servers

Config file: `scripts/restart_all_servers.json`

Run:

```bash
./scripts/restart_all_servers.sh
```

Behavior: syncs the config to every node, then, in parallel, runs `pkill raftd` on each node and starts a new background process with the matching ID. An optional first argument overrides the config path from the JSON file. All nodes restart from an empty log and empty KV state, and a leader is elected within about 1–2 seconds. Call it before each repeated experiment so that every run starts from the same initial state.

---

#### `scripts/start_all_clients.sh` — Run the benchmark client on all nodes in parallel

Config file: `scripts/start_all_clients.json`

Run:

```bash
./scripts/start_all_clients.sh
```

Behavior: assumes the servers are already running; launches the benchmark on all client nodes in parallel, waits for completion, and collects each node's CSV into `results/<experiment>/<run>/`. When `repeat > 1`, it waits `settle_delay_s` seconds between rounds. Before repeated experiments, it is advisable to call `restart_all_servers.sh` first to reset server state.

---

#### `scripts/run_sweep.sh` — Full multi-group sweep (recommended)

Config file: `scripts/run_sweep.json`

Run:

```bash
./scripts/run_sweep.sh
```

Behavior: runs the experiment groups sequentially (currently 3: `raft` / `netelect-raft` / `netelect-jacobson`; see §10.1 regarding `netelect-CI`). For each group it first syncs the config, then performs 20 Mixed-workload runs (restarting the servers before each), then 20 Read-only runs (reusing the KV state, no restart). Results are written to `results/<label>/mixed/<run>/` and `results/<label>/read/<run>/`.

## 10. NetElect Experiment Guide

This section describes the full workflow for running the NetElect comparison experiments on the 5-node CloudLab cluster, covering the experiment matrix, configuration, the one-shot sweep scripts, and result analysis.

### 10.1 Experiment Matrix

| Condition | Config file | netelect | FDMode | Description |
|---|---|---|---|---|
| `raft` | `config/cluster.raft.json` | off | — | Standard Raft, no NetElect extension at all |
| `netelect-raft` | `config/cluster.netelect.raft.json` | on | `raft` | Penalty + petition enabled; election timeout still random in [150, 300) ms |
| `netelect-jacobson` | `config/cluster.netelect.jacobson.json` | on | `Jacobson` | Dynamic timeout: `srtt/2 + 4·stddev` (capped at 800 ms) + rank delay |
| `netelect-CI` | `config/cluster.netelect.ci.json` | on | `CI` | Dynamic timeout: half the confidence-interval upper bound (z = 1.96, level = 0.95) + rank delay |

The sweep scripts currently run only the first three conditions: `netelect-CI` has been removed from their `SWEEP_GROUPS` lists. Its config files (`config/cluster.netelect.ci*.json`) are still present, so it can be re-enabled by adding it back to a script's group list.

**Comparison goals**
- `raft` vs. `netelect-raft`: isolates the effect of the penalty + petition mechanism itself (FDMode held fixed)
- `netelect-raft` vs. `netelect-jacobson` vs. `netelect-CI`: compares how the three FDModes affect election-timeout estimation
- All conditions use the same benchmark parameters (workers = 128, duration = 30s), so results are directly comparable

### 10.2 Key Configuration Parameters

The fault-free NetElect configs (`config/cluster.netelect.{raft,jacobson,ci}.json`) share the following parameters (the fault-sweep configs override some of them; see §10.7):

| Parameter | Value | Description |
|---|---|---|
| `latency_threshold_ms` | 50 | Minimum leader penalty score (as broadcast by the leader) required before a follower petitions (§4.6) |
| `monitor_freq_hz` | 10 | 10 Ping probes per second, updating RTT and penalty |
| `penalty_weight` | 1.0 | Weight `w` of the above-threshold term in the penalty formula |
| `penalty_threshold_ms` | 100 | Threshold `T` in the penalty formula |
| `heartbeat_interval_margin_ms` | 50 | Fixed safety margin added to the dynamic timeout |
| `safety_margin_step_size_ms` | 20 | Per-rank step of the rank-based delay (rank 0–4 with 5 nodes) |
| `rtt_window_size` | 32 | Size of the RTT sample sliding window used by the Jacobson/CI estimators |

To tune a single condition (e.g. lower `latency_threshold_ms` to make petitions more aggressive), edit the corresponding JSON file directly.

### 10.3 One-Shot Sweep: `run_sweep.sh`

The script runs on your workstation, controls the remote nodes over SSH, and executes the condition groups **sequentially**. The full flow for each group is:

**Mixed phase (`mixed_runs` runs, default 20)**

1. Sync the config file to all nodes (`scp`)
2. Stop the old processes with `pkill raftd` in parallel
3. Restart `raftd` in the background with the corresponding config via `setsid -f`, in parallel
4. Wait `startup_settle_s` (default 5s) for the cluster to elect a leader
5. Launch the `client -mode mixed` benchmark on all nodes in parallel
6. Wait for all clients to finish, then `scp` the CSVs (and each node's server log) into `results/<label>/mixed/<run>/`
7. Write `exp_info.json` (with metadata such as `netelect_enabled`, `fd_mode`, `mode=mixed`)

**Read phase (`read_runs` runs, default 20, no server restart)**

- Reuses the KV state left by the last Mixed run and directly launches `client -mode read`
- Collects the CSVs into `results/<label>/read/<run>/`

```bash
# Requires jq (brew install jq)
./scripts/run_sweep.sh
```

Sweep parameters are configured in `scripts/run_sweep.json`:

```json
{
    "user": "Jiaxi",
    "ips": [ "clnode126...", "clnode111...", ... ],
    "nb_servers": 5,
    "csv": "results.csv",
    "read_ratio": 0.8,
    "mixed_runs": 20,
    "read_runs": 20,
    "startup_settle_s": 5,
    "run_settle_s": 0
}
```

`mixed_runs: 20` + `read_runs: 20` means each group runs each workload 20 times, with results stored in `results/<label>/mixed/1/` … `20/` and `results/<label>/read/1/` … `20/`.

### 10.4 Directory Layout (After the Sweep)

```
results/
├── raft/
│   ├── mixed/
│   │   ├── 1/  exp_info.json  clnode126.csv  clnode111.csv  ...
│   │   ├── 2/
│   │   └── 20/
│   └── read/
│       ├── 1/
│       └── 20/
├── netelect-raft/
│   ├── mixed/  ...
│   └── read/   ...
├── netelect-jacobson/
│   ├── mixed/  ...
│   └── read/   ...
└── netelect-CI/
    ├── mixed/  ...
    └── read/   ...
```

The `netelect-CI/` subtree appears only if that group is included in the sweep (see §10.1).

### 10.5 Result Analysis

**`analyze_sweep.py` (main analysis script, config-driven)**

The scope of the analysis is determined entirely by `scripts/analyze_sweep.json` (there are no CLI flags for selecting runs). The JSON contains a list of
**datasets**, each of the form `{name, title, fault_type, groups[], runs}`:

- `groups`: result directory names to analyze (e.g. `raft-wan`, `netelect-jacobson-wan`). **Only the listed groups are analyzed**;
  removing a group or an entire dataset from the list excludes it from the analysis.
- `runs`: which runs to use; accepts a range `"13-32"`, a non-contiguous list `"1,3,5-8"`, or `[..]`.
- The same directories can be referenced by several datasets, each with a different run window (this is how old and new binaries are compared).

```bash
python3 scripts/analyze_sweep.py                 # reads scripts/analyze_sweep.json
python3 scripts/analyze_sweep.py --config my.json
```

The selected runs of each group are summarized with trimmed statistics (drop the single largest and smallest value, then take the mean and sample standard deviation of the rest). Outputs:
- Console tables (`mean ±std`: QPS, p50/p95/p99/p99.9, success rate), also written to the Markdown report `results/report.md`
- **Petition/leadership table** (only for `wan`/`latency`, derived from server logs): migration success rate, approximate migration time,
  petition rate (pSend/run), who petitions (nearPetn%), acceptances (pAcc/run), **election message volume
  (eMsgSent/run, eMsgRecv/run, from the `election_msgs` lines, i.e. the cluster-wide cumulative RequestVote send/receive count per run,
  reflecting control-plane traffic caused by churn)**, and per-node election wins by node quality (outbound delay); `failure` datasets get a failure-recovery table instead
- One set of plots per dataset: `results/plots/<name>_{mixed,read_only}.png`, plus `<name>_timeseries.png` for fault datasets
- A cross-dataset comparison plot `results/plots/comparison_datasets.png` (x = variant, grouped by dataset, y = mixed QPS)


### 10.6 Running Manually, Step by Step (Alternative to the Sweep Script)

To test a single condition on its own:

```bash
# 1. Restart the servers (netelect-jacobson as an example)
./scripts/restart_all_servers.sh
# (first set config in restart_all_servers.json to config/cluster.netelect.jacobson.json)

# 2. Run the client on all nodes
./scripts/start_all_clients.sh
# (first update the config and experiment fields in start_all_clients.json accordingly)

# 3. Analyze the results
python3 scripts/analyze_sweep.py
```

Instead of editing the JSON files, both scripts also accept overrides as positional arguments: `restart_all_servers.sh [config_path]` and `start_all_clients.sh [config_path] [experiment_label] [fd_mode]`.

Using `run_sweep.sh` directly is recommended, since it keeps conditions consistent and produces the correct result directory format (`analyze_sweep.py` expects the two-level `<group>/mixed/<run>/` and `<group>/read/<run>/` structure).

### 10.7 Fault-Injection Sweeps (WAN / Failure) and the Baseline Benchmark

On top of the group structure of §10, further sweep scripts share the same "sync config -> restart servers -> run clients -> collect results" skeleton as `run_sweep.sh`. In addition, they (a) collect each node's server log `raftd-<id>.log` and (b) emit per-interval timeseries CSVs (`-timeseries-csv`) to capture the curves before and after an injected fault; the fault sweeps also (c) use dedicated configs carrying `nodes[].fault` blocks (see §4.7). The shipped JSON files run 20 mixed + 20 read runs per group (`mixed_runs`/`read_runs` are configurable). Each sweep runs the `raft`, `netelect-raft`, and `netelect-jacobson` variants; the fault sweeps suffix the group labels with `-wan`, `-wan-pde`, or `-failure`, while the baseline benchmark uses the unsuffixed labels.

| Script | Config family | Fault design |
|---|---|---|
| `scripts/run_wan_latency_sweep.sh` | `config/cluster.*.wan.json` | **The latency experiment** (the earlier standalone latency sweep has been retired). Tiered WAN, activated after `delay_start_after_s=5`: far nodes (0,1) outbound `[30,40]ms`, near nodes (2,3,4) outbound `[10,20]ms`, `inbound_ping=0` (so `RTT = follower.outbound`), `latency_threshold_ms=25` (= the leader-penalty floor), `petition_penalty_margin_ms=5`, `initial_eligible_leaders=[0]`. The full report is in `EXPERIMENT.md` |
| `scripts/run_wan_latency_petition_direct_election.sh` | `config/cluster.*.wan.pde.json` | Same tiered-WAN setup, but the NetElect groups run with `petition_direct_election=true`; results go to the `*-wan-pde` groups |
| `scripts/run_wan_latency_petition_compare.sh` | — | Runs the `petition_direct_election=true` sweep and then the baseline WAN sweep back to back, for an A/B comparison |
| `scripts/run_failure_sweep.sh` | `config/cluster.*.failure.json` | Recurring forced failover: all nodes set `leader_stepdown_after_s=7`, so every elected leader steps down about 7s after winning and each run sees repeated re-elections (in both the mixed and read phases). Fixed, distinct per-node outbound delays (node 0=20, 1=25, 2=0, 3=5, 4=10 ms) from t=5s; petitions are effectively disabled (`latency_threshold_ms=10000`), so leadership moves only through re-election; `heartbeat_interval_margin_ms=150` and `safety_margin_step_size_ms=30` level the dynamic timeout to roughly Raft's `[150,300)ms` envelope; `initial_eligible_leaders=[0]` |
| `scripts/run_benchmark_sweep.sh` | `config/cluster.{raft,netelect.*}.json` | Fault-free baseline benchmark (compares the base performance/overhead of raft and netelect after implementation changes), `fault_type=baseline` |
| `scripts/run_all_fault_sweeps.sh` | — | Runs the failure sweep, then the WAN-latency sweep, sequentially |

Config naming: `config/cluster.{raft,netelect.raft,netelect.jacobson,netelect.ci}.{wan,failure}.json`, plus `config/cluster.{raft,netelect.raft,netelect.jacobson}.wan.pde.json` for the `petition_direct_election` variant.

The result directory layout matches §10.4 (`results/<label>/{mixed,read}/<run>/`), but each run directory additionally contains `raftd-<id>.log` and `*.ts.csv`. `exp_info.json` records the group's fault parameters (`fault_type`, per-node delays, thresholds, etc.).

> For delay placement in the WAN experiment (symmetric vs. asymmetric) and the trade-off against election storms under high latency, see §4.7 of this document; the WAN configuration and its rationale are described in `EXPERIMENT.md`.

## 11. Documentation Maintenance Conventions

If you change any of the following, you must update this document and the README accordingly:

- The `server.Transport` interface and the method-name routing rules
- The RPC types and semantics in `server/rpc.go`
- The log replication flow (`AppendEntries`, `sendHeartbeats`, `maybeAdvanceCommitIndex`, `applyLoop`)
- The ReadIndex batching and confirmation logic (`batchReadIndexLoop` / `confirmLeadership`)
- The key parameters and behavior of NetElect's penalty/petition/FDMode
- The fields and behavior of fault injection (`config.FaultInjection`, `DelayTransport`, `SetPingDelay`, `StepDown`, `WithLeaderStepdown`) (§4.7)
- The experiment sweep script family (`run_sweep` / `run_benchmark_sweep` / `run_wan_latency_sweep` / `run_wan_latency_petition_direct_election` / `run_failure_sweep`) and the corresponding `config/cluster.*.{wan,wan.pde,failure}.json` (the standalone latency sweep has been retired; the latency experiment is now part of the WAN sweep)
- The structured log lines of `cmd/raftd` and the server, and the timeseries CSV output of `cmd/client`
- How to run `cmd/*` and the field definitions in `config/`
