# Experiment Reports

> **Status.** These write-ups predate the fixes planned in [KNOWN_ISSUES.md](KNOWN_ISSUES.md) and will be rewritten after the experiments are re-run. Until then:
> - The **WAN section** reports runs 33–52 (four variants, an earlier CloudLab allocation). `results/report.md` and the current analysis configuration use runs 53–72 (three variants); see [D1](KNOWN_ISSUES.md#d1-experimentmd-wan-section-uses-an-earlier-batch) and [M1](KNOWN_ISSUES.md#m1-results-are-sensitive-to-the-cloudlab-allocation).
> - The **baseline and failure** numbers match `results/report.md`, except for the `netelect-CI` rows, which are not part of the current analysis ([D2](KNOWN_ISSUES.md#d2-experimentmd-baseline-and-failure-sections-include-the-ci-variant)).
> - The **`petition_direct_election` comparison** is not yet written up; its numbers are in `results/report.md`.
> - Petition acceptance as described here ("a majority of petitions") is stronger than what the code does ([C1](KNOWN_ISSUES.md#c1-self-petitions-count-toward-the-petition-quorum)).

## Benchmark (Fault-Free Baseline)

### Goal

Measure the steady-state **overhead** of the NetElect machinery (the latency-monitor ping loop and the penalty-broadcast loop) relative to plain Raft, with no faults injected. All nodes are network-equal, so NetElect has nothing to act on — it should neither migrate nor petition — and the four variants should be statistically indistinguishable. This re-validates that enabling NetElect does not regress base throughput or latency.

### Configuration

Same testbed as the experiments below (5 CloudLab nodes, gRPC, 128 workers/client, 30s/run, 20 runs, trimmed mean). No `fault` block; base configs `config/cluster.{raft,netelect.raft,netelect.jacobson,netelect.ci}.json`.

### Results (trimmed mean over 20 runs)

| Variant | Mixed read QPS | Mixed write QPS | Read-only QPS | ok% |
|---|---|---|---|---|
| raft | 16,546 | 4,137 | 65,806 | 100 |
| netelect-raft | 16,497 | 4,127 | 66,140 | 100 |
| netelect-jacobson | 16,320 | 4,081 | 65,837 | 100 |
| netelect-CI | 16,612 | 4,150 | 70,609\* | 100 |


### Conclusion

All four variants land within run-to-run noise of plain Raft on every metric at 100% success: the NetElect ping/penalty loops add no measurable steady-state overhead, and with no latency to act on NetElect correctly stays inert (no migration, no petition). This baseline licenses the fault experiments below to attribute any difference to the fault response itself, not to background overhead.

---

## WAN-Latency Leader-Migration Experiment

---

### 1. Goal

The goal of WAN-Latency Leader-Migration Experiment is to show that, in a geo-distributed (WAN) cluster where the Raft leader is slow or congested, NetElect migrates leadership to a low-latency node and keeps it there, improving read/write latency and throughput vs plain Raft. The migration is expected to be driven **reliably** by the petition protocol.

---

### 2. Testbed & Configuration

| Item | Value |
|---|---|
| Cluster | 5 nodes on CloudLab (Clemson Cluster), 1 `raftd` + 1 `client` per node |
| Transport | gRPC (gob), `rpc_timeout = 400ms` |
| Raft timing | heartbeat `50ms`; election timeout `[150,300)ms` |
| Workload | 128 workers/client (640 total), duration 30s (per run), client timeout 250ms |
| Mixed ratio | read:write = 80:20 (`read_ratio=0.8`) |
| Repetitions | 20 runs × {mixed, read} per variant |
| Analysis | Trimmed mean (drop min+max) |


#### 2.1 Injected WAN latency
The latency injected is an "outbound" latency, which would result in the delay of sending messages. Replying to a "ping" RPC does not have the latency, so we could view it as a uniform one. For example, one congested node sending ping-pong would get the latency injected into itself instead of the sum of its latency and the destination node's latency. It is designed to reach a balance between latency injection and threshold-control simplicity.

| Group | Nodes | latency (random per run) |
|---|---|---|
| Far / Congestion / Slow| node0, node1 | `[30,40]ms` |
| Near / None-congestion / Fast| node2, node3, node4 | `[10,20]ms` |
| `latency_threshold_ms` | 25 | **Leader-penalty floor for petition.** Petition only if `leaderPenalty > 25`. 25 sits cleanly between near penalty (≤20) and far penalty (≥30): a far leader is "slow enough to replace", a near leader is left alone. Also a **noise floor** — during the 0–5s zero-delay window penalty≈0 < 25, so no spurious petition fires.
| `petition_penalty_margin_ms` | 5 | **Anti-churn hysteresis**: petition only if a candidate is >5ms better than the current leader. The two fastest near nodes differ by only ~1–2ms, within Ping measurement noise; with margin=0 they ping-ponged leadership (observed as near↔near churn). 5ms > the near-pair gap ⇒ leadership stabilizes on the first near node reached. It might lead to the selection of a leader which is "better", but not the best. But the cost is acceptable compared to frequent election messages.|

- `delay_start_after_s = 5` -> Latency is injected into the node after 5 seconds of each run's start to make sure the leadership of initial designated leader. 
- `initial_eligible_leaders = [0]` → node0 (a slow node) is the pinned first leader.


#### 2.2 The four variants

| Variant | netelect | FD mode | What it isolates |
|---|---|---|---|
| `raft` | off | — | Plain-Raft baseline (stuck on the slow far leader) |
| `netelect-raft` | on | `raft` (150–300ms random) | Petition **without** FD rank-bias |
| `netelect-jacobson` | on | Jacobson RTO | Petition + Jacobson adaptive timeout |
| `netelect-CI` | on | CI (95%) | Petition + CI adaptive timeout |

---

### 3. Mechanism

1. **t = 0–5s** — all nodes at zero delay; node0 (pinned) wins term 1 and leads stably.
2. **t = 5s** — tiered WAN delays activate; node0 and 1 becomes slow (`penalty ≈ 35ms`), near nodes are fast (`penalty ≈ 10–20ms`).
3. **NetElect petition (penalty-based trigger)** — a follower petitions when **both**: (a) `leaderPenalty > latency_threshold_ms` (leader slow), and (b) `bestCandidatePenalty < leaderPenalty − petition_penalty_margin_ms` (a clearly better node exists).
4. **Migration & stability** — the proposed (lowest-penalty near) node collects a majority of petitions, forces node0 to step down via `RequestVote{IsPetition}`, and wins the new election. Once on the fastest near node, the margin gate blocks further petitions (other near nodes differ by < 5ms) → **stable**.
5. **Plain raft** — no petition path → node0 stays leader the whole run (slow).

---

### 4. Per-node expectations

| Node | Quality | Expected role |
|---|---|---|
| node0 | slow | Wins term 1 (pinned), then petitioned away at t≈5s; never leads again.|
| node1 | slow | Follower; high penalty → never elected leader.|
| node2/3/4 | fast | The lowest-penalty one becomes the stable post-migration leader.|

**Cluster-level expectation:** exactly one clean migration (node0 → fastest near), then a stable leader; netelect variants beat raft on latency/throughput; petition success (migration) rate ≈ 100%.

---

### 5. Observed behavior

Measured from per-node server logs (`[leader_elected]`, `[petition_*]`, `fault_latency_active`) and CSV/timeseries via `scripts/analyze_sweep.py` (the `wan` dataset, 20 runs).

#### 5.1 Migration reliability (mixed phase, 20 runs)

| Variant | migrate% | near nodes petition? | petition_accept / run | leader_elected / run | RequestVote / run |
|---|---|---|---|---|---|
| raft | **0%** | — (no petition path) | 0.0 | 1.0 | 4 |
| netelect-raft | **100%** | yes (100% of runs) | 3.8 | 2.4 | 18 |
| netelect-jacobson | **100%** | yes (95% of runs) | 2.5 | 2.1 | 12 |
| netelect-CI | **100%** | yes (100% of runs) | 2.8 | 2.2 | 13 |

A follower petitions when **both**: (a) the leader's broadcast `penalty > latency_threshold_ms` (the leader is absolutely slow) and (b) a candidate is `> petition_penalty_margin_ms` better. The proposed near node collects a majority of petitions, forces node0 to step down via `RequestVote{IsPetition}`, and wins the re-election — **100% migration** across all three NetElect variants. Plain Raft has no petition path, so it never migrates (0%) and stays on the pinned slow leader for the whole run.

#### 5.2 Per-node election by quality (20 runs)

The pinned far node0 wins term 1 every run but is **never the final leader** (always migrated away); the fastest near nodes provide the stable post-migration leader; the slowest far node1 is essentially never elected. Representative (`netelect-jacobson`):

```text
node   avg_out(ms)  elected  share%  final_leader×
n3              14        9      21%             9
n2              15        7      17%             7
n4              16        5      12%             4
n0              34       20      48%             0   <- pinned term-1 wins, never final
n1              35        1       2%             0
```

#### 5.3 Throughput & latency (trimmed mean over 20 runs)

| Variant | Read-only QPS | Read p50 | Mixed read QPS | Mixed read p50 | ok% |
|---|---|---|---|---|---|
| raft | 8,064 | 79.3 ms | 7,787 | 71.6 ms | 100 |
| netelect-raft | 22,225 (+176%) | 27.6 ms | 11,186 (+44%) | 47.4 ms | 100 |
| netelect-jacobson | **22,989 (+185%)** | 25.6 ms | **11,419 (+47%)** | 46.4 ms | 100 |
| netelect-CI | 22,091 (+174%) | 27.7 ms | 11,527 (+48%) | 45.9 ms | 100 |

Once leadership sits on a fast near node, both ReadIndex confirmation and write replication shorten: read-only throughput roughly **triples** (+174–185%, p50 −65%) and mixed read throughput rises **+44–48%** (p50 −35%). Plain Raft, stuck on the far leader, stays slow.

---

### 6. Conclusion

With the penalty-based petition trigger plus the anti-churn margin gate, NetElect reliably (**100%**) migrates leadership off the pinned slow far leader to the network-fastest near node and keeps it there, delivering **+174–185% read-only QPS / −65% p50** and **+44–48% mixed read QPS / −35% p50** over plain Raft at 100% success. Petition is the reliable workhorse that guarantees migration across all FD modes; the Jacobson/CI rank bias is secondary, reducing churn and run-to-run variance relative to `netelect-raft`. Plain Raft, lacking any petition path, never migrates and remains on the slow leader.

---

## Leader-Crash Failure-Recovery Experiment

### 1. Goal

This experiment isolates the **rank-biased election-timeout path** (the FD-mode dynamic timeout) from the petition protocol. When the leader *crashes* with petitions disabled, does the re-election land on the network-fastest node purely because the rank-biased timeout makes it fire first? Is recovery faster or more reliable than plain Raft?

Here, leadership moves **only** through crash-triggered re-election, so the result is attributable to the FD timeout, not to petition.

---

### 2. Testbed & Configuration

Same testbed as above (5 CloudLab nodes, gRPC `rpc_timeout=400ms`, heartbeat 50ms, 128 workers/client, 30s/run, 20 runs, trimmed mean). Differences:

| Item | Value |
|---|---|
| Fault | **Recurring forced failover** — every elected leader steps down 7s after winning (`leader_stepdown_after_s=7`). Each 30s run therefore sees ~6 independent crashes, and the read phase keeps crashing too. |
| Latency | **Fixed, distinct** per node (not random): node2=0, node3=5, node4=10 ms (near); node0=20, node1=25 ms (far). Distinct values make the penalty rank order unambiguous. |
| Petition | **Disabled** — `latency_threshold_ms = 10000` (≫ any penalty), so no petition ever fires; leadership moves only via re-election. |
| Timeout leveling | `heartbeat_interval_margin_ms = 150`, `safety_margin_step_size_ms = 30` ⇒ the FD dynamic timeout spans ≈`[150,300)ms`, matching Raft's fixed envelope. Differences are thus attributable to **rank ordering, not timer magnitude**. |
| First leader | `initial_eligible_leaders = [0]` pins a far node as the term-1 leader. |

**Recovery metric definitions (all on the winner's single clock — no NTP / cross-node skew):**
- `detect` = last heartbeat received → became candidate.
- `recovery` = `detect` + (became candidate → won leadership).

Recovery metrics are reported as a **distribution (median / p95 / max / std)**, because a single election-storm run can inflate the mean while the median stays normal.

The four variants are the same as before, but here **`netelect-raft` is the control**: its `fd_mode=raft` has no rank bias and petition is off, so it should behave like plain Raft for leader selection — isolating whether any benefit comes from the FD dynamic timeout (Jacobson/CI) rather than from "NetElect enabled".

---

### 3. Mechanism & Expectations

After each crash, the just-stepped-down leader resets its heartbeat clock and cannot immediately re-campaign; among the remaining followers, whichever has the lowest FD election timeout fires first and wins.

- **raft / netelect-raft** (`fd_mode=raft`): flat random `[150,300)ms`, no rank bias → the winner is essentially the minimum of ~4 independent draws (random among nodes, with a mild edge to the 0 ms node whose votes propagate fastest).
- **jacobson / CI**: rank-biased delay (`margin + base + step·rank`) makes the lowest-penalty (fastest) eligible node time out first → it wins. Because crashes recur, leadership **ping-pongs between the two fastest nodes** (the one that just stepped down yields to the next-fastest), so nodes 2 and 3 split the recoveries while node 4 and the far nodes are locked out.

---

### 4. Observed behavior

#### 4.1 Where recovery lands (recovery events only — initial pinned election excluded)

| Variant | fastest node (n2, 0 ms) win% | 2nd-fastest (n3, 5 ms) win% | two-fastest combined | winNear% (lands on 0/5/10 ms) |
|---|---|---|---|---|
| raft | 34% | 21% | 55% | 76% |
| netelect-raft | 33% | 22% | 56% | 75% |
| **netelect-jacobson** | **56%** | 43% | **99%** | **100%** |
| **netelect-CI** | **54%** | 46% | **100%** | **100%** |

Jacobson/CI put leadership on the **two fastest nodes ~100%** of recoveries, with the far nodes winning **0**. Plain raft and netelect-raft are random: far nodes still win 14–25% of recoveries and winNear is only ~75%.

#### 4.2 Recovery time — median equal, tail differs sharply

| Variant | rec median | rec p95 | **rec max** | rec std | det max | eMsg/run |
|---|---|---|---|---|---|---|
| raft | 193 ms | 240 ms | **7327 ms** | 646 | 7320 ms | 39 |
| netelect-raft | 197 ms | 256 ms | 322 ms | 29 | 322 ms | 36 |
| netelect-jacobson | 198 ms | 229 ms | **243 ms** | 23 | 238 ms | 29 |
| netelect-CI | 191 ms | 220 ms | **228 ms** | 21 | 222 ms | 29 |

Median recovery is ~190 ms for **all four** and the difference is entirely in the **tail**: plain raft suffered a single **7.3-second election storm** (flat random timeouts → simultaneous candidates → split votes → repeated rounds), whereas the staggered FD timeouts cap netelect's worst recovery at **≤243 ms**. Jacobson/CI also cut election traffic ~25% (29 vs 39 RequestVotes/run).

#### 4.3 Throughput & stability (trimmed mean; raw CV from the Stability block)

| Variant | Mixed read QPS | Read-only QPS | Read CV (raw) | Mixed-read CV |
|---|---|---|---|---|
| raft | 13,213 | 31,889 | 22.9% | 7.5% |
| netelect-raft | 12,063 | 35,603 | 17.6% | 6.7% |
| **netelect-jacobson** | **14,062** | **51,796** | **3.5%** | **2.4%** |
| **netelect-CI** | **14,032** | **51,709** | **3.8%** | **1.6%** |

Under continuous failover, Jacobson/CI keep leadership pinned to the two fastest nodes, giving **+62% read throughput** over raft **and ~6× lower run-to-run variance** (read CV 3.5% vs 22.9%). The mixed gain is small (+6–7%): writes are pipeline-bound, and the ~6 brief recovery windows cost every variant similarly.

#### 4.4 The netelect-raft control

`netelect-raft` (fd_mode=raft, petition off) tracks plain raft on selection (fastest-win 33%, winNear 75%) and is in fact the **worst on throughput** (mixed read 12,063; read CV still 17.6%): it carries NetElect's ping/penalty overhead with none of the rank-bias benefit. This confirms the gains come specifically from the **Jacobson/CI dynamic timeout**, not from enabling NetElect.

---

### 5. Conclusion

With election timeouts leveled to Raft's envelope and petitions disabled, the FD rank-biased timeout (Jacobson/CI) steers **every** crash recovery onto the two network-fastest nodes (fastest ~55%, two-fastest ~100%, far nodes 0%) at the **same median recovery latency as plain Raft (~190 ms)**. The win is therefore not a shorter timer but **better target selection plus contention avoidance**: it eliminates Raft's election-storm tail (7.3 s → ≤243 ms), cuts election traffic ~25%, and delivers **+62% read throughput with ~6× lower variance**.
