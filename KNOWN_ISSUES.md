# Known Issues and Planned Work

This file lists everything known to be wrong, imprecise or out of date in this repository, and the work planned to address it. It is a working document for the authors.

**Scope.** The code in this repository is the code that produced the data in `results/`. Items marked *re-run* change protocol or client-visible behaviour: once any of them is fixed, all experiments must be re-run before the numbers in `EXPERIMENT.md`, `results/report.md` or any paper draft can be reused.

Line numbers refer to the code as committed alongside this file.

## Summary

| ID | Area | Issue | Severity | Re-run |
|---|---|---|---|---|
| [C1](#c1-self-petitions-count-toward-the-petition-quorum) | NetElect | Self-petitions count toward the petition quorum | High | yes |
| [C2](#c2-reads-are-not-linearizable-after-a-leader-change) | Raft reads | Reads are not linearizable after a leader change | High | yes |
| [C3](#c3-write-completion-is-matched-by-log-index-only) | Raft writes | Write completion is matched by log index only | Medium | yes |
| [C4](#c4-petition-majority-is-wrong-for-even-cluster-sizes) | NetElect | Petition majority is wrong for even cluster sizes | Low | no (N = 5) |
| [C5](#c5-petition-stepdown-does-not-reset-the-election-timer) | NetElect | Petition stepdown does not reset the election timer | Low | yes |
| [H1](#h1-server-logs-are-not-captured-per-run) | Harness | Server logs are not captured per run | Medium | n/a |
| [H2](#h2-exp_infojson-is-incomplete-and-partly-stale) | Harness | `exp_info.json` is incomplete and partly stale | Medium | n/a |
| [H3](#h3-legacy-run_sweepsh-is-broken) | Harness | Legacy `run_sweep.sh` is broken | Low | n/a |
| [H4](#h4-restart_all_serversjson-points-to-a-missing-config) | Harness | `restart_all_servers.json` points to a missing config | Low | n/a |
| [H5](#h5-stale-script-comments) | Harness | Stale script comments | Low | n/a |
| [H6](#h6-hostnames-are-hard-coded-per-allocation) | Harness | Hostnames are hard-coded per allocation | Low | n/a |
| [D1](#d1-experimentmd-wan-section-uses-an-earlier-batch) | Docs | `EXPERIMENT.md` WAN section uses an earlier batch | High | n/a |
| [D2](#d2-experimentmd-baseline-and-failure-sections-include-the-ci-variant) | Docs | `EXPERIMENT.md` baseline/failure include the CI variant | Low | n/a |
| [D3](#d3-designmd) | Docs | `DESIGN.md` claims affected by C1/C2; stale operations sections | Medium | n/a |
| [D4](#d4-readmemd) | Docs | `README.md` claims affected by C1/C2 | Medium | n/a |
| [M1](#m1-results-are-sensitive-to-the-cloudlab-allocation) | Method | Results are sensitive to the CloudLab allocation | High | yes |
| [M2](#m2-experiments-span-two-allocations-and-several-code-states) | Method | Experiments span two allocations and several code states | Medium | yes |
| [M3](#m3-ci-variant-status) | Method | CI variant status | Low | — |

---

## Code (protocol and correctness)

### C1. Self-petitions count toward the petition quorum

**What the code does.**

- In `broadcastPenaltyAndCheckPetition` (`server/netelect.go:300`), a follower picks the proposed leader as the lowest-penalty node excluding the current leader (`netelect.go:329-340`). The candidate set is `penaltyScores`, which includes the follower's own score (`netelect.go:318`). The best follower therefore proposes itself.
- `sendPetition` (`netelect.go:396`) sends the petition to that node. Every node holds a transport to itself (`cmd/raftd/main.go:91-107`), so the self-petition is delivered.
- The `Petition` handler (`netelect.go:418`) does not reject `SenderId == rf.me`.
- Acceptance requires `majorityCount - 1` petitioners (`netelect.go:445`); the comment there says the proposer counts itself. The self-petition then counts the proposer a second time.
- The RTT check on petitioners skips the self entry, because there is no RTT sample to self (`netelect.go:455-457`).

**Impact.** In a 5-node cluster, `needed = 2`, so a petition can be accepted with the proposer's own petition plus **one** other follower: 2 of 5 nodes, not a majority. This is the common case in the data (mixed-phase logs of the analysed runs):

| Group (runs) | `petition_recv` | self-petitions | `petition_accept` events where the proposer's own petition had been received that term |
|---|---|---|---|
| netelect-raft-wan (53–72) | 117 | 28 | 63 of 81 |
| netelect-jacobson-wan (53–72) | 72 | 17 | 39 of 50 |
| netelect-raft-wan-pde (1–20) | 54 | 9 | 8 of 18 |
| netelect-jacobson-wan-pde (1–20) | 67 | 20 | 21 of 30 |

The documentation (README, DESIGN.md) describes acceptance as "a majority of followers have petitioned", which does not match this behaviour.

**Proposed fix.** Decide the intended semantics first (compare with frugal-leader-election):

- (a) A follower never proposes itself: exclude `rf.me` from the candidate search; or
- (b) Self-proposal is allowed, but the proposer's implicit vote is its only self-contribution: ignore `SenderId == rf.me` in `Petition`.

Either way, require `majorityCount - 1` *distinct other* followers.

**Tests to add.** 5-node mock cluster: with only one other follower petitioning, the proposer must not accept; with two others, it must.

### C2. Reads are not linearizable after a leader change

**What the code does.** `ClientRead` (`server/raft.go:319`) queues the request, and `batchReadIndexLoop` (`raft.go:400`) runs one `confirmLeadership` round (`raft.go:436`) per batch. On success the handler reads `kvStore` directly (`raft.go:345-350`). There is no read index:

- `commitIndex` is not recorded when the read arrives;
- the read does not wait for `lastApplied` to reach it;
- a new leader does not append a no-op entry on election (`raft.go:538-554`).

Because only current-term entries can be committed (`maybeAdvanceCommitIndex`, `raft.go:683`), a newly elected leader's `commitIndex` (and `lastApplied`) can lag entries the previous leader had already committed and acknowledged.

**Impact.** Right after a leader change, a read can return a value older than a write that was already acknowledged to a client. In a read-only phase there are no new writes, so the lag can persist for the whole phase. The README and DESIGN.md call these reads "linearizable" / "ReadIndex".

**Proposed fix.** Standard ReadIndex:

1. On becoming leader, append a no-op entry for the new term.
2. Serve reads only after the leader has committed an entry in its current term.
3. For each batch, record `readIndex = commitIndex`, confirm leadership, wait until `lastApplied >= readIndex`, then read.

Expected cost: negligible in steady state (`lastApplied` normally equals `commitIndex`), a short delay right after elections.

**Tests to add.** Write a value, force a leader change, and check that a read at the new leader returns the written value.

### C3. Write completion is matched by log index only

**What the code does.** `ClientWrite` (`raft.go:361`) appends the command at index `i` and waits on `pendingWrites[i]` (`raft.go:382`). `applyLoop` (`raft.go:653`) signals `pendingWrites[lastApplied]` (`raft.go:672-674`) whatever command is actually at that index. Waiters are not failed when the node steps down; they only time out after `ElectionTimeoutMax` (300 ms, `raft.go:388-395`).

**Impact.** If a leader appends command A at index `i`, loses leadership before A is replicated, and the new leader commits a different command B at index `i`, the old leader applies B and reports A as successful. The window is bounded by the 300 ms timeout but is reachable during failover.

**Proposed fix.** Store the term with each waiter and complete it only if `log[i].Term` matches (otherwise fail it), and fail all pending writes on stepdown. Optionally carry `CommandId` in `KVCommand` so completion can be matched by ID.

**Tests to add.** Partition the leader right after it appends a write, let a new leader commit a different write at the same index, heal the partition, and check that the first write is reported as failed.

### C4. Petition majority is wrong for even cluster sizes

`majorityCount = (len(rf.peers) + 1) / 2` (`netelect.go:152`) is a majority only for odd N: it gives 2 for N = 4 (the majority is 3). Raft's own vote counting and commit rule use `> len(peers)/2` and are correct (`raft.go:540`, `raft.go:694`). The experiments use N = 5 and are unaffected.

**Fix.** `len(rf.peers)/2 + 1`.

### C5. Petition stepdown does not reset the election timer

**What the code does.** A leader forced down by `RequestVote{IsPetition: true}` sets `state = Follower` and `leaderId = -1` but does not reset `lastHeartbeat` (`raft.go:193-202`). `StepDown()` does reset it (`raft.go:176`).

**Impact.**

1. **Metric artifact.** When the deposed leader later starts an election, its `[election_start] … detect_ms` measures the time since its own election (observed values 4.8–10.5 s) rather than a failure-detection delay. This does not affect the failure experiment, which uses `StepDown()`, but it makes `detect_ms` unusable in the WAN logs.
2. **Design question.** With `petition_direct_election = false` and `fd_mode = raft`, the deposed (slow) leader races in the re-election with the same uniform timeout as everyone else and can win back leadership. In netelect-raft-wan runs 53–72 it started a campaign for the next term after 10 of 27 petition stepdowns, and node 0 regained leadership twice. With Jacobson this happened 0 of 17 times, since the slow node's rank-based delay is the largest.

**Proposed fix.** Reset `lastHeartbeat` on petition stepdown. Consider having the deposed leader skip one election (for example, not campaign until it hears from a new leader or one full timeout passes) when `petition_direct_election = false`.

---

## Experiment harness

### H1. Server logs are not captured per run

`run_wan_latency_sweep.sh`, `run_wan_latency_petition_direct_election.sh` and `run_benchmark_sweep.sh` restart the servers before each mixed run and collect the previous run's `raftd-<id>.log` at that point. After the last mixed run, the servers keep running through all read runs, and the cumulative log is copied into the last mixed run's directory and the last read run's directory (`run_wan_latency_sweep.sh:202-207, 239-244`).

Consequences:

- The last mixed run of every group (for example run 72 of `netelect-raft-wan`) has a log that also covers the read phase, so its leadership, petition and election-message counts can be inflated (1 of 20 runs per group).
- Read runs other than the last have no server logs at all.

`run_failure_sweep.sh` already snapshots logs per run.

**Fix.** Use the same per-run snapshot in all sweeps.

### H2. `exp_info.json` is incomplete and partly stale

- `run_failure_sweep.sh` writes hard-coded `"stepdown_after_s": 15` and `"benchmark_failure_at_s": 10`. The configs actually use a recurring `leader_stepdown_after_s = 7`.
- No sweep records the NetElect thresholds and margins, `rpc_timeout`, the per-node fault settings actually drawn, or the code version.

**Fix.** Copy the exact cluster config into each run directory and record the git commit hash and the actual random delay draws.

### H3. Legacy `run_sweep.sh` is broken

- It starts `raftd` without `cd ~/raft` (`run_sweep.sh:64-65`), so `./bin/raftd` and the config path do not resolve under the documented node layout.
- It saves server logs as `<host>.server.log`, which `analyze_sweep.py` does not parse.

**Fix.** Fix it or remove it in favour of `run_benchmark_sweep.sh`.

### H4. `restart_all_servers.json` points to a missing config

It references `config/cluster.grpc.json`, which is not in the repository. **Fix:** point it at an existing `config/cluster.*.json`, or add the file.

### H5. Stale script comments

- `run_wan_latency_sweep.sh` (header) and the `_comment` in `run_wan_latency_sweep.json` say `petition_penalty_margin_ms` is 0 and describe the old trigger (a follower's RTT to the leader). The configs use margin 5, and the trigger is the leader's broadcast penalty.
- `run_failure_sweep.sh` (header) still describes `stepdown_after_s = 15` and four variants.
- The `_comment` in `run_failure_sweep.json` says about 4 crashes per run; the measured value is 6.0.

### H6. Hostnames are hard-coded per allocation

Each new CloudLab allocation requires editing the node hostnames in every `config/cluster.*.json` (15 files) and in the `scripts/*.json` settings files.

**Fix.** Generate the configs from one hosts file before each sweep.

---

## Documentation

### D1. `EXPERIMENT.md` WAN section uses an earlier batch

The WAN section reports runs 33–52: four variants including CI, run on allocation A (see M2). `results/report.md`, `scripts/analyze_sweep.json` and the paper draft use runs 53–72: three variants, allocation B. Both batches use the same protocol logic and the same configuration (same delay tiers, threshold 25 ms, margin 5 ms; the penalty-based trigger is visible in both), but the numbers differ (see M1). The `petition_direct_election` comparison (`wan-pde`) is not written up at all.

**Fix.** After the re-run, regenerate `results/report.md` and rewrite the WAN section and a new WAN-pde section from it.

### D2. `EXPERIMENT.md` baseline and failure sections include the CI variant

Both sections include `netelect-CI` rows. The data exists in `results/netelect-CI*`, but the current analysis configuration and report do not include CI. Also:

- The baseline table marks `70,609*` with no footnote.
- The baseline conclusion still says "all four variants".

The other numbers in these two sections match `results/report.md`.

### D3. `DESIGN.md`

- **Affected by C1 and C2:** the petition acceptance rule (§4.6) and the linearizable-read claim (§1.1, §3–§4).
- **Operations sections (§9–§10) out of date:**
  - Log collection (H1) and `exp_info.json` contents (H2).
  - Run numbering: sweeps append after the highest existing run, not 1–20.
  - `run_sweep.sh` is still presented as the recommended script (H3).
  - Hostnames from an older allocation; `cluster.grpc.json` is not in the repository (H4).
  - The §10.1 timeout formulas omit `heartbeat_interval_margin_ms`.
  - The failure section says the first leader is pinned for all variants, but `cluster.raft.failure.json` has no `netelect` block, so plain Raft is unpinned.
  - Omissions: the `benchmark.addrs` key, the WAN overrides `rpc_timeout = 400ms` and `heartbeat_interval_margin_ms = 80`, the analyzer's `LE/run` column and Stability block, and the fact that the petition and failure tables use only mixed-phase logs.

### D4. `README.md`

The "Linearizable reads" feature (C2) and the "majority of followers" acceptance rule (C1) describe intended rather than actual behaviour.

---

## Methodology (for the re-run)

### M1. Results are sensitive to the CloudLab allocation

The two WAN batches ran the same protocol logic and configuration on different machines. The difference between batches is far larger than the run-to-run spread within a batch:

| WAN read-only QPS (trimmed mean) | runs 33–52 (allocation A) | runs 53–72 (allocation B) | within-batch std (B) |
|---|---|---|---|
| raft | 8,064 | 8,273 | ±5 |
| netelect-raft | 22,225 | 19,411 | ±68 |
| netelect-jacobson | 22,989 | 25,409 | ±147 |

The qualitative results reproduce across both batches: 100% migration, and both NetElect variants far ahead of Raft. The gap between NetElect-Jacobson and NetElect-Raft does not: +3% on A, +31% on B.

**Plan.** Run each sweep on at least two allocations and report results per allocation, or pool them with between-allocation variance shown.

### M2. Experiments span two allocations and several code states

| Data | Dates | Allocation |
|---|---|---|
| Baseline (`raft`, `netelect-*`) runs 1–20 | 2026-06-08 | A: clnode005, clnode007, clnode013, clstore03, clstore04 |
| Failure (`*-failure`) runs 1–20 | 2026-06-08 | A |
| WAN runs 33–52 | 2026-06-02/03 | A |
| WAN runs 53–72, WAN-pde runs 1–20 | 2026-06-22/23 | B: clnode002, clnode004, clnode005, clnode008, clnode016 |

Code changes between these dates either do not affect a given experiment or are disabled in it (for example `petition_direct_election = false`, recurring stepdown off in WAN), but no run records the exact code version.

**Plan.** After the fixes, tag one commit, record its hash in every run (H2), and run all sweeps from that commit on the same allocation(s).

### M3. CI variant status

`config/cluster.netelect.ci.*.json` are kept but not swept (`SWEEP_GROUPS` in the scripts lists three groups). Decide whether CI is part of the evaluation. If not, remove its configs and the CI rows in `EXPERIMENT.md`; if so, re-enable it in the sweeps and in `analyze_sweep.json`.

---

## Suggested order of work

1. Fix C1–C5 and add the tests listed above.
2. Fix the harness (H1–H6), in particular per-run log capture and complete run metadata.
3. Tag the commit; run all four sweeps (baseline, WAN, WAN-pde, failure) on one allocation, then repeat on a second allocation (M1, M2).
4. Update `scripts/analyze_sweep.json` run ranges, regenerate `results/report.md` and the plots.
5. Rewrite `EXPERIMENT.md` from the new report (D1, D2), and update `DESIGN.md` and `README.md` (D3, D4).
