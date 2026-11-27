#!/bin/bash
# Usage: ./scripts/run_failure_sweep.sh
#
# Failure-injection experiment sweep (4 Raft variants × mixed + read).
#
# All 5 nodes carry stepdown_after_s=15.  With startup_settle_s=5 the
# leader steps down at ≈ benchmark t=10s (server t=15s).  Only the current
# leader actually steps down; the other nodes' timers fire and return false.
#
# Per-second time-series CSVs capture:
#   0–10s   steady-state baseline traffic
#   ~10s    leader step-down  (not_leader / timeout spike in time-series)
#   ~10.3s  new leader elected (QPS recovers)
#   10–30s  post-failure steady state
#
# Server logs (raftd-<id>.log) contain:
#   "fault_stepdown: node=X stepped=true/false"  at the event
#   "election_msgs: node=X sent=Y recv=Z"         at SIGTERM shutdown

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/run_failure_sweep.json"

command -v jq >/dev/null 2>&1 || { echo "error: jq required (brew install jq)"; exit 1; }

USER=$(jq -r         '.user'                          "$JSON")
CSV=$(jq -r          '.csv            // "results.csv"'    "$JSON")
TS_CSV=$(jq -r       '.timeseries_csv // "timeseries.csv"' "$JSON")
TS_INTERVAL=$(jq -r  '.timeseries_interval // "1s"'    "$JSON")
NB_SERVERS=$(jq -r   '.nb_servers'                    "$JSON")
READ_RATIO=$(jq -r   '.read_ratio     // 0.8'         "$JSON")
MIXED_RUNS=$(jq -r   '.mixed_runs     // 10'          "$JSON")
READ_RUNS=$(jq -r    '.read_runs      // 10'          "$JSON")
STARTUP_SETTLE=$(jq -r '.startup_settle_s // 5'       "$JSON")
RUN_SETTLE=$(jq -r   '.run_settle_s   // 2'           "$JSON")

IPS=()
while IFS= read -r ip; do
    IPS+=("$ip")
done < <(jq -r '.ips[]' "$JSON")

NB_CLIENTS=${#IPS[@]}

# ── helpers ────────────────────────────────────────────────────────────────────

sync_config() {
    local cfg="$1"
    echo "  Syncing $cfg to all nodes..."
    for ip in "${IPS[@]}"; do
        (
            ssh -o ConnectTimeout=5 "${USER}@${ip}" "mkdir -p ~/raft/config ~/raft/logs"
            scp -o ConnectTimeout=5 "$REPO_DIR/$cfg" "${USER}@${ip}:~/raft/$cfg" >/dev/null \
                && echo "    [$ip] config ok" \
                || echo "    [$ip] FAILED to sync config"
        ) &
    done
    wait
}

stop_all_servers() {
    echo "  Stopping all servers..."
    for ip in "${IPS[@]}"; do
        ssh -o ConnectTimeout=5 "${USER}@${ip}" "pkill -TERM raftd; true" &
    done
    wait
    # Give servers time to flush shutdown logs (election_msgs, fault_stepdown lines).
    sleep 2
}

collect_server_logs() {
    local outdir="$1"
    local _idx
    echo "  Collecting server logs → $outdir"
    for _idx in "${!IPS[@]}"; do
        local ip="${IPS[$_idx]}"
        (
            scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/logs/raftd-${_idx}.log" \
                "$outdir/raftd-${_idx}.log" >/dev/null \
                && echo "    [id=$_idx $ip] log ok" \
                || echo "    [id=$_idx $ip] FAILED to collect log"
        ) &
    done
    wait
}

start_all_servers() {
    local cfg="$1"
    local _idx
    echo "  Starting all servers (stepdown timer starts now)..."
    for _idx in "${!IPS[@]}"; do
        local ip="${IPS[$_idx]}"
        (
            ssh -o ConnectTimeout=5 "${USER}@${ip}" \
                "cd ~/raft && setsid -f ./bin/raftd -config $cfg -id $_idx \
                 >~/raft/logs/raftd-${_idx}.log 2>&1 </dev/null"
            echo "    [id=$_idx $ip] started"
        ) &
    done
    wait
    echo "  Waiting ${STARTUP_SETTLE}s for leader election..."
    sleep "$STARTUP_SETTLE"
    # stepdown_after_s=15; benchmark starts now (server t=5s), failure fires at server t=15s = benchmark t=10s.
}

run_clients() {
    local cfg="$1"
    local bench_mode="$2"   # mixed | read
    local outdir="$3"
    local label="$4"
    local fdmode="$5"
    local netelect="$6"

    mkdir -p "$outdir"

    local cmd
    cmd="rm -f ~/raft/$CSV ~/raft/$TS_CSV && cd ~/raft && \
         ./bin/client -config $cfg -mode $bench_mode -csv $CSV \
         -timeseries-csv $TS_CSV -timeseries-interval $TS_INTERVAL"
    [ "$bench_mode" = "mixed" ] && cmd="$cmd -read-ratio $READ_RATIO"

    echo "  Running clients (mode=$bench_mode, failure fires at benchmark t≈10s)..."
    for ip in "${IPS[@]}"; do
        (
            echo "  ===== $ip ====="
            ssh -o ConnectTimeout=5 "${USER}@${ip}" "$cmd"
        ) &
    done
    wait

    echo "  Collecting results → $outdir"
    for ip in "${IPS[@]}"; do
        (
            local host="${ip%%.*}"
            scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/${CSV}"    "$outdir/${host}.csv"    >/dev/null &
            scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/${TS_CSV}" "$outdir/${host}.ts.csv" >/dev/null &
            wait
        ) &
    done
    wait

    cat > "$outdir/exp_info.json" <<EOF
{
    "nb_clients": $NB_CLIENTS,
    "nb_servers": $NB_SERVERS,
    "label": "$label",
    "mode": "$bench_mode",
    "read_ratio": $READ_RATIO,
    "fault_type": "failure",
    "stepdown_after_s": 15,
    "benchmark_failure_at_s": 10,
    "netelect_enabled": $netelect,
    "fd_mode": "$fdmode"
}
EOF
}

# ── experiment groups ──────────────────────────────────────────────────────────
SWEEP_GROUPS=(
    "raft-failure:config/cluster.raft.failure.json:none:false"
    "netelect-raft-failure:config/cluster.netelect.raft.failure.json:raft:true"
    "netelect-jacobson-failure:config/cluster.netelect.jacobson.failure.json:Jacobson:true"
)

# ── main sweep ─────────────────────────────────────────────────────────────────
for group in "${SWEEP_GROUPS[@]}"; do
    LABEL=$(echo "$group"    | cut -d: -f1)
    CONFIG=$(echo "$group"   | cut -d: -f2)
    FDMODE=$(echo "$group"   | cut -d: -f3)
    NETELECT=$(echo "$group" | cut -d: -f4)
    RESULTS_BASE="$REPO_DIR/results/$LABEL"

    echo ""
    echo "════════════════════════════════════════════════════"
    echo "  Group  : $LABEL"
    echo "  Config : $CONFIG"
    echo "  FDMode : $FDMODE  |  netelect=$NETELECT"
    echo "  Timing : leader steps down at benchmark t≈10s (server t=15s)"
    echo "════════════════════════════════════════════════════"
    echo ""

    sync_config "$CONFIG"
    echo ""

    # ── Mixed phase ───────────────────────────────────────────────────────────
    echo "--- Mixed phase ($MIXED_RUNS runs) ---"
    PREV_OUTDIR=""
    for ((i = 1; i <= MIXED_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/mixed/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [mixed $i/$MIXED_RUNS  →  run dir $RUN]"

        if [ -n "$PREV_OUTDIR" ]; then
            stop_all_servers
            collect_server_logs "$PREV_OUTDIR"
        else
            stop_all_servers
        fi
        start_all_servers "$CONFIG"
        run_clients "$CONFIG" "mixed" "$RESULTS_BASE/mixed/$RUN" "$LABEL" "$FDMODE" "$NETELECT"
        PREV_OUTDIR="$RESULTS_BASE/mixed/$RUN"

        if (( i < MIXED_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    # Snapshot the last mixed run's logs NOW — before the read phase runs. The
    # servers stay up (read reuses the warm KV state written during mixed), but
    # collect_server_logs only scp's the current append-only log, no stop needed.
    # Taking it here keeps mixed/<last> uncontaminated by the read phase.
    collect_server_logs "$PREV_OUTDIR"

    # ── Read phase ────────────────────────────────────────────────────────────
    # Servers are NOT restarted, so the KV state written during the mixed phase is
    # reused as a warm cache. With recurring leader_stepdown_after_s, the read phase
    # still experiences leader crashes throughout (failover under a read workload).
    # Each read run's log is snapshotted live; note the snapshot is cumulative from
    # the last mixed run (servers never restarted), so slice by the ts= fields if a
    # per-read-run window is needed.
    echo ""
    echo "--- Read-Only phase ($READ_RUNS runs, no restart — warm KV reused) ---"
    echo ""

    for ((i = 1; i <= READ_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/read/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [read $i/$READ_RUNS  →  run dir $RUN]"

        run_clients "$CONFIG" "read" "$RESULTS_BASE/read/$RUN" "$LABEL" "$FDMODE" "$NETELECT"
        collect_server_logs "$RESULTS_BASE/read/$RUN"   # live snapshot (no stop)

        if (( i < READ_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    # mixed/<last> and every read/<n> log were already snapshotted above, so just
    # stop the servers here (do NOT re-collect mixed/<last> — that would re-pollute it).
    stop_all_servers

    echo ""
    echo "  Group '$LABEL' complete. Results in $RESULTS_BASE/"
done

echo ""
echo "Failure sweep complete."
