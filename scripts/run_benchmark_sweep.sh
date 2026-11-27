#!/bin/bash
# Usage: ./scripts/run_benchmark_sweep.sh
#
# Fault-free baseline benchmark sweep (4 variants × mixed + read).
# Re-validates the implementation after the netelect/raft changes
# (WithNetElect honoring `enabled`, penalty-based petition trigger,
# penalty-margin gate, event-driven [leader_elected] logging).
#
# No latency is injected — all nodes are equal, so netelect should neither
# migrate nor petition. The point is to measure the steady-state OVERHEAD of
# the netelect machinery (ping/penalty/petition loops) relative to plain raft,
# and confirm the changes did not regress base throughput/latency.
#
# Structure mirrors run_wan_latency_sweep.sh (per-group: 20× mixed with server
# restart each, then 20× read reusing servers), but uses the fault-free base
# configs and writes to results/<label>/{mixed,read}/<run>/.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/run_benchmark_sweep.json"

command -v jq >/dev/null 2>&1 || { echo "error: jq required (brew install jq)"; exit 1; }

USER=$(jq -r         '.user'                          "$JSON")
CSV=$(jq -r          '.csv            // "results.csv"'    "$JSON")
TS_CSV=$(jq -r       '.timeseries_csv // "timeseries.csv"' "$JSON")
TS_INTERVAL=$(jq -r  '.timeseries_interval // "1s"'    "$JSON")
NB_SERVERS=$(jq -r   '.nb_servers'                    "$JSON")
READ_RATIO=$(jq -r   '.read_ratio     // 0.8'         "$JSON")
MIXED_RUNS=$(jq -r   '.mixed_runs     // 20'          "$JSON")
READ_RUNS=$(jq -r    '.read_runs      // 20'          "$JSON")
STARTUP_SETTLE=$(jq -r '.startup_settle_s // 5'       "$JSON")
RUN_SETTLE=$(jq -r   '.run_settle_s   // 2'           "$JSON")

IPS=()
while IFS= read -r ip; do
    IPS+=("$ip")
done < <(jq -r '.ips[]' "$JSON")

NB_CLIENTS=${#IPS[@]}

# ── helpers ──────────────────────────────────────────────────────────────────

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
    sleep 2
}

collect_server_logs() {
    local outdir="$1"
    local _idx
    mkdir -p "$outdir"
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
    echo "  Starting all servers..."
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
}

run_clients() {
    local cfg="$1" bench_mode="$2" outdir="$3" label="$4" fdmode="$5" netelect="$6"
    mkdir -p "$outdir"

    local cmd
    cmd="rm -f ~/raft/$CSV ~/raft/$TS_CSV && cd ~/raft && \
         ./bin/client -config $cfg -mode $bench_mode -csv $CSV \
         -timeseries-csv $TS_CSV -timeseries-interval $TS_INTERVAL"
    [ "$bench_mode" = "mixed" ] && cmd="$cmd -read-ratio $READ_RATIO"

    echo "  Running clients (mode=$bench_mode)..."
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
    "fault_type": "baseline",
    "netelect_enabled": $netelect,
    "fd_mode": "$fdmode"
}
EOF
}

# ── experiment groups (fault-free base configs) ──────────────────────────────
# "label:config:fd_mode:netelect_enabled"
SWEEP_GROUPS=(
    "raft:config/cluster.raft.json:none:false"
    "netelect-raft:config/cluster.netelect.raft.json:raft:true"
    "netelect-jacobson:config/cluster.netelect.jacobson.json:Jacobson:true"
)

# ── main sweep ───────────────────────────────────────────────────────────────
for group in "${SWEEP_GROUPS[@]}"; do
    LABEL=$(echo "$group"    | cut -d: -f1)
    CONFIG=$(echo "$group"   | cut -d: -f2)
    FDMODE=$(echo "$group"   | cut -d: -f3)
    NETELECT=$(echo "$group" | cut -d: -f4)
    RESULTS_BASE="$REPO_DIR/results/$LABEL"

    echo ""
    echo "════════════════════════════════════════════════════"
    echo "  Group  : $LABEL  (fault-free baseline)"
    echo "  Config : $CONFIG  |  FDMode=$FDMODE netelect=$NETELECT"
    echo "════════════════════════════════════════════════════"

    sync_config "$CONFIG"

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
        if (( i < MIXED_RUNS )) && (( RUN_SETTLE > 0 )); then sleep "$RUN_SETTLE"; fi
    done

    # ── Read-Only phase (no restart) ──────────────────────────────────────────
    echo ""
    echo "--- Read-Only phase ($READ_RUNS runs, no server restart) ---"
    LAST_READ_OUTDIR=""
    for ((i = 1; i <= READ_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/read/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [read $i/$READ_RUNS  →  run dir $RUN]"
        run_clients "$CONFIG" "read" "$RESULTS_BASE/read/$RUN" "$LABEL" "$FDMODE" "$NETELECT"
        LAST_READ_OUTDIR="$RESULTS_BASE/read/$RUN"
        if (( i < READ_RUNS )) && (( RUN_SETTLE > 0 )); then sleep "$RUN_SETTLE"; fi
    done

    stop_all_servers
    collect_server_logs "$PREV_OUTDIR"
    [ -n "$LAST_READ_OUTDIR" ] && collect_server_logs "$LAST_READ_OUTDIR"

    echo ""
    echo "  Group '$LABEL' complete. Results in $RESULTS_BASE/"
done

echo ""
echo "Baseline benchmark sweep complete."
