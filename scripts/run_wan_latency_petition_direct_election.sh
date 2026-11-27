#!/bin/bash
# Usage: ./scripts/run_wan_latency_petition_direct_election.sh
#
# WAN-latency experiment sweep, petition_direct_election variant
# (3 Raft variants × mixed + read).
#
# Identical tiered-delay setup to run_wan_latency_sweep.sh:
#   Far  (nodes 0,1):   outbound ∈ [30,40]ms
#   Near (nodes 2,3,4): outbound ∈ [10,20]ms  (inbound_ping=0 → RTT = follower.outbound)
#   latency_threshold_ms=25, delay_start_after_s=5, initial_eligible_leaders=[0].
#
# The ONE difference: the netelect groups use petition_direct_election=true
# (config/cluster.netelect.*.wan.pde.json). When a petition is accepted, the
# proposed leader (the node that received a majority of petitions) immediately
# starts its OWN election (term++, broadcast RequestVote) right after forcing the
# old leader to step down, instead of forcing stepdown and then waiting for an
# election timeout to decide the winner. This removes the timeout race in which a
# non-receiver node occasionally times out first and wins, so the post-petition
# leader should match the proposed leader far more reliably.
#
# Compare against run_wan_latency_sweep.sh (petition_direct_election=false) on:
# migration success rate, which node actually wins, election-message count.
# The raft baseline group is unchanged (netelect disabled, petition not used).
#
# Server logs (raftd-<id>.log) contain:
#   "fault_latency_active: node=X outbound_delay=Yms inbound_ping_delay=Zms random=true"
#   "[petition_accept] ... direct=true" and "[petition_direct_election] ..." (netelect groups)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/run_wan_latency_petition_direct_election.json"

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
    echo "  Starting all servers (each draws random delay at startup)..."
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
    local cfg="$1"
    local bench_mode="$2"   # mixed | read
    local outdir="$3"
    local label="$4"
    local fdmode="$5"
    local netelect="$6"
    local pde="$7"          # petition_direct_election: true | false

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
    "fault_type": "wan_tiered_delayed",
    "far_nodes": [0, 1],
    "far_outbound_delay_min_ms": 30,
    "far_outbound_delay_max_ms": 40,
    "near_nodes": [2, 3, 4],
    "near_outbound_delay_min_ms": 10,
    "near_outbound_delay_max_ms": 20,
    "inbound_ping_delay_min_ms": 0,
    "inbound_ping_delay_max_ms": 0,
    "delay_start_after_s": 5.0,
    "netelect_enabled": $netelect,
    "fd_mode": "$fdmode",
    "petition_direct_election": $pde
}
EOF
}

# ── experiment groups ──────────────────────────────────────────────────────────
# "label:config:fd_mode:netelect_enabled:petition_direct_election"
SWEEP_GROUPS=(
    "raft-wan-pde:config/cluster.raft.wan.pde.json:none:false:false"
    "netelect-raft-wan-pde:config/cluster.netelect.raft.wan.pde.json:raft:true:true"
    "netelect-jacobson-wan-pde:config/cluster.netelect.jacobson.wan.pde.json:Jacobson:true:true"
)

# ── main sweep ─────────────────────────────────────────────────────────────────
for group in "${SWEEP_GROUPS[@]}"; do
    LABEL=$(echo "$group"    | cut -d: -f1)
    CONFIG=$(echo "$group"   | cut -d: -f2)
    FDMODE=$(echo "$group"   | cut -d: -f3)
    NETELECT=$(echo "$group" | cut -d: -f4)
    PDE=$(echo "$group"      | cut -d: -f5)
    RESULTS_BASE="$REPO_DIR/results/$LABEL"

    echo ""
    echo "════════════════════════════════════════════════════"
    echo "  Group  : $LABEL"
    echo "  Config : $CONFIG"
    echo "  FDMode : $FDMODE  |  netelect=$NETELECT  |  petition_direct_election=$PDE"
    echo "  Latency: far(0,1) out=[30,40]ms  near(2,3,4) out=[10,20]ms in=0  threshold=25ms  delay_start=5s"
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
        run_clients "$CONFIG" "mixed" "$RESULTS_BASE/mixed/$RUN" "$LABEL" "$FDMODE" "$NETELECT" "$PDE"
        PREV_OUTDIR="$RESULTS_BASE/mixed/$RUN"

        if (( i < MIXED_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    echo ""
    echo "--- Read-Only phase ($READ_RUNS runs, no server restart) ---"
    echo "  (servers stay up with the same random delays as last mixed run)"
    echo ""

    LAST_READ_OUTDIR=""
    for ((i = 1; i <= READ_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/read/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [read $i/$READ_RUNS  →  run dir $RUN]"

        run_clients "$CONFIG" "read" "$RESULTS_BASE/read/$RUN" "$LABEL" "$FDMODE" "$NETELECT" "$PDE"
        LAST_READ_OUTDIR="$RESULTS_BASE/read/$RUN"

        if (( i < READ_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    # Servers ran continuously through the last mixed run + all read runs, so the
    # cumulative raftd-<id>.log covers both. Collect it into the last mixed dir
    # (existing) AND the last read dir (so read-phase analysis has server logs).
    stop_all_servers
    collect_server_logs "$PREV_OUTDIR"
    [ -n "$LAST_READ_OUTDIR" ] && collect_server_logs "$LAST_READ_OUTDIR"

    echo ""
    echo "  Group '$LABEL' complete. Results in $RESULTS_BASE/"
done

echo ""
echo "WAN-latency petition_direct_election sweep complete."
