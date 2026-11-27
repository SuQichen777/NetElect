#!/bin/bash
# Usage: ./scripts/run_sweep.sh
#
# Full experiment sweep.
# For each config (raft / netelect-raft / netelect-jacobson):
#   1. Sync config to all nodes
#   2. MIXED_RUNS × Mixed  : stop servers → restart servers → run clients → collect
#   3. READ_RUNS  × Read   : run clients → collect  (no restart; reuses KV state)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/run_sweep.json"

command -v jq >/dev/null 2>&1 || { echo "error: jq required (brew install jq)"; exit 1; }

USER=$(jq -r        '.user'                    "$JSON")
CSV=$(jq -r         '.csv          // "results.csv"' "$JSON")
NB_SERVERS=$(jq -r  '.nb_servers'              "$JSON")
READ_RATIO=$(jq -r  '.read_ratio   // 0.8'     "$JSON")
MIXED_RUNS=$(jq -r  '.mixed_runs   // 20'      "$JSON")
READ_RUNS=$(jq -r   '.read_runs    // 20'      "$JSON")
STARTUP_SETTLE=$(jq -r '.startup_settle_s // 5' "$JSON")
RUN_SETTLE=$(jq -r  '.run_settle_s // 0'       "$JSON")
SCP_TO=$(jq -r      '.scp_results_to // ""'    "$JSON")

IPS=()
while IFS= read -r ip; do
    IPS+=("$ip")
done < <(jq -r '.ips[]' "$JSON")

NB_CLIENTS=${#IPS[@]}

# ── helpers ────────────────────────────────────────────────────────────────────

sync_config() {
    _SC_CFG="$1"
    echo "  Syncing $_SC_CFG to all nodes..."
    for ip in "${IPS[@]}"; do
        (
            ssh -o ConnectTimeout=5 "${USER}@${ip}" "mkdir -p ~/raft/config"
            scp -o ConnectTimeout=5 "$REPO_DIR/$_SC_CFG" "${USER}@${ip}:~/raft/$_SC_CFG" >/dev/null \
                && echo "    [$ip] config ok" \
                || echo "    [$ip] FAILED to sync config"
        ) &
    done
    wait
}

stop_all_servers() {
    echo "  Stopping all servers..."
    for ip in "${IPS[@]}"; do
        ssh -o ConnectTimeout=5 "${USER}@${ip}" "pkill raftd; true" &
    done
    wait
}

start_all_servers() {
    _SA_CFG="$1"
    echo "  Starting all servers..."
    for _sa_i in "${!IPS[@]}"; do
        _SA_IP="${IPS[$_sa_i]}"
        _SA_ID=$_sa_i
        (
            ssh -o ConnectTimeout=5 "${USER}@${_SA_IP}" \
                "mkdir -p ~/raft/logs && setsid -f ./bin/raftd -config $_SA_CFG -id $_SA_ID >~/raft/logs/raftd.log 2>&1 </dev/null"
            echo "    [id=$_SA_ID $_SA_IP] started"
        ) &
    done
    wait
    echo "  Waiting ${STARTUP_SETTLE}s for leader election..."
    sleep "$STARTUP_SETTLE"
}

run_clients() {
    _RC_CFG="$1"
    _RC_MODE="$2"       # read | mixed
    _RC_LABEL="$3"
    _RC_FDMODE="$4"
    _RC_NETELECT="$5"   # true | false
    _RC_OUTDIR="$6"

    mkdir -p "$_RC_OUTDIR"

    _RC_CMD="rm -f ~/raft/$CSV && cd ~/raft && ./bin/client -config $_RC_CFG -mode $_RC_MODE -csv $CSV"
    [ "$_RC_MODE" = "mixed" ] && _RC_CMD="$_RC_CMD -read-ratio $READ_RATIO"

    echo "  Running clients (mode=$_RC_MODE)..."
    for ip in "${IPS[@]}"; do
        (
            echo "  ===== $ip ====="
            ssh -o ConnectTimeout=5 "${USER}@${ip}" "$_RC_CMD"
        ) &
    done
    wait

    echo "  Collecting results → $_RC_OUTDIR"
    for ip in "${IPS[@]}"; do
        (
            host="${ip%%.*}"
            scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/${CSV}" "$_RC_OUTDIR/${host}.csv" \
                && echo "    collected ${host}.csv" \
                || echo "    FAILED to collect from $ip"
        ) &
    done
    wait

    echo "  Collecting server logs → $_RC_OUTDIR"
    for ip in "${IPS[@]}"; do
        (
            host="${ip%%.*}"
            scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/logs/raftd.log" "$_RC_OUTDIR/${host}.server.log" \
                && echo "    collected ${host}.server.log" \
                || echo "    FAILED to collect server log from $ip"
        ) &
    done
    wait

    cat > "$_RC_OUTDIR/exp_info.json" <<EOF
{
    "nb_clients": $NB_CLIENTS,
    "nb_servers": $NB_SERVERS,
    "label": "$_RC_LABEL",
    "mode": "$_RC_MODE",
    "read_ratio": $READ_RATIO,
    "fault_type": "baseline",
    "netelect_enabled": $_RC_NETELECT,
    "fd_mode": "$_RC_FDMODE"
}
EOF
}

# ── experiment groups ─────────────────────────────────────────────────────────
# Each entry: "label:config:fd_mode:netelect_enabled"
SWEEP_GROUPS=(
    "raft:config/cluster.raft.json:none:false"
    "netelect-raft:config/cluster.netelect.raft.json:raft:true"
    "netelect-jacobson:config/cluster.netelect.jacobson.json:Jacobson:true"
)

# ── main sweep ────────────────────────────────────────────────────────────────
for group in "${SWEEP_GROUPS[@]}"; do
    LABEL=$(echo "$group"   | cut -d: -f1)
    CONFIG=$(echo "$group"  | cut -d: -f2)
    FDMODE=$(echo "$group"  | cut -d: -f3)
    NETELECT=$(echo "$group" | cut -d: -f4)
    RESULTS_BASE="$REPO_DIR/results/$LABEL"

    echo ""
    echo "════════════════════════════════════════════════════"
    echo "  Group  : $LABEL"
    echo "  Config : $CONFIG"
    echo "  FDMode : $FDMODE  |  netelect=$NETELECT"
    echo "════════════════════════════════════════════════════"
    echo ""

    sync_config "$CONFIG"
    echo ""

    # ── Mixed phase ───────────────────────────────────────────────────────────
    echo "--- Mixed phase ($MIXED_RUNS runs) ---"
    for ((i = 1; i <= MIXED_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/mixed/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [mixed $i/$MIXED_RUNS  →  run dir $RUN]"

        stop_all_servers
        start_all_servers "$CONFIG"
        run_clients "$CONFIG" "mixed" "$LABEL" "$FDMODE" "$NETELECT" "$RESULTS_BASE/mixed/$RUN"

        if (( i < MIXED_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    echo ""
    echo "--- Read-Only phase ($READ_RUNS runs, no server restart) ---"

    # ── Read phase ────────────────────────────────────────────────────────────
    for ((i = 1; i <= READ_RUNS; i++)); do
        RUN=1
        while [ -d "$RESULTS_BASE/read/$RUN" ]; do RUN=$((RUN + 1)); done
        echo ""
        echo "  [read $i/$READ_RUNS  →  run dir $RUN]"

        run_clients "$CONFIG" "read" "$LABEL" "$FDMODE" "$NETELECT" "$RESULTS_BASE/read/$RUN"

        if (( i < READ_RUNS )) && (( RUN_SETTLE > 0 )); then
            echo "  Settling ${RUN_SETTLE}s..."
            sleep "$RUN_SETTLE"
        fi
    done

    echo ""
    echo "  Group '$LABEL' complete. Results in $RESULTS_BASE/"
done

echo ""
echo "All groups complete. Results in $REPO_DIR/results/"

if [ -n "$SCP_TO" ]; then
    echo "Copying results to $SCP_TO ..."
    scp -r "$REPO_DIR/results/" "$SCP_TO" \
        && echo "Done." \
        || echo "scp failed — results still available locally at $REPO_DIR/results/"
fi
