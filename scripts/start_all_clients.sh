#!/bin/bash
# Usage: ./scripts/start_all_clients.sh [config_path] [experiment_label] [fd_mode]
#   config_path      overrides .config in start_all_clients.json
#   experiment_label overrides .experiment (used as results subdirectory name)
#   fd_mode          overrides .fd_mode   (e.g. none / raft / Jacobson / CI)
#
# Examples:
#   ./scripts/start_all_clients.sh
#   ./scripts/start_all_clients.sh config/cluster.netelect.jacobson.json netelect-jacobson Jacobson

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/start_all_clients.json"

command -v jq >/dev/null 2>&1 || { echo "error: jq is required (brew install jq)"; exit 1; }

USER=$(jq -r '.user'                      "$JSON")
MODE=$(jq -r '.mode'                      "$JSON")
CSV=$(jq -r '.csv'                        "$JSON")
READ_RATIO=$(jq -r '.read_ratio // 0.8'   "$JSON")
NB_SERVERS=$(jq -r '.nb_servers'          "$JSON")
FAILURE=$(jq -r '.failure_injection'      "$JSON")
NETELECT=$(jq -r '.netelect_enabled'      "$JSON")
REPEAT=$(jq -r '.repeat // 1'             "$JSON")
SETTLE=$(jq -r '.settle_delay_s // 0'     "$JSON")

CONFIG="${1:-$(jq -r '.config' "$JSON")}"
EXPERIMENT="${2:-$(jq -r '.experiment // .mode' "$JSON")}"
FDMODE="${3:-$(jq -r '.fd_mode // "none"' "$JSON")}"

IPS=()
while IFS= read -r ip; do
    IPS+=("$ip")
done < <(jq -r '.ips[]' "$JSON")

NB_CLIENTS=${#IPS[@]}
RESULTS_DIR="$REPO_DIR/results/$EXPERIMENT"

if ! [[ "$REPEAT" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: repeat must be a positive integer"
    exit 1
fi

CMD="rm -f ~/raft/$CSV && cd ~/raft && ./bin/client -config $CONFIG -mode $MODE -csv $CSV"
if [ "$MODE" = "mixed" ]; then
    CMD="$CMD -read-ratio $READ_RATIO"
fi

for ((repeat_idx = 1; repeat_idx <= REPEAT; repeat_idx++)); do
    RUN=1
    while [ -d "$RESULTS_DIR/$RUN" ]; do
        RUN=$((RUN + 1))
    done
    OUT_DIR="$RESULTS_DIR/$RUN"
    mkdir -p "$OUT_DIR"

    echo "Experiment : $EXPERIMENT  (run $RUN, repeat $repeat_idx/$REPEAT)"
    echo "Config     : $CONFIG"
    echo "Mode       : $MODE  |  read_ratio=$READ_RATIO"
    echo "Clients    : $NB_CLIENTS  |  servers=$NB_SERVERS"
    echo "Failure    : $FAILURE  |  netelect=$NETELECT  |  fd_mode=$FDMODE"
    echo ""

    for ip in "${IPS[@]}"; do
      (
        echo "===== $ip ====="
        ssh -o ConnectTimeout=5 "${USER}@${ip}" "$CMD"
      ) &
    done

    wait
    echo ""
    echo "All clients done. Collecting results..."

    for ip in "${IPS[@]}"; do
      (
        host="${ip%%.*}"
        scp -o ConnectTimeout=5 "${USER}@${ip}:~/raft/${CSV}" "$OUT_DIR/${host}.csv" \
          && echo "  collected ${host}.csv" \
          || echo "  FAILED to collect from ${ip}"
      ) &
    done

    wait

    cat > "$OUT_DIR/exp_info.json" <<EOF
{
    "nb_clients": $NB_CLIENTS,
    "nb_servers": $NB_SERVERS,
    "mode": "$EXPERIMENT",
    "read_ratio": $READ_RATIO,
    "failure_injection": $FAILURE,
    "netelect_enabled": $NETELECT,
    "fd_mode": "$FDMODE",
    "repeat": $REPEAT,
    "repeat_index": $repeat_idx
}
EOF

    echo ""
    echo "Done. Results in $OUT_DIR/"
    echo ""

    if (( repeat_idx < REPEAT )) && (( SETTLE > 0 )); then
        echo "Settling for ${SETTLE}s before next run..."
        sleep "$SETTLE"
        echo ""
    fi
done
