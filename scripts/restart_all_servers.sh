#!/bin/bash
# Usage: ./scripts/restart_all_servers.sh [config_path]
#   config_path overrides the value in restart_all_servers.json
#   e.g. ./scripts/restart_all_servers.sh config/cluster.netelect.jacobson.json

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JSON="$SCRIPT_DIR/restart_all_servers.json"

command -v jq >/dev/null 2>&1 || { echo "error: jq is required (brew install jq)"; exit 1; }

USER=$(jq -r '.user' "$JSON")
CONFIG="${1:-$(jq -r '.config' "$JSON")}"

IPS=()
while IFS= read -r ip; do
    IPS+=("$ip")
done < <(jq -r '.ips[]' "$JSON")

echo "Syncing config to all nodes: $CONFIG"
for ip in "${IPS[@]}"; do
    (
        ssh -o ConnectTimeout=5 "${USER}@${ip}" "mkdir -p ~/raft/config"
        scp -o ConnectTimeout=5 "$REPO_DIR/$CONFIG" "${USER}@${ip}:~/raft/$CONFIG" >/dev/null \
            && echo "  [$ip] config ok" \
            || echo "  [$ip] FAILED to sync config"
    ) &
done
wait
echo ""

echo "Restarting ${#IPS[@]} servers with config: $CONFIG"
echo ""

for i in "${!IPS[@]}"; do
    ip="${IPS[$i]}"
    id=$i
    (
        echo "  [$id] $ip — stopping..."
        ssh -o ConnectTimeout=5 "${USER}@${ip}" "pkill raftd; true"

        echo "  [$id] $ip — starting (id=$id)..."
        ssh -o ConnectTimeout=5 "${USER}@${ip}" \
            "cd ~/raft && setsid -f ./bin/raftd -config $CONFIG -id $id >/dev/null 2>&1 </dev/null"

        echo "  [$id] $ip — done"
    ) &
done

wait
echo ""
echo "All servers restarted."
