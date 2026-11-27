#!/bin/bash
# Runs failure sweep then WAN-latency sweep sequentially.
# Usage: ./scripts/run_all_fault_sweeps.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=========================================="
echo "  [1/2] Starting failure sweep"
echo "=========================================="
"$SCRIPT_DIR/run_failure_sweep.sh"

echo ""
echo "=========================================="
echo "  [2/2] Starting WAN-latency sweep"
echo "=========================================="
"$SCRIPT_DIR/run_wan_latency_sweep.sh"

echo ""
echo "All fault sweeps complete."
