#!/bin/bash
# Runs the WAN-latency sweep (petition_direct_election=false, baseline) then the
# petition_direct_election=true variant sequentially, so the two arms of the A/B
# comparison are collected back-to-back on the same cluster under the same
# conditions. Usage: ./scripts/run_wan_latency_petition_compare.sh
#
# Baseline  → results/{raft,netelect-raft,netelect-jacobson}-wan/
# Direct    → results/{raft,netelect-raft,netelect-jacobson}-wan-pde/
#
# Stops on the first failing sweep so you don't waste a cluster reservation
# finishing the second arm when the first already broke.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "=========================================="
echo "  [1/2] WAN-latency sweep (petition_direct_election=true)"
echo "=========================================="
"$SCRIPT_DIR/run_wan_latency_petition_direct_election.sh"

echo "=========================================="
echo "  [2/2] WAN-latency sweep (petition_direct_election=false, baseline)"
echo "=========================================="
"$SCRIPT_DIR/run_wan_latency_sweep.sh"


echo ""
echo "Both WAN-latency sweeps complete (baseline + petition_direct_election)."
