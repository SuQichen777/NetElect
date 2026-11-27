#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

ADDRS="${ADDRS:-c220g1-030805.wisc.cloudlab.us:17000,c220g1-030820.wisc.cloudlab.us:17000,c220g1-030810.wisc.cloudlab.us:17000,c220g1-030819.wisc.cloudlab.us:17000,c220g1-030817.wisc.cloudlab.us:17000}"
TIMEOUT="${TIMEOUT:-250ms}"

STABILITY_WORKERS="${STABILITY_WORKERS:-64}"
STABILITY_DURATIONS="${STABILITY_DURATIONS:-${STABILITY_DURATION:-60s}}"
STABILITY_REPEATS="${STABILITY_REPEATS:-3}"

SWEEP_WORKERS="${SWEEP_WORKERS:-16 32 64 128 256}"
SWEEP_DURATIONS="${SWEEP_DURATIONS:-10s}"
SWEEP_REPEATS="${SWEEP_REPEATS:-3}"

COOLDOWN="${COOLDOWN:-1}"

BUILD_BIN="${BUILD_BIN:-1}"
BENCH_BIN="${BENCH_BIN:-}"

OUT_DIR="${OUT_DIR:-$ROOT_DIR/out}"
RUN_ID="${RUN_ID:-grpcbench-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="$OUT_DIR/$RUN_ID"
mkdir -p "$RUN_DIR"

echo "run_dir=$RUN_DIR"
echo "addrs=$ADDRS"
echo "timeout=$TIMEOUT"
echo "stability_workers=$STABILITY_WORKERS stability_durations=$STABILITY_DURATIONS stability_repeats=$STABILITY_REPEATS"
echo "sweep_workers=$SWEEP_WORKERS sweep_durations=$SWEEP_DURATIONS sweep_repeats=$SWEEP_REPEATS"
echo "cooldown=$COOLDOWN"

run_one() {
  local workers="$1"
  local duration="$2"
  local label="$3"
  local log="$RUN_DIR/${label}.log"

  echo "workers=$workers duration=$duration label=$label"
  if [[ -n "$BENCH_BIN" ]]; then
    "$BENCH_BIN" -addrs "$ADDRS" -workers "$workers" -duration "$duration" -timeout "$TIMEOUT" | tee "$log"
  else
    go run ./cmd/grpcbench -addrs "$ADDRS" -workers "$workers" -duration "$duration" -timeout "$TIMEOUT" | tee "$log"
  fi
}

extract_row() {
  local phase="$1"
  local workers="$2"
  local duration="$3"
  local iter="$4"
  local label="$5"
  local log="$RUN_DIR/${label}.log"

  local total success failed qps p50 p95 p99 samples
  total="$(awk -F'[ =]+' '/^total=/{print $2}' "$log" | tail -n 1)"
  success="$(awk -F'[ =]+' '/^total=/{print $4}' "$log" | tail -n 1)"
  failed="$(awk -F'[ =]+' '/^total=/{print $6}' "$log" | tail -n 1)"
  qps="$(awk -F'[ =]+' '/^total=/{print $8}' "$log" | tail -n 1)"
  p50="$(awk -F'[ =]+' '/^p50=/{print $2}' "$log" | tail -n 1)"
  p95="$(awk -F'[ =]+' '/^p50=/{print $4}' "$log" | tail -n 1)"
  p99="$(awk -F'[ =]+' '/^p50=/{print $6}' "$log" | tail -n 1)"
  samples="$(awk -F'[ =]+' '/^p50=/{print $8}' "$log" | tail -n 1)"

  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" "$phase" "$workers" "$duration" "$iter" "$total" "$success" "$failed" "$qps" "$p50" "$p95" "$p99" "$samples"
}

CSV="$RUN_DIR/results.csv"
echo "phase,workers,duration,iter,total,success,failed,qps,p50,p95,p99,samples" > "$CSV"

if [[ "$BUILD_BIN" != "0" ]]; then
  if [[ -z "$BENCH_BIN" ]]; then
    BENCH_BIN="$RUN_DIR/grpcbench"
  fi
  echo "build_bin=$BENCH_BIN"
  go build -o "$BENCH_BIN" ./cmd/grpcbench
else
  if [[ -n "$BENCH_BIN" ]]; then
    echo "bench_bin=$BENCH_BIN"
  else
    echo "bench_bin=go-run"
  fi
fi

for i in $(seq 1 "$STABILITY_REPEATS"); do
  for d in $STABILITY_DURATIONS; do
    label="stable-w${STABILITY_WORKERS}-d${d}-r${i}"
    run_one "$STABILITY_WORKERS" "$d" "$label"
    extract_row "stable" "$STABILITY_WORKERS" "$d" "$i" "$label" >> "$CSV"
    sleep "$COOLDOWN"
  done
done

for w in $SWEEP_WORKERS; do
  for d in $SWEEP_DURATIONS; do
    for i in $(seq 1 "$SWEEP_REPEATS"); do
      label="sweep-w${w}-d${d}-r${i}"
      run_one "$w" "$d" "$label"
      extract_row "sweep" "$w" "$d" "$i" "$label" >> "$CSV"
      sleep "$COOLDOWN"
    done
  done
done

echo "csv=$CSV"
cat "$CSV"
