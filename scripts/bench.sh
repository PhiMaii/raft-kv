#!/usr/bin/env bash
# Runs benchmarks, each against a fresh cluster on this machine.
#
# Usage:
#   scripts/bench.sh run NAME [setting=value ...] [-- bench options]
#       Starts a new cluster whose node configurations get the given
#       settings (e.g. batch_appends=false), runs `raft-kv bench` with the
#       given options (see `raft-kv bench --help`), writes the report to
#       bench/results/NAME.local.json (git-ignored) and stops the cluster.
#   scripts/bench.sh matrix
#       Runs every scenario listed in this script once and writes the
#       reports to bench/results/NAME.json.
#   scripts/bench.sh record LABEL
#       Runs every scenario three times and appends, per scenario, the run
#       with the median throughput to bench/history/MACHINE.jsonl, with the
#       commit, the machine, the power source and an fsync calibration.
#       LABEL names the milestone or release measured. Run once per merged
#       milestone, from a clean checkout, on mains power.
#
# Environment: NODES (default 3) sets the cluster size; BASE_PORT (default
# 7200) the first port, node i listens on BASE_PORT + i; ALLOW_BATTERY=1
# lets `record` run on battery power; RAFT_KV_BENCH_DURATION and
# RAFT_KV_BENCH_WARMUP (seconds) shorten the runs, for trying the script.
#
# Every node runs on this machine with its data in a temporary directory,
# so all nodes share one CPU and one disk, and fsync is real.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
BIN="target/release/raft-kv"
NODES="${NODES:-3}"
BASE_PORT="${BASE_PORT:-7200}"
RESULTS="bench/results"

PIDS=()
WORKDIR=""
RUNS=""

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
  PIDS=()
  [[ -n "$WORKDIR" ]] && rm -rf "$WORKDIR"
  WORKDIR=""
}
trap 'cleanup; [[ -n "$RUNS" ]] && rm -rf "$RUNS"' EXIT

address() { echo "127.0.0.1:$((BASE_PORT + $1))"; }

cluster_addresses() {
  local addresses=()
  for id in $(seq 1 "$NODES"); do
    addresses+=("$(address "$id")")
  done
  local IFS=,
  echo "${addresses[*]}"
}

# start_cluster SETTING=VALUE...: writes configs and starts every node.
start_cluster() {
  WORKDIR="$(mktemp -d)"
  for id in $(seq 1 "$NODES"); do
    local config="$WORKDIR/node$id.toml"
    {
      echo "id = $id"
      echo "listen = \"$(address "$id")\""
      echo "data_dir = \"$WORKDIR/node$id\""
      for setting in "$@"; do
        local key="${setting%%=*}" value="${setting#*=}"
        # Strings need quotes in TOML; numbers and booleans do not.
        if [[ "$value" =~ ^([0-9]+|true|false)$ ]]; then
          echo "$key = $value"
        else
          echo "$key = \"$value\""
        fi
      done
      for peer in $(seq 1 "$NODES"); do
        [[ "$peer" == "$id" ]] && continue
        printf '\n[[peers]]\nid = %s\naddress = "%s"\n' "$peer" "$(address "$peer")"
      done
    } >"$config"
    "$BIN" server --config "$config" --init >"$WORKDIR/node$id.log" 2>&1 &
    PIDS+=($!)
  done
  # Wait for a leader.
  for _ in $(seq 1 100); do
    local status
    status="$(RAFT_KV_CLUSTER="$(cluster_addresses)" "$BIN" status 2>/dev/null || true)"
    if [[ "$status" == *" leader "* ]]; then
      return
    fi
    sleep 0.1
  done
  echo "error: no leader elected. Last lines of each node's log:" >&2
  for id in $(seq 1 "$NODES"); do
    echo "-- node $id" >&2
    tail -5 "$WORKDIR/node$id.log" >&2
  done
  exit 1
}

# run NAME OUTPUT SETTINGS... [-- BENCH OPTIONS...]
run() {
  local name="$1" output="$2"
  shift 2
  local settings=() options=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    settings+=("$1")
    shift
  done
  [[ $# -gt 0 ]] && shift
  options=("$@")
  echo "== $name ($NODES nodes): ${settings[*]:-defaults} ${options[*]:-}"
  # What the history needs to know about the scenario besides the report.
  python3 -I -c 'import json, sys
print(json.dumps({"nodes": int(sys.argv[1]),
                  "settings": dict(s.split("=", 1) for s in sys.argv[2:])}))' \
    "$NODES" "${settings[@]:+${settings[@]}}" >"${output%.json}.setup"
  start_cluster "${settings[@]:+${settings[@]}}"
  RAFT_KV_CLUSTER="$(cluster_addresses)" "$BIN" bench "${options[@]:+${options[@]}}" \
    --output "$output"
  cleanup
  echo
}

build() {
  cargo build --release --quiet -p cli
  mkdir -p "$RESULTS"
}

# scenarios DIR SUFFIX: runs every scenario once, writing DIR/NAME$SUFFIX.json.
# Defaults: 32 clients, 1,000 keys, 100-byte values, 5 s warm-up and 20 s
# measured, batching and early sends on, ReadIndex reads.
scenarios() {
  local dir="$1" suffix="$2"
  run mixed "$dir/mixed$suffix.json" -- --reads 50
  run writes "$dir/writes$suffix.json" -- --reads 0
  run reads "$dir/reads$suffix.json" -- --reads 95
  run writes-unbatched "$dir/writes-unbatched$suffix.json" batch_appends=false -- --reads 0
  run writes-persist-first "$dir/writes-persist-first$suffix.json" send_before_persist=false -- --reads 0
  run reads-through-log "$dir/reads-through-log$suffix.json" read_mode=log -- --reads 95
  run single-client "$dir/single-client$suffix.json" -- --reads 50 --clients 1
  run writes-128-clients "$dir/writes-128-clients$suffix.json" -- --reads 0 --clients 128
  NODES=5 run writes-5-nodes "$dir/writes-5-nodes$suffix.json" -- --reads 0
}

# Prints "ac", "battery" or "unknown".
power_source() {
  if command -v pmset >/dev/null; then
    case "$(pmset -g batt | head -1)" in
      *"AC Power"*) echo ac ;;
      *"Battery Power"*) echo battery ;;
      *) echo unknown ;;
    esac
    return
  fi
  local supply
  for supply in /sys/class/power_supply/*; do
    if [[ "$(cat "$supply/type" 2>/dev/null)" == Mains ]]; then
      [[ "$(cat "$supply/online" 2>/dev/null)" == 1 ]] && echo ac || echo battery
      return
    fi
  done
  echo unknown
}

# Refuses to measure on battery power: laptops throttle then.
check_power() {
  local power
  power="$(power_source)"
  if [[ "$power" == battery && "${ALLOW_BATTERY:-}" != 1 ]]; then
    echo "error: running on battery; plug in, or set ALLOW_BATTERY=1" >&2
    exit 1
  fi
  [[ "$power" == unknown ]] && echo "warning: cannot tell the power source" >&2
  echo "$power"
}

case "${1:-}" in
  run)
    [[ $# -ge 2 ]] || { echo "usage: $0 run NAME [setting=value ...] [-- options]" >&2; exit 1; }
    build
    name="$2"
    shift 2
    run "$name" "$RESULTS/$name.local.json" "$@"
    rm -f "$RESULTS/$name.local.setup"
    ;;
  matrix)
    build
    scenarios "$RESULTS" ""
    rm -f "$RESULTS"/*.setup
    ;;
  record)
    [[ $# -ge 2 ]] || { echo "usage: $0 record LABEL" >&2; exit 1; }
    label="$2"
    if [[ -n "$(git status --porcelain)" ]]; then
      echo "error: uncommitted changes; a record must belong to a commit" >&2
      exit 1
    fi
    power="$(check_power)"
    build
    RUNS="$(mktemp -d)"
    echo "== fsync calibration"
    one="$(python3 -I scripts/fsync-probe.py --json --processes 1)"
    three="$(python3 -I scripts/fsync-probe.py --json --processes 3)"
    python3 -I -c 'import json, sys
print(json.dumps({"one_process": json.loads(sys.argv[1]),
                  "three_processes": json.loads(sys.argv[2])}))' "$one" "$three" \
      >"$RUNS/fsync.json"
    cat "$RUNS/fsync.json"
    # Whole rounds rather than three runs in a row, so that a slow drift
    # (heat, background work) spreads over all scenarios alike.
    for round in 1 2 3; do
      [[ "$(check_power)" == "$power" ]] || { echo "error: power source changed" >&2; exit 1; }
      scenarios "$RUNS" ".run$round"
    done
    for setup in "$RUNS"/*.run1.setup; do
      name="$(basename "$setup" .run1.setup)"
      mv "$setup" "$RUNS/$name.scenario.json"
    done
    rm -f "$RUNS"/*.setup
    python3 -I scripts/bench-record.py "$RUNS" bench/history "$label" "$power"
    ;;
  *)
    sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
