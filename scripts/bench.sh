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
#       Runs every scenario listed in this script and writes the reports to
#       bench/results/NAME.json. These are the numbers in
#       docs/src/benchmarks.md.
#
# Environment: NODES (default 3) sets the cluster size; BASE_PORT (default
# 7200) the first port, node i listens on BASE_PORT + i.
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
trap cleanup EXIT

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
  echo "== $name: ${settings[*]:-defaults} ${options[*]:-}"
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

case "${1:-}" in
  run)
    [[ $# -ge 2 ]] || { echo "usage: $0 run NAME [setting=value ...] [-- options]" >&2; exit 1; }
    build
    name="$2"
    shift 2
    run "$name" "$RESULTS/$name.local.json" "$@"
    ;;
  matrix)
    build
    # Defaults: 32 clients, 1,000 keys, 100-byte values, 5 s warm-up and
    # 20 s measured, batching and early sends on, ReadIndex reads.
    run mixed "$RESULTS/mixed.json" -- --reads 50
    run writes "$RESULTS/writes.json" -- --reads 0
    run reads "$RESULTS/reads.json" -- --reads 95
    run writes-unbatched "$RESULTS/writes-unbatched.json" batch_appends=false -- --reads 0
    run writes-persist-first "$RESULTS/writes-persist-first.json" send_before_persist=false -- --reads 0
    run reads-through-log "$RESULTS/reads-through-log.json" read_mode=log -- --reads 95
    run single-client "$RESULTS/single-client.json" -- --reads 50 --clients 1
    run writes-128-clients "$RESULTS/writes-128-clients.json" -- --reads 0 --clients 128
    NODES=5 run writes-5-nodes "$RESULTS/writes-5-nodes.json" -- --reads 0
    ;;
  *)
    sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
