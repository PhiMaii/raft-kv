#!/usr/bin/env bash
# Runs a five-node raft-kv cluster on this machine, on ports 7001-7005.
#
# Usage:
#   scripts/cluster.sh start        build, then start every node that is not running
#   scripts/cluster.sh stop         stop all nodes (SIGTERM)
#   scripts/cluster.sh kill N       kill node N with SIGKILL, like a crash
#   scripts/cluster.sh start N      start node N again from its data directory
#   scripts/cluster.sh status       show every node's role, term and log progress
#   scripts/cluster.sh logs N       follow node N's log
#   scripts/cluster.sh clean        stop all nodes and delete all data
#
# Data and logs live in data/ (git-ignored). A brand-new cluster is created
# with --init the first time; after that, a node whose data directory was
# deleted refuses to start, because a node that lost its disk must not
# rejoin under its old id. Use `clean` to reset the whole cluster.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
NODES=(1 2 3 4 5)
BIN="target/release/raft-kv"
DATA="data"

pid_file() { echo "$DATA/run/node$1.pid"; }
log_file() { echo "$DATA/logs/node$1.log"; }

running() {
  local pid_file
  pid_file="$(pid_file "$1")"
  [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

start_node() {
  local id="$1" init="$2"
  if running "$id"; then
    echo "node $id is already running"
    return
  fi
  local args=(server --config "config/node$id.toml")
  if [[ "$init" == yes ]]; then
    args+=(--init)
  fi
  "$BIN" "${args[@]}" >>"$(log_file "$id")" 2>&1 &
  echo $! >"$(pid_file "$id")"
  echo "started node $id (pid $!, log $(log_file "$id"))"
}

build() {
  cargo build --release --quiet -p cli
  mkdir -p "$DATA/run" "$DATA/logs"
}

check_node() {
  if [[ ! " ${NODES[*]} " =~ \ $1\  ]]; then
    echo "error: no node $1; nodes are ${NODES[*]}" >&2
    exit 1
  fi
}

cluster() {
  local addresses=()
  for id in "${NODES[@]}"; do
    addresses+=("127.0.0.1:700$id")
  done
  local IFS=,
  echo "${addresses[*]}"
}

command="${1:-}"
case "$command" in
  start)
    build
    if [[ $# -ge 2 ]]; then
      check_node "$2"
      start_node "$2" no
    else
      # A cluster that has never run gets --init; an existing one does not.
      init=yes
      for id in "${NODES[@]}"; do
        [[ -d "$DATA/node$id" ]] && init=no
      done
      for id in "${NODES[@]}"; do
        start_node "$id" "$init"
      done
      echo
      echo "Try:  $BIN put greeting hello && $BIN get greeting"
    fi
    ;;
  stop)
    for id in "${NODES[@]}"; do
      if running "$id"; then
        kill "$(cat "$(pid_file "$id")")"
        echo "stopped node $id"
      fi
      rm -f "$(pid_file "$id")"
    done
    ;;
  kill)
    [[ $# -ge 2 ]] || { echo "usage: $0 kill N" >&2; exit 1; }
    check_node "$2"
    if running "$2"; then
      kill -9 "$(cat "$(pid_file "$2")")"
      echo "killed node $2"
    else
      echo "node $2 is not running"
    fi
    rm -f "$(pid_file "$2")"
    ;;
  status)
    [[ -x "$BIN" ]] || build
    RAFT_KV_CLUSTER="$(cluster)" "$BIN" status
    ;;
  logs)
    [[ $# -ge 2 ]] || { echo "usage: $0 logs N" >&2; exit 1; }
    check_node "$2"
    tail -f "$(log_file "$2")"
    ;;
  clean)
    "$0" stop
    rm -rf "$DATA"
    echo "deleted $DATA/"
    ;;
  *)
    sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
