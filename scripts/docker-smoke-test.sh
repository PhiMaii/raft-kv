#!/usr/bin/env bash
# Smoke-tests the Docker Compose cluster: build, start, elect a leader,
# write and read a key, crash the leader, check the cluster still serves
# reads and writes, and bring the crashed node back as a follower.
#
# Usage:
#   scripts/docker-smoke-test.sh          run the test, then tear the cluster down
#   KEEP=1 scripts/docker-smoke-test.sh   leave the cluster running afterwards
#
# The test starts from a fresh cluster: it runs `docker compose down -v`
# first, which deletes the volumes of any cluster you already had running.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
TIMEOUT=60

log() { echo "==> $*"; }

fail() {
  echo "error: $*" >&2
  exit 1
}

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    echo
    log "test failed; node logs follow"
    docker compose logs --no-color || true
  fi
  if [[ "${KEEP:-}" != 1 ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$status"
}

# Runs a client command inside the given node's container.
client() {
  local node="$1"
  shift
  docker compose exec -T "$node" raft-kv "$@"
}

# Prints the status table rows as seen from the given node, without the header.
status_rows() {
  client "$1" status 2>/dev/null | awk 'NR > 1 && $2 ~ /^[0-9]+$/'
}

# Waits until exactly one node is leader; prints its id.
wait_for_leader() {
  local via="$1" deadline=$((SECONDS + TIMEOUT)) leaders
  while ((SECONDS < deadline)); do
    leaders="$(status_rows "$via" | awk '$3 == "leader" { print $2 }' || true)"
    if [[ -n "$leaders" && "$(wc -l <<<"$leaders")" -eq 1 ]]; then
      echo "$leaders"
      return
    fi
    sleep 1
  done
  client "$via" status || true
  fail "no single leader after ${TIMEOUT}s"
}

# Waits until the given node id reports itself as a follower.
wait_for_follower() {
  local via="$1" id="$2" deadline=$((SECONDS + TIMEOUT))
  while ((SECONDS < deadline)); do
    if status_rows "$via" | awk -v id="$id" '$2 == id && $3 == "follower" { found = 1 } END { exit !found }'; then
      return
    fi
    sleep 1
  done
  client "$via" status || true
  fail "node $id is not a follower after ${TIMEOUT}s"
}

expect_get() {
  local node="$1" key="$2" want="$3" got
  got="$(client "$node" get "$key")"
  [[ "$got" == "$want" ]] || fail "get $key via $node returned '$got', want '$want'"
}

trap cleanup EXIT

log "building the image"
docker compose build

log "starting a fresh cluster"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d

log "waiting for a leader"
leader="$(wait_for_leader node1)"
client node1 status
log "node $leader is leader"

log "writing and reading a key"
client node1 put greeting hello
expect_get node1 greeting hello

log "killing the leader (node $leader)"
docker compose kill "node$leader"
other=$((leader % 5 + 1))
new_leader="$(wait_for_leader "node$other")"
[[ "$new_leader" != "$leader" ]] || fail "killed node $leader is still reported as leader"
log "node $new_leader is the new leader"
expect_get "node$other" greeting hello
client "node$other" put greeting again
expect_get "node$other" greeting again

log "restarting node $leader"
docker compose start "node$leader"
wait_for_follower "node$other" "$leader"
client "node$other" status

log "smoke test passed"
