#!/usr/bin/env bash
# Checks that the simulator catches known bugs.
#
# Each mutation injects one known bug into raft-core, the storage layer or
# the key-value state machine, runs the simulator over a range of seeds, and
# reports how many seeds detected it. Source files are restored afterwards,
# also on failure or Ctrl-C.
#
# Usage: scripts/sim-mutations.sh [seeds]   (default: 1000)

set -euo pipefail

SEEDS="${1:-1000}"
RAFT="crates/raft-core/src/raft.rs"
WAL="crates/storage/src/wal.rs"
KV="crates/kv/src/store.rs"
FILES=("$RAFT" "$WAL" "$KV")
cd "$(git rev-parse --show-toplevel)"

if ! git diff --quiet -- "${FILES[@]}"; then
  echo "error: mutated files have uncommitted changes; commit or stash them first" >&2
  exit 1
fi
trap 'git checkout -q -- "${FILES[@]}"' EXIT

mutate() {
  local file="$1" name="$2" from="$3" to="$4"
  FROM="$from" TO="$to" python3 - "$file" <<'EOF'
import os, sys
path = sys.argv[1]
text = open(path).read()
old, new = os.environ["FROM"], os.environ["TO"]
if old not in text:
    sys.exit(f"pattern not found in {path}: {old}")
open(path, "w").write(text.replace(old, new, 1))
EOF
  # Mutations leave variables unused; hide those warnings, not build errors.
  if ! cargo build -q -p sim --release 2>/dev/null; then
    echo "error: build failed for mutation: $name" >&2
    exit 1
  fi
  local output failed kind
  output="$(./target/release/raft-kv-sim --seeds "$SEEDS" 2>&1 || true)"
  failed="$(grep -o '[0-9]* failed' <<<"$output" | head -1)"
  kind="$(grep -m1 -o 'violated [a-z ]* at' <<<"$output" | sed 's/violated //; s/ at$//' || true)"
  printf '%-45s %-12s %s\n' "$name" "$failed" "${kind:--}"
  git checkout -q -- "$file"
}

printf '%-45s %-12s %s\n' "mutation" "seeds" "first violation"
mutate "$RAFT" "vote twice in one term" \
  'let granted = can_vote && log_ok;' 'let granted = log_ok;'
mutate "$RAFT" "skip the election restriction" \
  'let granted = can_vote && log_ok;' 'let granted = can_vote;'
mutate "$RAFT" "commit older terms by counting (Figure 8)" \
  '&& self.log.term_at(majority_index) == Some(self.current_term)' ''
mutate "$RAFT" "follower commits beyond the proven prefix" \
  'let new_commit = leader_commit.min(last_new_index);' 'let new_commit = leader_commit;'
mutate "$WAL" "shorten a segment before removing later ones" \
  'disk.remove(&later.name)?;
        }
        if !removed.is_empty() {
            disk.sync_dir()?;
        }
        let current = &mut self.segments[segment];
        disk.truncate(&current.name, offset)?;
        disk.sync(&current.name)?;' \
  'disk.remove(&later.name)?;
        }
        let current = &mut self.segments[segment];
        disk.truncate(&current.name, offset)?;
        disk.sync(&current.name)?;
        if !removed.is_empty() {
            disk.sync_dir()?;
        }'
mutate "$KV" "apply a retried request again" \
  'if sequence == session.sequence {' 'if false && sequence == session.sequence {'
