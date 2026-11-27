# 0006: Persistent maps for snapshots, serialized off the apply loop

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

A snapshot must capture the key-value store exactly as it was after
applying some index. Serializing a large store takes time proportional to
its size, on the order of a second or more for gigabytes of data. If the
node loop does it, nothing else happens meanwhile: no entries are applied,
no client is answered, no heartbeat goes out. A leader that stalls long
enough triggers an election.

The serializer needs a view of the store that does not change while it
runs, while the node loop goes on applying entries. The options:

1. **Stop the world.** Serialize on the node loop and accept the pause.
   Simplest, but the pause grows with the data.
2. **Copy, then serialize elsewhere.** Cloning a `BTreeMap` is itself
   proportional to the data, so this only shortens the pause.
3. **`fork()` the process**, as Redis does, and let the child serialize
   its copy-on-write view of memory. Unsafe with a multi-threaded Tokio
   runtime, and not portable.
4. **Persistent maps**, which share structure between versions: a clone
   is O(1), and later writes copy only the paths they touch.

## Decision

`KvStore` keeps its data and session table in `imbl::OrdMap`, a persistent
B-tree. To take a snapshot, the node clones the store and serializes the
clone on a blocking thread, then hands the result to `Raft::compact` and
`Storage::save_snapshot` on the node loop.

`imbl` is the maintained fork of `im`, which has had no release since 2022.
It is licensed MPL-2.0, which was added to the license allow list in
`deny.toml`. MPL-2.0 is a file-level copyleft: it covers changes to `imbl`'s
own files, not this crate.

## Consequences

- The node loop never waits for serialization. It still writes the
  finished snapshot file and syncs it, which takes about as long as
  writing that many bytes; moving that to a background task as well is
  possible later if measurements show it matters.
- Every lookup and update on the store pays a constant factor over
  `BTreeMap` for the persistent tree. The store stays ordered, so
  iteration (and with it the snapshot encoding) is deterministic.
- Entries applied while a snapshot is being serialized are not in it. The
  snapshot is taken at the index the clone was made at, and those entries
  stay in the log after it.
