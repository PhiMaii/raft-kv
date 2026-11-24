# 0003: ReadIndex instead of leader leases

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

Reads must be linearizable. Writing every read to the log is correct but
costs a full replication round and an fsync per read. Two cheaper options are
described in the Raft thesis:

- **ReadIndex:** the leader records its commit index, confirms it is still
  leader with one heartbeat round, waits until it has applied up to that
  index, then answers from local state.
- **Leader leases:** the leader answers reads locally while a lease is valid,
  without any network round. Correctness depends on bounded clock drift
  between nodes.

## Decision

Use ReadIndex. Concurrent reads share one confirmation round. Leader leases
are not used in the MVP; they may be added later as an explicit opt-in.

## Consequences

- Reads cost one heartbeat round but no log write and no fsync.
- Safety does not depend on clock assumptions, which the simulator can then
  freely violate.
- A new leader must commit a no-op entry from its own term before serving
  reads, so reads are briefly unavailable after an election.
