# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `raft-core`: leader election (Raft §5.2) with the election restriction
  (§5.4.1), randomized election timeouts from a seeded generator, heartbeats,
  and a `tick` / `step` / `take_ready` API that performs no I/O.
- `raft-core`: a `Log` type that addresses entries through a snapshot offset
  from the start.
- `raft-core`: log replication (Raft §5.3) with the consistency check, suffix
  replacement and fast backtracking by term; the commit rule for the current
  term (§5.4.2); a no-op entry on election; `propose`; and in-order,
  exactly-once delivery of committed entries through `Ready`.
- `sim`: deterministic, seed-driven cluster simulator with message loss,
  duplication, delay and latency spikes, symmetric and asymmetric
  partitions, crashes with restart from persisted state, targeted leader
  crashes and pauses. Checks election safety, log matching, leader
  completeness and state machine safety on every tick, and convergence
  after healing.
- `raft-kv-sim` binary to run many seeds, a regression seed list, a nightly
  CI job running 10,000 seeds, and `scripts/sim-mutations.sh` to check that
  the simulator detects injected Raft bugs.
- `storage`: write-ahead log in checksummed, size-limited segment files, an
  atomically replaced hard state file, and crash recovery that cuts off torn
  tails but refuses to start on any other damage. All file access goes
  through a `Disk` trait with a real (`FsDisk`) and a simulated (`SimDisk`)
  implementation.
- `sim`: nodes persist through the storage crate on a `SimDisk` and can
  crash in the middle of a write, losing unsynced data.
- `kv`: the key-value state machine with put, delete, compare-and-swap and
  log reads; client sessions that deduplicate retries; deterministic
  session expiry by leader timestamps (ADR 0004); key and value size limits;
  and a versioned binary encoding for commands in the log.
- `sim`: a linearizability checker (Wing and Gong's algorithm with Lowe's
  memoization) that checks key-value histories one key at a time and
  handles operations with unknown outcomes.
- `sim`: simulated clients with sessions, retries, leader hints and
  deadlines run a key-value workload in every run. The simulator applies
  committed entries to each node's `KvStore`, checks that all replicas end
  up identical, and checks the recorded history for linearizability.
- Cargo workspace with one crate per component (`raft-core`, `storage`,
  `transport`, `kv`, `server`, `client`, `sim`, `chaos`, `bench`, `cli`).
- Documentation site (mdBook) with the specification, design overview,
  architecture decision records, testing methodology and bug log.
- CI: formatting, Clippy, tests, dependency audit and documentation build.
- Documentation site on GitHub Pages, published from `main`, with the
  mdBook and the API reference.
