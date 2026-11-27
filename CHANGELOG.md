# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- CI: a Docker workflow that builds the image, starts the Compose cluster,
  crashes the leader and checks reads and writes (`scripts/docker-smoke-test.sh`).
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
- `transport`: gRPC services generated from `/proto` with a vendored
  `protoc`. Raft messages travel as one long-lived, ordered stream per peer
  with a bounded queue and reconnection (ADR 0005). The client API
  (`RegisterClient`, `Get`, `Put`, `Delete`, `CompareAndSwap`) returns
  typed errors, including `NotLeader` with a leader hint; `KvService`
  delegates to a `KvHandler`, and `KvClient` is a typed client.
- `kv`: a `Read` command that reads through the log without a session.
- `transport`: a `Status` call that reports a node's role, term, leader and
  log progress.
- `server`: runs a node from a TOML configuration. One task drives Raft,
  persists each round with one fsync before sending, applies committed
  entries and answers waiting clients. Refuses to start without its data
  directory unless `--init` is given. Logs role changes with `tracing`.
- `client`: client library with leader discovery, `NotLeader` hints,
  retries with the same sequence number, backoff and a deadline.
- `raft-kv` binary: `server`, `get`, `put`, `del`, `cas` and `status`.
- `raft-core`: linearizable reads with ReadIndex (dissertation §6.4):
  `read_index`, `Ready::read_states`, and a read round carried in
  `AppendEntries` and echoed in its response. Concurrent reads share
  confirmation rounds.
- `server`: reads use ReadIndex by default; `read_mode = "log"` keeps
  reads through the log for benchmarks.
- `sim`: three in four client reads use ReadIndex, so the linearizability
  check covers both read paths. A new fault isolates the leader for 1–3
  seconds, and clients switch nodes after a timeout.
- `raft-core`: log compaction and `InstallSnapshot` (Raft §7). `compact`
  takes a snapshot of the state machine and keeps a configurable tail of
  entries before it; the leader sends its snapshot in chunks, stop-and-wait,
  to followers that need compacted entries; `Ready::snapshot` hands an
  installed snapshot to the caller; `restore_with_snapshot` restarts a node
  from one.
- `storage`: an atomically replaced snapshot file. Saving a snapshot removes
  the WAL segments before it; installing one from the leader replaces the
  WAL, and recovery finishes a replacement that a crash interrupted.
- `kv`: snapshots of the store, session table included. Data and sessions
  live in persistent maps (`imbl`), so a snapshot is serialized from a
  cheap clone (ADR 0006).
- `server`: snapshots once `snapshot_threshold_bytes` (64 MiB) of commands
  have been applied, serialized on a blocking thread; `retained_entries`
  sets the log tail kept behind each snapshot.
- `sim`: every seed picks its snapshot frequency, log tail and chunk size;
  restarted nodes recover from their snapshot; a long-run test checks that
  logs stay bounded; three snapshot bugs added to the mutation tests.
- `raft-core`: batched replication (`batch_appends`): while an append to a
  follower waits for its answer, new entries wait with it and go out
  together; an idle leader still sends at once. Appends are limited by
  `max_append_bytes` as well as by entry count.
  `Message::may_send_before_persisting` marks a leader's replication
  messages, which may go out before its fsync.
- `server`: `batch_appends`, `max_append_bytes` and `send_before_persist`
  settings, all on by default; the leader's fsync overlaps with sending its
  appends.
- `sim`: every seed turns batching and early sends on or off, and counts
  the entries sent.
- `bench` and `raft-kv bench`: a closed-loop load generator with one
  session per client, a configurable read/write mix, a warm-up, HDR
  histograms per operation type, and a JSON report with the configuration,
  the machine and the commit.
- `scripts/bench.sh` runs each benchmark scenario against a fresh cluster
  with its own server settings; `scripts/fsync-probe.py` measures how long
  a sync takes with one or several processes on the same disk. Results are
  in `bench/results/` and on the Benchmarks page.
- `scripts/bench.sh record`: every scenario three times, the median run
  per scenario appended to `bench/history/MACHINE.jsonl` with the commit,
  machine, power source and an fsync calibration; refuses to run on
  battery or with uncommitted changes.
- A local five-node cluster: `scripts/cluster.sh` (start, stop, kill and
  restart single nodes, status, logs, clean), `docker-compose.yml` with a
  `Dockerfile`, and example configurations in `config/`.
- Cargo workspace with one crate per component (`raft-core`, `storage`,
  `transport`, `kv`, `server`, `client`, `sim`, `chaos`, `bench`, `cli`).
- Documentation site (mdBook) with the specification, design overview,
  architecture decision records, testing methodology and bug log.
- CI: formatting, Clippy, tests, dependency audit and documentation build.

### Fixed

- `raft-core`: answers to heartbeats and ReadIndex rounds no longer start
  extra chains of appends to a follower that is behind, which piled up into
  a message storm under load (see the bug log).
- Documentation site on GitHub Pages, published from `main`, with the
  mdBook and the API reference.
