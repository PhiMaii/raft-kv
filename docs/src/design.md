# Design overview

> This page grows with the implementation. Each milestone adds the design of
> the part it builds.

## Crates

| Crate | Responsibility |
|---|---|
| `raft-core` | The Raft state machine. Deterministic and free of I/O, clocks and randomness |
| `storage` | Write-ahead log, hard state and snapshot files, behind a `Disk` trait |
| `transport` | gRPC services for Raft RPCs and the client API |
| `kv` | The key-value state machine, client sessions and deduplication |
| `server` | Wires the crates above together into a running node |
| `client` | Client library with leader discovery and retries |
| `sim` | Seed-driven cluster simulator and linearizability checker |
| `chaos` | Fault-injection harness for real processes |
| `bench` | Load generator |
| `cli` | The `raft-kv` binary |

## Core principle: an I/O-free Raft core

`raft-core` takes inputs (received messages, clock ticks, client proposals)
and returns outputs (messages to send, entries to persist, entries to apply).
It never reads the clock, draws random numbers, touches the network or writes
to disk. See [ADR 0002](adr/0002-io-free-raft-core.md).

### Driving a node

The caller interacts with a `Raft` node through three calls:

| Call | Meaning |
|---|---|
| `tick()` | One unit of logical time has passed. The caller picks the tick length (e.g. 10 ms) |
| `step(message)` | A message from another node arrived |
| `take_ready()` | Collect what the calls above produced: a `Ready` with the hard state to persist and the messages to send |

The caller must make `Ready::hard_state` durable **before** sending
`Ready::messages`. Otherwise a node could promise its vote, crash, forget the
promise and vote again in the same term.

All timeouts are counted in ticks, and the only randomness (election timeout
jitter) comes from a seeded generator in the configuration. The same
configuration and the same sequence of calls always produce the same outputs,
which is what the simulator relies on.

### The log and snapshots

`Log` addresses entries by their global `LogIndex`, but stores only the
entries after `snapshot_index`. Before snapshots exist, `snapshot_index` is
simply 0. Every lookup goes through that offset from the start, so adding log
compaction later does not change how the rest of the protocol uses the log.

## Leader election

Implemented as in Raft §5.2 and §5.4.1:

- Every node starts as a **follower**. If it hears nothing from a leader for
  its election timeout (randomized per election, 15–30 ticks by default), it
  becomes a **candidate**: it increments its term, votes for itself and sends
  `RequestVote` to all peers.
- A node grants **at most one vote per term**, and only to a candidate whose
  log is at least as up to date as its own: a higher last term, or the same
  last term and a last index at least as high.
- A candidate with votes from a **majority** becomes **leader** and sends a
  heartbeat (an empty `AppendEntries`) immediately, then every heartbeat
  interval (5 ticks by default).
- Any message with a **higher term** turns the receiver into a follower of
  that term. Requests from a **lower term** are rejected with the receiver's
  term, so the stale sender learns it is out of date.
- A candidate that receives `AppendEntries` from a leader of its own term
  becomes a follower of that leader.

Pre-vote and check-quorum are not implemented in the MVP (see future feature
F2 in the specification). Without them, a leader cut off in a minority keeps
believing it is leader until it hears a higher term. It cannot commit
anything in that state, and reads are protected by ReadIndex.

## Write path

_Written in milestone 2._

## Read path

_Written in milestone 10._ See [ADR 0003](adr/0003-readindex-over-leases.md)
for the decision.

## Storage and recovery

_Written in milestone 4._

## Snapshots

_Written in milestone 11._
