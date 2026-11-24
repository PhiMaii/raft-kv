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

## Write path

_Written in milestone 2._

## Read path

_Written in milestone 10._ See [ADR 0003](adr/0003-readindex-over-leases.md)
for the decision.

## Storage and recovery

_Written in milestone 4._

## Snapshots

_Written in milestone 11._
