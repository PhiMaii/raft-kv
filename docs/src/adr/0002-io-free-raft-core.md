# 0002: Deterministic, I/O-free Raft core

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

Consensus bugs tend to appear only under rare interleavings of message
delays, crashes and timeouts. Tests against real processes find them by
chance and cannot replay them. To explore many interleavings and reproduce
every failure exactly, the protocol logic has to run without real time,
real randomness, a real network or a real disk.

## Decision

`raft-core` is a pure state machine. It takes inputs (received messages,
clock ticks, client proposals) and returns outputs (messages to send, entries
to persist, entries to apply). Time and randomness, such as election timeout
jitter, are injected by the caller. The crate has no dependency on Tokio,
the network or the file system.

Storage follows the same idea: all file access goes through a `Disk` trait,
so the simulator can substitute a disk that loses unsynced data on a crash.

## Consequences

- A single-process simulator can run whole clusters from one seed and
  replay any failure exactly (see [Testing methodology](../testing.md)).
- The same core can later be compiled to WebAssembly for a browser demo.
- The `server` crate has to drive the core: it owns timers, sockets and
  fsync, and feeds results back in. That glue is not covered by the
  simulator and is tested by the real-process fault-injection harness.
