# Testing methodology

> This page grows with the implementation. The full plan is in
> [section 3 of the specification](spec.md#3-verification-proving-the-websites-claims).

raft-kv is tested at four levels:

1. **Unit and property tests** for individual state transitions and for the
   on-disk formats.
2. **Deterministic simulation testing**, the main correctness tool. Whole
   clusters run in one process with a simulated network, clock and disk, all
   driven by a single random seed. Every failing seed reproduces exactly and
   becomes a regression test.
3. **Linearizability checking** of the operation histories recorded in
   simulation and in real-process runs.
4. **Real-process fault injection**: five real server processes with
   `kill -9`, paused processes and network partitions.

## Running the tests

```sh
cargo nextest run --workspace
```

Simulation and fault-injection commands are added here as those milestones
land.
