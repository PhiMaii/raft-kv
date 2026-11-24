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

## Current coverage

- **`raft-core` unit tests** (`crates/raft-core/tests/election.rs`): every
  election transition, driven with hand-written messages. They cover vote
  granting and the election restriction, stepping down on higher terms,
  stale messages, heartbeats, split votes, and restoring from persisted state.
- **`raft-core` replication tests** (`crates/raft-core/tests/replication.rs`):
  the leader's no-op, proposals, the commit rule including Raft's Figure 8
  case, fast backtracking in all three variants, follower consistency checks,
  suffix replacement, late duplicate messages, and commit index bounds.
- **In-memory cluster tests** (`crates/raft-core/tests/cluster.rs`):
  - 3- and 5-node clusters across hundreds of seeds elect exactly one leader,
    re-elect after the leader is cut off, and never elect a leader in a
    minority;
  - proposals reach every node and are applied in the same order;
  - a cut-off leader's uncommitted entries are replaced after the partition
    heals;
  - a small fuzz run (100 seeds × 1,500 steps) cuts off random nodes while
    proposals keep coming, and checks **log matching** and **state machine
    safety** after every step.

  This harness only cuts nodes off. Message loss, delay, reordering,
  duplication and crashes come with the simulator (milestone 3).

## Running the tests

```sh
cargo nextest run --workspace
```

Simulation and fault-injection commands are added here as those milestones
land.
