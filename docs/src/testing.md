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

  This harness only cuts nodes off; the simulator below covers everything
  else.

## Deterministic simulation

The `sim` crate runs a whole cluster in one process. Network, clock and
disks are simulated, and every random decision comes from a single seed, so
**the same seed always produces the same run**.

### Faults

With the default settings, a run lasts 3,000 ticks (30 simulated seconds at
10 ms per tick) on five nodes and injects:

| Fault | Default |
|---|---|
| Message loss | 5 % of messages |
| Message duplication | 2 % of messages |
| Delay and reordering | 1–4 ticks per message |
| Latency spikes | 1 % of messages held for 10–80 ticks |
| Partitions | symmetric splits, isolated nodes, and one-way link cuts; about one per second, healing after a few seconds |
| Crashes | 0.1 % per node and tick, down for 10–150 ticks, restarted from persisted state |
| Leader crashes | 2 % per tick for the current leader |
| Pauses | 0.1 % per node and tick, frozen for 5–60 ticks |

Each seed also picks how many entries an `AppendEntries` may carry (1 to
1,024). Small batches make followers catch up piece by piece, which opens
timing windows that some bugs need.

Clients propose commands at random nodes throughout the run, following the
leader hint once, like the client library will.

### Invariants

Checked on every tick (log matching every 10 ticks):

- **Election safety:** at most one leader per term.
- **Log matching:** two logs that agree on an entry's index and term are
  identical up to that entry.
- **Leader completeness:** a new leader has every entry committed in an
  earlier term.
- **State machine safety:** no two nodes apply different entries at the same
  index.

At the end of a run all faults are healed. After 500 fault-free ticks the
cluster must have exactly one leader, and every node must hold the leader's
log and have applied all of it (**liveness**). A panic inside a node also
fails the run.

### Does it find bugs?

A simulator that never fails proves nothing on its own. To check that it can
find real bugs, `scripts/sim-mutations.sh` injects four classic Raft bugs
into `raft-core`, one at a time, and counts how many seeds detect each:

| Injected bug | Seeds that caught it (of 1,000) | Detected as |
|---|---|---|
| Vote twice in one term | 976 | election safety |
| Skip the election restriction | 995 | leader completeness |
| Commit an older term's entry by counting replicas (Figure 8) | 6 | leader completeness |
| Follower commits beyond what the leader's message proved | 2 | state machine safety |

The last two need rare interleavings: a leader that crashes right after its
election, and a badly delayed message. They were not caught at all until the
simulator gained targeted leader crashes, latency spikes and small append
batches. At their rates, the nightly run of 10,000 seeds catches each of them
dozens of times. Both are also covered by dedicated unit tests.

Not simulated yet: losing data that was written but not fsynced (comes with
the storage layer and its `Disk` abstraction) and client-visible
linearizability (comes with the checker).

### Regression seeds

Any seed that ever fails is added to `crates/sim/regressions.txt` with a
comment pointing to its entry in the [bug log](bugs.md). These seeds run on
every `cargo test`.

## Running the tests

```sh
# Everything, including 200 simulation seeds in debug mode
cargo nextest run --workspace

# More seeds in the test suite
SIM_SEEDS=2000 cargo nextest run -p sim --release

# Many seeds, fast; failing seeds are printed with a reproduce command
cargo run -p sim --release -- --seeds 10000

# Replay one seed and print its statistics
cargo run -p sim --release -- --seed 1234

# Check that the simulator catches injected bugs
scripts/sim-mutations.sh 1000
```

A nightly CI job runs 10,000 new seeds every night, starting at a different
offset each time.
