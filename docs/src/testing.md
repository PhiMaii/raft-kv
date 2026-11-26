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
- **ReadIndex tests** (`crates/raft-core/tests/read_index.rs`): followers
  refuse reads and name the leader; a read waits for a majority to answer a
  round sent after it, and an answer to an older round does not count; a
  failed consistency check still confirms leadership; a new leader holds
  reads until it has committed in its own term; concurrent reads share
  rounds; pending reads are dropped when the leader steps down; a single
  node serves reads at once.
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

- **Storage tests** (`crates/storage/tests/storage.rs` and unit tests):
  - persisting and recovering across simulated crashes, segment rollover,
    and replacing a suffix across segments;
  - torn and zero-filled tails are cut off; corruption in the middle, a
    missing segment and a damaged hard state are errors;
  - **every crash point:** a write that replaces part of the log and the
    hard state is crashed after 0, 1, 2, … disk operations, with and without
    partially surviving unsynced bytes. Recovery must always succeed and
    yield the old state or a valid partial state on the way to the new one,
    and the node's term must never be lower than its last entry's term;
  - a property test (`proptest`) with random appends and suffix
    replacements, then a cut at a random byte of the last segment: recovery
    returns a prefix of the expected log and keeps everything before the cut
    segment;
  - the same storage on a real temporary directory.

- **Key-value state machine tests** (`crates/kv/tests/store.rs`):
  - every operation, compare-and-swap with and without an expected value;
  - retries get the original answer and are not applied again; delayed
    older requests and unknown sessions are rejected;
  - idle sessions expire by leader timestamps, active ones do not, and a
    leader clock behind the previous one does not move time backwards;
  - size limits, and malformed entries rejected without changing state;
  - **property tests**: random workloads with retries must behave exactly
    like a plain map where each request is applied once, and two stores fed
    the same log must end up identical; arbitrary bytes never panic; every
    command survives encoding. Disabling deduplication makes the first of
    these fail immediately.

- **Linearizability checker tests** (`crates/sim/src/linearizability.rs`,
  `crates/sim/src/kv_history.rs`): hand-written histories with known
  answers (stale reads, reads going back in time, concurrent writes seen in
  different orders, operations with unknown outcomes), and a comparison
  against a brute-force search over all orderings on 3,000 random small
  histories. The checker and the brute-force search must agree on every
  one, and both answers must be common in the sample.

- **Transport tests** (`crates/transport`): every Raft message and every
  error survives encoding to protobuf bytes and back, and incomplete
  messages are rejected. Over real TCP connections on localhost:
  1,000 messages arrive in order; a peer waits for a node that is not up
  yet, and reconnects after the node is killed and restarted (the node runs
  in a runtime of its own, so killing it closes its connections like a
  crashed process); a message for the wrong node is refused; and the client
  API round-trips every operation against a real `KvStore`, including a
  retried compare-and-swap that gets its original answer, rejections, leader
  hints and a request without a header.

  The simulator does not cover this crate; the real-process harness
  (milestone 14) will.

- **Cluster tests with the client library** (`crates/client/tests/cluster.rs`):
  three nodes in one process, each in a Tokio runtime of its own so it can
  be killed together with its connections. A client that only knows a
  follower finds the leader and runs every operation, with reads through
  ReadIndex and through the log; acknowledged writes
  survive the leader being killed; the old leader restarts from its data
  directory and catches up as a follower; a node without its data
  directory refuses to start.
- **Process tests** (`crates/cli/tests/cluster.rs`): five real
  `raft-kv server` processes, driven through the `raft-kv` command line.
  They elect a leader and serve every operation; the leader is killed with
  `SIGKILL`, the other four elect a new one and keep every acknowledged
  write; the old leader restarts from disk and catches up.

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
| Partitions | symmetric splits, isolated nodes, and one-way link cuts; about one per second, healing after a quarter of a second on average |
| Leader isolation | 0.2 % per tick: the current leader is cut off from all other nodes for 100–300 ticks, whatever else heals |
| Crashes | 0.1 % per node and tick, down for 10–150 ticks, restarted from persisted state |
| Leader crashes | 2 % per tick for the current leader |
| Pauses | 0.1 % per node and tick, frozen for 5–60 ticks |
| Crashes while writing | 0.2 % of writes crash after 0–7 disk operations |

Nodes store their state through the real `storage` crate on a `SimDisk`. A
crash loses everything not yet synced, except that a random part of
unsynced appended bytes may survive, like a write that partly reached the
disk. The node then restarts by recovering from that disk.

Each seed also picks how many entries an `AppendEntries` may carry (1 to
1,024). Small batches make followers catch up piece by piece, which opens
timing windows that some bugs need.

Five clients run a key-value workload against the cluster, the way the
client library will:

- Each client opens a session (`RegisterClient`) and then sends one request
  at a time: gets, puts, deletes and compare-and-swaps on three keys. Few
  keys mean many concurrent operations on each. Three in four gets use
  ReadIndex; the rest go through the log, so both read paths are checked.
- Every written value is unique (`client.sequence`), so a read shows exactly
  which write it saw.
- A request goes to the node the client believes is leader, or to a random
  node. A follower that knows the leader points the client there. A client
  that gets no answer in time tries another node, and one operation in ten
  starts at a random node, as a newly connected client would.
- The leader that accepted a proposal answers the client once it applies
  that entry, but only if the entry at that index is still the one it
  proposed. If it loses leadership first, it tells the client, which retries
  elsewhere with the same sequence number. A ReadIndex read is answered once
  the leader has confirmed it and applied up to its index. 5 % of responses
  are lost on the way back, which also forces retries.
- After 400 ticks a client gives up. The operation's outcome is then
  **unknown**: it may still be applied later, or never.

Every invocation and completion is recorded in a history for the
linearizability check.

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
log and have applied all of it (**liveness**). Every node's key-value state
must then be identical. A panic inside a node also fails the run.

Finally, every key is read once more, and the whole history must be
**linearizable** (see below). The final reads turn a lost acknowledged write
into a visible stale read.

### Linearizability checking

A history is linearizable if every operation can be placed at one point in
time between its invocation and its completion, so that executing the
operations one by one in that order gives exactly the results the clients
saw. This is the guarantee raft-kv promises to clients, and the only check
that looks at the system from the outside: the Raft invariants above can
all hold while clients still see wrong answers, for example if the state
machine applied a retried request twice.

The checker (`sim::linearizability`) implements the algorithm of Wing and
Gong with Lowe's memoization, as Porcupine does:

- It searches for a valid order depth first, and caches every pair of
  (operations placed so far, register value). A state reached along two
  different orders is explored only once, which keeps the search fast in
  practice although it is exponential in the worst case.
- Keys are independent registers, so a history is linearizable exactly if
  each key's part of it is. Every key is checked on its own.
- Operations with an unknown outcome complete "at the end of time": the
  search may place them anywhere after their invocation, including after
  everything else, which is the same as never.
- A step budget turns a search that runs away into an explicit failure
  instead of a hang. Typical runs stay far below it.
- A failure reports the longest valid order the search found and the
  operation that could not follow it.

Across 1,000 seeds, a run checks a median of about 330 completed operations,
including about 80 ReadIndex reads, and 75 % of runs include operations with
unknown outcomes.

### Does it find bugs?

A simulator that never fails proves nothing on its own. To check that it can
find real bugs, `scripts/sim-mutations.sh` injects known bugs into
`raft-core`, the storage layer and the key-value state machine, one at a
time, and counts how many seeds detect each:

| Injected bug | Seeds that caught it (of 1,000) | Detected as |
|---|---|---|
| Vote twice in one term | 934 | election safety |
| Skip the election restriction | 999 | leader completeness |
| Commit an older term's entry by counting replicas (Figure 8) | 9 | leader completeness |
| Follower commits beyond what the leader's message proved | 767 | state machine safety |
| Storage: shorten a WAL segment before removing the segments after it | 3 | recovery |
| ReadIndex: serve reads without confirming leadership | 14 | linearizability |
| ReadIndex: serve reads before the leader commits in its term | 592 | linearizability |
| State machine: apply a retried request again | 967 | linearizability |

Measured with `scripts/sim-mutations.sh 1000` on the default configuration.

The Figure 8 and follower-commit bugs need rare interleavings: a leader that
crashes right after its election, and a badly delayed message. They were not
caught at all until the simulator gained targeted leader crashes, latency
spikes and small append batches. The client workload made the
follower-commit bug far easier to hit (1 seed in 1,000 before, 767 now),
because clients retry and re-route as soon as a leader steps down. The
Figure 8 bug is still caught only rarely, so a quick local run can miss it;
the nightly run of 10,000 seeds covers it. Both are also covered by
dedicated unit tests.

Serving reads without confirming leadership was at first caught by **no**
seed at all. It needs a deposed leader that still believes it leads while
the new leader completes writes, and random partitions heal too quickly
for that. Only after adding long leader isolations (Jepsen's
partition-leader), and making clients switch nodes after a timeout like the
client library does, did seeds start to fail, each with a textbook stale
read: a client reads a value it had already seen overwritten. It is still
a rare interleaving, covered properly by the nightly run and by the
dedicated unit test.

Applying a retried request twice breaks none of Raft's invariants: every
node applies the same log in the same order. Only the linearizability check
sees it, because clients get answers that no single execution order can
explain.

Not every bug is reachable from the simulator. Writing a `Ready`'s entries
before its hard state can leave a node whose term is lower than its last
entry's term, but Raft's vote majority keeps that node from winning an
election in such a term, so no seed ever turns it into a visible violation.
The storage crash-point test checks that invariant directly instead.

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
