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

The caller interacts with a `Raft` node through a handful of calls:

| Call | Meaning |
|---|---|
| `tick()` | One unit of logical time has passed. The caller picks the tick length (e.g. 10 ms) |
| `step(message)` | A message from another node arrived |
| `propose(command)` | Submit a command; only the leader accepts it |
| `read_index(id)` | Start a linearizable read; only the leader accepts it (see [Read path](#read-path)) |
| `compact(index, data)` | Hand over a snapshot of the state machine, so the log before it can go (see [Snapshots](#snapshots)) |
| `take_ready()` | Collect what the calls above produced: a `Ready` with the hard state, snapshot and entries to persist, the messages to send and the committed entries to apply |

The caller handles a `Ready` in a fixed order:

1. Make `hard_state`, `snapshot` and `entries` durable.
2. Send `messages`.
3. If there is a `snapshot` (one received from the leader), replace the
   state machine with it. Then apply `committed_entries`.

Persisting first is what makes votes and acknowledgements binding. Otherwise
a node could promise its vote, crash, forget the promise and vote again in
the same term, or acknowledge an entry it then loses.

All timeouts are counted in ticks, and the only randomness (election timeout
jitter) comes from a seeded generator in the configuration. The same
configuration and the same sequence of calls always produce the same outputs,
which is what the simulator relies on.

### The log and snapshots

`Log` addresses entries by their global `LogIndex`, but stores only the
entries after its start position (`snapshot_index`, 0 for a log that was
never compacted). Every lookup goes through that offset, so the rest of the
protocol does not care whether the log was compacted. The start need not be
the latest snapshot's index: a node keeps a tail of entries before its
snapshot (see [Snapshots](#snapshots)).

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

Implemented as in Raft §5.3 and §5.4.2.

### On the leader

- A new leader appends a **no-op entry** in its own term before anything
  else, and sends it to every follower immediately. A leader may only commit
  entries of its own term by counting replicas, so the no-op lets it commit
  whatever earlier terms left behind. ReadIndex reads depend on it too.
- `propose` appends the command to the leader's log. It goes out right
  away to every follower that has no `AppendEntries` waiting for an answer;
  for the others it waits (see [Batching](#batching)). Either way, it does
  not wait for the next heartbeat.
- For each follower the leader tracks `next_index` (the next entry to send)
  and `match_index` (the highest entry known to be on the follower). Every
  `AppendEntries` carries the entries from `next_index` on, capped at
  `max_entries_per_append` entries and about `max_append_bytes` bytes (1 MiB),
  but always at least one. Heartbeats are the same message, so they also
  carry any entries a follower is still missing.
- After a successful response the leader raises `match_index` (never lowers
  it, since responses can be reordered). If that is progress and the
  follower is still behind, it sends the next batch. An answer that shows
  no progress, such as a duplicate or the answer to a heartbeat, sends
  nothing: otherwise every heartbeat would start another chain of appends
  (see the [bug log](bugs.md)).
- The leader **commits** the highest index stored on a majority (counting
  itself), but only if that entry is from the current term (the Figure 8
  rule). Earlier entries then become committed along with it.

### On a follower

- **Consistency check:** the follower accepts entries only if its log has an
  entry at `prev_log_index` with `prev_log_term`.
- **Fast backtracking:** if the check fails, the follower tells the leader
  where to continue. If its log is too short, that is its last index + 1. If
  the terms differ, it sends the conflicting term and the first index of that
  term in its log. The leader then jumps past its own entries of that term,
  or straight to the hint if it has none, skipping a whole term per round
  trip instead of one entry.
- **Appending:** entries already present with the same term are kept. The
  first entry with a different term and everything after it are replaced.
  This way a delayed or duplicated message can never remove entries that a
  newer message added. Overwriting a committed entry would violate Raft's
  safety argument; the node panics instead of doing it.
- **Commit:** the follower sets its commit index to
  `min(leader_commit, last index of this message)`. Entries after what the
  message proved may still differ from the leader's log, so they are not
  committed yet.

### Batching

Under load, many writes arrive while the leader's last `AppendEntries` to a
follower is still on its way. Sending each one at once, with everything the
follower has not acknowledged yet, would put most entries on the wire many
times. Instead (`batch_appends`, on by default):

- a follower with **nothing in flight** gets a new entry at once, so an idle
  cluster pays no extra latency;
- for a follower with an append **in flight**, new entries wait. The answer
  to that append sends all of them in one message, up to the size limits;
- a lost message or answer leaves the batch waiting until the next
  heartbeat (50 ms), which resends everything the follower is missing.

The node loop batches too: everything that arrives while it handles one
round is taken in the next, so a round's entries share one fsync and one
message per follower.

This is stop-and-wait per follower: one append in flight at a time. In
fault-free simulations it cuts the entries sent per committed entry by
40–50 %. It can add up to one round trip of latency to a write that arrives
just after a send, so with clients that each wait for their last answer,
the unbatched mode commits 15–20 % more. Allowing several appends in flight (pipelining, as etcd does)
would remove that cost. On one machine the [benchmarks](benchmarks.md) show
no difference beyond run-to-run noise either way, because every node's
fsync waits on the same disk; the question needs separate machines.

**The leader's fsync overlaps with the network** (`send_before_persist`,
Raft dissertation §10.2.1). A leader's `AppendEntries` carry its own log and
promise nothing about its disk, so the node loop sends them before it
persists the round, and only holds back votes and answers until the fsync
is done. The leader still counts itself towards a majority only for entries
on its disk: answers are handled in a later round, after this round's
fsync. If the leader crashes before its fsync, followers may hold entries
it lost; they were never committed, and the next leader overwrites them as
usual. The simulator runs every seed with each of these on or off.

### Applying

Every node hands committed entries to the state machine through
`Ready::committed_entries`, strictly in order and exactly once. The commit
index is not persisted: after a restart it begins at the snapshot index and
catches up from the leader's next message.

## Read path

Reads use **ReadIndex** (Raft dissertation §6.4): they are linearizable but
write nothing to the log and need no fsync. See
[ADR 0003](adr/0003-readindex-over-leases.md) for why leader leases are not
used.

`Raft::read_index(id)` accepts a read on the leader. The read becomes a
`ReadState { id, index }` in `Ready::read_states` once two things hold:

1. **The leader knows the commit index the read must observe.** A new
   leader's commit index can be behind what the previous leader already
   committed, so reads wait until the leader has committed an entry in its
   own term, its no-op. The read's index is the commit index from then on.
2. **The leader was still leader after the read arrived.** It sends a round
   of `AppendEntries` and waits until a majority, counting itself, has
   answered that round. Each `AppendEntries` carries the leader's current
   `read_round`, which the follower echoes; answers to messages sent before
   the read carry an older round and do not count. Any answer in the
   leader's term counts, even a failed consistency check, since it shows
   the follower still accepts this leader. If another node had become
   leader in a later term, that majority could not have answered.

The server then answers from its key-value store as soon as it has applied
everything up to the read's index.

**Batching:** at most one round is in flight. Reads that arrive meanwhile
wait for the next round, which starts as soon as the current one is
confirmed, so any number of concurrent reads costs at most two rounds.
Ordinary replication messages carry the round too, so under write load
confirmation often comes for free.

**Leadership changes:** a leader that steps down drops its unconfirmed
reads, and the server answers them with `NotLeader`. A read that was
already confirmed is still answered: it was valid when it was confirmed.

**Reads through the log** remain available with `read_mode = "log"` in the
node's configuration: the leader proposes a `Read` command and answers when
it is applied. This costs an fsync per batch on every node and exists to
measure what ReadIndex saves.

## Storage and recovery

The `storage` crate makes a `Ready`'s hard state and entries durable before
`persist` returns. All file access goes through a `Disk` trait: `FsDisk` uses
a real directory, and `SimDisk` keeps files in memory and loses every
unsynced change on a simulated crash.

### Files

| File | Contents |
|---|---|
| `00000000000000000001.wal`, … | WAL segments, each named after the index of its first entry |
| `hardstate` | Current term and vote |
| `snapshot` | The latest snapshot: its index, term and the serialized state machine |
| `hardstate.tmp`, `snapshot.tmp` | Only exist briefly while a file is replaced |

A WAL record is `len: u32 | crc32: u32 | payload`, where the payload is an
encoded entry: index, term, kind (no-op or command) and the command bytes. A
new segment starts once the current one would exceed `segment_size` (64 MiB
by default).

### Write order

`persist` writes in this order, and every step is synced before the next:

1. **Hard state**, replaced atomically: write `hardstate.tmp`, sync it,
   rename it over `hardstate`, sync the directory. A crash leaves either the
   old or the new hard state, never a mix.
2. **Replaced entries**, if the new entries overwrite part of the log:
   remove later segments and sync the directory, *then* shorten the segment
   that holds the first replaced entry and sync it. In the opposite order, a
   crash could bring the removed segments back after a shortened one, a gap
   recovery cannot accept.
3. **New entries**, appended to the current segment (or a new one, followed
   by a directory sync) and synced.

The hard state goes first because a node that learns of a new term gets the
term and that term's entries in the same `Ready`. Written the other way
round, a crash in between would leave entries from a later term than the
node's current term.

On macOS and iOS, `std::fs::File::sync_all` issues `F_FULLFSYNC`, so data
reaches stable storage and not just the drive's cache.

### Recovery

On startup, `Storage::open`:

- removes a leftover `hardstate.tmp` or `snapshot.tmp` (its rename never
  happened);
- reads and checks the hard state and the snapshot, and fails if either is
  damaged;
- reads every segment in order and checks that segment names and entry
  indices are contiguous;
- treats an invalid record at the **end of the last segment** as an
  interrupted write and cuts it off. A record counts as torn if it runs past
  the end of the file, is the last thing in the file, or is followed only by
  zeros (space the file system allocated but never wrote);
- fails with `StorageError::Corrupt` on any other damage, such as an invalid
  record with valid data after it, or a missing segment. Skipping damaged
  entries silently could lose acknowledged writes;
- checks that the log continues the snapshot (see
  [Snapshots](#snapshots)).

## Key-value state machine

The `kv` crate is the state machine Raft replicates. Every node applies the
same log entries in the same order to its own `KvStore`, so all nodes hold
the same data and give the same answers.

### Commands in the log

Each log entry holds an encoded `Envelope`: the leader's timestamp and a
command. The commands are:

| Command | Effect | Response |
|---|---|---|
| `RegisterClient` | Opens a session; its id is the entry's log index | `Registered { client_id }` |
| `Put { key, value }` | Sets the key | `Written` |
| `Delete { key }` | Removes the key | `Deleted { existed }` |
| `CompareAndSwap { key, expected, new }` | Sets `key` to `new` if its value equals `expected` (`None` = absent) | `Swapped { succeeded, previous }` |
| `Read { key }` | Reads through the log, without a session | `Value(..)` |
| `Get { key }` | Reads through the log within a session (used by the simulator) | `Value(..)` |

Normal reads do not go through the log; they use ReadIndex and read the
local store. Keys are at most 1 KiB and values at most 1 MiB by default;
larger ones are rejected when applied, identically on every node.

### Sessions and deduplication

A client registers once and then numbers its requests 1, 2, 3, … The store
keeps, per session, the latest request number and its response:

- a request with the **same** number is a retry: it gets the stored
  response and changes nothing;
- a request with a **lower** number is a delayed duplicate: it is rejected
  as `StaleSequence`, because the client already moved on;
- a request with a **higher** number is applied and becomes the latest.

Without this, a client that retries after a leader change, not knowing
whether its first attempt was committed, could apply a write twice. A
retried compare-and-swap would then report failure for a write that had in
fact succeeded.

Each client has at most one request in flight, so remembering one response
per session is enough.

### Expiry

Sessions that have been idle for 5 minutes are removed. Time comes from the
leader's timestamps in the log, never from a node's own clock, so all nodes
expire the same sessions at the same log position. See
[ADR 0004](adr/0004-session-expiry-by-leader-timestamps.md).

## Network

The `transport` crate carries two gRPC services, both defined in `/proto`.
It only moves messages and translates types; leaders, retries and
replication are other crates' business.

### Between nodes

Raft messages are one-way: a response is a message of its own, not the
result of a call (see [ADR 0005](adr/0005-one-way-message-streams.md)). So
the `Raft` service has a single method, `Send`, which takes a stream of
messages. Each node keeps one stream open to every peer:

- a `Peer` queues messages for one node and returns at once; a background
  task owns the connection and writes the queue into the stream in order;
- if the connection breaks, the task reconnects with exponential backoff
  (50 ms up to 100 ms, well below the election timeout, so that a node
  that comes back hears from the leader before it starts an election), and
  HTTP/2 pings notice a peer that stopped answering;
- if a peer is unreachable long enough for its queue (4,096 messages) to
  fill up, further messages are dropped. Raft repairs lost messages, and a
  dead peer never slows down the others;
- the receiving `RaftService` checks each message and passes it to the local
  node through a channel. A malformed message, or one addressed to another
  node (a misconfigured cluster), ends the stream with an error.

### Client API

The `KeyValue` service offers `RegisterClient`, `Get`, `Put`, `Delete` and
`CompareAndSwap`. Writes carry a header with the session id and sequence
number. Every response holds either its result or a typed error:

| Error | Meaning | What the client does |
|---|---|---|
| `NotLeader` | This node is not the leader; carries the leader's id and address if known | Retries at the hinted node |
| `SessionExpired` | The session is unknown or expired | Registers again; the last write's outcome is unknown |
| `StaleSequence` | The request is older than the session's latest | Nothing; it is a delayed duplicate |
| `KeyTooLarge`, `ValueTooLarge` | A size limit was exceeded | Gives up |
| `Unavailable` | No answer in time, e.g. no leader or no majority | Retries with the same sequence number |
| `Internal` | A bug or a corrupt entry | Gives up |

gRPC status codes are used only for requests the server cannot interpret
and for failures of the call itself. Either way, a write whose call failed
has an unknown outcome and is retried with the same sequence number.

`KvService` turns requests into calls on a `KvHandler`, which the server
implements, and `KvClient` is the matching typed client for a single node.

## Running a node

The `server` crate turns the pieces into a process. `raft-kv server
--config node1.toml` reads the node's id, listen address, data directory
and peers from a TOML file, recovers its state from the data directory, and
serves both gRPC services on one address.

### The node loop

One task owns the node's state: `Raft`, its `Storage`, the `KvStore` and
the clients waiting for answers. The gRPC handlers talk to it through
channels, so nothing needs a lock. Each round of the loop:

1. takes one event (a tick, a message from a peer or a client request), then
   drains whatever else is already waiting, up to 256 of each;
2. sends a leader's `AppendEntries`, then persists the resulting hard state
   and entries with one fsync for the whole round;
3. sends the other messages (votes and answers) only now, once what they
   promise is on disk;
4. applies committed entries to the `KvStore` and answers waiting clients.

A client request becomes a proposal, remembered with its index and the
leader's term. It is answered when the entry at that index is applied, but
only if the entry still has that term; otherwise it was replaced in a leader
change, and the client gets `Unavailable` and retries. When the node stops
being leader, every waiting client gets `NotLeader` right away. Their
entries may still commit under the next leader, which is why retries carry
the same sequence number. A request that does not commit within the request
timeout (5 s) gets `Unavailable`.

A tick is 10 ms, so elections time out after 150–300 ms and leaders send
heartbeats every 50 ms. If a write to disk fails, the node stops: it can no
longer keep the promises its votes and acknowledgements made.

### A lost data directory

A node refuses to start if its data directory does not exist, unless it is
started with `--init`. With an empty log and no memory of its votes, a node
that lost its disk could vote twice in one term or help elect a leader that
lacks committed entries. Bringing such a node back safely needs membership
changes (spec F3); `--init` is only for a node that has never run.

### The client library

A `Client` gets the addresses of all nodes and finds the leader itself. It
sends each request to the node it believes is leader and follows
`NotLeader` hints. When a node does not answer, is not the leader and knows
of none, or reports `Unavailable`, the client moves on to the next node,
backing off from 50 ms up to 1 s, until the request's deadline (30 s)
passes. Writes are retried with the same sequence number, so a write is
applied at most once. A client has one session and one request in flight,
so a program with several concurrent workers uses one client per worker.

## Snapshots

Without compaction the log grows forever, and a restarted node replays all
of it. Snapshots (Raft §7) bound both.

### Taking a snapshot

A node counts the command bytes it applies. Once they reach
`snapshot_threshold_bytes` (64 MiB by default; a byte count, because an
entry count would fire every second at high throughput and rarely with
large values), it snapshots its `KvStore`:

1. It clones the store. The store's maps are persistent (`imbl::OrdMap`),
   so the clone shares all data and costs almost nothing (see
   [ADR 0006](adr/0006-persistent-maps-for-snapshots.md)).
2. A blocking thread serializes the clone, while the node loop keeps
   applying entries to the original.
3. Back on the node loop, `Raft::compact(index, data)` records the
   snapshot and drops the log before it, except for a tail of
   `retained_entries` (1,024 by default). Followers that are only slightly
   behind catch up from that tail instead of receiving the whole snapshot.
4. `Storage::save_snapshot` replaces the snapshot file atomically, then
   removes the WAL segments that hold only entries before the log's new
   start, oldest first.

The snapshot holds the data **and the session table**: a node restored
without its sessions would apply retried requests again (the simulator
catches exactly that when sessions are left out). It also holds the
store's logical clock, so sessions keep expiring at the same log position
on every node.

### Sending a snapshot

When a follower needs entries the leader has already compacted, the leader
sends its latest snapshot with `InstallSnapshot` messages instead:

- The snapshot goes in chunks of `snapshot_chunk_size` bytes (1 MiB),
  **stop-and-wait**: the next chunk leaves when the follower acknowledges
  the previous one with how many bytes it holds. Heartbeats resend the
  current chunk, which covers lost messages.
- The leader moves on only when an acknowledgement shows progress.
  Otherwise every duplicated or resent chunk would answer with an
  acknowledgement that starts yet another chain of chunks.
- The follower accepts only the chunk that continues what it has. If a
  transfer starts mid-way, for example after the follower restarted, it
  asks for the beginning.
- Each chunk carries the leader's `read_round`, so followers receiving a
  snapshot still help confirm ReadIndex reads.

Once the follower has every byte, it installs the snapshot:

- If its log has the snapshot's last entry with the same term, the entries
  after it are kept: they agree with the leader's log so far. Otherwise
  the whole log is discarded.
- If it has already committed past the snapshot, it ignores the snapshot
  and just acknowledges it.
- Its commit and applied index jump to the snapshot's index.
  `Ready::snapshot` hands the snapshot out: the server persists it, then
  replaces its `KvStore` with it before applying any later entries.

### Persisting an installed snapshot

A snapshot from the leader replaces the whole stored log. `Storage`
replaces the snapshot file, removes every WAL segment (newest first, each
removal synced), and writes the entries after the snapshot again. Raft
hands those out in the same `Ready`. This is simpler than working out
which stored entries survive, and installs are rare.

A crash can interrupt this at any point. Recovery therefore checks that the
WAL **continues** the snapshot: it must contain the snapshot's last entry
with the same term, or start right after it. If it does not, the WAL is
left over from before the install, and recovery finishes the job by
removing it. So a crash never leaves the old log's entries after the new
snapshot. A WAL that starts after the snapshot (a gap) is an error. The
storage tests crash both kinds of snapshot write after every disk
operation.

After a restart, the node's log starts at the first stored entry. Its
commit and applied index start at the snapshot, and the server rebuilds
its `KvStore` from the snapshot before replaying the rest of the log.
