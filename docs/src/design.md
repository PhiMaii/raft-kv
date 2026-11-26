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
| `propose(command)` | Submit a command; only the leader accepts it |
| `take_ready()` | Collect what the calls above produced: a `Ready` with the hard state and entries to persist, the messages to send and the committed entries to apply |

The caller handles a `Ready` in a fixed order:

1. Make `hard_state` and `entries` durable.
2. Send `messages`.
3. Apply `committed_entries` to the state machine.

Persisting first is what makes votes and acknowledgements binding. Otherwise
a node could promise its vote, crash, forget the promise and vote again in
the same term, or acknowledge an entry it then loses.

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

Implemented as in Raft §5.3 and §5.4.2.

### On the leader

- A new leader appends a **no-op entry** in its own term before anything
  else, and sends it to every follower immediately. A leader may only commit
  entries of its own term by counting replicas, so the no-op lets it commit
  whatever earlier terms left behind. ReadIndex reads depend on it too.
- `propose` appends the command to the leader's log and sends it to every
  follower right away; it does not wait for the next heartbeat.
- For each follower the leader tracks `next_index` (the next entry to send)
  and `match_index` (the highest entry known to be on the follower). Every
  `AppendEntries` carries all entries from `next_index` on, capped at
  `max_entries_per_append`. Heartbeats are the same message, so they also
  carry any entries a follower is still missing.
- After a successful response the leader raises `match_index` (never lowers
  it, since responses can be reordered) and, if the follower is still behind,
  sends the next batch.
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

### Applying

Every node hands committed entries to the state machine through
`Ready::committed_entries`, strictly in order and exactly once. The commit
index is not persisted: after a restart it begins at the snapshot index and
catches up from the leader's next message.

## Read path

_Written in milestone 10._ See [ADR 0003](adr/0003-readindex-over-leases.md)
for the decision.

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
| `hardstate.tmp` | Only exists briefly while the hard state is replaced |

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

- removes a leftover `hardstate.tmp` (its rename never happened);
- reads and checks the hard state, and fails if it is damaged;
- reads every segment in order and checks that segment names and entry
  indices are contiguous;
- treats an invalid record at the **end of the last segment** as an
  interrupted write and cuts it off. A record counts as torn if it runs past
  the end of the file, is the last thing in the file, or is followed only by
  zeros (space the file system allocated but never wrote);
- fails with `StorageError::Corrupt` on any other damage, such as an invalid
  record with valid data after it, or a missing segment. Skipping damaged
  entries silently could lose acknowledged writes.

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
| `Get { key }` | Reads through the log (benchmark comparison only) | `Value(..)` |

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
  (50 ms up to 1 s), and HTTP/2 pings notice a peer that stopped answering;
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

## Snapshots

_Written in milestone 11._
