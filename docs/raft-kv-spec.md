# raft-kv — project specification

A replicated, linearizable key-value store built from scratch on the Raft consensus algorithm, written in Rust. Built as a portfolio project. The goal is a system small enough to understand end to end but correct under real failures, with evidence for that correctness that a reviewer can check.

This document lists:

1. **What the portfolio website currently claims.** Everything in that section has to be true before the site goes live.
2. **The MVP requirements** that make those claims true.
3. **How each claim gets verified and measured.**
4. **A suggested milestone plan.**
5. **Future features.**

---

## 1. What the website currently says

The prototype website already describes raft-kv. Each statement below is either a **requirement** (build it) or a **placeholder** (replace with your real number or wording once you have it).

| Where on the site | Claim                                                                                                  | Type                                                                                                         | Covered in |
| ----------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ---------- |
| Project card      | "A distributed key-value store built on Raft"                                                          | Requirement                                                                                                  | §2         |
| Project card      | "Survives node failures with zero data loss"                                                           | Requirement (true only while a *majority* of nodes keep their disks, see §2.4)                               | §2.4, §3   |
| Project card tags | Rust, gRPC                                                                                             | Requirement                                                                                                  | §2.1       |
| Project card tags | "Jepsen-tested"                                                                                        | **Wording to fix.** Say "fault-injection tested" unless you actually run Jepsen or Maelstrom (see §3.3)      | §3.3       |
| Case study meta   | Solo project, 4 months, Rust + Tokio + gRPC, GitHub and Docs links                                     | Requirement (public repo + docs site); timeline is a placeholder                                             | §2.1, §2.9 |
| Constraint        | **Linearizable**: every read sees the latest committed write, even during a leader change              | Requirement                                                                                                  | §2.5, §3.2 |
| Constraint        | **No data loss**: an acknowledged write survives any minority of nodes crashing or being cut off       | Requirement. Crashes mean restarts with the disk intact; a node whose disk is wiped must not rejoin (§2.4)    | §2.4, §3   |
| Constraint        | **Fast enough**: target of 100k+ ops/s on five nodes on commodity hardware                             | Requirement (target)                                                                                         | §2.6, §3.4 |
| Decision 1        | Batch log entries per heartbeat. Unbatched ≈ 30k ops/s, batched ≈ 218k ops/s                           | Requirement (batching); **wording to fix**: entries are batched per replication round, not per heartbeat (a 50 ms heartbeat wait would rule out a ms-level p99). **Numbers are placeholders**: with fsync on, unbatched will likely be far below 30k. Measure both | §2.6, §3.4 |
| Decision 2        | Read-index reads instead of writing reads to the log; leader leases considered and rejected            | Requirement                                                                                                  | §2.5       |
| Decision 3        | Jepsen-style harness that partitions the network, kills nodes and checks histories for linearizability | Requirement                                                                                                  | §3.2, §3.3 |
| Decision 3        | "Found three bugs unit tests never would have, including a stale leader serving reads"                 | **Placeholder**: keep a bug log and report what you really find                                              | §3.5       |
| Architecture      | Client `put(k, v)` → leader appends to log → AppendEntries → 3 of 5 ack → apply to state machine       | Requirement                                                                                                  | §2.2       |
| Results           | 218k ops/s throughput on five nodes                                                                    | **Placeholder**: replace with your measurement                                                               | §3.4       |
| Results           | 3.8 ms p99 write latency                                                                               | **Placeholder**                                                                                              | §3.4       |
| Results           | 0 acknowledged writes lost across 10,000 fault-injection runs                                          | Requirement (the "0") + **placeholder** (the run count). **Wording to fix**: say where the count comes from, e.g. "10,000 simulated runs (§3.2) + N real-process chaos runs (§3.3)" | §3.2, §3.3 |
| Retro             | Snapshotting and log compaction were added late                                                        | Requirement: the final system must have snapshots. **Placeholder**: keep this retro only if it is what actually happened (§4 plans for the log offset from week 1) | §2.7, §4   |
| Interactive demo  | Five nodes, leader election with terms, crashing nodes, no progress below a majority                   | Requirement (the system has to behave like this); see future feature F1 to power the demo with the real code | §2.3, F1   |

> **Honesty rule for the site:** every number on the website must come from a benchmark or test run you can reproduce from the repository. Put the exact command and hardware next to each result in the README.

---

## 2. MVP requirements

### 2.1 Technology and repository layout

- **Language:** Rust (stable), async runtime **Tokio**
- **Network layer:** **gRPC** via `tonic` + `prost`, used both between nodes and from client to server
- **Single public repository** with a Cargo workspace:

```
raft-kv/
├── crates/
│   ├── raft-core/     # pure Raft state machine, no I/O, no clock, no randomness
│   ├── storage/       # write-ahead log + snapshot files, behind a `Disk` trait
│   ├── transport/     # gRPC services (Raft RPCs + client API)
│   ├── kv/            # the key-value state machine + client sessions
│   ├── server/        # library: wires core + storage + transport together
│   ├── client/        # Rust client library with leader discovery + retries
│   ├── sim/           # deterministic simulator for fault testing (§3.2)
│   ├── chaos/         # real-process fault-injection harness (§3.3)
│   ├── bench/         # load generator (§3.4)
│   └── cli/           # the single `raft-kv` binary (server, client commands, bench, chaos)
├── docs/              # docs site (mdBook)
└── proto/             # .proto definitions
```

**Key design rule:** `raft-core` must be **deterministic and I/O-free**. It takes inputs (messages, ticks, client proposals) and returns outputs (messages to send, entries to persist, entries to apply). Time and randomness are injected from outside. This one rule is what makes deterministic simulation testing (§3.2) and the WASM browser demo (F1) possible. It matches the website's retro theme of "design for it up front".

The same idea applies to `storage`: all file access goes through a `Disk` trait, so the simulator can plug in a fake disk that drops everything not yet fsynced when a node crashes. The simulator therefore covers `raft-core` + `kv` + `storage`. The Tokio/gRPC glue in `server` and `transport` is covered by the real-process harness (§3.3).

### 2.2 Write path (matches the architecture diagram)

1. The client sends `Put(key, value)` to any node. Followers reply `NotLeader { leader_hint }`. The client library follows the hint and retries.
2. The leader appends the command to its log and starts **persisting it (fsync)**.
3. **At the same time**, the leader replicates the entries to followers with `AppendEntries`. It does not wait for its own fsync first (Raft thesis §10.2.1).
4. Once a **majority (3 of 5)** have persisted the entry, it is **committed**. The leader counts itself only after its own fsync has finished. Only entries from the leader's current term are committed by counting replicas (Raft §5.4.2).
5. Committed entries are applied in order to the KV state machine, and the result is returned to the client.

### 2.3 Raft core: required features

| Feature | Details |
|---|---|
| Leader election | Randomized election timeouts (e.g. 150–300 ms), `RequestVote`, terms, at most one vote per term. The election restriction allows votes only for candidates whose log is at least as up to date as the voter's |
| Persistent state | Any change to `current_term`, `voted_for` or the log is fsynced **before** responding to the RPC that caused it. RPCs that change nothing on disk (e.g. plain heartbeats) need no fsync |
| Log indexing | The log stores entries starting after `(last_included_index, last_included_term)` from day one, even before snapshots exist (§2.7). Retrofitting this offset later is painful |
| Log replication | `AppendEntries` with the `prev_log_index` / `prev_log_term` consistency check. Delete conflicting entries. Fast backtracking: the follower returns the conflicting term and its first index so the leader can skip back a whole term at once |
| Commit + apply | `commit_index` / `last_applied` tracking, apply strictly in order, exactly once per node |
| No-op on election | A new leader commits an empty entry in its term. ReadIndex (§2.5) requires this |
| Heartbeats | An empty `AppendEntries` sent when a follower has had no message for a fixed interval (e.g. 50 ms). Heartbeats only keep leadership alive. New entries never wait for the heartbeat timer; they are sent right away (§2.6) |
| Static membership | Five-node cluster from a config file. Dynamic membership is future work (F3) |
| Step-down on higher term | Any node that sees a higher term becomes a follower immediately |

### 2.4 Durability and crash recovery

- **Write-ahead log (WAL):** append-only segment files. Each record has a length, a **CRC32 checksum** and the payload.
- **Hard state file:** `current_term` and `voted_for` live in a small separate file, updated atomically: write a temp file, fsync it, rename it over the old one, fsync the directory.
- **fsync discipline:** nothing is acknowledged (vote granted, entry acked, client answered) before the relevant data is on disk. Creating or deleting a segment file also fsyncs the directory.
- **Real durability:** on Linux use `fdatasync`/`fsync`. On macOS, `fsync` does not flush the drive cache, so use `F_FULLFSYNC` there. Durability and benchmark numbers on the website come from Linux runs.
- **Recovery:** on startup, replay the latest snapshot plus the WAL. Detect a torn or corrupt **tail** record by its checksum and truncate it. Corruption in the middle of the log is a hard error, never silently skipped.
- **Guarantee, stated precisely:** an acknowledged write survives as long as **a majority of nodes (3 of 5) keep their disks**. Up to 2 nodes can crash, restart or lose their network at any time. If 3 or more are down, the cluster **stops accepting writes** until enough nodes come back, rather than losing data. Once they restart with their disks intact, the cluster recovers with nothing lost. The website should never suggest it survives more than that.
- **Lost disks:** a node whose disk was wiped must **not** rejoin under its old identity. With an empty log and an empty `voted_for` it could vote twice in one term or help form a majority that has lost committed entries. Bringing it back safely needs dynamic membership (F3). Until then the node refuses to start without its data directory, and the chaos harness never wipes disks.

### 2.5 Linearizable reads (ReadIndex)

Reads must not go through the log, but they must also never return stale data:

1. The leader records the current `commit_index` as the `read_index`. This requires the no-op from the current term to be committed first.
2. The leader sends one heartbeat round and waits for a majority to respond. That proves it is still the leader.
3. The leader waits until `last_applied ≥ read_index`, then answers the read from local state.

Concurrent reads share one confirmation round (batch them). **Leader leases are explicitly not used in the MVP**, because their correctness depends on bounded clock drift. They are an opt-in future feature (F6).

### 2.6 Performance: batching

- When a follower's link is idle, a new proposal is sent to it **immediately**. All proposals that arrive while a replication round to that follower is in flight are collected and sent together in the **next `AppendEntries`**, with one fsync per batch on every node (group commit). Under low load this behaves like no batching (low latency); under high load batches grow on their own (high throughput).
- Make batching configurable (`max_batch_entries`, `max_batch_bytes`, on/off) so you can measure **with vs. without**. Decision 1 on the website depends on that comparison. "Off" means one entry per `AppendEntries` and one fsync per entry.
- **Target:** ≥ 100k ops/s on five nodes (see §3.4 for how to measure).
- **Known risk:** every client request goes through the leader as its own unary gRPC call. At 100k+ ops/s, the leader's gRPC handling (especially with all five nodes on one machine) may become the bottleneck before Raft does. Profile early. If needed, add a streaming client API or client-side request batching, and say so in the write-up.

### 2.7 Snapshots and log compaction

- The KV state machine can serialize itself to a snapshot file at a given `(last_included_index, last_included_term)`. The snapshot contains the key-value data **and the client session table** (§2.8). Without the sessions, a node restored from a snapshot would apply retried requests twice.
- After a successful snapshot, log entries up to that index are deleted (keeping a configurable tail so slightly lagging followers can still catch up from the log).
- Snapshots are triggered automatically by **log size in bytes** (e.g. 64 MB). An entry-count trigger like "every 100k entries" would fire every second at the target throughput.
- **`InstallSnapshot` RPC:** a leader streams its snapshot, in chunks, to a follower that is too far behind for the log alone. The follower keeps any log entries after the snapshot that match it, and discards the rest.
- Writing a snapshot must not block the apply loop for long. A plain `HashMap` cannot be copied cheaply, so either use a persistent map with cheap clones (e.g. the `im` crate) and serialize the clone in a background task, or accept a short pause and measure it.

### 2.8 Key-value API (gRPC)

| Call | Semantics |
|---|---|
| `Put(key, value)` | Write. Linearizable, goes through the log |
| `Get(key)` | Read. Linearizable, uses ReadIndex |
| `Delete(key)` | Write, goes through the log |
| `CompareAndSwap(key, expected, new)` | Write that returns whether it succeeded. `expected` is optional, so "only if the key does not exist" can be expressed. **Needed for meaningful linearizability tests** (register workloads) |

- **Client sessions and deduplication:** every write carries `(client_id, sequence_number)`. The state machine remembers each client's last applied sequence number and response, so a retried request is never applied twice and gets the original answer. Without this, retries after a leader change break linearizability.
  - A client registers through the log (`RegisterClient`) and receives its `client_id`.
  - Each `client_id` has at most **one request in flight**. A load generator with N concurrent workers uses N sessions.
  - Sessions **expire** after a period of inactivity, so the table stays bounded. Expiry must be deterministic on every node: use a timestamp the leader writes into each log entry, never a node's local clock (Raft thesis §6.3). A request from an expired session fails with `SessionExpired` instead of being applied.
  - The session table is part of every snapshot (§2.7).
- Keys and values are bytes, with configurable size limits (e.g. key ≤ 1 KB, value ≤ 1 MB).

### 2.9 Operability and documentation

- **CLI:** `raft-kv server --config node1.toml`, `raft-kv put/get/del/cas`, `raft-kv status` (shows role, term, commit index, peers), `raft-kv bench ...`, `raft-kv chaos ...`
- **Local cluster:** `docker compose up` or a script that starts five nodes on one machine
- **Metrics:** Prometheus endpoint with term, role, commit and apply index, election count, replication lag per follower, batch size histogram, fsync latency, request latency
- **Structured logs:** `tracing`, including term and node id
- **README:** what it is, a diagram, the guarantees as stated in §2.4, a quickstart, and how to reproduce every number on the website
- **Docs site** (the "Docs" link on the website): mdBook with a design doc, the testing methodology and the bug log

---

## 3. Verification: proving the website's claims

### 3.1 Unit and property tests

- Unit tests for `raft-core` transitions: elections, log conflict handling, the commit rule, snapshot installation
- Property tests (`proptest`) for WAL encoding, decoding and recovery with randomly truncated files

### 3.2 Deterministic simulation testing (main correctness tool)

Run whole clusters in a single process, with a simulated network, clock and disk, all driven by one **random seed**:

- **Faults:** message drop, delay, reordering and duplication; network partitions (including asymmetric ones); node crash and restart (losing anything not fsynced, via the fake `Disk` from §2.1); torn writes at the tail of the WAL. Clock jumps add little while leader leases are not used, but become essential once F6 exists
- **Workload:** random concurrent `Put`, `Get` and `CAS` operations from several simulated clients
- **History:** record every operation's invocation and completion (ok / failed / unknown)
- **Checks after every run:**
  - **Linearizability** of the history, using the Wing–Gong / Porcupine algorithm. Port it to Rust, or export JSON and check it with Porcupine. The check is exponential in the worst case, so **split the history by key** (each key is an independent register) and keep the number of concurrent clients and the history length bounded per run.
  - **Raft invariants:** at most one leader per term; log matching; once an entry is committed, every later leader has it; state machine safety
  - **No lost writes:** every acknowledged write is visible afterwards
- Any failing seed must reproduce exactly. Store failing seeds as regression tests.
- **This is where the website's "10,000 runs" number comes from.** Run N seeds in CI or a nightly job, and report the real N. On the website, call these **simulated** runs; real-process runs from §3.3 are counted and reported separately.

### 3.3 Real-process fault injection (the "Jepsen-style" harness)

The simulator tests the algorithm. This harness tests the real binary, network and disk:

- Start five real `server` processes.
- **Faults:** `kill -9`, pausing processes with `SIGSTOP`/`SIGCONT`, and network partitions through a small TCP proxy between nodes (or `iptables` / `tc` in Docker). Killed nodes always restart with their data directory intact (see "Lost disks" in §2.4)
- Concurrent clients record histories, which are checked with the same linearizability checker.
- Run it on Linux with real fsync, so a "0 lost writes" result actually means something (§2.4).
- **Tag wording:** if you only use your own harness, call it "**fault-injection tested**" or "**Jepsen-style**". Say "**Jepsen-tested**" only if you also run actual Jepsen, or the **Maelstrom** `lin-kv` workload, against it. Maelstrom is a low-effort path: write a small stdin/stdout adapter around `raft-core`.

### 3.4 Benchmarks

- **Load generator:** N concurrent clients, a configurable read/write mix (e.g. 100% writes, 50/50, 95/5), 16-byte keys and 128-byte values, and a fixed duration with a warm-up period
- **Latency:** recorded with HdrHistogram; report p50, p99 and p99.9
- **Load:** enough concurrent clients (each with its own session, §2.8) to saturate the cluster. By Little's law, 200k ops/s at ~2 ms latency needs about 400 requests in flight
- **Required comparisons for the website:**
  - batching off vs. on (Decision 1), with "off" defined as in §2.6. Expect a much larger gap than the 30k → 218k placeholders when fsync is on
  - ReadIndex reads vs. reads through the log (Decision 2)
- **Disclose the setup:** CPU, disk type, whether all five nodes share one machine or run on separate VMs, network latency, and fsync on or off. Numbers from one machine with no fsync are not comparable to real deployments. Say which you used.
- The website's **218k ops/s** and **3.8 ms p99** are placeholders. Replace them with your measured values.

### 3.5 Bug log

Keep `docs/bugs.md` from day one. For each bug, record:
- how it was found (simulation seed, chaos run, test)
- the symptom
- the root cause
- the fix
- the regression test

The website's "found three bugs …, including a stale leader serving reads" must come from this file.

---

## 4. Suggested milestones (~16 weeks)

| Weeks | Milestone | Done when |
|---|---|---|
| 1–2 | `raft-core` election + replication, in-memory, with the log offset (`last_included_index`) built in from the start | Unit tests pass, and a 3-node in-memory cluster elects a leader and replicates |
| 3–4 | Deterministic simulator + invariant checks | Thousands of seeds pass with drops, partitions and crashes |
| 5–6 | WAL + hard state file + crash recovery behind the `Disk` trait | Simulated crashes that lose unsynced data don't violate safety |
| 7 | KV state machine, client sessions (with expiry), CAS | Linearizability checker passes on simulated histories |
| 8–9 | gRPC transport, server binary, client library, CLI | A 5-node cluster runs locally with `docker compose` |
| 10 | ReadIndex + no-op on election | Read latency improves, and the checker still passes |
| 11 | **Snapshots + InstallSnapshot** (session table included) | The log stays bounded in long runs, and lagging followers catch up |
| 12 | Batching + benchmarks (on Linux, fsync on) | Before and after numbers recorded, gRPC bottleneck profiled |
| 13–14 | Real-process chaos harness (+ optional Maelstrom) | N runs with 0 lost acknowledged writes |
| 15 | Metrics, README, docs site | Every website number is reproducible from the README |
| 16 | Update the website | Placeholders replaced with real numbers, bugs and wording |

The plan is tight. The simulator, the checker, the chaos harness and snapshots each tend to take longer than expected. If time runs out, the Maelstrom adapter (§3.3) is a cheaper substitute for the full chaos harness.

---

## 5. Future features

Ordered roughly by how much they add to the portfolio story.

| # | Feature | Why it's interesting |
|---|---|---|
| **F1** | **Compile `raft-core` to WebAssembly and power the website demo with it** | The demo on the site becomes the real algorithm, not a JavaScript imitation. That makes a memorable "this is the actual code" moment. It only works if `raft-core` stays I/O-free (§2.1) |
| F2 | **PreVote + CheckQuorum** | Stops a node coming back from a partition from disrupting the cluster with higher terms. A leader without a majority steps down instead of staying leader. (The current website demo keeps the leader in that case. Update the demo to match.) |
| F3 | **Dynamic membership** (single-server changes or joint consensus) | Add and remove nodes without downtime. One of the hardest parts of Raft to get right |
| F4 | **Leadership transfer** | Drain a node for maintenance, with a graceful handover via `TimeoutNow` |
| F5 | **Learners (non-voting members)** | New nodes catch up before they get a vote |
| F6 | **Leader leases (opt-in)** | Faster reads under an explicit clock-drift bound. A nice contrast to Decision 2 |
| F7 | **Follower reads** | Read scaling using ReadIndex forwarded from followers |
| F8 | **Multi-Raft sharding** | Range-partitioned keys, many Raft groups, a placement driver, split and merge. Turns it into a "real database" story |
| F9 | **Watch / subscribe API** | Stream changes to a key or prefix, like etcd |
| F10 | **Range scans + MVCC** | Versioned keys, snapshot reads, compaction of old versions |
| F11 | **Transactions** | Multi-key atomic operations (within a shard, then across shards with two-phase commit) |
| F12 | **Formal model** | A TLA+ spec of the protocol, or model checking with `stateright`. Very strong signal for systems roles |
| F13 | **mTLS + authentication** | Production-style security between nodes and clients |
| F14 | **Pluggable storage engine** | Swap the in-memory map for RocksDB or your own small LSM tree |
| F15 | **Pipelined replication** | Multiple AppendEntries in flight per follower, with flow control (the option Decision 1 rejected) |
| F16 | **Kubernetes deployment** | Helm chart or operator, plus a Grafana dashboard for the metrics |
| F17 | **Clients in other languages** | Go and TypeScript clients generated from the `.proto` files |

---

## 6. Non-goals (for the MVP)

- Byzantine fault tolerance (Raft assumes nodes don't lie)
- Running across regions or over a WAN
- Persistence beyond a single disk per node (no erasure coding or backups)
- Using an existing Raft library (`raft-rs`, `openraft`). The point is to build consensus yourself. Read them for reference only.

---

## 7. References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014 — the Raft paper
- Ongaro, *Consensus: Bridging Theory and Practice*, PhD thesis, 2014 — ReadIndex, membership changes, PreVote, leadership transfer
- Jepsen (jepsen.io) and **Maelstrom** — fault-injection testing and the `lin-kv` workload
- Porcupine — fast linearizability checker (Wing & Gong algorithm)
- etcd `raft` and TiKV `raft-rs` — reference implementations to read, not to depend on
- The MIT 6.5840 (formerly 6.824) Raft labs — a good test-case checklist
