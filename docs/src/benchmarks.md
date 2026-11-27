# Benchmarks

Measured on **2026-11-27** at commit `9f3de5e`, with every node on **one
laptop**. Read the numbers as a description of this implementation on this
machine, not as what a deployed cluster would do: the nodes share one CPU and,
more importantly, one SSD, and the network between them is the loopback
interface.

## Setup

| | |
|---|---|
| Machine | Apple M3, 8 cores, 16 GiB, internal SSD, macOS |
| Cluster | 3 nodes (one scenario: 5), all on this machine, each its own process |
| Network | loopback, round trip far below 1 ms |
| Durability | real: every round is synced with `F_FULLFSYNC` before anything it promises is sent |
| Clients | closed loop: each client has its own session and one request in flight |
| Workload | 1,000 keys written once beforehand, 100-byte values, keys picked uniformly |
| Duration | 5 s warm-up, then 20 s measured |

Every scenario starts a fresh cluster with empty data directories. The
reports, with the full configuration and machine description, are in
`bench/results/`. To reproduce all of them:

```bash
cargo build --release -p cli
scripts/bench.sh matrix
```

`scripts/bench.sh run NAME [setting=value ...] -- [options]` runs a single
scenario with any server setting and any `raft-kv bench` option; for
example, `scripts/bench.sh run test batch_appends=false -- --reads 0
--clients 64`.

## Results

Latencies are in milliseconds, from the client's point of view: from
sending the request until the answer arrived, including any retries.

| Scenario | Reads | Clients | Throughput (ops/s) | Read p50 / p99 / p99.9 | Write p50 / p99 / p99.9 |
|---|---|---|---|---|---|
| `writes` | 0 % | 32 | 1,620 | – | 19.6 / 30.1 / 37.1 |
| `mixed` | 50 % | 32 | 1,583 | 20.4 / 36.4 / 50.8 | 19.5 / 31.2 / 43.1 |
| `reads` | 95 % | 32 | 4,169 | 6.9 / 20.8 / 29.0 | 10.0 / 21.7 / 28.0 |
| `reads-through-log` | 95 % | 32 | 1,593 | 19.7 / 32.1 / 63.1 | 19.8 / 32.5 / 84.9 |
| `single-client` | 50 % | 1 | 318 | 0.35 / 1.45 / 4.32 | 4.56 / 29.3 / 43.2 |
| `writes-128-clients` | 0 % | 128 | 5,949 | – | 21.1 / 49.4 / 121.5 |
| `writes-5-nodes` | 0 % | 32 | 1,274 | – | 24.8 / 41.9 / 104.1 |
| `writes-unbatched` | 0 % | 32 | 1,804 | – | 17.3 / 31.1 / 41.6 |
| `writes-persist-first` | 0 % | 32 | 1,608 | – | 19.6 / 32.8 / 76.6 |

No request failed in any scenario.

## What the numbers show

**The disk decides everything.** A single client's write takes 4.6 ms, but
32 clients see about 20 ms each, and 128 clients still about 21 ms, while
throughput grows almost in proportion to the number of clients. So a
replication round under load takes about 20 ms whatever it carries, and
more clients simply mean bigger batches per round. The reason is the
shared SSD: `F_FULLFSYNC` flushes the drive's whole cache, and when
several processes do it at once they queue. `scripts/fsync-probe.py`
measures this directly. One process appending and syncing 200 bytes waits
3.5 ms per sync, three processes at once 7.5 ms each, five 8.5 ms. A
round needs the leader's sync and then a follower's, and under load each
waits behind the others'. With five nodes on the same disk, there is more
of that, and throughput drops by a fifth.

**ReadIndex pays off.** With 95 % reads, ReadIndex serves 2.6 times as many
reads as reading through the log, at a third of the median latency: a read
waits for one round of heartbeats instead of a log entry and its fsync on
every node. A lone client reads in 0.35 ms. Under a write load, ReadIndex
reads get slower (`mixed`: 20 ms), because followers answer the
confirmation round only between their own fsyncs.

**Batching and early sends make no measurable difference here.** Both are
meant to cut per-message and network costs, and on one machine those are
not the bottleneck. To check whether the differences in the table are real,
the three write scenarios were each run three times:

| Scenario | Runs (ops/s) | Mean |
|---|---|---|
| `writes` (batching on, early sends on) | 1,620, 1,740, 1,671 | 1,677 |
| `writes-unbatched` | 1,804, 1,708, 1,820 | 1,777 |
| `writes-persist-first` (early sends off) | 1,608, 1,635, 1,700 | 1,648 |

The runs of one scenario spread by about 7 %, as much as the scenarios
differ from each other. Unbatched replication may be slightly faster with
these closed-loop clients, as the simulator also suggested (see
[Batching](design.md#batching)): a write that arrives just after a send
waits for the next round. Sending the leader's appends before its fsync
cannot gain much when a round trip takes microseconds and a sync
milliseconds; it matters when the network is slow. Neither question can be
settled on one laptop. Both settings stay configurable so that they can be
measured on separate machines.

## History

The results above are single runs. To follow performance across
milestones, `scripts/bench.sh record LABEL` runs every scenario three
times, in whole rounds so that slow drift (heat, background work) affects
all scenarios alike, and keeps the run with the median throughput. It
appends one JSON line per scenario to `bench/history/MACHINE.jsonl`, which
is committed with the milestone's pull request, so every data point
belongs to the code it measured. Each line holds:

- the label (milestone or release), the commit and the date;
- the machine: CPU, cores, memory, OS and its version;
- the power source: `record` refuses to run on battery, where laptops
  throttle;
- an **fsync calibration**: how long a 200-byte append takes to sync with
  one process and with three at once (`scripts/fsync-probe.py`). If the
  machine gets slower or faster, for example after an OS update, this
  shows it, and the change is not mistaken for one in the code;
- the scenario, its cluster size and server settings, the three runs'
  throughputs, and the median run's throughput and latency percentiles.

**One machine, one series.** All history so far comes from one Apple M3
laptop with every node on it and real `F_FULLFSYNC`
(`bench/history/apple-m3-16gib-macos.jsonl`). The file is named after the
machine, so results from another machine start a series of their own and
are never mixed into this one. History is recorded at the end of each
milestone, by hand: a scheduled job on a laptop runs whenever the laptop
happens to be awake, on whatever power, which would make the series
noisier, not better.

## Limitations

- These are macOS numbers. The [specification](spec.md) asks for published
  numbers to come from Linux, where the storage crate syncs with `fsync`
  instead of `F_FULLFSYNC`; runs on separate Linux machines are still to
  do, and only those belong on the website.
- All nodes share one machine and one disk. On separate machines, each
  node's sync would not wait for the others', so latency under load should
  be much lower, while network round trips would add to it.
- The clients run on the same machine as the nodes and compete with them
  for the CPU.
- Keys are picked uniformly from a small set, and values are small. Large
  values, skewed keys and much larger data sets (where snapshots get large)
  are not measured yet.
- The numbers come from a laptop on mains power, with background
  processes running. Runs vary by several percent.
