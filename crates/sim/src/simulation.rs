//! Runs a whole cluster in one process, driven by a single seed.

use std::collections::{BTreeMap, VecDeque};
use std::panic::{self, AssertUnwindSafe};

use kv::{Command, Envelope, KvConfig, KvStore, Operation, Rejection, Response};
use raft_core::{
    Config, EntryPayload, LogIndex, Message, MessageBody, NodeId, ProposeError, Raft,
    ReadIndexError, ReadState, Ready, Role, Term,
};
use storage::{SimDisk, Storage, StorageError, StorageOptions};

use crate::client::{Client, InFlight, Pending, PendingRead, Request, Tag};
use crate::config::SimConfig;
use crate::invariants::{Checker, Failure};
use crate::kv_history::{check_kv_history, KvOp};
use crate::linearizability::{Op, Outcome};
use crate::network::Network;
use crate::report::{Report, Stats, Violation, ViolationKind};
use crate::rng::SimRng;

/// How many recent events a violation report includes.
const TRACE_LEN: usize = 60;

/// Small WAL segments, so that runs exercise segment rollover and
/// truncation across segment boundaries.
const STORAGE: StorageOptions = StorageOptions { segment_size: 2048 };

/// Search steps the linearizability check may take per key. Typical runs
/// need a few thousand.
const CHECK_BUDGET: u64 = 20_000_000;

/// The wall-clock time at tick 0, in milliseconds. Leaders stamp commands
/// with it (plus their clock offset) for session expiry.
const EPOCH_MS: u64 = 1_700_000_000_000;

/// Milliseconds per tick.
const TICK_MS: u64 = 10;

/// How long a client waits before trying again when no node accepted its
/// request, in ticks.
const RETRY_BACKOFF: u64 = 5;

#[derive(Debug)]
enum State {
    Up {
        raft: Box<Raft>,
        storage: Box<Storage<SimDisk>>,
        /// The node's copy of the state machine, rebuilt from the log after
        /// every restart.
        kv: Box<KvStore>,
        /// Client proposals this node accepted as leader, by log index.
        pending: BTreeMap<LogIndex, Pending>,
        /// ReadIndex reads this node accepted as leader, by read id.
        reads: BTreeMap<u64, PendingRead>,
        /// Confirmed reads waiting for the state machine to reach their
        /// index.
        confirmed_reads: Vec<(LogIndex, PendingRead)>,
        /// Command bytes applied since the last snapshot.
        applied_bytes: u64,
        /// The node is paused (receives and does nothing) until this tick.
        paused_until: Option<u64>,
    },
    Down {
        restart_at: u64,
        /// The disk as it survived the crash.
        disk: Box<SimDisk>,
    },
}

/// How a client request reaches a node.
#[derive(Debug)]
enum Submission {
    /// As a proposal, through the log.
    Propose(Command),
    /// As a ReadIndex read of a key.
    Read(Vec<u8>),
}

#[derive(Debug)]
struct Node {
    id: NodeId,
    config: Config,
    state: State,
    /// The commit index last observed, to detect when a leader advances it.
    observed_commit: LogIndex,
    restarts: u64,
    /// How far this node's wall clock is ahead, in milliseconds. Leaders
    /// with different clocks stamp commands differently, which the state
    /// machine's logical clock must tolerate.
    clock_offset_ms: u64,
}

impl Node {
    fn raft(&self) -> Option<&Raft> {
        match &self.state {
            State::Up { raft, .. } => Some(raft),
            State::Down { .. } => None,
        }
    }

    fn kv(&self) -> Option<&KvStore> {
        match &self.state {
            State::Up { kv, .. } => Some(kv),
            State::Down { .. } => None,
        }
    }

    fn is_active(&self, now: u64) -> bool {
        matches!(&self.state, State::Up { paused_until, .. } if paused_until.is_none_or(|t| now >= t))
    }
}

/// A simulated cluster.
pub(crate) struct Simulation {
    seed: u64,
    config: SimConfig,
    now: u64,
    rng: SimRng,
    nodes: Vec<Node>,
    network: Network,
    checker: Checker,
    stats: Stats,
    trace: VecDeque<String>,
    fingerprint: u64,
    clients: Vec<Client>,
    /// Every client operation, for the linearizability check.
    history: Vec<KvOp>,
    /// Logical time for the history: increases with every invocation and
    /// completion, so that their order is exact even within one tick.
    history_clock: u64,
    /// The id of the last ReadIndex read, unique across the run.
    last_read_id: u64,
    /// A node cut off from all others, and until which tick.
    isolated: Option<(NodeId, u64)>,
    /// Nodes take a snapshot once they have applied this many command bytes
    /// since the last one.
    snapshot_threshold: u64,
    /// Whether leaders send their replication messages before persisting
    /// (see `Message::may_send_before_persisting`).
    send_before_persist: bool,
}

impl Simulation {
    pub(crate) fn new(seed: u64, config: SimConfig) -> Self {
        let mut rng = SimRng::new(seed);
        let max_entries_per_append = config.max_entries_per_append.unwrap_or_else(|| {
            // Small batches make followers acknowledge a log piece by piece,
            // which opens the timing windows several classic Raft bugs need.
            [1, 2, 4, 16, 1024][rng.index(5)]
        });
        // Snapshot settings vary per seed: from snapshots every few entries,
        // sent in tiny chunks with no log kept behind them, to no snapshots
        // at all.
        let snapshot_threshold = config
            .snapshot_threshold
            .unwrap_or_else(|| [100, 400, 1_500, 5_000, u64::MAX][rng.index(5)]);
        let retained_entries = [0, 1, 8, 64][rng.index(4)];
        let snapshot_chunk_size = [16, 128, 1 << 20][rng.index(3)];
        // Batched replication and sending a leader's appends before its own
        // fsync are each on or off per seed, unless the configuration fixes
        // them.
        let batch_appends = rng.below(2) == 0;
        let batch_appends = config.batch_appends.unwrap_or(batch_appends);
        let send_before_persist = rng.below(2) == 0;
        let send_before_persist = config.send_before_persist.unwrap_or(send_before_persist);
        let ids: Vec<NodeId> = (1..=config.nodes).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                let mut raft_config = Config::new(id, peers, rng.next_u64());
                raft_config.max_entries_per_append = max_entries_per_append;
                raft_config.retained_entries = retained_entries;
                raft_config.snapshot_chunk_size = snapshot_chunk_size;
                raft_config.batch_appends = batch_appends;
                let raft = Raft::new(raft_config.clone()).expect("valid raft config");
                let (storage, _) =
                    Storage::open(SimDisk::new(), STORAGE).expect("empty disk opens");
                Node {
                    id,
                    config: raft_config,
                    state: State::Up {
                        raft: Box::new(raft),
                        storage: Box::new(storage),
                        kv: Box::new(KvStore::new(KvConfig::default())),
                        pending: BTreeMap::new(),
                        reads: BTreeMap::new(),
                        confirmed_reads: Vec::new(),
                        applied_bytes: 0,
                        paused_until: None,
                    },
                    observed_commit: LogIndex(0),
                    restarts: 0,
                    clock_offset_ms: rng.below(2_000),
                }
            })
            .collect();
        Self {
            seed,
            now: 0,
            rng,
            nodes,
            network: Network::default(),
            checker: Checker::default(),
            stats: Stats::default(),
            trace: VecDeque::new(),
            fingerprint: 0xCBF2_9CE4_8422_2325,
            clients: (0..config.clients).map(Client::new).collect(),
            history: Vec::new(),
            history_clock: 0,
            last_read_id: 0,
            isolated: None,
            snapshot_threshold,
            send_before_persist,
            config,
        }
    }

    /// Runs the simulation to completion. Panics inside a node are caught
    /// and reported as violations.
    pub(crate) fn run(mut self) -> Result<Report, Violation> {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| self.run_phases()));
        match outcome {
            Ok(Ok(())) => {
                self.stats.leaders_elected = self.checker.leaders_elected();
                self.stats.committed_index = self.checker.committed_index().0;
                for op in &self.history {
                    match op.outcome {
                        Outcome::Returned { .. } => self.stats.operations_completed += 1,
                        Outcome::Unknown => self.stats.operations_unknown += 1,
                    }
                }
                Ok(Report {
                    seed: self.seed,
                    stats: self.stats,
                    fingerprint: self.fingerprint,
                })
            }
            Ok(Err(failure)) => Err(self.violation(failure)),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                    .unwrap_or_else(|| "unknown panic".to_owned());
                Err(self.violation((ViolationKind::Panic, message)))
            }
        }
    }

    fn run_phases(&mut self) -> Result<(), Failure> {
        for _ in 0..self.config.ticks {
            self.inject_faults();
            self.tick(true)?;
        }
        self.heal_everything();
        for _ in 0..self.config.settle_ticks {
            self.tick(false)?;
        }
        self.check_logs()?;
        self.check_converged()?;
        self.check_history()
    }

    /// One unit of simulated time.
    fn tick(&mut self, with_workload: bool) -> Result<(), Failure> {
        self.now += 1;
        self.restart_due_nodes()?;
        self.deliver_messages();
        for node in &mut self.nodes {
            if node.is_active(self.now) {
                if let State::Up { raft, .. } = &mut node.state {
                    raft.tick();
                }
            }
        }
        self.drive_clients(with_workload);
        self.process_readies()?;
        if self.now.is_multiple_of(self.config.log_check_interval) {
            self.check_logs()?;
        }
        Ok(())
    }

    fn deliver_messages(&mut self) {
        for message in self
            .network
            .take_due(self.now, &mut self.rng, &mut self.stats)
        {
            let Some(node) = self.nodes.iter_mut().find(|n| n.id == message.to) else {
                continue;
            };
            match &mut node.state {
                State::Down { .. } => self.stats.messages_dropped += 1,
                State::Up {
                    paused_until: Some(until),
                    ..
                } if self.now < *until => self.network.hold(self.now, message),
                State::Up { raft, .. } => {
                    self.fingerprint = fnv(
                        self.fingerprint,
                        &[self.now, message.from.0, message.to.0, message.term.0],
                    );
                    raft.step(message);
                }
            }
        }
    }

    /// Lets every client make progress: open a session if it has none,
    /// start a new operation if it is idle (only while `start_new`), and send
    /// its request again if the last attempt went unanswered for too long.
    fn drive_clients(&mut self, start_new: bool) {
        for c in 0..self.clients.len() {
            let client = &mut self.clients[c];
            let due = match &client.in_flight {
                Some(in_flight) if self.now >= in_flight.deadline => {
                    // Give up. The history keeps the operation as unknown;
                    // the next operation uses a higher sequence number.
                    client.in_flight = None;
                    false
                }
                Some(in_flight) => {
                    let due = self.now >= in_flight.retry_at;
                    if due {
                        // No answer in time: like the client library, try
                        // another node instead of insisting on this one.
                        client.leader_guess = None;
                    }
                    due
                }
                None if client.session.is_none() => {
                    client.in_flight = Some(InFlight {
                        request: Request::Register,
                        retry_at: 0,
                        deadline: self.now + self.config.client_deadline,
                    });
                    true
                }
                None if start_new && self.rng.chance(self.config.request) => {
                    if self.rng.chance(self.config.forget_leader) {
                        // As if a new client process connected: it knows no
                        // leader and starts at an arbitrary node, possibly a
                        // deposed leader that does not know it yet.
                        client.leader_guess = None;
                    }
                    let (sequence, operation) =
                        client.next_operation(&mut self.rng, self.config.keys);
                    let read_index = matches!(operation, Operation::Get { .. })
                        && !self.rng.chance(self.config.log_reads);
                    self.history_clock += 1;
                    self.history.push(Op {
                        client: c as u64,
                        call: self.history_clock,
                        input: operation.clone(),
                        outcome: Outcome::Unknown,
                    });
                    client.in_flight = Some(InFlight {
                        request: Request::Operation {
                            sequence,
                            operation,
                            history_index: self.history.len() - 1,
                            read_index,
                        },
                        retry_at: 0,
                        deadline: self.now + self.config.client_deadline,
                    });
                    true
                }
                None => false,
            };
            if due {
                self.send_request(c);
            }
        }
    }

    /// Sends client `c`'s request to the node it believes is leader, or to a
    /// random node. A node that is not leader but knows who is points the
    /// client there, and the client follows the hint once right away.
    fn send_request(&mut self, c: usize) {
        let client = &self.clients[c];
        let Some(in_flight) = &client.in_flight else {
            return;
        };
        let submission = match &in_flight.request {
            Request::Register => Submission::Propose(Command::RegisterClient),
            Request::Operation {
                operation: Operation::Get { key },
                read_index: true,
                ..
            } => Submission::Read(key.clone()),
            Request::Operation {
                sequence,
                operation,
                ..
            } => Submission::Propose(Command::Request {
                client_id: client.session.expect("operations need a session"),
                sequence: *sequence,
                operation: operation.clone(),
            }),
        };
        let tag = in_flight.request.tag();
        let target = match client.leader_guess {
            Some(id) => id,
            None => self.nodes[self.rng.index(self.nodes.len())].id,
        };
        let mut result = self.submit(target, &submission, c, tag).map(|()| target);
        if let Err(Some(leader)) = result {
            if leader != target {
                result = self.submit(leader, &submission, c, tag).map(|()| leader);
            }
        }
        let client = &mut self.clients[c];
        client.leader_guess = result.ok();
        // Wait for an answer if a leader took the request; otherwise try
        // again shortly, once an election may have produced a leader.
        let wait = if result.is_ok() {
            self.config.client_timeout
        } else {
            RETRY_BACKOFF
        };
        if let Some(in_flight) = &mut client.in_flight {
            in_flight.retry_at = self.now + wait;
        }
    }

    /// Hands a request to node `id`. On failure, returns the leader the node
    /// knows of, if any.
    fn submit(
        &mut self,
        id: NodeId,
        submission: &Submission,
        client: usize,
        tag: Tag,
    ) -> Result<(), Option<NodeId>> {
        match submission {
            Submission::Propose(command) => self
                .propose_to(id, command, client, tag)
                .map_err(|ProposeError::NotLeader { leader_hint }| leader_hint),
            Submission::Read(key) => self
                .read_from(id, key, client, tag)
                .map_err(|ReadIndexError::NotLeader { leader_hint }| leader_hint),
        }
    }

    /// Starts a ReadIndex read on node `id`.
    fn read_from(
        &mut self,
        id: NodeId,
        key: &[u8],
        client: usize,
        tag: Tag,
    ) -> Result<(), ReadIndexError> {
        let now = self.now;
        let not_reachable = ReadIndexError::NotLeader { leader_hint: None };
        let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) else {
            return Err(not_reachable);
        };
        if !node.is_active(now) {
            return Err(not_reachable);
        }
        let State::Up { raft, reads, .. } = &mut node.state else {
            return Err(not_reachable);
        };
        self.last_read_id += 1;
        raft.read_index(self.last_read_id)?;
        reads.insert(
            self.last_read_id,
            PendingRead {
                pending: Pending {
                    term: raft.term(),
                    client,
                    tag,
                },
                key: key.to_vec(),
            },
        );
        Ok(())
    }

    fn propose_to(
        &mut self,
        id: NodeId,
        command: &Command,
        client: usize,
        tag: Tag,
    ) -> Result<(), ProposeError> {
        let now = self.now;
        let not_reachable = ProposeError::NotLeader { leader_hint: None };
        let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) else {
            return Err(not_reachable);
        };
        if !node.is_active(now) {
            return Err(not_reachable);
        }
        let clock_ms = EPOCH_MS + now * TICK_MS + node.clock_offset_ms;
        let State::Up { raft, pending, .. } = &mut node.state else {
            return Err(not_reachable);
        };
        let envelope = Envelope {
            timestamp_ms: clock_ms,
            command: command.clone(),
        };
        let index = raft.propose(envelope.encode())?;
        pending.insert(
            index,
            Pending {
                term: raft.term(),
                client,
                tag,
            },
        );
        self.stats.proposals_accepted += 1;
        Ok(())
    }

    /// The node gave up on a proposal; its client sends the request again
    /// on the next tick.
    fn retry_soon(&mut self, pending: Pending) {
        let client = &mut self.clients[pending.client];
        if let Some(in_flight) = &mut client.in_flight {
            if in_flight.request.tag() == pending.tag {
                in_flight.retry_at = self.now + 1;
                client.leader_guess = None;
            }
        }
    }

    /// Hands a node's response to the client that sent the request, unless
    /// the response is lost on the way or answers a request the client has
    /// already moved past.
    fn deliver(&mut self, pending: Pending, response: Response) -> Result<(), Failure> {
        if self.rng.chance(self.config.faults.drop) {
            self.stats.responses_lost += 1;
            return Ok(());
        }
        let client = &mut self.clients[pending.client];
        let answers_current = client
            .in_flight
            .as_ref()
            .is_some_and(|in_flight| in_flight.request.tag() == pending.tag);
        if !answers_current {
            return Ok(());
        }
        let in_flight = client.in_flight.take().expect("checked above");
        match (in_flight.request, response) {
            (Request::Register, Response::Registered { client_id }) => {
                client.registered(client_id);
            }
            (Request::Operation { .. }, Response::Rejected(Rejection::SessionExpired)) => {
                // The outcome stays unknown; start over with a new session.
                client.session = None;
            }
            (
                Request::Operation {
                    operation,
                    history_index,
                    ..
                },
                response,
            ) if !matches!(
                response,
                Response::Rejected(_) | Response::Registered { .. }
            ) =>
            {
                client.observe(&operation, &response);
                self.history_clock += 1;
                self.history[history_index].outcome = Outcome::Returned {
                    at: self.history_clock,
                    output: response,
                };
            }
            (request, response) => {
                return Err((
                    ViolationKind::Linearizability,
                    format!("client {} got {response:?} for {request:?}", pending.client),
                ));
            }
        }
        Ok(())
    }

    /// Persists, sends and applies each node's `Ready`, and checks the
    /// invariants against what changed. A node may crash in the middle of
    /// persisting; it then sends and applies nothing.
    fn process_readies(&mut self) -> Result<(), Failure> {
        let mut responses = Vec::new();
        let mut dropped = Vec::new();
        for i in 0..self.nodes.len() {
            let node = &mut self.nodes[i];
            let State::Up {
                raft,
                storage,
                kv,
                pending,
                reads,
                confirmed_reads,
                applied_bytes,
                ..
            } = &mut node.state
            else {
                continue;
            };
            let mut ready = raft.take_ready();
            // A leader's replication messages may go out before its fsync,
            // so they are sent even if it crashes while persisting.
            let early = take_early_messages(&mut ready, self.send_before_persist);
            if self.rng.chance(self.config.faults.write_crash) {
                let ops = self.rng.below(8);
                storage.disk_mut().fail_after(ops);
            }
            if let Err(error) = storage.persist(&ready) {
                let failed = storage.disk().has_failed();
                self.send_all(early);
                self.crash_while_writing(i, failed, &error, "writing")?;
                continue;
            }

            let id = node.id;
            let (role, term) = (raft.role(), raft.term());
            check_leader(&mut self.checker, id, raft, &mut node.observed_commit)?;

            if let Some(snapshot) = &ready.snapshot {
                **kv = KvStore::restore(KvConfig::default(), &snapshot.data).map_err(|error| {
                    (
                        ViolationKind::Recovery,
                        format!("{id} received a snapshot it cannot read: {error}"),
                    )
                })?;
                *applied_bytes = 0;
                self.stats.snapshots_installed += 1;
            }
            for entry in &ready.committed_entries {
                self.checker.on_apply(id, entry)?;
                let proposal = pending.remove(&entry.index);
                if let EntryPayload::Command(bytes) = &entry.payload {
                    *applied_bytes += bytes.len() as u64;
                    let response = kv.apply(entry.index.0, bytes);
                    // Answer only if this is the entry the node proposed; a
                    // different term means it was replaced after a leader
                    // change, and the client will retry.
                    match proposal {
                        Some(p) if p.term == entry.term => responses.push((p, response)),
                        Some(p) => dropped.push(p),
                        None => {}
                    }
                } else if let Some(p) = proposal {
                    dropped.push(p);
                }
            }
            self.stats.reads_confirmed += handle_reads(
                raft,
                kv,
                reads,
                confirmed_reads,
                ready.read_states,
                &mut responses,
                &mut dropped,
            );
            // A node that lost leadership tells its waiting clients, which
            // then retry elsewhere. Their proposals may still commit under
            // the next leader; the session table keeps a retry from being
            // applied twice.
            if role != Role::Leader {
                dropped.extend(std::mem::take(pending).into_values());
            }
            if *applied_bytes >= self.snapshot_threshold {
                *applied_bytes = 0;
                if let Err(error) = take_snapshot(raft, storage, kv) {
                    // The messages went out before the snapshot was taken.
                    let failed = storage.disk().has_failed();
                    self.send_all(early.into_iter().chain(ready.messages));
                    self.crash_while_writing(i, failed, &error, "saving a snapshot")?;
                    continue;
                }
                self.stats.snapshots_taken += 1;
            }
            self.stats.max_log_entries = self
                .stats
                .max_log_entries
                .max(raft.log().entries().len() as u64);
            self.send_all(early.into_iter().chain(ready.messages));
            if role == Role::Leader && !self.trace_mentions_leader(id, term) {
                self.event(&format!("{id} is leader in {term}"));
            }
        }
        for (pending, response) in responses {
            self.deliver(pending, response)?;
        }
        for pending in dropped {
            self.retry_soon(pending);
        }
        Ok(())
    }

    /// Hands messages to the network.
    fn send_all(&mut self, messages: impl IntoIterator<Item = Message>) {
        for message in messages {
            self.stats.entries_sent += entries_in(&message);
            self.network.send(
                self.now,
                message,
                &self.config.faults,
                &mut self.rng,
                &mut self.stats,
            );
        }
    }

    /// Handles a failed write on node `i`: a crash injected by the
    /// simulator takes the node down for a while; any other storage error is
    /// a violation.
    fn crash_while_writing(
        &mut self,
        i: usize,
        injected: bool,
        error: &StorageError,
        what: &str,
    ) -> Result<(), Failure> {
        let id = self.nodes[i].id;
        if !injected {
            return Err((ViolationKind::Recovery, format!("{id}: {error}")));
        }
        let downtime = self.config.faults.downtime;
        let restart_at = self.now + self.rng.between(downtime.0, downtime.1);
        self.stats.write_crashes += 1;
        self.crash(i, restart_at);
        self.event(&format!(
            "{id} crashed while {what}, until tick {restart_at}"
        ));
        Ok(())
    }

    /// Stops node `i` until `restart_at`. Everything its disk had not synced
    /// is lost, except that a random part of unsynced appends may survive,
    /// like a write that partly reached the disk.
    fn crash(&mut self, i: usize, restart_at: u64) {
        self.stats.crashes += 1;
        let placeholder = State::Down {
            restart_at,
            disk: Box::default(),
        };
        let State::Up { storage, .. } = std::mem::replace(&mut self.nodes[i].state, placeholder)
        else {
            return;
        };
        let mut disk = storage.into_disk();
        let rng = &mut self.rng;
        let mut lost = 0;
        disk.crash(&mut |unsynced| {
            let kept = rng.below(unsynced + 1);
            lost += unsynced - kept;
            kept
        });
        self.stats.bytes_lost += lost;
        self.nodes[i].state = State::Down {
            restart_at,
            disk: Box::new(disk),
        };
    }

    fn inject_faults(&mut self) {
        let faults = self.config.faults.clone();
        if self.network.is_partitioned() && self.rng.chance(faults.heal) {
            self.network.heal();
            self.event("partitions healed");
        }
        if self.rng.chance(faults.partition) {
            self.partition();
        }
        self.isolate_leader(&faults);
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_active(self.now) {
                continue;
            }
            let is_leader = self.nodes[i]
                .raft()
                .is_some_and(|r| r.role() == Role::Leader);
            let crash = if is_leader {
                faults.leader_crash
            } else {
                faults.crash
            };
            if self.rng.chance(crash) {
                let restart_at = self.now + self.rng.between(faults.downtime.0, faults.downtime.1);
                self.crash(i, restart_at);
                self.event(&format!(
                    "{} crashed until tick {restart_at}",
                    self.nodes[i].id
                ));
            } else if self.rng.chance(faults.pause) {
                let until = self.now
                    + self
                        .rng
                        .between(faults.pause_length.0, faults.pause_length.1);
                if let State::Up { paused_until, .. } = &mut self.nodes[i].state {
                    *paused_until = Some(until);
                }
                self.stats.pauses += 1;
                self.event(&format!("{} paused until tick {until}", self.nodes[i].id));
            }
        }
    }

    /// Starts, keeps up or ends a long isolation of the leader.
    fn isolate_leader(&mut self, faults: &crate::config::FaultConfig) {
        if let Some((victim, until)) = self.isolated {
            if self.now >= until {
                self.isolated = None;
                self.network.heal();
                self.event(&format!("{victim} reconnected"));
            } else {
                // Random healing must not end the isolation early.
                self.cut_off(victim);
            }
            return;
        }
        if !self.rng.chance(faults.leader_isolation) {
            return;
        }
        let Some(leader) = self
            .nodes
            .iter()
            .find(|n| n.raft().is_some_and(|r| r.role() == Role::Leader))
            .map(|n| n.id)
        else {
            return;
        };
        let until = self.now
            + self
                .rng
                .between(faults.isolation_length.0, faults.isolation_length.1);
        self.isolated = Some((leader, until));
        self.cut_off(leader);
        self.stats.leader_isolations += 1;
        self.event(&format!("leader {leader} isolated until tick {until}"));
    }

    /// Blocks every link to and from `victim`.
    fn cut_off(&mut self, victim: NodeId) {
        for other in self.nodes.iter().map(|n| n.id) {
            if other != victim {
                self.network.block(victim, other);
                self.network.block(other, victim);
            }
        }
    }

    /// Cuts the network in one of three ways: a symmetric split into two
    /// groups, one node isolated, or a single link cut in one direction.
    fn partition(&mut self) {
        let mut ids: Vec<NodeId> = self.nodes.iter().map(|n| n.id).collect();
        if ids.len() < 2 {
            return;
        }
        self.stats.partitions += 1;
        match self.rng.below(3) {
            0 => {
                self.rng.shuffle(&mut ids);
                let cut = 1 + self.rng.index(ids.len() - 1);
                let (left, right) = ids.split_at(cut);
                for &a in left {
                    for &b in right {
                        self.network.block(a, b);
                        self.network.block(b, a);
                    }
                }
                self.event(&format!("partition {left:?} | {right:?}"));
            }
            1 => {
                let victim = ids[self.rng.index(ids.len())];
                for &other in &ids {
                    if other != victim {
                        self.network.block(victim, other);
                        self.network.block(other, victim);
                    }
                }
                self.event(&format!("{victim} isolated"));
            }
            _ => {
                self.rng.shuffle(&mut ids);
                self.network.block(ids[0], ids[1]);
                self.event(&format!("link {} -> {} cut", ids[0], ids[1]));
            }
        }
    }

    /// Restarts nodes whose downtime is over, recovering their state from
    /// what their disk kept.
    fn restart_due_nodes(&mut self) -> Result<(), Failure> {
        for i in 0..self.nodes.len() {
            let node = &mut self.nodes[i];
            let State::Down { restart_at, disk } = &mut node.state else {
                continue;
            };
            if *restart_at > self.now {
                continue;
            }
            let disk = std::mem::take(disk.as_mut());
            let (storage, recovered) = Storage::open(disk, STORAGE).map_err(|error| {
                (
                    ViolationKind::Recovery,
                    format!("{} failed to recover: {error}", node.id),
                )
            })?;
            node.restarts += 1;
            let mut config = node.config.clone();
            config.seed ^= node.restarts;
            let kv = match &recovered.snapshot {
                Some(snapshot) => {
                    KvStore::restore(KvConfig::default(), &snapshot.data).map_err(|error| {
                        (
                            ViolationKind::Recovery,
                            format!("{} cannot read its snapshot: {error}", node.id),
                        )
                    })?
                }
                None => KvStore::new(KvConfig::default()),
            };
            let raft = Raft::restore_with_snapshot(
                config,
                recovered.hard_state,
                recovered.snapshot,
                recovered.log,
            )
            .expect("valid raft config");
            node.state = State::Up {
                raft: Box::new(raft),
                storage: Box::new(storage),
                kv: Box::new(kv),
                pending: BTreeMap::new(),
                reads: BTreeMap::new(),
                confirmed_reads: Vec::new(),
                applied_bytes: 0,
                paused_until: None,
            };
            node.observed_commit = LogIndex(0);
            let id = node.id;
            self.event(&format!("{id} restarted"));
        }
        Ok(())
    }

    fn heal_everything(&mut self) {
        self.isolated = None;
        self.network.heal();
        self.config.faults = crate::config::FaultConfig::none();
        for node in &mut self.nodes {
            match &mut node.state {
                State::Down { restart_at, .. } => *restart_at = self.now,
                State::Up { paused_until, .. } => *paused_until = None,
            }
        }
        self.event("all faults healed; settling");
    }

    /// Checks log matching across the running nodes. A crashed node's log
    /// is checked again once it has restarted.
    fn check_logs(&self) -> Result<(), Failure> {
        let logs: Vec<(NodeId, &raft_core::Log)> = self
            .nodes
            .iter()
            .filter_map(|n| n.raft().map(|r| (n.id, r.log())))
            .collect();
        Checker::check_log_matching(&logs)
    }

    /// After healing, exactly one leader must exist and every node must have
    /// the leader's log and have applied all of it. Logs may start at
    /// different points, depending on when each node took its last
    /// snapshot; log matching makes equal last entries imply equal logs.
    fn check_converged(&self) -> Result<(), Failure> {
        let rafts: Vec<&Raft> = self.nodes.iter().filter_map(Node::raft).collect();
        let leaders: Vec<&&Raft> = rafts.iter().filter(|r| r.role() == Role::Leader).collect();
        let [leader] = leaders.as_slice() else {
            return Err((
                ViolationKind::Liveness,
                format!("{} leaders after settling", leaders.len()),
            ));
        };
        for raft in &rafts {
            let same_end = (raft.log().last_index(), raft.log().last_term())
                == (leader.log().last_index(), leader.log().last_term());
            if !same_end || raft.last_applied() != leader.log().last_index() {
                return Err((
                    ViolationKind::Liveness,
                    format!(
                        "{} did not converge: last index {}, applied {}, leader {} has {}",
                        raft.id(),
                        raft.log().last_index(),
                        raft.last_applied(),
                        leader.id(),
                        leader.log().last_index()
                    ),
                ));
            }
        }
        let leader_kv = self
            .nodes
            .iter()
            .find(|n| n.id == leader.id())
            .and_then(Node::kv);
        for node in &self.nodes {
            if node.kv() != leader_kv {
                return Err((
                    ViolationKind::StateMachineSafety,
                    format!(
                        "{} applied the same log as {} but its key-value state differs",
                        node.id,
                        leader.id()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Reads every key once more after the cluster has converged, so that a
    /// lost write shows up as a stale read, then checks the whole history
    /// for linearizability.
    fn check_history(&mut self) -> Result<(), Failure> {
        let kv = self
            .nodes
            .iter()
            .find(|n| n.raft().is_some_and(|r| r.role() == Role::Leader))
            .and_then(Node::kv)
            .expect("a converged cluster has a leader");
        let final_reads: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..self.config.keys)
            .map(|k| {
                let key = format!("k{k}").into_bytes();
                let value = kv.get(&key).map(<[u8]>::to_vec);
                (key, value)
            })
            .collect();
        for (key, value) in final_reads {
            self.history.push(Op {
                client: u64::MAX,
                call: self.history_clock + 1,
                input: Operation::Get { key },
                outcome: Outcome::Returned {
                    at: self.history_clock + 2,
                    output: Response::Value(value),
                },
            });
            self.history_clock += 2;
        }
        check_kv_history(&self.history, CHECK_BUDGET)
            .map_err(|error| (ViolationKind::Linearizability, error.to_string()))
    }

    fn trace_mentions_leader(&self, id: NodeId, term: Term) -> bool {
        let line = format!("{id} is leader in {term}");
        self.trace.iter().rev().take(10).any(|l| l.ends_with(&line))
    }

    fn event(&mut self, text: &str) {
        if self.trace.len() == TRACE_LEN {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("tick {:>5}: {text}", self.now));
    }

    fn violation(&self, (kind, details): Failure) -> Violation {
        Violation {
            seed: self.seed,
            tick: self.now,
            kind,
            details,
            trace: self.trace.iter().cloned().collect(),
        }
    }
}

/// Checks election safety and leader completeness if `raft` is leader, and
/// records the entries it committed since `observed_commit`.
fn check_leader(
    checker: &mut Checker,
    id: NodeId,
    raft: &Raft,
    observed_commit: &mut LogIndex,
) -> Result<(), Failure> {
    let (term, commit) = (raft.term(), raft.commit_index());
    if raft.role() == Role::Leader {
        checker.on_leader(id, term, raft.log())?;
        if commit > *observed_commit {
            checker.on_leader_commit(id, term, *observed_commit, commit, raft.log())?;
        }
    }
    *observed_commit = (*observed_commit).max(commit);
    Ok(())
}

/// Removes the messages that may be sent before persisting from `ready`,
/// if sending early is enabled.
fn take_early_messages(ready: &mut Ready, enabled: bool) -> Vec<Message> {
    if !enabled {
        return Vec::new();
    }
    let (early, late) = std::mem::take(&mut ready.messages)
        .into_iter()
        .partition(Message::may_send_before_persisting);
    ready.messages = late;
    early
}

/// The number of log entries a message carries.
fn entries_in(message: &Message) -> u64 {
    match &message.body {
        MessageBody::AppendEntries { entries, .. } => entries.len() as u64,
        _ => 0,
    }
}

/// Snapshots the state machine at the applied index, compacts the log and
/// saves the snapshot.
fn take_snapshot(
    raft: &mut Raft,
    storage: &mut Storage<SimDisk>,
    kv: &KvStore,
) -> Result<(), StorageError> {
    let snapshot = raft
        .compact(raft.last_applied(), kv.snapshot())
        .expect("the applied state is newer than any snapshot");
    storage.save_snapshot(&snapshot, raft.log().first_index())
}

/// Folds values into an FNV-1a hash.
fn fnv(mut hash: u64, values: &[u64]) -> u64 {
    for value in values {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01B3);
        }
    }
    hash
}

/// Moves reads the leader confirmed to `confirmed`, answers those whose
/// index the state machine has reached, and gives up on unconfirmed reads
/// once the node is no longer leader in their term. Returns how many reads
/// were confirmed.
fn handle_reads(
    raft: &Raft,
    kv: &KvStore,
    reads: &mut BTreeMap<u64, PendingRead>,
    confirmed: &mut Vec<(LogIndex, PendingRead)>,
    read_states: Vec<ReadState>,
    responses: &mut Vec<(Pending, Response)>,
    dropped: &mut Vec<Pending>,
) -> u64 {
    let mut count = 0;
    for read in read_states {
        if let Some(waiting) = reads.remove(&read.id) {
            count += 1;
            confirmed.push((read.index, waiting));
        }
    }
    // A confirmed read is answered once the state machine reaches its
    // index, even if the node has lost leadership since.
    let applied = raft.last_applied();
    let (due, waiting): (Vec<_>, Vec<_>) = std::mem::take(confirmed)
        .into_iter()
        .partition(|(index, _)| *index <= applied);
    *confirmed = waiting;
    for (_, read) in due {
        let value = kv.get(&read.key).map(<[u8]>::to_vec);
        responses.push((read.pending, Response::Value(value)));
    }
    // Unconfirmed reads die with the node's leadership in their term.
    let (role, term) = (raft.role(), raft.term());
    let (keep, lost): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(reads)
        .into_iter()
        .partition(|(_, read)| role == Role::Leader && read.pending.term == term);
    *reads = keep;
    dropped.extend(lost.into_values().map(|read| read.pending));
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FaultConfig;

    #[test]
    fn fault_free_run_elects_and_commits() {
        let config = SimConfig {
            ticks: 500,
            faults: FaultConfig::none(),
            ..SimConfig::default()
        };
        let report = Simulation::new(1, config).run().expect("no violation");
        assert_eq!(report.stats.leaders_elected, 1);
        assert!(report.stats.committed_index > 50, "{report:?}");
        assert!(report.stats.operations_completed > 50, "{report:?}");
        assert_eq!(report.stats.operations_unknown, 0);
        assert_eq!(report.stats.messages_dropped, 0);
    }
}
