//! The loop that drives one Raft node.
//!
//! A single task owns the node's whole state: the Raft state machine, its
//! storage, the key-value store and the requests waiting for their entries
//! to commit. Everything else talks to it through channels, so none of that
//! state needs a lock.
//!
//! The one exception is serializing a snapshot, which takes time
//! proportional to the store's size. The node hands a clone of the store
//! (cheap: its maps are persistent) to a blocking thread and keeps applying;
//! the finished snapshot comes back through a channel.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kv::{Command, Envelope, KvStore, Response};
use raft_core::{
    EntryPayload, LogIndex, Message, NodeId, ProposeError, Raft, ReadIndexError, Role, Term,
};
use storage::{FsDisk, Storage, StorageError};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};
use transport::{ApiError, LeaderHint, Member, NodeStatus, Peer};

/// The most messages or requests taken from a channel before the node
/// persists and sends. Everything handled in one round shares one fsync.
const BATCH: usize = 256;

/// A request from a client handler to the node.
#[derive(Debug)]
pub(crate) enum Request {
    /// Replicate a command and answer with the state machine's response once
    /// it is applied.
    Propose {
        command: Command,
        reply: oneshot::Sender<Result<Response, ApiError>>,
    },
    /// Read a key with ReadIndex.
    Read {
        key: Vec<u8>,
        reply: oneshot::Sender<Result<Option<Vec<u8>>, ApiError>>,
    },
    /// Report the node's status.
    Status { reply: oneshot::Sender<NodeStatus> },
}

/// A ReadIndex read waiting for the leader to confirm it.
#[derive(Debug)]
struct WaitingRead {
    key: Vec<u8>,
    /// The term the read was accepted in. Raft drops pending reads when the
    /// node loses leadership, so a read from an earlier term never shows up.
    term: Term,
    reply: oneshot::Sender<Result<Option<Vec<u8>>, ApiError>>,
}

/// A client waiting for the entry at some index to be applied.
#[derive(Debug)]
struct Waiter {
    /// The term the entry was proposed in. If a different entry ends up at
    /// that index, the proposal was lost in a leader change.
    term: Term,
    reply: oneshot::Sender<Result<Response, ApiError>>,
}

pub(crate) struct Node {
    raft: Raft,
    storage: Storage<FsDisk>,
    kv: KvStore,
    peers: BTreeMap<NodeId, Peer>,
    addresses: BTreeMap<NodeId, String>,
    pending: BTreeMap<LogIndex, Waiter>,
    /// Reads waiting for confirmation, by read id.
    reads: BTreeMap<u64, WaitingRead>,
    next_read_id: u64,
    /// Confirmed reads waiting for the state machine to reach their index.
    confirmed_reads: Vec<(LogIndex, WaitingRead)>,
    /// The role, term and leader last logged, to log changes.
    observed: (Role, Term, Option<NodeId>),
    /// Command bytes applied since the last snapshot.
    applied_bytes: u64,
    /// Take a snapshot once `applied_bytes` reaches this.
    snapshot_threshold: u64,
    /// Whether a snapshot is being serialized.
    snapshotting: bool,
    /// Whether to send a leader's replication messages before persisting.
    send_before_persist: bool,
    /// Where serialized snapshots arrive: the index they were taken at and
    /// their data.
    snapshots: mpsc::Sender<(LogIndex, Vec<u8>)>,
    finished_snapshots: Option<mpsc::Receiver<(LogIndex, Vec<u8>)>>,
}

impl Node {
    pub(crate) fn new(
        raft: Raft,
        storage: Storage<FsDisk>,
        kv: KvStore,
        peers: BTreeMap<NodeId, Peer>,
        addresses: BTreeMap<NodeId, String>,
    ) -> Self {
        let observed = (raft.role(), raft.term(), raft.leader_id());
        let (snapshots, finished_snapshots) = mpsc::channel(1);
        Self {
            raft,
            storage,
            kv,
            peers,
            addresses,
            pending: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read_id: 0,
            confirmed_reads: Vec::new(),
            observed,
            applied_bytes: 0,
            snapshot_threshold: u64::MAX,
            snapshotting: false,
            send_before_persist: false,
            snapshots,
            finished_snapshots: Some(finished_snapshots),
        }
    }

    /// Takes a snapshot whenever `bytes` of commands have been applied
    /// since the last one. Without this, the node never takes one.
    pub(crate) fn with_snapshot_threshold(mut self, bytes: u64) -> Self {
        self.snapshot_threshold = bytes;
        self
    }

    /// Sends a leader's replication messages before persisting each round
    /// (see `raft_core::Message::may_send_before_persisting`). Safe because
    /// the loop finishes persisting before it hands Raft anything else.
    pub(crate) fn with_send_before_persist(mut self, enabled: bool) -> Self {
        self.send_before_persist = enabled;
        self
    }

    /// Runs until both channels are closed, or until storage fails. A node
    /// whose disk fails must stop: it can no longer keep the promises it
    /// made by voting and acknowledging entries.
    pub(crate) async fn run(
        mut self,
        mut inbox: mpsc::Receiver<Message>,
        mut requests: mpsc::Receiver<Request>,
        tick: Duration,
    ) -> Result<(), StorageError> {
        let mut ticker = tokio::time::interval(tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut finished_snapshots = self
            .finished_snapshots
            .take()
            .expect("a node runs only once");
        loop {
            tokio::select! {
                _ = ticker.tick() => self.raft.tick(),
                Some((index, data)) = finished_snapshots.recv() => {
                    self.finish_snapshot(index, data)?;
                }
                message = inbox.recv() => match message {
                    Some(message) => self.raft.step(message),
                    None => return Ok(()),
                },
                request = requests.recv() => match request {
                    Some(request) => self.handle(request),
                    None => return Ok(()),
                },
            }
            for _ in 0..BATCH {
                let Ok(message) = inbox.try_recv() else { break };
                self.raft.step(message);
            }
            for _ in 0..BATCH {
                let Ok(request) = requests.try_recv() else {
                    break;
                };
                self.handle(request);
            }
            self.process_ready()?;
        }
    }

    fn handle(&mut self, request: Request) {
        match request {
            Request::Propose { command, reply } => {
                let envelope = Envelope {
                    timestamp_ms: now_ms(),
                    command,
                };
                match self.raft.propose(envelope.encode()) {
                    Ok(index) => {
                        let waiter = Waiter {
                            term: self.raft.term(),
                            reply,
                        };
                        if let Some(replaced) = self.pending.insert(index, waiter) {
                            // An older proposal at the same index was
                            // overwritten after a leader change.
                            let _ = replaced.reply.send(Err(lost()));
                        }
                    }
                    Err(ProposeError::NotLeader { leader_hint }) => {
                        let _ = reply.send(Err(ApiError::NotLeader(self.hint(leader_hint))));
                    }
                }
            }
            Request::Read { key, reply } => {
                if key.len() > self.kv.config().max_key_len {
                    let _ = reply.send(Err(ApiError::KeyTooLarge));
                    return;
                }
                self.next_read_id += 1;
                let id = self.next_read_id;
                match self.raft.read_index(id) {
                    Ok(()) => {
                        let term = self.raft.term();
                        self.reads.insert(id, WaitingRead { key, term, reply });
                    }
                    Err(ReadIndexError::NotLeader { leader_hint }) => {
                        let _ = reply.send(Err(ApiError::NotLeader(self.hint(leader_hint))));
                    }
                }
            }
            Request::Status { reply } => {
                let _ = reply.send(self.status());
            }
        }
    }

    /// Persists, sends and applies what the last round produced, in the
    /// order [`raft_core::Ready`] prescribes.
    fn process_ready(&mut self) -> Result<(), StorageError> {
        let mut ready = self.raft.take_ready();
        if !ready.is_empty() {
            if self.send_before_persist {
                let (early, late) = std::mem::take(&mut ready.messages)
                    .into_iter()
                    .partition(Message::may_send_before_persisting);
                ready.messages = late;
                self.send(early);
            }
            // fsync blocks; tell the runtime so it can move other tasks to
            // another thread meanwhile.
            tokio::task::block_in_place(|| self.storage.persist(&ready))?;
            self.send(std::mem::take(&mut ready.messages));
            if let Some(snapshot) = &ready.snapshot {
                self.kv = KvStore::restore(*self.kv.config(), &snapshot.data).map_err(|error| {
                    StorageError::Corrupt {
                        file: "snapshot from the leader".to_owned(),
                        offset: 0,
                        reason: error.to_string(),
                    }
                })?;
                self.applied_bytes = 0;
                info!(
                    index = snapshot.index.0,
                    bytes = snapshot.data.len(),
                    "installed a snapshot from the leader"
                );
            }
            for entry in ready.committed_entries {
                let waiter = self.pending.remove(&entry.index);
                let response = match &entry.payload {
                    EntryPayload::Command(bytes) => {
                        self.applied_bytes += bytes.len() as u64;
                        Some(self.kv.apply(entry.index.0, bytes))
                    }
                    EntryPayload::Noop => None,
                };
                if let Some(waiter) = waiter {
                    let answer = match response {
                        Some(response) if waiter.term == entry.term => Ok(response),
                        _ => Err(lost()),
                    };
                    let _ = waiter.reply.send(answer);
                }
            }
            for read in ready.read_states {
                if let Some(waiting) = self.reads.remove(&read.id) {
                    self.confirmed_reads.push((read.index, waiting));
                }
            }
            self.serve_reads();
            self.maybe_snapshot();
        }
        self.observe_role();
        Ok(())
    }

    /// Starts serializing a snapshot of the store on a blocking thread, if
    /// enough has been applied since the last one and none is in progress.
    fn maybe_snapshot(&mut self) {
        if self.snapshotting || self.applied_bytes < self.snapshot_threshold {
            return;
        }
        self.snapshotting = true;
        self.applied_bytes = 0;
        // Every committed entry has been applied, so the store is exactly
        // the state at the applied index.
        let index = self.raft.last_applied();
        let kv = self.kv.clone();
        let snapshots = self.snapshots.clone();
        tokio::task::spawn_blocking(move || {
            let _ = snapshots.blocking_send((index, kv.snapshot()));
        });
    }

    /// Compacts the log with a serialized snapshot and makes it durable.
    fn finish_snapshot(&mut self, index: LogIndex, data: Vec<u8>) -> Result<(), StorageError> {
        self.snapshotting = false;
        match self.raft.compact(index, data) {
            Ok(snapshot) => {
                let keep_from = self.raft.log().first_index();
                tokio::task::block_in_place(|| self.storage.save_snapshot(&snapshot, keep_from))?;
                info!(
                    index = index.0,
                    bytes = snapshot.data.len(),
                    log_start = keep_from.0,
                    "took a snapshot"
                );
            }
            // A newer snapshot arrived from the leader meanwhile.
            Err(error) => warn!(%error, "snapshot discarded"),
        }
        Ok(())
    }

    /// Queues messages for their peers. A peer's queue only fills up while
    /// it is unreachable; Raft resends what is lost.
    fn send(&self, messages: Vec<Message>) {
        for message in messages {
            if let Some(peer) = self.peers.get(&message.to) {
                if !peer.send(message) {
                    debug!(peer = %peer.id(), "send queue full; message dropped");
                }
            }
        }
    }

    /// Answers confirmed reads whose index the state machine has reached.
    /// A confirmed read stays valid even if the node has stopped being
    /// leader since: it was confirmed while the node was leader.
    fn serve_reads(&mut self) {
        let applied = self.raft.last_applied();
        let (due, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.confirmed_reads)
            .into_iter()
            .partition(|(index, _)| *index <= applied);
        self.confirmed_reads = waiting;
        for (_, read) in due {
            let value = self.kv.get(&read.key).map(<[u8]>::to_vec);
            let _ = read.reply.send(Ok(value));
        }
    }

    /// Logs role changes, and releases waiting clients when the node is no
    /// longer leader. Their entries may still commit under the next leader;
    /// a client retries with the same sequence number, so the session table
    /// keeps the write from being applied twice.
    fn observe_role(&mut self) {
        let current = (self.raft.role(), self.raft.term(), self.raft.leader_id());
        if current != self.observed {
            let (role, term, leader) = current;
            let leader = leader.map_or_else(|| "unknown".to_owned(), |id| id.0.to_string());
            info!(?role, term = term.0, %leader, "role changed");
            self.observed = current;
        }
        if self.raft.role() != Role::Leader && !self.pending.is_empty() {
            let hint = self.hint(self.raft.leader_id());
            for (_, waiter) in std::mem::take(&mut self.pending) {
                let _ = waiter.reply.send(Err(ApiError::NotLeader(hint.clone())));
            }
        }
        // Unconfirmed reads from an earlier leadership will never be
        // confirmed: Raft dropped them when the node stepped down.
        let (role, term) = (self.raft.role(), self.raft.term());
        if self
            .reads
            .values()
            .any(|read| role != Role::Leader || read.term != term)
        {
            let hint = self.hint(self.raft.leader_id());
            let reads = std::mem::take(&mut self.reads);
            for (id, read) in reads {
                if role == Role::Leader && read.term == term {
                    self.reads.insert(id, read);
                } else {
                    let _ = read.reply.send(Err(ApiError::NotLeader(hint.clone())));
                }
            }
        }
    }

    fn hint(&self, leader: Option<NodeId>) -> Option<LeaderHint> {
        let id = leader?;
        let address = self.addresses.get(&id)?.clone();
        Some(LeaderHint { id, address })
    }

    fn status(&self) -> NodeStatus {
        NodeStatus {
            id: self.raft.id(),
            role: self.raft.role(),
            term: self.raft.term(),
            leader: self.raft.leader_id(),
            commit_index: self.raft.commit_index(),
            last_applied: self.raft.last_applied(),
            last_log_index: self.raft.log().last_index(),
            peers: self
                .addresses
                .iter()
                .map(|(&id, address)| Member {
                    id,
                    address: address.clone(),
                })
                .collect(),
        }
    }
}

/// The answer for a proposal that will not be answered: it was replaced in
/// a leader change before it committed. It may still have been applied by
/// way of an earlier attempt, so the client retries with the same sequence
/// number.
fn lost() -> ApiError {
    ApiError::Unavailable("leadership changed before the request committed".to_owned())
}

/// The wall clock in milliseconds, for the leader's timestamp in each entry.
fn now_ms() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}
