//! The loop that drives one Raft node.
//!
//! A single task owns the node's whole state: the Raft state machine, its
//! storage, the key-value store and the requests waiting for their entries
//! to commit. Everything else talks to it through channels, so none of that
//! state needs a lock.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kv::{Command, Envelope, KvStore, Response};
use raft_core::{EntryPayload, LogIndex, Message, NodeId, ProposeError, Raft, Role, Term};
use storage::{FsDisk, Storage, StorageError};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};
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
    /// Report the node's status.
    Status { reply: oneshot::Sender<NodeStatus> },
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
    /// The role, term and leader last logged, to log changes.
    observed: (Role, Term, Option<NodeId>),
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
        Self {
            raft,
            storage,
            kv,
            peers,
            addresses,
            pending: BTreeMap::new(),
            observed,
        }
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
        loop {
            tokio::select! {
                _ = ticker.tick() => self.raft.tick(),
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
            Request::Status { reply } => {
                let _ = reply.send(self.status());
            }
        }
    }

    /// Persists, sends and applies what the last round produced, in the
    /// order [`raft_core::Ready`] prescribes.
    fn process_ready(&mut self) -> Result<(), StorageError> {
        let ready = self.raft.take_ready();
        if !ready.is_empty() {
            // fsync blocks; tell the runtime so it can move other tasks to
            // another thread meanwhile.
            tokio::task::block_in_place(|| self.storage.persist(&ready))?;
            for message in ready.messages {
                if let Some(peer) = self.peers.get(&message.to) {
                    if !peer.send(message) {
                        debug!(peer = %peer.id(), "send queue full; message dropped");
                    }
                }
            }
            for entry in ready.committed_entries {
                let waiter = self.pending.remove(&entry.index);
                let response = match &entry.payload {
                    EntryPayload::Command(bytes) => Some(self.kv.apply(entry.index.0, bytes)),
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
        }
        self.observe_role();
        Ok(())
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
