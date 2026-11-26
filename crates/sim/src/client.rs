//! Simulated key-value clients.
//!
//! Each client behaves like the client library: it opens a session, sends one
//! request at a time, and retries the same request (same sequence number)
//! until it gets an answer, following leader hints along the way. Every
//! operation is recorded in the history with its invocation and completion
//! time, for the linearizability check at the end of the run.

use std::collections::BTreeMap;

use kv::{ClientId, Operation, Response};
use raft_core::{NodeId, Term};

use crate::rng::SimRng;

/// What a client is waiting for.
#[derive(Clone, Debug)]
pub(crate) enum Request {
    /// A new session.
    Register,
    /// An operation, recorded at `history[history_index]`.
    Operation {
        sequence: u64,
        operation: Operation,
        history_index: usize,
    },
}

/// Which request a response answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tag {
    Register,
    Sequence(u64),
}

impl Request {
    pub(crate) fn tag(&self) -> Tag {
        match self {
            Self::Register => Tag::Register,
            Self::Operation { sequence, .. } => Tag::Sequence(*sequence),
        }
    }
}

/// A proposal a leader accepted on a client's behalf. When the leader applies
/// the entry at that index, it answers the client, but only if the entry is
/// still the one it proposed: same index and same term.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pending {
    pub(crate) term: Term,
    pub(crate) client: usize,
    pub(crate) tag: Tag,
}

#[derive(Clone, Debug)]
pub(crate) struct InFlight {
    pub(crate) request: Request,
    /// When to send the request again if no answer has arrived.
    pub(crate) retry_at: u64,
    /// When to give up.
    pub(crate) deadline: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Client {
    pub(crate) index: usize,
    pub(crate) session: Option<ClientId>,
    pub(crate) in_flight: Option<InFlight>,
    /// The node this client believes is leader.
    pub(crate) leader_guess: Option<NodeId>,
    next_sequence: u64,
    /// The last value this client saw for each key, to make
    /// compare-and-swaps that succeed now and then.
    last_seen: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Client {
    pub(crate) fn new(index: usize) -> Self {
        Self {
            index,
            session: None,
            in_flight: None,
            leader_guess: None,
            next_sequence: 1,
            last_seen: BTreeMap::new(),
        }
    }

    /// The session was opened.
    pub(crate) fn registered(&mut self, client_id: ClientId) {
        self.session = Some(client_id);
        self.next_sequence = 1;
    }

    /// Picks the next operation and its sequence number. Written values are
    /// unique across the run (`client.sequence`), so that every read tells
    /// exactly which write it saw.
    pub(crate) fn next_operation(&mut self, rng: &mut SimRng, keys: u64) -> (u64, Operation) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        let key = format!("k{}", rng.below(keys)).into_bytes();
        let value = format!("{}.{}", self.index, sequence).into_bytes();
        let operation = match rng.below(10) {
            0..=2 => Operation::Get { key },
            3..=5 => Operation::Put { key, value },
            6 => Operation::Delete { key },
            _ => {
                // Mostly expect the last value seen, which may still be
                // current; sometimes expect the key to be absent.
                let expected = if rng.below(4) == 0 {
                    None
                } else {
                    self.last_seen.get(&key).cloned().flatten()
                };
                Operation::CompareAndSwap {
                    key,
                    expected,
                    new: value,
                }
            }
        };
        (sequence, operation)
    }

    /// Remembers what a completed operation revealed about its key.
    pub(crate) fn observe(&mut self, operation: &Operation, response: &Response) {
        let seen = match (operation, response) {
            (Operation::Get { .. }, Response::Value(value)) => value.clone(),
            (Operation::Put { value, .. }, _) => Some(value.clone()),
            (Operation::Delete { .. }, _) => None,
            (
                Operation::CompareAndSwap { new, .. },
                Response::Swapped {
                    succeeded,
                    previous,
                },
            ) => {
                if *succeeded {
                    Some(new.clone())
                } else {
                    previous.clone()
                }
            }
            _ => return,
        };
        self.last_seen.insert(operation.key().to_vec(), seen);
    }
}
