//! The replicated key-value state machine.

use std::collections::BTreeMap;

use crate::command::{ClientId, Command, Envelope, Operation};

/// Limits and timeouts of the state machine. Every node must use the same
/// values, or their states could diverge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvConfig {
    /// Longest allowed key, in bytes.
    pub max_key_len: usize,
    /// Longest allowed value, in bytes.
    pub max_value_len: usize,
    /// A session that has not been used for this long, measured in leader
    /// timestamps, is removed.
    pub session_timeout_ms: u64,
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            max_key_len: 1024,
            max_value_len: 1 << 20,
            session_timeout_ms: 5 * 60 * 1000,
        }
    }
}

/// The result of applying a command, returned to the client that sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    /// A new session was opened.
    Registered {
        /// The session's id, to send with every request.
        client_id: ClientId,
    },
    /// The value of a key read through the log, `None` if absent.
    Value(Option<Vec<u8>>),
    /// The put was applied.
    Written,
    /// The delete was applied.
    Deleted {
        /// Whether the key existed.
        existed: bool,
    },
    /// The compare-and-swap was applied.
    Swapped {
        /// Whether the comparison matched and the value was replaced.
        succeeded: bool,
        /// The key's value before the operation.
        previous: Option<Vec<u8>>,
    },
    /// The request was rejected without changing anything.
    Rejected(Rejection),
}

/// Why a request was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// The session does not exist: it expired, or never existed. The client
    /// must register again. Its earlier requests may or may not have been
    /// applied.
    SessionExpired,
    /// The request repeats one older than the session's latest request. The
    /// client already received a newer response, so this is a delayed
    /// duplicate.
    StaleSequence,
    /// The key exceeds `max_key_len`.
    KeyTooLarge,
    /// The value exceeds `max_value_len`.
    ValueTooLarge,
    /// The log entry could not be decoded.
    Malformed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Session {
    sequence: u64,
    response: Response,
    active_at_ms: u64,
}

/// The key-value state machine.
///
/// Applying the same entries in the same order always produces the same
/// state and the same responses on every node. Everything that could differ
/// between nodes (wall clocks, iteration order) is kept out: time comes from
/// the leader's timestamps in the log, and all maps are ordered.
///
/// **Client sessions** make retries safe. Each request carries
/// `(client_id, sequence)`. The store remembers every session's latest
/// sequence number and response; a retry of that request gets the remembered
/// response instead of being applied again. Without this, a client that
/// retries after a leader change could apply the same write twice.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvStore {
    config: KvConfig,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: BTreeMap<ClientId, Session>,
    /// The latest leader timestamp seen. Never moves backwards, even if a new
    /// leader's clock is behind the previous one.
    clock_ms: u64,
    /// When idle sessions were last removed.
    last_sweep_ms: u64,
}

impl KvStore {
    /// An empty store.
    #[must_use]
    pub fn new(config: KvConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// Applies the command in the log entry at `index`.
    pub fn apply(&mut self, index: u64, bytes: &[u8]) -> Response {
        let envelope = match Envelope::decode(bytes) {
            Ok(envelope) => envelope,
            Err(error) => return Response::Rejected(Rejection::Malformed(error.to_string())),
        };
        self.advance_clock(envelope.timestamp_ms);
        match envelope.command {
            Command::RegisterClient => {
                let client_id = ClientId(index);
                let response = Response::Registered { client_id };
                self.sessions.insert(
                    client_id,
                    Session {
                        sequence: 0,
                        response: response.clone(),
                        active_at_ms: self.clock_ms,
                    },
                );
                response
            }
            Command::Request {
                client_id,
                sequence,
                operation,
            } => self.apply_request(client_id, sequence, operation),
            Command::Read { key } => self.apply_operation(Operation::Get { key }),
        }
    }

    fn apply_request(
        &mut self,
        client_id: ClientId,
        sequence: u64,
        operation: Operation,
    ) -> Response {
        let Some(session) = self.sessions.get(&client_id) else {
            return Response::Rejected(Rejection::SessionExpired);
        };
        if sequence == session.sequence {
            // A retry of the latest request: answer it again, apply nothing.
            let response = session.response.clone();
            self.touch(client_id);
            return response;
        }
        if sequence < session.sequence {
            return Response::Rejected(Rejection::StaleSequence);
        }

        let response = self.apply_operation(operation);
        let clock_ms = self.clock_ms;
        let session = self.sessions.get_mut(&client_id).expect("session exists");
        session.sequence = sequence;
        session.response = response.clone();
        session.active_at_ms = clock_ms;
        response
    }

    fn apply_operation(&mut self, operation: Operation) -> Response {
        if let Err(rejection) = self.check_limits(&operation) {
            return Response::Rejected(rejection);
        }
        match operation {
            Operation::Get { key } => Response::Value(self.data.get(&key).cloned()),
            Operation::Put { key, value } => {
                self.data.insert(key, value);
                Response::Written
            }
            Operation::Delete { key } => Response::Deleted {
                existed: self.data.remove(&key).is_some(),
            },
            Operation::CompareAndSwap { key, expected, new } => {
                let previous = self.data.get(&key).cloned();
                let succeeded = previous == expected;
                if succeeded {
                    self.data.insert(key, new);
                }
                Response::Swapped {
                    succeeded,
                    previous,
                }
            }
        }
    }

    fn check_limits(&self, operation: &Operation) -> Result<(), Rejection> {
        let (key, values): (&[u8], Vec<&[u8]>) = match operation {
            Operation::Get { key } | Operation::Delete { key } => (key, vec![]),
            Operation::Put { key, value } => (key, vec![value]),
            Operation::CompareAndSwap { key, expected, new } => {
                let mut values = vec![new.as_slice()];
                values.extend(expected.as_deref());
                (key, values)
            }
        };
        if key.len() > self.config.max_key_len {
            return Err(Rejection::KeyTooLarge);
        }
        if values.iter().any(|v| v.len() > self.config.max_value_len) {
            return Err(Rejection::ValueTooLarge);
        }
        Ok(())
    }

    fn touch(&mut self, client_id: ClientId) {
        let clock_ms = self.clock_ms;
        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.active_at_ms = clock_ms;
        }
    }

    /// Moves the clock forward and, at most once per timeout interval's
    /// tenth, removes sessions that have been idle longer than the timeout.
    fn advance_clock(&mut self, timestamp_ms: u64) {
        self.clock_ms = self.clock_ms.max(timestamp_ms);
        let sweep_interval = (self.config.session_timeout_ms / 10).max(1);
        if self.clock_ms < self.last_sweep_ms + sweep_interval {
            return;
        }
        self.last_sweep_ms = self.clock_ms;
        let deadline = self.clock_ms.saturating_sub(self.config.session_timeout_ms);
        self.sessions.retain(|_, s| s.active_at_ms >= deadline);
    }

    /// Reads a key from local state. Callers must make sure the read is
    /// linearizable (`ReadIndex`) before trusting the result.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(Vec::as_slice)
    }

    /// The number of keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the store holds no keys.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The number of open client sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}
