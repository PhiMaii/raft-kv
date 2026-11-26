//! Rust client library with leader discovery and retries.
//!
//! A [`Client`] knows the addresses of the cluster's nodes and finds the
//! leader on its own: it sends a request to the node it believes is leader,
//! follows `NotLeader` hints, and moves on to the next node when one does
//! not answer. Writes are retried with the same sequence number, so a
//! write is applied at most once however often it is retried.
//!
//! ```no_run
//! # async fn example() -> Result<(), client::Error> {
//! let mut client = client::Client::new(
//!     vec!["127.0.0.1:7001".into(), "127.0.0.1:7002".into(), "127.0.0.1:7003".into()],
//!     client::ClientConfig::default(),
//! );
//! client.put(b"greeting".to_vec(), b"hello".to_vec()).await?;
//! assert_eq!(client.get(b"greeting".to_vec()).await?, Some(b"hello".to_vec()));
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use kv::ClientId;
use tokio::time::Instant;
use transport::{ApiError, ClientError, KvClient, WriteId};

pub use transport::Swapped;

/// Client settings.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// How long a single attempt at one node may take, including connecting.
    pub attempt_timeout: Duration,
    /// How long a request may take in total, across all retries, before the
    /// client gives up with [`Error::Unavailable`].
    pub deadline: Duration,
    /// The wait before retrying after a failed attempt. It doubles with
    /// every failure, up to one second.
    pub backoff: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_secs(10),
            deadline: Duration::from_secs(30),
            backoff: Duration::from_millis(50),
        }
    }
}

/// Why a request failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// No node answered before the deadline. A write may or may not have
    /// been applied.
    Unavailable {
        /// What went wrong on the last attempt.
        last_error: String,
    },
    /// The client's session expired. A write may or may not have been
    /// applied. The next request opens a new session.
    SessionExpired,
    /// The cluster refused the request, for example because a key or value
    /// is too large. Nothing was applied.
    Rejected(ApiError),
}

impl Error {
    /// Whether a write that failed with this error may have been applied.
    pub fn outcome_unknown(&self) -> bool {
        matches!(self, Self::Unavailable { .. } | Self::SessionExpired)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { last_error } => {
                write!(f, "cluster unavailable (last error: {last_error})")
            }
            Self::SessionExpired => f.write_str("session expired"),
            Self::Rejected(error) => write!(f, "rejected: {error}"),
        }
    }
}

impl std::error::Error for Error {}

/// A request, kept so it can be sent again unchanged.
#[derive(Clone, Debug)]
enum Call {
    Register,
    Get(Vec<u8>),
    Put(WriteId, Vec<u8>, Vec<u8>),
    Delete(WriteId, Vec<u8>),
    CompareAndSwap(WriteId, Vec<u8>, Option<Vec<u8>>, Vec<u8>),
}

#[derive(Debug)]
enum Answer {
    Registered(ClientId),
    Value(Option<Vec<u8>>),
    Written,
    Deleted(bool),
    Swapped(Swapped),
}

/// A client of a raft-kv cluster.
///
/// A client has one session and one request in flight at a time, which is
/// what `&mut self` enforces. Use one client per concurrent worker.
#[derive(Debug)]
pub struct Client {
    nodes: Vec<String>,
    /// The next node to try when the leader is unknown.
    next_node: usize,
    /// The node believed to be leader.
    leader: Option<String>,
    connections: HashMap<String, KvClient>,
    session: Option<ClientId>,
    next_sequence: u64,
    config: ClientConfig,
}

impl Client {
    /// Creates a client for the cluster whose nodes listen at `nodes`
    /// (`host:port`). Connections are opened on first use.
    ///
    /// # Panics
    ///
    /// Panics if `nodes` is empty.
    pub fn new(nodes: Vec<String>, config: ClientConfig) -> Self {
        assert!(
            !nodes.is_empty(),
            "a client needs at least one node address"
        );
        Self {
            nodes,
            next_node: 0,
            leader: None,
            connections: HashMap::new(),
            session: None,
            next_sequence: 1,
            config,
        }
    }

    /// Reads a key; `None` if it does not exist. Linearizable.
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        match self.send(Call::Get(key)).await? {
            Answer::Value(value) => Ok(value),
            other => Err(unexpected(&other)),
        }
    }

    /// Sets a key to a value.
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let id = self.next_write().await?;
        match self.send(Call::Put(id, key, value)).await? {
            Answer::Written => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    /// Removes a key. Returns whether it existed.
    pub async fn delete(&mut self, key: Vec<u8>) -> Result<bool, Error> {
        let id = self.next_write().await?;
        match self.send(Call::Delete(id, key)).await? {
            Answer::Deleted(existed) => Ok(existed),
            other => Err(unexpected(&other)),
        }
    }

    /// Sets `key` to `new` if its current value is `expected`, where `None`
    /// means the key must not exist.
    pub async fn compare_and_swap(
        &mut self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<Swapped, Error> {
        let id = self.next_write().await?;
        match self
            .send(Call::CompareAndSwap(id, key, expected, new))
            .await?
        {
            Answer::Swapped(swapped) => Ok(swapped),
            other => Err(unexpected(&other)),
        }
    }

    /// The id for the next write, opening a session first if needed.
    async fn next_write(&mut self) -> Result<WriteId, Error> {
        let client_id = if let Some(client_id) = self.session {
            client_id
        } else {
            let Answer::Registered(client_id) = self.send(Call::Register).await? else {
                return Err(Error::Rejected(ApiError::Internal(
                    "unexpected answer to RegisterClient".to_owned(),
                )));
            };
            self.session = Some(client_id);
            self.next_sequence = 1;
            client_id
        };
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        Ok(WriteId {
            client_id,
            sequence,
        })
    }

    /// Sends `call` until a node answers it or the deadline passes.
    async fn send(&mut self, call: Call) -> Result<Answer, Error> {
        let deadline = Instant::now() + self.config.deadline;
        let mut backoff = self.config.backoff;
        let mut last_error;
        loop {
            let address = self.target();
            let attempt = tokio::time::timeout(self.config.attempt_timeout, async {
                let connection = self.connection(&address).await?;
                attempt(connection, call.clone()).await
            })
            .await
            .unwrap_or_else(|_| Err(ClientError::Rpc(transport_timeout())));

            let retry_now = match attempt {
                Ok(answer) => {
                    self.leader = Some(address);
                    return Ok(answer);
                }
                Err(ClientError::Api(ApiError::NotLeader(Some(hint))))
                    if hint.address != address =>
                {
                    last_error = format!("{address} is not the leader");
                    self.leader = Some(hint.address);
                    true
                }
                Err(ClientError::Api(ApiError::NotLeader(_))) => {
                    last_error = format!("{address} is not the leader and knows of none");
                    false
                }
                Err(ClientError::Api(ApiError::Unavailable(reason))) => {
                    last_error = format!("{address}: {reason}");
                    false
                }
                Err(ClientError::Api(ApiError::SessionExpired)) => {
                    self.session = None;
                    return Err(Error::SessionExpired);
                }
                Err(ClientError::Api(error)) => return Err(Error::Rejected(error)),
                Err(ClientError::Rpc(status)) => {
                    last_error = format!("{address}: {}", status.message());
                    self.connections.remove(&address);
                    false
                }
            };
            if !retry_now {
                // Try the next node, after a pause: the cluster may be in
                // the middle of an election.
                self.leader = None;
                self.next_node = (self.next_node + 1) % self.nodes.len();
                if Instant::now() + backoff >= deadline {
                    return Err(Error::Unavailable { last_error });
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
            if Instant::now() >= deadline {
                return Err(Error::Unavailable { last_error });
            }
        }
    }

    /// The node to send the next attempt to.
    fn target(&self) -> String {
        self.leader
            .clone()
            .unwrap_or_else(|| self.nodes[self.next_node].clone())
    }

    async fn connection(&mut self, address: &str) -> Result<&mut KvClient, ClientError> {
        if !self.connections.contains_key(address) {
            let connection = KvClient::connect(&format!("http://{address}"))
                .await
                .map_err(|error| {
                    ClientError::Rpc(transport::Status::unavailable(error.to_string()))
                })?;
            self.connections.insert(address.to_owned(), connection);
        }
        Ok(self.connections.get_mut(address).expect("inserted above"))
    }
}

fn transport_timeout() -> transport::Status {
    transport::Status::deadline_exceeded("no answer in time")
}

fn unexpected(answer: &Answer) -> Error {
    Error::Rejected(ApiError::Internal(format!("unexpected answer {answer:?}")))
}

async fn attempt(client: &mut KvClient, call: Call) -> Result<Answer, ClientError> {
    Ok(match call {
        Call::Register => Answer::Registered(client.register_client().await?),
        Call::Get(key) => Answer::Value(client.get(key).await?),
        Call::Put(id, key, value) => {
            client.put(id, key, value).await?;
            Answer::Written
        }
        Call::Delete(id, key) => Answer::Deleted(client.delete(id, key).await?),
        Call::CompareAndSwap(id, key, expected, new) => {
            Answer::Swapped(client.compare_and_swap(id, key, expected, new).await?)
        }
    })
}
