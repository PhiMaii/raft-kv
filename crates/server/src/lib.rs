//! Wires raft-core, storage and transport together into a running node.
//!
//! [`start`] recovers the node's state from its data directory, opens
//! connections to its peers, and serves the Raft and key-value gRPC
//! services on one address. One task drives the Raft state machine; it
//! persists, sends and applies in the order `raft-core` prescribes (see
//! `raft_core::Ready`):
//!
//! 1. every tick, message from a peer and client request goes into Raft;
//! 2. the resulting log entries and hard state are synced to disk; a
//!    leader's `AppendEntries` go out just before, so that its fsync
//!    overlaps with the round trip to its followers;
//! 3. only then are the other messages sent, so no vote or acknowledgement
//!    is ever sent for state that could still be lost;
//! 4. committed entries are applied to the key-value store, and waiting
//!    clients get their answers.
//!
//! Everything taken from the channels in one round shares one fsync.
//!
//! Once enough commands have been applied, the node snapshots the key-value
//! store on a blocking thread, from a cheap clone, and then compacts its log
//! (see `raft_core::Raft::compact`).

mod config;
mod handler;
mod node;

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kv::{KvConfig, KvStore};
use raft_core::{Config, NodeId, Raft};
use storage::{FsDisk, Storage, StorageError, StorageOptions};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{info, Instrument};
use transport::{KvService, Peer, PeerConfig, RaftService};

pub use config::{ConfigError, PeerAddress, ReadMode, ServerConfig};

use crate::handler::Handler;
use crate::node::Node;

/// What to do if the data directory does not exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataDir {
    /// Refuse to start. This is the normal case: a node that has run before
    /// and lost its data must not rejoin under its old id, because with an
    /// empty log and no memory of its votes it could vote twice in one term
    /// or help elect a leader that lacks committed entries.
    MustExist,
    /// Create it: this is a brand-new node, started for the first time.
    CreateIfMissing,
}

/// Why a node could not start.
#[derive(Debug)]
pub enum StartError {
    /// The configuration is not usable.
    Config(ConfigError),
    /// The data directory does not exist and [`DataDir::MustExist`] was
    /// given.
    MissingDataDir(PathBuf),
    /// The stored state could not be recovered.
    Storage(StorageError),
    /// The listen address could not be bound.
    Bind(SocketAddr, std::io::Error),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(f),
            Self::MissingDataDir(dir) => write!(
                f,
                "data directory {} does not exist. If this is a new node, start it with \
                 --init. A node that lost its data must not rejoin under its old id.",
                dir.display()
            ),
            Self::Storage(error) => write!(f, "cannot recover stored state: {error}"),
            Self::Bind(address, error) => write!(f, "cannot listen on {address}: {error}"),
        }
    }
}

impl std::error::Error for StartError {}

/// Why a running node stopped.
#[derive(Debug)]
pub enum StopError {
    /// Writing to disk failed. The node stopped rather than keep running
    /// without durable state.
    Storage(StorageError),
    /// The gRPC server failed.
    Server(tonic::transport::Error),
    /// A task panicked or was cancelled.
    Task(tokio::task::JoinError),
}

impl fmt::Display for StopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "storage failed: {error}"),
            Self::Server(error) => write!(f, "gRPC server failed: {error}"),
            Self::Task(error) => write!(f, "task failed: {error}"),
        }
    }
}

impl std::error::Error for StopError {}

/// A running node.
#[derive(Debug)]
pub struct Server {
    address: SocketAddr,
    node: JoinHandle<Result<(), StorageError>>,
    grpc: JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Capacity of the channel from the Raft service to the node.
const INBOX: usize = 4096;
/// Capacity of the channel from client handlers to the node.
const REQUESTS: usize = 4096;

/// Recovers the node's state, connects to its peers and starts serving.
///
/// Must be called from within a multi-threaded Tokio runtime.
pub async fn start(config: ServerConfig, data_dir: DataDir) -> Result<Server, StartError> {
    config.validate().map_err(StartError::Config)?;
    if data_dir == DataDir::MustExist && !config.data_dir.is_dir() {
        return Err(StartError::MissingDataDir(config.data_dir.clone()));
    }
    let id = NodeId(config.id);
    let disk = FsDisk::open(&config.data_dir)
        .map_err(|error| StartError::Storage(StorageError::Io(error)))?;
    let (storage, recovered) =
        Storage::open(disk, StorageOptions::default()).map_err(StartError::Storage)?;
    info!(
        node = id.0,
        term = recovered.hard_state.current_term.0,
        snapshot = recovered.snapshot.as_ref().map_or(0, |s| s.index.0),
        last_index = recovered.log.last_index().0,
        "recovered stored state"
    );
    let kv = match &recovered.snapshot {
        Some(snapshot) => {
            KvStore::restore(KvConfig::default(), &snapshot.data).map_err(|error| {
                StartError::Storage(StorageError::Corrupt {
                    file: "snapshot".to_owned(),
                    offset: 0,
                    reason: error.to_string(),
                })
            })?
        }
        None => KvStore::new(KvConfig::default()),
    };

    let peer_ids = config.peers.iter().map(|peer| NodeId(peer.id)).collect();
    let mut raft_config = Config::new(id, peer_ids, seed(id));
    raft_config.retained_entries = config.retained_entries;
    raft_config.batch_appends = config.batch_appends;
    raft_config.max_append_bytes = config.max_append_bytes;
    let raft = Raft::restore_with_snapshot(
        raft_config,
        recovered.hard_state,
        recovered.snapshot,
        recovered.log,
    )
    .map_err(|error| StartError::Config(ConfigError::Invalid(error.to_string())))?;

    let mut peers = BTreeMap::new();
    let mut addresses = BTreeMap::new();
    for peer in &config.peers {
        let peer_id = NodeId(peer.id);
        let uri = format!("http://{}", peer.address);
        let sender = Peer::spawn(peer_id, &uri, PeerConfig::default())
            .map_err(|error| StartError::Config(ConfigError::Invalid(error.to_string())))?;
        peers.insert(peer_id, sender);
        addresses.insert(peer_id, peer.address.clone());
    }

    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|error| StartError::Bind(config.listen, error))?;
    let address = listener
        .local_addr()
        .map_err(|error| StartError::Bind(config.listen, error))?;

    let (inbox, inbox_receiver) = mpsc::channel(INBOX);
    let (requests, request_receiver) = mpsc::channel(REQUESTS);
    let handler = Handler::new(requests, config.request_timeout(), config.read_mode);
    let grpc = tonic::transport::Server::builder()
        .add_service(RaftService::new(id, inbox).into_server())
        .add_service(KvService::new(Arc::new(handler)).into_server())
        .serve_with_incoming(TcpListenerStream::new(listener));

    let node = Node::new(raft, storage, kv, peers, addresses)
        .with_snapshot_threshold(config.snapshot_threshold_bytes)
        .with_send_before_persist(config.send_before_persist);
    let span = tracing::info_span!("node", id = id.0);
    let node = tokio::spawn(
        node.run(inbox_receiver, request_receiver, config.tick())
            .instrument(span),
    );
    let grpc = tokio::spawn(grpc);
    info!(node = id.0, %address, "serving");
    Ok(Server {
        address,
        node,
        grpc,
    })
}

impl Server {
    /// The address the node listens on.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Waits until the node stops, which only happens on an error.
    pub async fn wait(mut self) -> Result<(), StopError> {
        let result = tokio::select! {
            result = &mut self.node => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(StopError::Storage(error)),
                Err(error) => Err(StopError::Task(error)),
            },
            result = &mut self.grpc => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(StopError::Server(error)),
                Err(error) => Err(StopError::Task(error)),
            },
        };
        self.stop();
        result
    }

    /// Stops the node's tasks. Connections that are already open close
    /// with the runtime or the process.
    pub fn stop(&self) {
        self.node.abort();
        self.grpc.abort();
    }
}

/// Seeds the node's election timeouts. Each node and each start gets a
/// different seed, so that nodes rarely time out at the same moment.
fn seed(id: NodeId) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    #[allow(clippy::cast_possible_truncation)] // only the low bits matter
    let nanos = nanos as u64;
    nanos ^ id.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}
