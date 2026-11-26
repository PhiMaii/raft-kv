//! Raft messages between nodes: the receiving service and the sending side.
//!
//! Every node runs one [`RaftService`] and one [`Peer`] per other node.
//! A peer keeps a single long-lived stream open to its node and sends
//! messages down it in the order they were queued. If the connection breaks,
//! it reconnects with exponential backoff.
//!
//! Delivery is best effort, which is all Raft needs: the protocol already
//! tolerates lost, delayed and duplicated messages (and the simulator tests
//! that it does). So the sender never blocks the caller. When a peer is
//! unreachable for long enough that its queue fills up, new messages are
//! dropped, and messages that were in flight on a broken connection are
//! lost. Within one connection, messages arrive in order.

use std::time::Duration;

use raft_core::{Message, NodeId};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use crate::convert::{decode_message, encode_message};
use crate::proto::raft as wire;
use crate::proto::raft::raft_client::RaftClient;
use crate::proto::raft::raft_server::{Raft, RaftServer};
use crate::MAX_MESSAGE_BYTES;

/// Receives Raft messages from other nodes and passes them on to the local
/// node through a channel.
///
/// Messages are checked before they are passed on: a malformed message, or
/// one addressed to a different node (a sign of a misconfigured cluster),
/// ends the sender's stream with an error.
#[derive(Debug)]
pub struct RaftService {
    local: NodeId,
    inbox: mpsc::Sender<Message>,
}

impl RaftService {
    /// Creates the service for node `local`, which delivers incoming
    /// messages to `inbox`. When `inbox` is full, the service stops reading
    /// from the network until there is room again.
    pub fn new(local: NodeId, inbox: mpsc::Sender<Message>) -> Self {
        Self { local, inbox }
    }

    /// Wraps the service for a [`tonic::transport::Server`].
    pub fn into_server(self) -> RaftServer<Self> {
        RaftServer::new(self).max_decoding_message_size(MAX_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn send(
        &self,
        request: Request<Streaming<wire::RaftMessage>>,
    ) -> Result<Response<wire::SendSummary>, Status> {
        let mut stream = request.into_inner();
        let mut received = 0;
        while let Some(message) = stream.message().await? {
            let message = decode_message(message)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if message.to != self.local {
                return Err(Status::failed_precondition(format!(
                    "message for {} was delivered to {}",
                    message.to, self.local
                )));
            }
            self.inbox
                .send(message)
                .await
                .map_err(|_| Status::unavailable("the node is shutting down"))?;
            received += 1;
        }
        Ok(Response::new(wire::SendSummary { received }))
    }
}

/// Settings for a [`Peer`].
#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// How many messages may wait for the connection. Further messages are
    /// dropped.
    pub queue_capacity: usize,
    /// How long to wait for a connection to be established.
    pub connect_timeout: Duration,
    /// The wait before the first reconnection attempt. It doubles with every
    /// failed attempt, up to `max_backoff`.
    pub min_backoff: Duration,
    /// The longest wait between reconnection attempts. Keep it well below
    /// Raft's election timeout: a node that restarts and hears nothing from
    /// the leader for that long starts an election, deposing a healthy
    /// leader.
    pub max_backoff: Duration,
    /// How often to check an idle connection with an HTTP/2 ping, so that a
    /// peer that stopped responding is noticed and reconnected.
    pub keep_alive_interval: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096,
            connect_timeout: Duration::from_millis(500),
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
            keep_alive_interval: Duration::from_secs(1),
        }
    }
}

/// The sending side of the connection to one other node.
///
/// Sending queues the message and returns at once; a background task owns
/// the connection. Dropping the `Peer` stops that task.
#[derive(Debug)]
pub struct Peer {
    id: NodeId,
    queue: mpsc::Sender<Message>,
    task: JoinHandle<()>,
}

impl Peer {
    /// Starts sending to node `id` at `address` (for example
    /// `"http://10.0.0.2:7000"`). The connection is opened in the
    /// background, so the node does not have to be up yet.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn spawn(id: NodeId, address: &str, config: PeerConfig) -> Result<Self, InvalidAddress> {
        let endpoint = Endpoint::from_shared(address.to_owned())
            .map_err(|_| InvalidAddress(address.to_owned()))?
            .connect_timeout(config.connect_timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(config.keep_alive_interval)
            .keep_alive_timeout(config.keep_alive_interval * 2)
            .keep_alive_while_idle(true);
        let (queue, receiver) = mpsc::channel(config.queue_capacity);
        let task = tokio::spawn(run(endpoint, receiver, config));
        Ok(Self { id, queue, task })
    }

    /// The node this peer sends to.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Queues `message` for sending. Returns `false` if it was dropped
    /// because the queue is full.
    pub fn send(&self, message: Message) -> bool {
        debug_assert_eq!(message.to, self.id, "message sent to the wrong peer");
        self.queue.try_send(message).is_ok()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A peer address that is not a valid URI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidAddress(pub String);

impl std::fmt::Display for InvalidAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid peer address: {:?}", self.0)
    }
}

impl std::error::Error for InvalidAddress {}

/// Messages buffered between the queue and an open stream.
const STREAM_BUFFER: usize = 64;

/// Connects, streams, and reconnects until the [`Peer`] is dropped.
async fn run(endpoint: Endpoint, mut queue: mpsc::Receiver<Message>, config: PeerConfig) {
    let mut backoff = config.min_backoff;
    loop {
        if let Ok(channel) = endpoint.connect().await {
            backoff = config.min_backoff;
            stream(channel, &mut queue).await;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(config.max_backoff);
    }
}

/// Forwards queued messages into one stream until the connection breaks.
async fn stream(channel: Channel, queue: &mut mpsc::Receiver<Message>) {
    let mut client = RaftClient::new(channel);
    let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
    let call = client.send(ReceiverStream::new(receiver));
    tokio::pin!(call);
    loop {
        tokio::select! {
            // The receiver ended the stream: it failed or shut down.
            _ = &mut call => return,
            message = queue.recv() => {
                // `None` cannot happen: the task is aborted when the `Peer`
                // and with it the queue's only sender are dropped.
                let Some(message) = message else { return };
                tokio::select! {
                    sent = sender.send(encode_message(&message)) => {
                        if sent.is_err() {
                            return;
                        }
                    }
                    _ = &mut call => return,
                }
            }
        }
    }
}
