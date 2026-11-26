//! Both services over real TCP connections on localhost.

use std::future::{ready, Future};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kv::{ClientId, Command, Envelope, KvConfig, KvStore, Operation, Response};
use raft_core::{LogIndex, Message, MessageBody, NodeId, Term};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use transport::proto::kv::key_value_client::KeyValueClient;
use transport::proto::kv::PutRequest;
use transport::{
    ApiError, ClientError, KvClient, KvHandler, KvService, LeaderHint, Peer, PeerConfig,
    RaftService, Swapped, WriteId,
};

const WAIT: Duration = Duration::from_secs(10);

/// Serves `RaftService` for `node` on `listener`; abort the handle to kill
/// the server and every connection to it.
fn serve_raft(listener: TcpListener, node: NodeId) -> (JoinHandle<()>, mpsc::Receiver<Message>) {
    let (inbox, received) = mpsc::channel(1024);
    let server = Server::builder()
        .add_service(RaftService::new(node, inbox).into_server())
        .serve_with_incoming(TcpListenerStream::new(listener));
    let handle = tokio::spawn(async move {
        server.await.expect("server runs");
    });
    (handle, received)
}

async fn listen() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    (listener, address)
}

fn heartbeat(from: u64, to: u64, commit: u64) -> Message {
    Message {
        from: NodeId(from),
        to: NodeId(to),
        term: Term(1),
        body: MessageBody::AppendEntries {
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: Vec::new(),
            leader_commit: LogIndex(commit),
        },
    }
}

fn commit_of(message: &Message) -> u64 {
    match message.body {
        MessageBody::AppendEntries { leader_commit, .. } => leader_commit.0,
        _ => panic!("unexpected message {message:?}"),
    }
}

fn peer(id: u64, address: SocketAddr) -> Peer {
    Peer::spawn(
        NodeId(id),
        &format!("http://{address}"),
        PeerConfig::default(),
    )
    .expect("valid address")
}

#[tokio::test]
async fn raft_messages_arrive_in_order() {
    let (listener, address) = listen().await;
    let (_server, mut received) = serve_raft(listener, NodeId(2));
    let peer = peer(2, address);
    for commit in 1..=1000 {
        assert!(peer.send(heartbeat(1, 2, commit)));
    }
    for commit in 1..=1000 {
        let message = timeout(WAIT, received.recv())
            .await
            .expect("message arrives")
            .expect("inbox open");
        assert_eq!(message, heartbeat(1, 2, commit));
    }
}

#[tokio::test]
async fn peer_waits_for_a_node_that_is_not_up_yet() {
    let (listener, address) = listen().await;
    drop(listener);
    let peer = peer(2, address);
    assert!(peer.send(heartbeat(1, 2, 1)));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let listener = TcpListener::bind(address).await.expect("rebind");
    let (_server, mut received) = serve_raft(listener, NodeId(2));
    let message = timeout(WAIT, received.recv())
        .await
        .expect("message arrives");
    assert_eq!(message, Some(heartbeat(1, 2, 1)));
}

/// A node's Raft service in a runtime of its own, so that it can be killed
/// together with all of its connections, like a crashed process.
struct Process {
    runtime: tokio::runtime::Runtime,
}

impl Process {
    fn start(address: SocketAddr, node: NodeId) -> (Self, mpsc::Receiver<Message>) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        // The killed process's listener closes in the background, so the
        // port may still be taken for a moment.
        let listener = (0..500)
            .find_map(|_| {
                std::net::TcpListener::bind(address)
                    .inspect_err(|_| std::thread::sleep(Duration::from_millis(10)))
                    .ok()
            })
            .expect("bind");
        listener.set_nonblocking(true).expect("non-blocking");
        let received = {
            let _context = runtime.enter();
            let listener = TcpListener::from_std(listener).expect("listener");
            serve_raft(listener, node).1
        };
        (Self { runtime }, received)
    }

    fn kill(self) {
        self.runtime.shutdown_background();
    }
}

#[tokio::test]
async fn peer_reconnects_after_the_node_restarts() {
    let (listener, address) = listen().await;
    drop(listener);
    let (process, mut received) = Process::start(address, NodeId(2));
    let peer = peer(2, address);
    assert!(peer.send(heartbeat(1, 2, 1)));
    assert_eq!(
        timeout(WAIT, received.recv()).await.expect("first message"),
        Some(heartbeat(1, 2, 1))
    );

    process.kill();
    let (process, mut received) = Process::start(address, NodeId(2));

    // Messages sent while the connection breaks may be lost, so keep
    // sending until one gets through, as Raft's heartbeats would.
    let delivered = timeout(WAIT, async {
        let mut commit = 2;
        loop {
            assert!(peer.send(heartbeat(1, 2, commit)));
            commit += 1;
            if let Ok(Some(message)) = timeout(Duration::from_millis(50), received.recv()).await {
                return message;
            }
        }
    })
    .await
    .expect("a message arrives after the restart");
    assert!(commit_of(&delivered) >= 2);
    process.kill();
}

#[tokio::test]
async fn messages_for_another_node_are_rejected() {
    let (listener, address) = listen().await;
    let (_server, mut received) = serve_raft(listener, NodeId(2));
    // A misconfigured cluster: node 3's address points at node 2.
    let peer = peer(3, address);
    assert!(peer.send(heartbeat(1, 3, 1)));
    assert!(
        timeout(Duration::from_millis(500), received.recv())
            .await
            .is_err(),
        "the message must not be delivered"
    );
}

/// Applies commands straight to a local state machine, as a single-node
/// cluster would.
struct LocalStore {
    state: Mutex<(KvStore, u64)>,
}

impl LocalStore {
    fn new() -> Self {
        let config = KvConfig {
            max_key_len: 16,
            ..KvConfig::default()
        };
        Self {
            state: Mutex::new((KvStore::new(config), 0)),
        }
    }

    fn apply(&self, command: Command) -> Response {
        let mut state = self.state.lock().expect("not poisoned");
        let (store, index) = &mut *state;
        *index += 1;
        let entry = Envelope {
            timestamp_ms: 1_700_000_000_000,
            command,
        };
        store.apply(*index, &entry.encode())
    }
}

// The handlers answer synchronously, so their futures are ready at once.
impl KvHandler for LocalStore {
    fn register_client(&self) -> impl Future<Output = Result<ClientId, ApiError>> + Send {
        ready(match self.apply(Command::RegisterClient) {
            Response::Registered { client_id } => Ok(client_id),
            other => Err(ApiError::Internal(format!("{other:?}"))),
        })
    }

    fn get(&self, key: Vec<u8>) -> impl Future<Output = Result<Option<Vec<u8>>, ApiError>> + Send {
        let state = self.state.lock().expect("not poisoned");
        ready(Ok(state.0.get(&key).map(<[u8]>::to_vec)))
    }

    fn write(
        &self,
        id: WriteId,
        operation: Operation,
    ) -> impl Future<Output = Result<Response, ApiError>> + Send {
        ready(Ok(self.apply(Command::Request {
            client_id: id.client_id,
            sequence: id.sequence,
            operation,
        })))
    }
}

/// A node that is not the leader.
struct Follower;

fn leader_hint() -> LeaderHint {
    LeaderHint {
        id: NodeId(3),
        address: "http://127.0.0.1:7003".to_owned(),
    }
}

impl KvHandler for Follower {
    fn register_client(&self) -> impl Future<Output = Result<ClientId, ApiError>> + Send {
        ready(Err(ApiError::NotLeader(Some(leader_hint()))))
    }

    fn get(&self, _key: Vec<u8>) -> impl Future<Output = Result<Option<Vec<u8>>, ApiError>> + Send {
        ready(Err(ApiError::NotLeader(Some(leader_hint()))))
    }

    fn write(
        &self,
        _id: WriteId,
        _operation: Operation,
    ) -> impl Future<Output = Result<Response, ApiError>> + Send {
        ready(Err(ApiError::NotLeader(None)))
    }
}

async fn serve_kv<H: KvHandler>(handler: H) -> String {
    let (listener, address) = listen().await;
    let server = Server::builder()
        .add_service(KvService::new(Arc::new(handler)).into_server())
        .serve_with_incoming(TcpListenerStream::new(listener));
    tokio::spawn(async move { server.await.expect("server runs") });
    format!("http://{address}")
}

#[tokio::test]
async fn client_api_round_trips_every_operation() {
    let address = serve_kv(LocalStore::new()).await;
    let mut client = KvClient::connect(&address).await.expect("connect");
    let client_id = client.register_client().await.expect("register");
    let id = |sequence| WriteId {
        client_id,
        sequence,
    };

    assert_eq!(client.get(b"k".to_vec()).await.expect("get"), None);
    client
        .put(id(1), b"k".to_vec(), b"a".to_vec())
        .await
        .expect("put");
    assert_eq!(
        client.get(b"k".to_vec()).await.expect("get"),
        Some(b"a".to_vec())
    );

    let swap = client
        .compare_and_swap(id(2), b"k".to_vec(), Some(b"a".to_vec()), b"b".to_vec())
        .await
        .expect("cas");
    assert_eq!(
        swap,
        Swapped {
            succeeded: true,
            previous: Some(b"a".to_vec()),
        }
    );
    // A retry gets the original answer, although the value has changed
    // since and the comparison would now fail.
    let retry = client
        .compare_and_swap(id(2), b"k".to_vec(), Some(b"a".to_vec()), b"b".to_vec())
        .await
        .expect("retried cas");
    assert_eq!(retry, swap);

    let absent = client
        .compare_and_swap(id(3), b"k".to_vec(), None, b"c".to_vec())
        .await
        .expect("cas on absent");
    assert_eq!(
        absent,
        Swapped {
            succeeded: false,
            previous: Some(b"b".to_vec()),
        }
    );

    assert!(client.delete(id(4), b"k".to_vec()).await.expect("delete"));
    assert!(!client.delete(id(5), b"k".to_vec()).await.expect("delete"));
    assert_eq!(client.get(b"k".to_vec()).await.expect("get"), None);
}

#[tokio::test]
async fn state_machine_rejections_reach_the_client() {
    let address = serve_kv(LocalStore::new()).await;
    let mut client = KvClient::connect(&address).await.expect("connect");
    let client_id = client.register_client().await.expect("register");

    let unknown = WriteId {
        client_id: ClientId(999),
        sequence: 1,
    };
    let result = client.put(unknown, b"k".to_vec(), b"v".to_vec()).await;
    assert!(matches!(
        result,
        Err(ClientError::Api(ApiError::SessionExpired))
    ));

    let id = WriteId {
        client_id,
        sequence: 1,
    };
    let result = client.put(id, vec![b'k'; 17], b"v".to_vec()).await;
    assert!(matches!(
        result,
        Err(ClientError::Api(ApiError::KeyTooLarge))
    ));
}

#[tokio::test]
async fn followers_point_to_the_leader() {
    let address = serve_kv(Follower).await;
    let mut client = KvClient::connect(&address).await.expect("connect");
    match client.register_client().await {
        Err(ClientError::Api(ApiError::NotLeader(Some(hint)))) => {
            assert_eq!(hint, leader_hint());
        }
        other => panic!("expected a leader hint, got {other:?}"),
    }
    let id = WriteId {
        client_id: ClientId(1),
        sequence: 1,
    };
    assert!(matches!(
        client.delete(id, b"k".to_vec()).await,
        Err(ClientError::Api(ApiError::NotLeader(None)))
    ));
}

#[tokio::test]
async fn writes_without_a_header_are_invalid() {
    let address = serve_kv(LocalStore::new()).await;
    let mut raw = KeyValueClient::connect(address).await.expect("connect");
    let status = raw
        .put(PutRequest {
            header: None,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })
        .await
        .expect_err("rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
