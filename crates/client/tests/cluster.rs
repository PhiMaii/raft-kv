//! The client library against real nodes on localhost.
//!
//! Each node runs in a Tokio runtime of its own, so a node can be killed
//! together with its connections, like a crashed process, and restarted
//! from its data directory.

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use client::{Client, ClientConfig, Swapped};
use raft_core::Role;
use server::{DataDir, PeerAddress, ServerConfig, StartError};
use tokio::runtime::Runtime;
use transport::{KvClient, NodeStatus};

struct Node {
    config: ServerConfig,
    runtime: Option<Runtime>,
}

impl Node {
    fn start(&mut self, data_dir: DataDir) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let config = self.config.clone();
        // The previous run's listener may close in the background for a
        // moment after a kill.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match runtime.block_on(server::start(config.clone(), data_dir)) {
                Ok(_server) => break,
                Err(StartError::Bind(..)) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("node {} failed to start: {error}", config.id),
            }
        }
        self.runtime = Some(runtime);
    }

    fn kill(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }

    fn address(&self) -> String {
        self.config.listen.to_string()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

struct Cluster {
    nodes: Vec<Node>,
    client_runtime: Runtime,
    _dir: tempfile::TempDir,
}

fn free_addresses(n: usize) -> Vec<SocketAddr> {
    let listeners: Vec<_> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("address"))
        .collect()
}

impl Cluster {
    fn start(size: u64) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let addresses = free_addresses(usize::try_from(size).expect("small"));
        let nodes = (1..=size)
            .map(|id| {
                let index = usize::try_from(id - 1).expect("small");
                let peers = (1..=size)
                    .filter(|&peer| peer != id)
                    .map(|peer| PeerAddress {
                        id: peer,
                        address: addresses[usize::try_from(peer - 1).expect("small")].to_string(),
                    })
                    .collect();
                let mut node = Node {
                    config: ServerConfig {
                        id,
                        listen: addresses[index],
                        data_dir: dir.path().join(format!("node{id}")),
                        peers,
                        tick_ms: 10,
                        request_timeout_ms: 2_000,
                    },
                    runtime: None,
                };
                node.start(DataDir::CreateIfMissing);
                node
            })
            .collect();
        Self {
            nodes,
            client_runtime: Runtime::new().expect("runtime"),
            _dir: dir,
        }
    }

    fn client(&self, nodes: &[usize]) -> Client {
        let addresses = nodes.iter().map(|&i| self.nodes[i].address()).collect();
        Client::new(addresses, ClientConfig::default())
    }

    fn status(&self, i: usize) -> Option<NodeStatus> {
        let address = format!("http://{}", self.nodes[i].address());
        self.client_runtime.block_on(async {
            let mut client = KvClient::connect(&address).await.ok()?;
            client.status().await.ok()
        })
    }

    /// Waits until a node is leader and returns its index.
    fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            for i in 0..self.nodes.len() {
                if self.nodes[i].runtime.is_some()
                    && self.status(i).is_some_and(|s| s.role == Role::Leader)
                {
                    return i;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("no leader elected within 10 s");
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.client_runtime.block_on(future)
    }
}

#[test]
fn a_client_that_only_knows_a_follower_finds_the_leader() {
    let cluster = Cluster::start(3);
    let leader = cluster.leader();
    let follower = (leader + 1) % 3;
    let mut client = cluster.client(&[follower]);
    cluster.block_on(async {
        client.put(b"k".to_vec(), b"1".to_vec()).await.expect("put");
        assert_eq!(
            client.get(b"k".to_vec()).await.expect("get"),
            Some(b"1".to_vec())
        );
        let swapped = client
            .compare_and_swap(b"k".to_vec(), Some(b"1".to_vec()), b"2".to_vec())
            .await
            .expect("cas");
        assert_eq!(
            swapped,
            Swapped {
                succeeded: true,
                previous: Some(b"1".to_vec()),
            }
        );
        assert!(client.delete(b"k".to_vec()).await.expect("delete"));
        assert_eq!(client.get(b"k".to_vec()).await.expect("get"), None);
    });
}

#[test]
fn writes_survive_a_leader_crash_and_the_old_leader_catches_up() {
    let mut cluster = Cluster::start(3);
    let mut client = cluster.client(&[0, 1, 2]);
    cluster.block_on(async {
        client
            .put(b"before".to_vec(), b"1".to_vec())
            .await
            .expect("put before the crash");
    });

    let old_leader = cluster.leader();
    cluster.nodes[old_leader].kill();
    cluster.block_on(async {
        client
            .put(b"after".to_vec(), b"2".to_vec())
            .await
            .expect("put after the crash");
        assert_eq!(
            client.get(b"before".to_vec()).await.expect("get"),
            Some(b"1".to_vec()),
            "an acknowledged write survives the leader's crash"
        );
    });
    let new_leader = cluster.leader();
    assert_ne!(new_leader, old_leader);

    // The old leader restarts from its disk and catches up.
    cluster.nodes[old_leader].start(DataDir::MustExist);
    let target = cluster
        .status(new_leader)
        .expect("leader answers")
        .commit_index;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = cluster.status(old_leader);
        if status.as_ref().is_some_and(|s| s.last_applied >= target) {
            assert_eq!(status.expect("checked").role, Role::Follower);
            break;
        }
        assert!(Instant::now() < deadline, "restarted node did not catch up");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_node_without_its_data_directory_refuses_to_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    let missing: PathBuf = dir.path().join("lost");
    let config = ServerConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().expect("address"),
        data_dir: missing.clone(),
        peers: Vec::new(),
        tick_ms: 10,
        request_timeout_ms: 1_000,
    };
    let runtime = Runtime::new().expect("runtime");
    let result = runtime.block_on(server::start(config, DataDir::MustExist));
    assert!(matches!(result, Err(StartError::MissingDataDir(dir)) if dir == missing));
    assert!(!missing.exists(), "nothing was created");
}
