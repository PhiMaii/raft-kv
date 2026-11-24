//! Leader election in a small in-memory cluster.
//!
//! A minimal deterministic harness: nodes tick in lockstep and every message
//! is delivered within the same step, unless the receiver is cut off from the
//! sender. The full fault-injecting simulator lives in the `sim` crate.

use std::collections::{BTreeSet, VecDeque};

use raft_core::{Config, Message, NodeId, Raft, Role, Term};

struct Cluster {
    nodes: Vec<Raft>,
    /// Nodes that can neither send nor receive messages.
    isolated: BTreeSet<NodeId>,
}

impl Cluster {
    fn new(size: u64, seed: u64) -> Self {
        let ids: Vec<_> = (1..=size).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                Raft::new(Config::new(id, peers, seed)).expect("valid config")
            })
            .collect();
        Self {
            nodes,
            isolated: BTreeSet::new(),
        }
    }

    fn node(&self, id: NodeId) -> &Raft {
        self.nodes
            .iter()
            .find(|n| n.id() == id)
            .expect("known node")
    }

    /// Advances every node by one tick and delivers all resulting messages.
    fn step(&mut self) {
        for node in &mut self.nodes {
            node.tick();
        }
        self.deliver_all();
    }

    fn deliver_all(&mut self) {
        let mut queue: VecDeque<Message> = VecDeque::new();
        loop {
            for node in &mut self.nodes {
                // Hard state would be persisted here; this harness is in
                // memory only.
                queue.extend(node.take_ready().messages);
            }
            let Some(message) = queue.pop_front() else {
                break;
            };
            if self.isolated.contains(&message.from) || self.isolated.contains(&message.to) {
                continue;
            }
            if let Some(node) = self.nodes.iter_mut().find(|n| n.id() == message.to) {
                node.step(message);
            }
        }
    }

    /// Steps until exactly one reachable node is leader, and returns it.
    fn run_until_leader(&mut self, max_steps: usize) -> NodeId {
        for _ in 0..max_steps {
            self.step();
            let leaders = self.leaders();
            if let [leader] = leaders.as_slice() {
                return *leader;
            }
        }
        panic!("no single leader after {max_steps} steps");
    }

    /// Leaders among the nodes that are not isolated.
    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.role() == Role::Leader && !self.isolated.contains(&n.id()))
            .map(Raft::id)
            .collect()
    }
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    for seed in 0..200 {
        let mut cluster = Cluster::new(3, seed);
        let leader = cluster.run_until_leader(500);
        let term = cluster.node(leader).term();

        // Let the cluster run on: the leader must stay in place.
        for _ in 0..200 {
            cluster.step();
        }
        assert_eq!(cluster.leaders(), vec![leader], "seed {seed}");
        for node in &cluster.nodes {
            assert_eq!(node.term(), term, "seed {seed}: terms agree");
            assert_eq!(node.leader_id(), Some(leader), "seed {seed}: leader known");
        }
    }
}

#[test]
fn five_nodes_elect_exactly_one_leader() {
    for seed in 0..100 {
        let mut cluster = Cluster::new(5, seed);
        let leader = cluster.run_until_leader(500);
        for node in &cluster.nodes {
            assert_eq!(node.leader_id(), Some(leader), "seed {seed}");
        }
    }
}

#[test]
fn majority_elects_a_new_leader_when_the_leader_is_cut_off() {
    for seed in 0..50 {
        let mut cluster = Cluster::new(3, seed);
        let old_leader = cluster.run_until_leader(500);
        let old_term = cluster.node(old_leader).term();

        cluster.isolated.insert(old_leader);
        let new_leader = cluster.run_until_leader(500);
        assert_ne!(new_leader, old_leader, "seed {seed}");
        assert!(cluster.node(new_leader).term() > old_term, "seed {seed}");

        // When the partition heals, the old leader learns about the newer
        // term and steps down.
        cluster.isolated.clear();
        for _ in 0..100 {
            cluster.step();
        }
        assert_eq!(
            cluster.node(old_leader).role(),
            Role::Follower,
            "seed {seed}"
        );
        assert_eq!(cluster.leaders(), vec![new_leader], "seed {seed}");
    }
}

#[test]
fn minority_cannot_elect_a_leader() {
    let mut cluster = Cluster::new(5, 3);
    let leader = cluster.run_until_leader(500);

    // Cut off three nodes, including the leader: the remaining two must not
    // elect anyone, no matter how long they try.
    let cut_off: Vec<_> = cluster
        .nodes
        .iter()
        .map(Raft::id)
        .filter(|&id| id != leader)
        .take(2)
        .chain([leader])
        .collect();
    cluster.isolated.extend(cut_off);

    for _ in 0..2000 {
        cluster.step();
    }
    assert_eq!(cluster.leaders(), Vec::<NodeId>::new());
    assert!(cluster
        .nodes
        .iter()
        .filter(|n| !cluster.isolated.contains(&n.id()))
        .all(|n| n.term() > Term(1)));
}
