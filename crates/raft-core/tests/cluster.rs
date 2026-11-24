//! Leader election and log replication in a small in-memory cluster.
//!
//! A minimal deterministic harness: nodes tick in lockstep and every message
//! is delivered within the same step, unless the receiver is cut off from the
//! sender. The full fault-injecting simulator lives in the `sim` crate.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use raft_core::{Config, Entry, EntryPayload, LogIndex, Message, NodeId, Raft, Role, Term};

struct Cluster {
    nodes: Vec<Raft>,
    /// Nodes that can neither send nor receive messages.
    isolated: BTreeSet<NodeId>,
    /// Every entry each node has applied, in order.
    applied: BTreeMap<NodeId, Vec<Entry>>,
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
            applied: ids.iter().map(|&id| (id, Vec::new())).collect(),
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
                // Hard state and entries would be persisted here; this
                // harness is in memory only.
                let ready = node.take_ready();
                queue.extend(ready.messages);
                self.applied
                    .get_mut(&node.id())
                    .expect("known node")
                    .extend(ready.committed_entries);
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

    /// Proposes a command on `node` and delivers the resulting messages.
    fn propose(&mut self, node: NodeId, command: u8) -> bool {
        let raft = self
            .nodes
            .iter_mut()
            .find(|n| n.id() == node)
            .expect("known node");
        let accepted = raft.propose(vec![command]).is_ok();
        self.deliver_all();
        accepted
    }

    /// Checks Raft's safety properties across all nodes:
    ///
    /// - **Log matching:** if two logs have an entry with the same index and
    ///   term, the logs are identical up to that index.
    /// - **State machine safety:** no two nodes apply different entries at
    ///   the same index, so every applied sequence is a prefix of the longest.
    fn assert_safe(&self, context: &str) {
        for a in &self.nodes {
            for b in &self.nodes {
                let common = a.log().last_index().min(b.log().last_index());
                let last_match = (1..=common.0)
                    .rev()
                    .map(LogIndex)
                    .find(|&i| a.log().term_at(i) == b.log().term_at(i));
                if let Some(last_match) = last_match {
                    let prefix_a = a.log().entries_between(LogIndex(1), last_match);
                    let prefix_b = b.log().entries_between(LogIndex(1), last_match);
                    assert_eq!(prefix_a, prefix_b, "{context}: log matching");
                }
            }
        }
        let longest = self
            .applied
            .values()
            .max_by_key(|a| a.len())
            .expect("nodes");
        for (id, applied) in &self.applied {
            assert_eq!(
                applied.as_slice(),
                &longest[..applied.len()],
                "{context}: state machine safety on {id}"
            );
        }
    }

    /// The commands applied by `node`, in order.
    fn applied_commands(&self, node: NodeId) -> Vec<u8> {
        self.applied[&node]
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Command(bytes) => Some(bytes[0]),
                EntryPayload::Noop => None,
            })
            .collect()
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

#[test]
fn proposals_are_replicated_and_applied_everywhere() {
    for seed in 0..100 {
        let mut cluster = Cluster::new(3, seed);
        let leader = cluster.run_until_leader(500);
        for command in 0..20 {
            assert!(cluster.propose(leader, command), "seed {seed}");
            cluster.step();
        }
        for _ in 0..20 {
            cluster.step();
        }

        let expected: Vec<u8> = (0..20).collect();
        for node in &cluster.nodes {
            assert_eq!(node.log(), cluster.node(leader).log(), "seed {seed}");
            assert_eq!(node.commit_index(), node.log().last_index(), "seed {seed}");
            assert_eq!(cluster.applied_commands(node.id()), expected, "seed {seed}");
        }
        cluster.assert_safe(&format!("seed {seed}"));
    }
}

#[test]
fn uncommitted_entries_of_a_cut_off_leader_are_replaced() {
    for seed in 0..50 {
        let mut cluster = Cluster::new(3, seed);
        let old_leader = cluster.run_until_leader(500);
        assert!(cluster.propose(old_leader, 1));
        cluster.step();

        // The old leader accepts proposals it can never commit.
        cluster.isolated.insert(old_leader);
        for command in 10..13 {
            assert!(cluster.propose(old_leader, command));
        }

        let new_leader = cluster.run_until_leader(500);
        for command in 20..23 {
            assert!(cluster.propose(new_leader, command));
        }

        cluster.isolated.clear();
        for _ in 0..100 {
            cluster.step();
        }

        for node in &cluster.nodes {
            assert_eq!(
                node.log(),
                cluster.node(new_leader).log(),
                "seed {seed}: logs converge"
            );
            assert_eq!(
                cluster.applied_commands(node.id()),
                vec![1, 20, 21, 22],
                "seed {seed}: the cut-off leader's commands are gone"
            );
        }
        cluster.assert_safe(&format!("seed {seed}"));
    }
}

#[test]
fn safety_holds_under_random_partitions() {
    // A small fuzz run ahead of the full simulator: random nodes are cut off
    // and reconnected while whoever is leader keeps accepting proposals.
    for seed in 0..100 {
        let mut cluster = Cluster::new(5, seed);
        let mut rng = seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut command = 0u8;

        for step in 0..1500 {
            if step % 50 == 0 {
                cluster.isolated.clear();
                for _ in 0..next() % 3 {
                    let victim = NodeId(next() % 5 + 1);
                    cluster.isolated.insert(victim);
                }
            }
            if step % 7 == 0 {
                let leaders = cluster.leaders();
                if let Some(&leader) = leaders.first() {
                    cluster.propose(leader, command);
                    command = command.wrapping_add(1);
                }
            }
            cluster.step();
            cluster.assert_safe(&format!("seed {seed}, step {step}"));
        }

        // After healing, everything converges.
        cluster.isolated.clear();
        for _ in 0..300 {
            cluster.step();
        }
        let leader = cluster.leaders()[0];
        let committed = cluster.applied_commands(leader).len();
        assert!(
            committed > 100,
            "seed {seed}: only {committed} commands committed, the run is too disrupted to mean much"
        );
        assert!(
            cluster.node(leader).term() > Term(1),
            "seed {seed}: expected at least one leader change"
        );
        for node in &cluster.nodes {
            assert_eq!(node.log(), cluster.node(leader).log(), "seed {seed}");
            assert_eq!(node.last_applied(), node.log().last_index(), "seed {seed}");
        }
        cluster.assert_safe(&format!("seed {seed}, healed"));
    }
}
