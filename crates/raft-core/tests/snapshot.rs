//! Log compaction and `InstallSnapshot` (Raft §7).
//!
//! The state machine in these tests is the list of command bytes applied so
//! far; its snapshot is that list.

use std::collections::{BTreeSet, VecDeque};

use raft_core::{
    CompactError, Config, Entry, EntryPayload, HardState, Log, LogIndex, Message, MessageBody,
    NodeId, Raft, Role, Snapshot, Term,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

struct Node {
    raft: Raft,
    /// The toy state machine: every applied command byte, in order.
    state: Vec<u8>,
    snapshots_installed: usize,
}

struct Cluster {
    nodes: Vec<Node>,
    isolated: BTreeSet<NodeId>,
    /// `InstallSnapshot` messages delivered.
    chunks_delivered: usize,
}

impl Cluster {
    fn new(size: u64, configure: impl Fn(&mut Config)) -> Self {
        let ids: Vec<_> = (1..=size).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                let mut config = Config::new(id, peers, 7);
                configure(&mut config);
                Node {
                    raft: Raft::new(config).expect("valid config"),
                    state: Vec::new(),
                    snapshots_installed: 0,
                }
            })
            .collect();
        Self {
            nodes,
            isolated: BTreeSet::new(),
            chunks_delivered: 0,
        }
    }

    fn node(&mut self, id: NodeId) -> &mut Node {
        self.nodes
            .iter_mut()
            .find(|n| n.raft.id() == id)
            .expect("known node")
    }

    fn step(&mut self) {
        for node in &mut self.nodes {
            node.raft.tick();
        }
        self.deliver_all();
    }

    fn deliver_all(&mut self) {
        let mut queue = VecDeque::new();
        loop {
            for node in &mut self.nodes {
                let ready = node.raft.take_ready();
                if let Some(snapshot) = ready.snapshot {
                    node.state = snapshot.data.to_vec();
                    node.snapshots_installed += 1;
                }
                for entry in ready.committed_entries {
                    if let EntryPayload::Command(bytes) = entry.payload {
                        node.state.extend(bytes);
                    }
                }
                queue.extend(ready.messages);
            }
            let Some(message) = queue.pop_front() else {
                break;
            };
            if self.isolated.contains(&message.from) || self.isolated.contains(&message.to) {
                continue;
            }
            if matches!(message.body, MessageBody::InstallSnapshot { .. }) {
                self.chunks_delivered += 1;
            }
            let to = message.to;
            self.node(to).raft.step(message);
        }
    }

    fn run_until_leader(&mut self) -> NodeId {
        for _ in 0..500 {
            self.step();
            let leaders: Vec<_> = self
                .nodes
                .iter()
                .filter(|n| n.raft.role() == Role::Leader)
                .map(|n| n.raft.id())
                .collect();
            if let [leader] = leaders.as_slice() {
                return *leader;
            }
        }
        panic!("no leader");
    }

    fn propose(&mut self, leader: NodeId, command: u8) {
        self.node(leader)
            .raft
            .propose(vec![command])
            .expect("leader accepts");
        self.deliver_all();
    }

    /// Takes a snapshot of `id`'s state machine at its applied index.
    fn compact(&mut self, id: NodeId) -> Snapshot {
        let node = self.node(id);
        let applied = node.raft.last_applied();
        let data = node.state.clone();
        node.raft.compact(applied, data).expect("snapshot accepted")
    }
}

fn entries(range: std::ops::RangeInclusive<u64>, term: u64) -> Vec<Entry> {
    range
        .map(|index| Entry {
            term: Term(term),
            index: LogIndex(index),
            payload: EntryPayload::Command(vec![u8::try_from(index).expect("small")]),
        })
        .collect()
}

/// Node 1 of a three-node cluster, a follower in term 1 with `log`.
fn follower(log: Vec<Entry>) -> Raft {
    let log = Log::from_parts(LogIndex(0), Term(0), log).expect("valid log");
    let hard_state = HardState {
        current_term: Term(2),
        voted_for: None,
    };
    Raft::restore(Config::new(N1, vec![N2, N3], 7), hard_state, log).expect("valid config")
}

fn install(index: u64, term: u64, offset: u64, data: &[u8], total: u64) -> Message {
    Message {
        from: N2,
        to: N1,
        term: Term(2),
        body: MessageBody::InstallSnapshot {
            index: LogIndex(index),
            term: Term(term),
            offset,
            total,
            data: data.to_vec(),
            read_round: 0,
        },
    }
}

/// The `(received, done)` of the snapshot responses in `messages`.
fn responses(messages: &[Message]) -> Vec<(u64, bool)> {
    messages
        .iter()
        .filter_map(|m| match m.body {
            MessageBody::InstallSnapshotResponse { received, done, .. } => Some((received, done)),
            _ => None,
        })
        .collect()
}

#[test]
fn compaction_drops_entries_but_keeps_a_tail() {
    let mut cluster = Cluster::new(1, |config| config.retained_entries = 2);
    let leader = cluster.run_until_leader();
    for command in 1..=9 {
        cluster.propose(leader, command);
    }
    // The no-op at index 1, then nine commands.
    let raft = &mut cluster.node(leader).raft;
    assert_eq!(raft.last_applied(), LogIndex(10));

    assert_eq!(
        raft.compact(LogIndex(11), vec![]),
        Err(CompactError::NotApplied {
            index: LogIndex(11),
            last_applied: LogIndex(10)
        })
    );
    let snapshot = raft.compact(LogIndex(8), vec![1, 2, 3]).expect("accepted");
    assert_eq!((snapshot.index, snapshot.term), (LogIndex(8), raft.term()));
    assert_eq!(raft.snapshot(), Some(&snapshot));
    assert_eq!(raft.log().first_index(), LogIndex(7), "two entries kept");
    assert_eq!(raft.log().last_index(), LogIndex(10));
    assert_eq!(
        raft.compact(LogIndex(8), vec![]),
        Err(CompactError::Outdated {
            index: LogIndex(8),
            current: LogIndex(8)
        })
    );
    // A local snapshot is not handed out for installing.
    assert_eq!(raft.take_ready().snapshot, None);
}

#[test]
fn a_lagging_follower_catches_up_from_a_chunked_snapshot() {
    let mut cluster = Cluster::new(3, |config| {
        config.retained_entries = 0;
        config.snapshot_chunk_size = 3;
    });
    let leader = cluster.run_until_leader();
    let lagging = [N1, N2, N3]
        .into_iter()
        .find(|&id| id != leader)
        .expect("a follower");
    cluster.isolated.insert(lagging);
    for command in 1..=20 {
        cluster.propose(leader, command);
    }
    let snapshot = cluster.compact(leader);
    assert_eq!(snapshot.data.len(), 20);
    assert_eq!(cluster.node(leader).raft.log().entries().len(), 0);
    // Entries after the snapshot go through the log as usual.
    cluster.propose(leader, 21);

    cluster.isolated.clear();
    for _ in 0..50 {
        cluster.step();
    }
    let expected: Vec<u8> = (1..=21).collect();
    let follower = cluster.node(lagging);
    assert_eq!(follower.snapshots_installed, 1);
    assert_eq!(follower.state, expected);
    assert_eq!(follower.raft.snapshot(), Some(&snapshot));
    assert_eq!(follower.raft.log().snapshot_index(), snapshot.index);
    assert!(
        cluster.chunks_delivered >= 7,
        "20 bytes in chunks of 3: {} chunks",
        cluster.chunks_delivered
    );
    // The follower answers in the leader's term like any other, so it
    // counts towards commits again.
    cluster.isolated.insert(
        [N1, N2, N3]
            .into_iter()
            .find(|&id| id != leader && id != lagging)
            .expect("a third node"),
    );
    cluster.propose(leader, 22);
    // The follower learns the new commit index with the next heartbeat.
    for _ in 0..10 {
        cluster.step();
    }
    assert_eq!(cluster.node(lagging).state.last(), Some(&22));
}

#[test]
fn entries_after_a_matching_snapshot_are_kept() {
    let mut raft = follower(entries(1..=5, 1));
    raft.step(install(3, 1, 0, b"abc", 3));
    let ready = raft.take_ready();
    let snapshot = ready.snapshot.expect("installed");
    assert_eq!((snapshot.index, &*snapshot.data), (LogIndex(3), &b"abc"[..]));
    assert_eq!(raft.log().first_index(), LogIndex(4));
    assert_eq!(raft.log().last_index(), LogIndex(5));
    // Storage replaces its log with the snapshot, so the kept entries are
    // written again; nothing before the snapshot is applied.
    assert_eq!(ready.entries, entries(4..=5, 1));
    assert_eq!(ready.committed_entries, Vec::<Entry>::new());
    assert_eq!(raft.last_applied(), LogIndex(3));
    assert_eq!(responses(&ready.messages), vec![(3, true)]);
}

#[test]
fn a_log_that_conflicts_with_the_snapshot_is_discarded() {
    let mut raft = follower(entries(1..=5, 1));
    raft.step(install(4, 2, 0, b"x", 1));
    let ready = raft.take_ready();
    assert!(ready.snapshot.is_some());
    assert_eq!(ready.entries, Vec::<Entry>::new());
    assert_eq!(raft.log().last_index(), LogIndex(4));
    assert_eq!(raft.log().last_term(), Term(2));
    assert_eq!(raft.log().entries(), &[] as &[Entry]);
}

#[test]
fn chunks_are_assembled_in_order_whatever_the_network_does() {
    let mut raft = follower(Vec::new());
    // A chunk from the middle of an unknown transfer: start from 0.
    raft.step(install(9, 1, 2, b"cd", 6));
    assert_eq!(responses(&raft.take_ready().messages), vec![(0, false)]);

    raft.step(install(9, 1, 0, b"ab", 6));
    raft.step(install(9, 1, 0, b"ab", 6)); // duplicate
    raft.step(install(9, 1, 4, b"ef", 6)); // too early
    assert_eq!(
        responses(&raft.take_ready().messages),
        vec![(2, false), (2, false), (2, false)]
    );
    raft.step(install(9, 1, 2, b"cd", 6));
    raft.step(install(9, 1, 4, b"ef", 6));
    let ready = raft.take_ready();
    assert_eq!(responses(&ready.messages), vec![(4, false), (6, true)]);
    assert_eq!(&*ready.snapshot.expect("installed").data, b"abcdef");
}

#[test]
fn a_snapshot_the_follower_already_has_is_acknowledged_at_once() {
    let mut raft = follower(entries(1..=5, 1));
    // Learn that everything up to 5 is committed.
    raft.step(Message {
        from: N2,
        to: N1,
        term: Term(2),
        body: MessageBody::AppendEntries {
            prev_log_index: LogIndex(5),
            prev_log_term: Term(1),
            entries: Vec::new(),
            leader_commit: LogIndex(5),
            read_round: 0,
        },
    });
    let _ = raft.take_ready();
    raft.step(install(4, 1, 0, b"x", 100));
    let ready = raft.take_ready();
    assert_eq!(ready.snapshot, None);
    assert_eq!(responses(&ready.messages), vec![(100, true)]);
    assert_eq!(raft.log().first_index(), LogIndex(1), "log untouched");
}

#[test]
fn duplicate_acknowledgements_do_not_multiply_chunks() {
    let mut cluster = Cluster::new(3, |config| {
        config.retained_entries = 0;
        config.snapshot_chunk_size = 1;
    });
    let leader = cluster.run_until_leader();
    cluster.isolated.insert(N1);
    cluster.isolated.insert(N2);
    cluster.isolated.insert(N3);
    cluster.isolated.remove(&leader);
    let follower = [N1, N2, N3]
        .into_iter()
        .find(|&id| id != leader)
        .expect("a follower");
    let other = [N1, N2, N3]
        .into_iter()
        .find(|&id| id != leader && id != follower)
        .expect("a follower");
    cluster.isolated.remove(&other);
    for command in 1..=5 {
        cluster.propose(leader, command);
    }
    cluster.compact(leader);

    // Hand the same acknowledgement to the leader twice: it sends the next
    // chunk once.
    let raft = &mut cluster.node(leader).raft;
    let index = raft.snapshot().expect("snapshot").index;
    let term = raft.term();
    raft.step(Message {
        from: follower,
        to: leader,
        term,
        body: MessageBody::AppendEntriesResponse {
            success: false,
            match_index: LogIndex(0),
            conflict_index: LogIndex(1),
            conflict_term: None,
            read_round: 0,
        },
    });
    let ack = Message {
        from: follower,
        to: leader,
        term,
        body: MessageBody::InstallSnapshotResponse {
            index,
            received: 1,
            done: false,
            read_round: 0,
        },
    };
    raft.step(ack.clone());
    raft.step(ack);
    let offsets: Vec<u64> = raft
        .take_ready()
        .messages
        .iter()
        .filter(|m| m.to == follower)
        .filter_map(|m| match m.body {
            MessageBody::InstallSnapshot { offset, .. } => Some(offset),
            _ => None,
        })
        .collect();
    assert_eq!(offsets, vec![0, 1]);
}

#[test]
fn a_node_restarts_from_its_snapshot() {
    let snapshot = Snapshot {
        index: LogIndex(4),
        term: Term(1),
        data: vec![1, 2, 3, 4].into(),
    };
    let log = Log::from_parts(LogIndex(2), Term(1), entries(3..=6, 1)).expect("valid log");
    let raft = Raft::restore_with_snapshot(
        Config::new(N1, vec![N2, N3], 7),
        HardState::default(),
        Some(snapshot.clone()),
        log,
    )
    .expect("valid config");
    assert_eq!(raft.commit_index(), LogIndex(4));
    assert_eq!(raft.last_applied(), LogIndex(4));
    assert_eq!(raft.snapshot(), Some(&snapshot));
}

#[test]
#[should_panic(expected = "does not continue the snapshot")]
fn a_log_that_does_not_continue_the_snapshot_is_refused() {
    let snapshot = Snapshot {
        index: LogIndex(4),
        term: Term(2),
        data: Vec::new().into(),
    };
    let log = Log::from_parts(LogIndex(0), Term(0), entries(1..=6, 1)).expect("valid log");
    let _ = Raft::restore_with_snapshot(
        Config::new(N1, vec![N2, N3], 7),
        HardState::default(),
        Some(snapshot),
        log,
    );
}
