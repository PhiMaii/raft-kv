//! Linearizable reads with ReadIndex (Raft dissertation §6.4), driven on a
//! single node with hand-written messages.

use raft_core::{
    Config, Entry, EntryPayload, HardState, Log, LogIndex, Message, MessageBody, NodeId, Raft,
    ReadIndexError, ReadState, Role, Term,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

/// Node 1 of a three-node cluster with `entries` entries of term 1.
fn node(entries: u64) -> Raft {
    let entries = (1..=entries)
        .map(|index| Entry {
            term: Term(1),
            index: LogIndex(index),
            payload: EntryPayload::Command(vec![1]),
        })
        .collect();
    let log = Log::from_parts(LogIndex(0), Term(0), entries).expect("valid log");
    let hard_state = HardState {
        current_term: Term(1),
        voted_for: None,
    };
    Raft::restore(Config::new(N1, vec![N2, N3], 7), hard_state, log).expect("valid config")
}

/// Makes node 1 leader with node 2's vote and drains its output.
fn elect(raft: &mut Raft) {
    for _ in 0..raft.election_timeout() {
        raft.tick();
    }
    let term = raft.term();
    raft.step(Message {
        from: N2,
        to: N1,
        term,
        body: MessageBody::RequestVoteResponse { granted: true },
    });
    assert_eq!(raft.role(), Role::Leader);
    let _ = raft.take_ready();
}

fn ack(raft: &mut Raft, from: NodeId, match_index: u64, read_round: u64) {
    let term = raft.term();
    raft.step(Message {
        from,
        to: N1,
        term,
        body: MessageBody::AppendEntriesResponse {
            success: true,
            match_index: LogIndex(match_index),
            conflict_index: LogIndex(0),
            conflict_term: None,
            read_round,
        },
    });
}

/// The rounds of the `AppendEntries` messages in `messages`.
fn rounds(messages: &[Message]) -> Vec<u64> {
    messages
        .iter()
        .filter_map(|m| match m.body {
            MessageBody::AppendEntries { read_round, .. } => Some(read_round),
            _ => None,
        })
        .collect()
}

fn read(id: u64, index: u64) -> ReadState {
    ReadState {
        id,
        index: LogIndex(index),
    }
}

/// Elects node 1 in a log of two entries and commits its no-op at index 3.
fn committed_leader() -> Raft {
    let mut raft = node(2);
    elect(&mut raft);
    ack(&mut raft, N2, 3, 0);
    assert_eq!(raft.commit_index(), LogIndex(3));
    let _ = raft.take_ready();
    raft
}

#[test]
fn followers_refuse_reads_and_name_the_leader() {
    let mut raft = node(0);
    assert_eq!(
        raft.read_index(1),
        Err(ReadIndexError::NotLeader { leader_hint: None })
    );
    let term = raft.term();
    raft.step(Message {
        from: N3,
        to: N1,
        term,
        body: MessageBody::AppendEntries {
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: Vec::new(),
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    });
    assert_eq!(
        raft.read_index(1),
        Err(ReadIndexError::NotLeader {
            leader_hint: Some(N3)
        })
    );
}

#[test]
fn a_read_needs_a_majority_for_a_round_sent_after_it() {
    let mut raft = committed_leader();
    raft.read_index(7).expect("leader");
    let ready = raft.take_ready();
    assert_eq!(
        rounds(&ready.messages),
        vec![1, 1],
        "a round to each follower"
    );
    assert_eq!(ready.read_states, Vec::<ReadState>::new());
    assert!(ready.entries.is_empty(), "a read writes nothing to the log");

    // An answer to a message sent before the read proves nothing for it.
    ack(&mut raft, N2, 3, 0);
    assert_eq!(raft.take_ready().read_states, Vec::<ReadState>::new());

    ack(&mut raft, N2, 3, 1);
    assert_eq!(raft.take_ready().read_states, vec![read(7, 3)]);
}

#[test]
fn a_failed_consistency_check_also_confirms_leadership() {
    let mut raft = committed_leader();
    raft.read_index(7).expect("leader");
    let _ = raft.take_ready();
    let term = raft.term();
    raft.step(Message {
        from: N3,
        to: N1,
        term,
        body: MessageBody::AppendEntriesResponse {
            success: false,
            match_index: LogIndex(0),
            conflict_index: LogIndex(1),
            conflict_term: None,
            read_round: 1,
        },
    });
    assert_eq!(raft.take_ready().read_states, vec![read(7, 3)]);
}

#[test]
fn a_read_waits_until_the_leader_commits_in_its_own_term() {
    let mut raft = node(2);
    elect(&mut raft);
    // The no-op at index 3 is not committed yet, so the leader does not
    // know whether entries 1 and 2 are committed.
    raft.read_index(7).expect("leader");
    ack(&mut raft, N2, 0, 1);
    assert_eq!(
        raft.take_ready().read_states,
        Vec::<ReadState>::new(),
        "leadership confirmed, but the commit index is not known yet"
    );
    ack(&mut raft, N2, 3, 1);
    assert_eq!(raft.take_ready().read_states, vec![read(7, 3)]);
}

#[test]
fn concurrent_reads_share_confirmation_rounds() {
    let mut raft = committed_leader();
    raft.read_index(1).expect("leader");
    assert_eq!(rounds(&raft.take_ready().messages), vec![1, 1]);

    // Round 1 is in flight; these two wait for round 2 and send nothing.
    raft.read_index(2).expect("leader");
    raft.read_index(3).expect("leader");
    assert_eq!(rounds(&raft.take_ready().messages), Vec::<u64>::new());

    // Round 1 confirms read 1 and starts round 2 for the others.
    ack(&mut raft, N3, 3, 1);
    let ready = raft.take_ready();
    assert_eq!(ready.read_states, vec![read(1, 3)]);
    assert_eq!(rounds(&ready.messages), vec![2, 2]);

    ack(&mut raft, N2, 3, 2);
    assert_eq!(raft.take_ready().read_states, vec![read(2, 3), read(3, 3)]);
}

#[test]
fn a_read_observes_writes_committed_before_it() {
    let mut raft = committed_leader();
    raft.propose(vec![9]).expect("leader");
    ack(&mut raft, N2, 4, 0);
    assert_eq!(raft.commit_index(), LogIndex(4));
    let _ = raft.take_ready();
    raft.read_index(7).expect("leader");
    ack(&mut raft, N2, 4, 1);
    assert_eq!(raft.take_ready().read_states, vec![read(7, 4)]);
}

#[test]
fn pending_reads_are_dropped_when_the_leader_steps_down() {
    let mut raft = committed_leader();
    raft.read_index(7).expect("leader");
    let _ = raft.take_ready();
    let higher = raft.term().next();
    raft.step(Message {
        from: N3,
        to: N1,
        term: higher,
        body: MessageBody::RequestVote {
            last_log_index: LogIndex(9),
            last_log_term: higher,
        },
    });
    assert_eq!(raft.role(), Role::Follower);
    // A late confirmation from the old term must not release the read.
    ack(&mut raft, N2, 3, 1);
    assert_eq!(raft.take_ready().read_states, Vec::<ReadState>::new());
}

#[test]
fn a_single_node_serves_reads_at_once() {
    let mut raft = Raft::new(Config::new(N1, Vec::new(), 7)).expect("valid config");
    for _ in 0..raft.election_timeout() {
        raft.tick();
    }
    assert_eq!(raft.role(), Role::Leader);
    let _ = raft.take_ready();
    raft.read_index(7).expect("leader");
    assert_eq!(raft.take_ready().read_states, vec![read(7, 1)]);
}
