//! Batched replication: while an `AppendEntries` waits for its answer, new
//! entries wait with it and go out together.

use raft_core::{Config, EntryPayload, LogIndex, Message, MessageBody, NodeId, Raft, Role};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

/// Node 1 of a three-node cluster as leader, its no-op acknowledged by
/// both followers.
fn leader(configure: impl FnOnce(&mut Config)) -> Raft {
    let mut config = Config::new(N1, vec![N2, N3], 7);
    configure(&mut config);
    let mut raft = Raft::new(config).expect("valid config");
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
    ack(&mut raft, N2, 1);
    ack(&mut raft, N3, 1);
    let _ = raft.take_ready();
    raft
}

fn ack(raft: &mut Raft, from: NodeId, match_index: u64) {
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
            read_round: 0,
        },
    });
}

/// The entry indices of each `AppendEntries` to `peer`.
fn appends_to(messages: &[Message], peer: NodeId) -> Vec<Vec<u64>> {
    messages
        .iter()
        .filter(|m| m.to == peer)
        .filter_map(|m| match &m.body {
            MessageBody::AppendEntries { entries, .. } => {
                Some(entries.iter().map(|e| e.index.0).collect())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn an_idle_leader_sends_at_once_and_a_busy_one_batches() {
    let mut raft = leader(|_| {});
    for byte in 0..5 {
        raft.propose(vec![byte]).expect("leader");
    }
    // The first proposal goes out at once; the next four wait for its
    // answer.
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![vec![2]]);
    assert_eq!(appends_to(&ready.messages, N3), vec![vec![2]]);

    // The answer releases all four in one message, to that follower only.
    ack(&mut raft, N2, 2);
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![vec![3, 4, 5, 6]]);
    assert_eq!(appends_to(&ready.messages, N3), Vec::<Vec<u64>>::new());

    // Once everything is acknowledged, the next proposal goes out at once.
    ack(&mut raft, N2, 6);
    raft.propose(vec![9]).expect("leader");
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![vec![7]]);
}

#[test]
fn without_batching_every_proposal_sends_everything_unacknowledged() {
    let mut raft = leader(|config| config.batch_appends = false);
    for byte in 0..3 {
        raft.propose(vec![byte]).expect("leader");
    }
    let ready = raft.take_ready();
    assert_eq!(
        appends_to(&ready.messages, N2),
        vec![vec![2], vec![2, 3], vec![2, 3, 4]]
    );
}

#[test]
fn a_failed_consistency_check_also_releases_the_batch() {
    let mut raft = leader(|_| {});
    raft.propose(vec![1]).expect("leader");
    raft.propose(vec![2]).expect("leader");
    let _ = raft.take_ready();
    let term = raft.term();
    raft.step(Message {
        from: N3,
        to: N1,
        term,
        body: MessageBody::AppendEntriesResponse {
            success: false,
            match_index: LogIndex(0),
            conflict_index: LogIndex(2),
            conflict_term: None,
            read_round: 0,
        },
    });
    assert_eq!(
        appends_to(&raft.take_ready().messages, N3),
        vec![vec![2, 3]]
    );
}

#[test]
fn a_lost_batch_is_resent_with_the_next_heartbeat() {
    let mut raft = leader(|_| {});
    raft.propose(vec![1]).expect("leader");
    raft.propose(vec![2]).expect("leader");
    // Both messages are lost: no answer ever comes.
    let _ = raft.take_ready();
    let mut resent = Vec::new();
    for _ in 0..Config::new(N1, Vec::new(), 0).heartbeat_interval {
        raft.tick();
        resent.extend(appends_to(&raft.take_ready().messages, N2));
    }
    assert_eq!(resent, vec![vec![2, 3]]);
}

#[test]
fn appends_are_limited_in_bytes_but_carry_at_least_one_entry() {
    let mut raft = leader(|config| config.max_append_bytes = 250);
    // Each entry counts as its command plus 24 bytes.
    raft.propose(vec![0; 100]).expect("leader");
    for _ in 0..3 {
        raft.propose(vec![0; 100]).expect("leader");
    }
    raft.propose(vec![0; 1_000]).expect("leader");
    let _ = raft.take_ready();
    ack(&mut raft, N2, 2);
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        vec![vec![3, 4]]
    );
    ack(&mut raft, N2, 4);
    assert_eq!(appends_to(&raft.take_ready().messages, N2), vec![vec![5]]);
    ack(&mut raft, N2, 5);
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![vec![6]]);
    let MessageBody::AppendEntries { entries, .. } = &ready.messages[0].body else {
        panic!("an append");
    };
    assert_eq!(entries[0].payload, EntryPayload::Command(vec![0; 1_000]));
}

#[test]
fn only_a_leaders_replication_messages_may_go_before_persisting() {
    let mut raft = leader(|_| {});
    raft.propose(vec![1]).expect("leader");
    let ready = raft.take_ready();
    assert_eq!(ready.messages.len(), 2, "one append per follower");
    assert!(ready
        .messages
        .iter()
        .all(Message::may_send_before_persisting));

    // A vote, and an answer to the leader, promise durable state.
    let mut follower = Raft::new(Config::new(N2, vec![N1, N3], 7)).expect("valid config");
    for body in [
        MessageBody::RequestVote {
            last_log_index: LogIndex(9),
            last_log_term: raft_core::Term(9),
        },
        MessageBody::AppendEntries {
            prev_log_index: LogIndex(0),
            prev_log_term: raft_core::Term(0),
            entries: Vec::new(),
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    ] {
        follower.step(Message {
            from: N1,
            to: N2,
            term: raft_core::Term(9),
            body,
        });
    }
    let answers = follower.take_ready().messages;
    assert_eq!(answers.len(), 2);
    assert!(!answers.iter().any(Message::may_send_before_persisting));
}

#[test]
fn only_answers_that_show_progress_send_more() {
    // Regression test: every answer used to send the follower its missing
    // entries, so each heartbeat started another chain of appends while the
    // follower was behind, and the chains piled up into a message storm.
    let mut raft = leader(|config| config.max_entries_per_append = 1);
    for byte in 0..3 {
        raft.propose(vec![byte]).expect("leader");
    }
    let _ = raft.take_ready();

    ack(&mut raft, N2, 2);
    assert_eq!(appends_to(&raft.take_ready().messages, N2), vec![vec![3]]);
    // The same answer again, or an answer to an older heartbeat, tells the
    // leader nothing new.
    ack(&mut raft, N2, 2);
    ack(&mut raft, N2, 1);
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        Vec::<Vec<u64>>::new()
    );
    ack(&mut raft, N2, 3);
    assert_eq!(appends_to(&raft.take_ready().messages, N2), vec![vec![4]]);
}
