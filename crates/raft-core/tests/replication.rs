//! Log replication, commitment and apply tracking on single nodes
//! (Raft §5.3, §5.4.2).
//!
//! Each test drives one node directly with hand-written messages and checks
//! its log, commit index and output.

use raft_core::{
    Config, Entry, EntryPayload, HardState, Log, LogIndex, Message, MessageBody, NodeId,
    ProposeError, Raft, Role, Term,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

fn cmd(byte: u8) -> EntryPayload {
    EntryPayload::Command(vec![byte])
}

fn entry(index: u64, term: u64) -> Entry {
    Entry {
        term: Term(term),
        index: LogIndex(index),
        payload: cmd(u8::try_from(index).unwrap()),
    }
}

/// A log whose entry at position `i + 1` has term `terms[i]`.
fn log_with_terms(terms: &[u64]) -> Log {
    let entries = terms
        .iter()
        .zip(1..)
        .map(|(&term, index)| entry(index, term))
        .collect();
    Log::from_parts(LogIndex(0), Term(0), entries).expect("valid log")
}

/// Node 1 of a three-node cluster, restored in `term` with `log`.
fn node_with(term: u64, log: Log) -> Raft {
    let hard_state = HardState {
        current_term: Term(term),
        voted_for: None,
    };
    Raft::restore(Config::new(N1, vec![N2, N3], 7), hard_state, log).expect("valid config")
}

/// Makes `raft` the leader of the next term with the vote of node 2, and
/// drains its output.
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

fn append(term: u64, prev_index: u64, prev_term: u64, entries: Vec<Entry>, commit: u64) -> Message {
    Message {
        from: N2,
        to: N1,
        term: Term(term),
        body: MessageBody::AppendEntries {
            prev_log_index: LogIndex(prev_index),
            prev_log_term: Term(prev_term),
            entries,
            leader_commit: LogIndex(commit),
            read_round: 0,
        },
    }
}

fn success(from: NodeId, term: Term, match_index: u64) -> Message {
    Message {
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
    }
}

fn conflict(from: NodeId, term: Term, index: u64, conflict_term: Option<u64>) -> Message {
    Message {
        from,
        to: N1,
        term,
        body: MessageBody::AppendEntriesResponse {
            success: false,
            match_index: LogIndex(0),
            conflict_index: LogIndex(index),
            conflict_term: conflict_term.map(Term),
            read_round: 0,
        },
    }
}

/// The single response a follower sent, as
/// `(success, match_index, conflict_index, conflict_term)`.
fn response(raft: &mut Raft) -> (bool, u64, u64, Option<u64>) {
    let ready = raft.take_ready();
    match ready.messages.as_slice() {
        [Message {
            body:
                MessageBody::AppendEntriesResponse {
                    success,
                    match_index,
                    conflict_index,
                    conflict_term,
                    ..
                },
            ..
        }] => (
            *success,
            match_index.0,
            conflict_index.0,
            conflict_term.map(|t| t.0),
        ),
        other => panic!("expected one AppendEntries response, got {other:?}"),
    }
}

/// The `AppendEntries` the leader sent to `peer`, as
/// `(prev_log_index, entry indices)`.
fn appends_to(messages: &[Message], peer: NodeId) -> Vec<(u64, Vec<u64>)> {
    messages
        .iter()
        .filter(|m| m.to == peer)
        .filter_map(|m| match &m.body {
            MessageBody::AppendEntries {
                prev_log_index,
                entries,
                ..
            } => Some((
                prev_log_index.0,
                entries.iter().map(|e| e.index.0).collect(),
            )),
            _ => None,
        })
        .collect()
}

fn terms(log: &Log) -> Vec<u64> {
    log.entries().iter().map(|e| e.term.0).collect()
}

// --- Leader -----------------------------------------------------------------

#[test]
fn new_leader_appends_a_noop_in_its_term_and_replicates_it() {
    let mut raft = node_with(0, Log::new());
    for _ in 0..raft.election_timeout() {
        raft.tick();
    }
    let _ = raft.take_ready();
    raft.step(Message {
        from: N2,
        to: N1,
        term: Term(1),
        body: MessageBody::RequestVoteResponse { granted: true },
    });

    let noop = Entry {
        term: Term(1),
        index: LogIndex(1),
        payload: EntryPayload::Noop,
    };
    assert_eq!(raft.log().entries(), std::slice::from_ref(&noop));

    let ready = raft.take_ready();
    assert_eq!(ready.entries, vec![noop], "the no-op must be persisted");
    assert_eq!(appends_to(&ready.messages, N2), vec![(0, vec![1])]);
    assert_eq!(appends_to(&ready.messages, N3), vec![(0, vec![1])]);
}

#[test]
fn proposals_are_rejected_by_followers_with_a_leader_hint() {
    let mut raft = node_with(0, Log::new());
    assert_eq!(
        raft.propose(vec![1]),
        Err(ProposeError::NotLeader { leader_hint: None })
    );

    raft.step(append(1, 0, 0, Vec::new(), 0));
    assert_eq!(
        raft.propose(vec![1]),
        Err(ProposeError::NotLeader {
            leader_hint: Some(N2)
        })
    );
}

#[test]
fn leader_appends_a_proposal_and_sends_it_to_every_follower() {
    let mut raft = node_with(0, Log::new());
    elect(&mut raft);

    let index = raft.propose(vec![42]).expect("leader accepts proposals");
    assert_eq!(index, LogIndex(2), "index 1 holds the no-op");

    let ready = raft.take_ready();
    assert_eq!(ready.entries.len(), 1);
    assert_eq!(ready.entries[0].payload, EntryPayload::Command(vec![42]));
    // The no-op is still waiting for its answers, so the proposal waits
    // too (see tests/batching.rs) and goes out once a follower answers.
    assert_eq!(appends_to(&ready.messages, N2), vec![]);
    let term = raft.term();
    raft.step(success(N2, term, 1));
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![(1, vec![2])]);
    assert_eq!(appends_to(&ready.messages, N3), vec![]);
}

#[test]
fn leader_commits_once_a_majority_has_the_entry() {
    let mut raft = node_with(0, Log::new());
    elect(&mut raft);
    raft.propose(vec![7]).unwrap();
    let _ = raft.take_ready();
    let term = raft.term();

    assert_eq!(raft.commit_index(), LogIndex(0));
    raft.step(success(N2, term, 2));
    assert_eq!(raft.commit_index(), LogIndex(2), "leader + N2 = 2 of 3");

    let ready = raft.take_ready();
    let applied: Vec<_> = ready.committed_entries.iter().map(|e| e.index.0).collect();
    assert_eq!(applied, vec![1, 2]);
    assert_eq!(raft.last_applied(), LogIndex(2));
    assert_eq!(
        raft.take_ready().committed_entries.len(),
        0,
        "entries are applied once"
    );
}

#[test]
fn leader_does_not_commit_an_older_term_by_counting_replicas() {
    // Raft Figure 8: an entry from an earlier term may be on a majority and
    // still be overwritten later, so it must not count as committed until an
    // entry from the current term is replicated on top of it.
    let mut raft = node_with(2, log_with_terms(&[1, 2]));
    elect(&mut raft); // term 3; no-op at index 3
    let term = raft.term();
    assert_eq!(term, Term(3));

    raft.step(success(N2, term, 2));
    assert_eq!(raft.commit_index(), LogIndex(0));

    raft.step(success(N2, term, 3));
    assert_eq!(raft.commit_index(), LogIndex(3), "commits 1..=3 at once");
}

#[test]
fn leader_sends_missing_entries_after_an_acknowledgement() {
    let mut raft = node_with(0, Log::new());
    elect(&mut raft);
    raft.propose(vec![1]).unwrap();
    raft.propose(vec![2]).unwrap();
    let _ = raft.take_ready();
    let term = raft.term();

    // The follower only got the no-op so far.
    raft.step(success(N2, term, 1));
    let ready = raft.take_ready();
    assert_eq!(appends_to(&ready.messages, N2), vec![(1, vec![2, 3])]);

    // Once it is caught up, an acknowledgement triggers nothing further.
    raft.step(success(N2, term, 3));
    assert_eq!(appends_to(&raft.take_ready().messages, N2), vec![]);
}

#[test]
fn stale_acknowledgements_do_not_move_progress_backwards() {
    let mut raft = node_with(0, Log::new());
    elect(&mut raft);
    raft.propose(vec![1]).unwrap();
    let _ = raft.take_ready();
    let term = raft.term();

    raft.step(success(N2, term, 2));
    raft.step(success(N2, term, 1));
    assert_eq!(raft.commit_index(), LogIndex(2));

    // A heartbeat now starts after index 2.
    for _ in 0..Config::new(N1, vec![], 0).heartbeat_interval {
        raft.tick();
    }
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        vec![(2, vec![])]
    );
}

#[test]
fn leader_skips_a_whole_conflicting_term_it_does_not_have() {
    let mut raft = node_with(5, log_with_terms(&[1, 1, 4, 4, 4]));
    elect(&mut raft); // term 6, no-op at 6
    let term = raft.term();

    // The follower has term 2 at the probed index, starting at index 3. The
    // leader has no term-2 entries, so it continues at the hint.
    raft.step(conflict(N2, term, 3, Some(2)));
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        vec![(2, vec![3, 4, 5, 6])]
    );
}

#[test]
fn leader_jumps_past_its_own_entries_of_the_conflicting_term() {
    let mut raft = node_with(5, log_with_terms(&[1, 1, 4, 4, 4]));
    elect(&mut raft);
    let term = raft.term();

    // The follower has term 1 at the probed index. The leader's last term-1
    // entry is at index 2, so it continues right after it.
    raft.step(conflict(N2, term, 1, Some(1)));
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        vec![(2, vec![3, 4, 5, 6])]
    );
}

#[test]
fn leader_continues_after_a_short_followers_log() {
    let mut raft = node_with(5, log_with_terms(&[1, 1, 4, 4, 4]));
    elect(&mut raft);
    let term = raft.term();

    raft.step(conflict(N2, term, 2, None));
    assert_eq!(
        appends_to(&raft.take_ready().messages, N2),
        vec![(1, vec![2, 3, 4, 5, 6])]
    );
}

#[test]
fn appends_are_capped_at_the_configured_size() {
    let mut config = Config::new(N1, vec![N2, N3], 7);
    config.max_entries_per_append = 2;
    let mut raft = Raft::new(config).unwrap();
    elect(&mut raft);
    for i in 0..5 {
        raft.propose(vec![i]).unwrap();
    }
    let ready = raft.take_ready();
    assert!(appends_to(&ready.messages, N2)
        .iter()
        .all(|(_, entries)| entries.len() <= 2));
}

#[test]
fn single_node_commits_proposals_immediately() {
    let mut raft = Raft::new(Config::new(N1, vec![], 0)).unwrap();
    for _ in 0..raft.election_timeout() {
        raft.tick();
    }
    assert_eq!(raft.role(), Role::Leader);
    let index = raft.propose(vec![9]).unwrap();
    assert_eq!(raft.commit_index(), index);

    let ready = raft.take_ready();
    assert_eq!(ready.entries.len(), 2, "no-op and command are persisted");
    assert_eq!(ready.committed_entries.len(), 2);
}

#[test]
fn leader_loses_its_progress_when_it_steps_down() {
    let mut raft = node_with(0, Log::new());
    elect(&mut raft);
    raft.step(append(9, 0, 0, Vec::new(), 0));
    assert_eq!(raft.role(), Role::Follower);
    // A late acknowledgement for the old term is ignored.
    raft.step(success(N3, Term(9), 1));
    assert_eq!(raft.commit_index(), LogIndex(0));
}

// --- Follower ---------------------------------------------------------------

#[test]
fn follower_appends_entries_and_reports_its_match_index() {
    let mut raft = node_with(1, Log::new());
    raft.step(append(1, 0, 0, vec![entry(1, 1), entry(2, 1)], 0));

    assert_eq!(terms(raft.log()), vec![1, 1]);
    let ready = raft.take_ready();
    assert_eq!(ready.entries, vec![entry(1, 1), entry(2, 1)]);
    assert!(matches!(
        ready.messages.as_slice(),
        [Message {
            body: MessageBody::AppendEntriesResponse {
                success: true,
                match_index: LogIndex(2),
                ..
            },
            ..
        }]
    ));
}

#[test]
fn follower_rejects_a_gap_and_points_past_its_log() {
    let mut raft = node_with(1, log_with_terms(&[1, 1]));
    raft.step(append(1, 5, 1, vec![entry(6, 1)], 0));
    assert_eq!(response(&mut raft), (false, 0, 3, None));
    assert_eq!(raft.log().last_index(), LogIndex(2));
}

#[test]
fn follower_rejects_a_term_mismatch_with_the_start_of_that_term() {
    let mut raft = node_with(5, log_with_terms(&[1, 2, 2, 2]));
    raft.step(append(5, 4, 3, Vec::new(), 0));
    assert_eq!(response(&mut raft), (false, 0, 2, Some(2)));
}

#[test]
fn follower_replaces_a_conflicting_suffix() {
    let mut raft = node_with(3, log_with_terms(&[1, 2, 2, 2]));
    raft.step(append(3, 2, 2, vec![entry(3, 3)], 0));

    assert_eq!(terms(raft.log()), vec![1, 2, 3]);
    let ready = raft.take_ready();
    assert_eq!(
        ready.entries,
        vec![entry(3, 3)],
        "persist from the conflict on"
    );
}

#[test]
fn follower_keeps_entries_a_delayed_message_does_not_mention() {
    let mut raft = node_with(1, Log::new());
    raft.step(append(
        1,
        0,
        0,
        vec![entry(1, 1), entry(2, 1), entry(3, 1)],
        0,
    ));
    let _ = raft.take_ready();

    // An older, shorter message for the same term arrives late.
    raft.step(append(1, 0, 0, vec![entry(1, 1)], 0));
    assert_eq!(raft.log().last_index(), LogIndex(3));
    assert_eq!(
        response(&mut raft),
        (true, 1, 0, None),
        "only what this message proved is acknowledged"
    );
}

#[test]
fn follower_commit_index_is_bounded_by_what_the_message_proved() {
    let mut raft = node_with(2, log_with_terms(&[1, 1, 1]));
    // Entries 2 and 3 are not covered by this message and might still differ
    // from the leader's log, so the commit index stops at 1.
    raft.step(append(2, 1, 1, Vec::new(), 3));
    assert_eq!(raft.commit_index(), LogIndex(1));

    raft.step(append(2, 3, 1, Vec::new(), 3));
    assert_eq!(raft.commit_index(), LogIndex(3));
    let applied: Vec<_> = raft
        .take_ready()
        .committed_entries
        .iter()
        .map(|e| e.index.0)
        .collect();
    assert_eq!(applied, vec![1, 2, 3]);
}

#[test]
fn follower_commit_index_never_decreases() {
    let mut raft = node_with(1, Log::new());
    raft.step(append(1, 0, 0, vec![entry(1, 1), entry(2, 1)], 2));
    assert_eq!(raft.commit_index(), LogIndex(2));
    raft.step(append(1, 0, 0, Vec::new(), 1));
    assert_eq!(raft.commit_index(), LogIndex(2));
}

#[test]
#[should_panic(expected = "overwrite committed entry")]
fn overwriting_a_committed_entry_is_a_bug() {
    let mut raft = node_with(1, Log::new());
    raft.step(append(1, 0, 0, vec![entry(1, 1)], 1));
    // A leader of a later term claims a different entry at index 1. Raft's
    // safety argument rules this out, so the node refuses loudly.
    raft.step(append(2, 0, 0, vec![entry(1, 2)], 0));
}

#[test]
fn restarted_node_resumes_with_commit_index_at_the_snapshot() {
    let log = Log::from_parts(LogIndex(4), Term(2), vec![entry(5, 2)]).unwrap();
    let raft = node_with(2, log);
    assert_eq!(raft.commit_index(), LogIndex(4));
    assert_eq!(raft.last_applied(), LogIndex(4));
}
