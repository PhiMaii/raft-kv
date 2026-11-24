//! Leader election transitions of a single node (Raft §5.2, §5.4.1).
//!
//! Each test drives one node directly with ticks and hand-written messages and
//! checks the resulting role, hard state and outgoing messages.

use raft_core::{
    Config, Entry, EntryPayload, HardState, Log, LogIndex, Message, MessageBody, NodeId, Raft,
    Role, Term,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);

/// Node 1 of a three-node cluster.
fn node() -> Raft {
    node_with(HardState::default(), Log::new())
}

fn node_with(hard_state: HardState, log: Log) -> Raft {
    let config = Config::new(N1, vec![N2, N3], 7);
    Raft::restore(config, hard_state, log).expect("valid config")
}

/// A log whose last entry has the given index and term.
fn log_ending_at(index: u64, term: u64) -> Log {
    let entries = (1..=index)
        .map(|i| Entry {
            term: Term(term),
            index: LogIndex(i),
            payload: EntryPayload::Noop,
        })
        .collect();
    Log::from_parts(LogIndex(0), Term(0), entries).expect("valid log")
}

fn msg(from: NodeId, term: u64, body: MessageBody) -> Message {
    Message {
        from,
        to: N1,
        term: Term(term),
        body,
    }
}

fn request_vote(from: NodeId, term: u64, last_index: u64, last_term: u64) -> Message {
    msg(
        from,
        term,
        MessageBody::RequestVote {
            last_log_index: LogIndex(last_index),
            last_log_term: Term(last_term),
        },
    )
}

fn vote(from: NodeId, term: u64, granted: bool) -> Message {
    msg(from, term, MessageBody::RequestVoteResponse { granted })
}

fn heartbeat(from: NodeId, term: u64) -> Message {
    msg(
        from,
        term,
        MessageBody::AppendEntries {
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: Vec::new(),
            leader_commit: LogIndex(0),
        },
    )
}

/// Ticks until the election timeout fires.
fn time_out(raft: &mut Raft) {
    for _ in 0..raft.election_timeout() {
        raft.tick();
    }
}

/// Times out, wins the votes of both peers, and drains the output.
fn make_leader(raft: &mut Raft) {
    time_out(raft);
    let term = raft.term().0;
    raft.step(vote(N2, term, true));
    assert_eq!(raft.role(), Role::Leader);
    let _ = raft.take_ready();
}

fn vote_granted(raft: &mut Raft, to: NodeId) -> bool {
    let ready = raft.take_ready();
    match ready.messages.as_slice() {
        [Message {
            to: recipient,
            body: MessageBody::RequestVoteResponse { granted },
            ..
        }] if *recipient == to => *granted,
        other => panic!("expected one vote response to {to}, got {other:?}"),
    }
}

#[test]
fn new_node_starts_as_follower_in_term_zero() {
    let raft = node();
    assert_eq!(raft.role(), Role::Follower);
    assert_eq!(raft.term(), Term(0));
    assert_eq!(raft.voted_for(), None);
    assert_eq!(raft.leader_id(), None);
}

#[test]
fn follower_does_not_campaign_before_its_timeout() {
    let mut raft = node();
    for _ in 1..raft.election_timeout() {
        raft.tick();
    }
    assert_eq!(raft.role(), Role::Follower);
    assert!(raft.take_ready().is_empty());
}

#[test]
fn election_timeout_starts_a_campaign() {
    let mut raft = node();
    time_out(&mut raft);

    assert_eq!(raft.role(), Role::Candidate);
    assert_eq!(raft.term(), Term(1));
    assert_eq!(raft.voted_for(), Some(N1));

    let ready = raft.take_ready();
    assert_eq!(
        ready.hard_state,
        Some(HardState {
            current_term: Term(1),
            voted_for: Some(N1),
        })
    );
    let mut recipients: Vec<_> = ready.messages.iter().map(|m| m.to).collect();
    recipients.sort();
    assert_eq!(recipients, vec![N2, N3]);
    for message in &ready.messages {
        assert_eq!(message.term, Term(1));
        assert_eq!(
            message.body,
            MessageBody::RequestVote {
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }
        );
    }
}

#[test]
fn request_vote_carries_the_last_log_position() {
    let mut raft = node_with(HardState::default(), log_ending_at(4, 2));
    time_out(&mut raft);
    let ready = raft.take_ready();
    assert!(ready.messages.iter().all(|m| m.body
        == MessageBody::RequestVote {
            last_log_index: LogIndex(4),
            last_log_term: Term(2),
        }));
}

#[test]
fn election_timeouts_are_randomized_within_the_configured_range() {
    let mut timeouts = std::collections::BTreeSet::new();
    for seed in 0..50 {
        let config = Config::new(N1, vec![N2, N3], seed);
        let (min, max) = (config.election_timeout_min, config.election_timeout_max);
        let raft = Raft::new(config).expect("valid config");
        assert!((min..max).contains(&raft.election_timeout()));
        timeouts.insert(raft.election_timeout());
    }
    assert!(timeouts.len() > 1, "timeouts should vary with the seed");
}

#[test]
fn candidate_with_a_majority_becomes_leader_and_sends_heartbeats() {
    let mut raft = node();
    time_out(&mut raft);
    let _ = raft.take_ready();

    raft.step(vote(N2, 1, true));

    assert_eq!(raft.role(), Role::Leader);
    assert_eq!(raft.leader_id(), Some(N1));
    let ready = raft.take_ready();
    assert_eq!(ready.messages.len(), 2);
    assert!(ready
        .messages
        .iter()
        .all(|m| matches!(m.body, MessageBody::AppendEntries { .. })));
}

#[test]
fn rejected_votes_do_not_make_a_leader() {
    let mut raft = node();
    time_out(&mut raft);
    raft.step(vote(N2, 1, false));
    raft.step(vote(N3, 1, false));
    assert_eq!(raft.role(), Role::Candidate);
}

#[test]
fn the_same_voter_is_only_counted_once() {
    let config = Config::new(N1, vec![N2, NodeId(3), NodeId(4), NodeId(5)], 7);
    let mut raft = Raft::new(config).expect("valid config");
    time_out(&mut raft);
    raft.step(vote(N2, 1, true));
    raft.step(vote(N2, 1, true));
    assert_eq!(
        raft.role(),
        Role::Candidate,
        "2 of 5 votes is not a majority"
    );
}

#[test]
fn split_vote_leads_to_a_new_election_in_a_higher_term() {
    let mut raft = node();
    time_out(&mut raft);
    assert_eq!(raft.term(), Term(1));
    time_out(&mut raft);
    assert_eq!(raft.role(), Role::Candidate);
    assert_eq!(raft.term(), Term(2));
}

#[test]
fn votes_from_an_earlier_term_are_ignored() {
    let mut raft = node();
    time_out(&mut raft);
    time_out(&mut raft);
    raft.step(vote(N2, 1, true));
    assert_eq!(raft.role(), Role::Candidate);
}

#[test]
fn follower_grants_one_vote_per_term() {
    let mut raft = node();
    raft.step(request_vote(N2, 1, 0, 0));
    assert!(vote_granted(&mut raft, N2));
    assert_eq!(raft.voted_for(), Some(N2));

    raft.step(request_vote(N3, 1, 0, 0));
    assert!(!vote_granted(&mut raft, N3));
}

#[test]
fn repeated_request_from_the_same_candidate_is_granted_again() {
    let mut raft = node();
    raft.step(request_vote(N2, 1, 0, 0));
    assert!(vote_granted(&mut raft, N2));
    raft.step(request_vote(N2, 1, 0, 0));
    assert!(vote_granted(&mut raft, N2));
}

#[test]
fn vote_is_persisted_before_it_is_sent() {
    let mut raft = node();
    raft.step(request_vote(N2, 1, 0, 0));
    let ready = raft.take_ready();
    assert_eq!(
        ready.hard_state,
        Some(HardState {
            current_term: Term(1),
            voted_for: Some(N2),
        })
    );
}

#[test]
fn vote_is_denied_to_a_candidate_with_an_older_last_term() {
    let mut raft = node_with(HardState::default(), log_ending_at(2, 3));
    raft.step(request_vote(N2, 4, 10, 2));
    assert!(!vote_granted(&mut raft, N2));
    assert_eq!(raft.voted_for(), None);
}

#[test]
fn vote_is_denied_to_a_candidate_with_a_shorter_log_in_the_same_term() {
    let mut raft = node_with(HardState::default(), log_ending_at(5, 2));
    raft.step(request_vote(N2, 3, 4, 2));
    assert!(!vote_granted(&mut raft, N2));
}

#[test]
fn vote_is_granted_to_a_candidate_with_a_newer_or_equal_log() {
    let mut newer_term = node_with(HardState::default(), log_ending_at(5, 2));
    newer_term.step(request_vote(N2, 4, 1, 3));
    assert!(vote_granted(&mut newer_term, N2));

    let mut equal = node_with(HardState::default(), log_ending_at(5, 2));
    equal.step(request_vote(N2, 4, 5, 2));
    assert!(vote_granted(&mut equal, N2));
}

#[test]
fn vote_request_from_an_older_term_is_rejected_with_the_current_term() {
    let hard_state = HardState {
        current_term: Term(5),
        voted_for: None,
    };
    let mut raft = node_with(hard_state, Log::new());
    raft.step(request_vote(N2, 4, 0, 0));

    let ready = raft.take_ready();
    assert_eq!(
        ready.messages,
        vec![Message {
            from: N1,
            to: N2,
            term: Term(5),
            body: MessageBody::RequestVoteResponse { granted: false },
        }]
    );
    assert_eq!(raft.term(), Term(5));
}

#[test]
fn higher_term_resets_the_vote_and_makes_a_follower() {
    let mut raft = node();
    time_out(&mut raft);
    assert_eq!(raft.voted_for(), Some(N1));

    raft.step(request_vote(N2, 3, 0, 0));
    assert_eq!(raft.role(), Role::Follower);
    assert_eq!(raft.term(), Term(3));
    assert_eq!(raft.voted_for(), Some(N2), "vote moved to the new term");
}

#[test]
fn leader_steps_down_on_a_higher_term() {
    let mut raft = node();
    make_leader(&mut raft);

    raft.step(msg(
        N3,
        9,
        MessageBody::AppendEntriesResponse {
            success: false,
            match_index: LogIndex(0),
        },
    ));
    assert_eq!(raft.role(), Role::Follower);
    assert_eq!(raft.term(), Term(9));
    assert_eq!(raft.leader_id(), None);
}

#[test]
fn candidate_steps_down_when_a_leader_of_its_term_appears() {
    let mut raft = node();
    time_out(&mut raft);
    raft.step(heartbeat(N2, 1));
    assert_eq!(raft.role(), Role::Follower);
    assert_eq!(raft.term(), Term(1));
    assert_eq!(raft.leader_id(), Some(N2));
}

#[test]
fn heartbeats_prevent_an_election() {
    let mut raft = node();
    raft.step(heartbeat(N2, 1));
    for _ in 0..1000 {
        raft.tick();
        raft.step(heartbeat(N2, 1));
    }
    assert_eq!(raft.role(), Role::Follower);
    assert_eq!(raft.term(), Term(1));
    assert_eq!(raft.leader_id(), Some(N2));
}

#[test]
fn heartbeat_from_a_stale_leader_is_rejected() {
    let hard_state = HardState {
        current_term: Term(4),
        voted_for: None,
    };
    let mut raft = node_with(hard_state, Log::new());
    raft.step(heartbeat(N2, 3));

    let ready = raft.take_ready();
    assert_eq!(ready.messages.len(), 1);
    assert_eq!(ready.messages[0].term, Term(4));
    assert!(matches!(
        ready.messages[0].body,
        MessageBody::AppendEntriesResponse { success: false, .. }
    ));
    assert_eq!(raft.leader_id(), None);
}

#[test]
fn leader_sends_heartbeats_at_the_configured_interval() {
    let mut raft = node();
    make_leader(&mut raft);

    let interval = Config::new(N1, vec![], 0).heartbeat_interval;
    for _ in 1..interval {
        raft.tick();
    }
    assert_eq!(raft.take_ready().messages, Vec::<Message>::new());
    raft.tick();
    assert_eq!(raft.take_ready().messages.len(), 2);
}

#[test]
fn leader_never_starts_an_election() {
    let mut raft = node();
    make_leader(&mut raft);
    for _ in 0..1000 {
        raft.tick();
    }
    assert_eq!(raft.role(), Role::Leader);
    assert_eq!(raft.term(), Term(1));
}

#[test]
fn single_node_cluster_elects_itself() {
    let mut raft = Raft::new(Config::new(N1, vec![], 0)).expect("valid config");
    time_out(&mut raft);
    assert_eq!(raft.role(), Role::Leader);
    assert_eq!(raft.term(), Term(1));
}

#[test]
fn restart_keeps_the_vote_of_the_current_term() {
    let hard_state = HardState {
        current_term: Term(2),
        voted_for: Some(N3),
    };
    let mut raft = node_with(hard_state, Log::new());
    assert!(
        raft.take_ready().hard_state.is_none(),
        "nothing new to persist"
    );

    raft.step(request_vote(N2, 2, 0, 0));
    assert!(!vote_granted(&mut raft, N2));
}

#[test]
fn messages_from_unknown_nodes_or_for_other_nodes_are_ignored() {
    let mut raft = node();
    raft.step(request_vote(NodeId(99), 1, 0, 0));
    let mut misaddressed = request_vote(N2, 1, 0, 0);
    misaddressed.to = N3;
    raft.step(misaddressed);

    assert!(raft.take_ready().is_empty());
    assert_eq!(raft.term(), Term(0));
}

#[test]
fn hard_state_is_reported_only_when_it_changes() {
    let mut raft = node();
    time_out(&mut raft);
    assert!(raft.take_ready().hard_state.is_some());
    raft.step(vote(N2, 1, false));
    assert!(raft.take_ready().hard_state.is_none());
}
