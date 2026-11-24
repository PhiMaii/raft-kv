//! The Raft state machine of a single node.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::config::{Config, ConfigError};
use crate::log::{Entry, EntryPayload, Log};
use crate::message::{Message, MessageBody};
use crate::rng::SplitMix64;
use crate::types::{LogIndex, NodeId, Term};

/// The role a node currently plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Follows a leader and grants votes. Every node starts as a follower.
    Follower,
    /// Running an election.
    Candidate,
    /// Won an election for the current term.
    Leader,
}

/// The state Raft requires to survive a crash, apart from the log
/// (Raft Figure 2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// The latest term this node has seen.
    pub current_term: Term,
    /// The candidate this node voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

/// The work produced by the state machine since the last call to
/// [`Raft::take_ready`].
///
/// The caller must handle a `Ready` in this order:
///
/// 1. Make `hard_state` and `entries` durable (fsync).
/// 2. Send `messages`.
/// 3. Apply `committed_entries` to the state machine, in order.
///
/// Persisting before sending is what makes votes and acknowledgements
/// binding: a node that sent a vote or acked an entry and then forgot it in a
/// crash could vote twice in one term or lose a committed entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[must_use = "a Ready holds state to persist, messages to send and entries to apply"]
pub struct Ready {
    /// The new hard state, if it changed since the last `Ready`.
    pub hard_state: Option<HardState>,
    /// Log entries to persist. If the first entry's index is at or below the
    /// last stored index, the stored log from that index on is replaced.
    pub entries: Vec<Entry>,
    /// Messages to send to other nodes.
    pub messages: Vec<Message>,
    /// Newly committed entries to apply to the state machine, in order.
    /// Each committed entry is handed out exactly once.
    pub committed_entries: Vec<Entry>,
}

impl Ready {
    /// Whether there is nothing to persist, send or apply.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.committed_entries.is_empty()
    }
}

/// Why a proposal was not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Only the leader accepts proposals. `leader_hint` is the leader this
    /// node knows of, if any.
    NotLeader {
        /// The current leader, if known.
        leader_hint: Option<NodeId>,
    },
}

impl fmt::Display for ProposeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader {
                leader_hint: Some(leader),
            } => write!(f, "not the leader; the leader is {leader}"),
            Self::NotLeader { leader_hint: None } => f.write_str("not the leader; leader unknown"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// What the leader knows about one follower's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Progress {
    /// The next entry to send.
    next_index: LogIndex,
    /// The highest entry known to be replicated on the follower.
    match_index: LogIndex,
}

/// A single Raft node.
///
/// `Raft` performs no I/O and never reads a clock. The caller drives it:
///
/// - [`tick`](Self::tick) advances logical time by one tick,
/// - [`step`](Self::step) delivers a message from another node,
/// - [`propose`](Self::propose) submits a command on the leader,
/// - [`take_ready`](Self::take_ready) collects the state to persist, the
///   messages to send and the entries to apply that resulted from the calls
///   above.
///
/// Given the same configuration and the same sequence of calls, a node always
/// behaves the same way.
#[derive(Clone, Debug)]
pub struct Raft {
    config: Config,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    log: Log,
    /// The highest entry known to be committed.
    commit_index: LogIndex,
    /// The highest entry handed out for applying.
    last_applied: LogIndex,
    /// Votes received in the current election, including our own.
    votes: BTreeSet<NodeId>,
    /// Replication state per follower. Only meaningful while leader.
    progress: BTreeMap<NodeId, Progress>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    rng: SplitMix64,
    /// The hard state as of the last `Ready`, to detect changes.
    persisted: HardState,
    /// The lowest log index changed since the last `Ready`, if any.
    unstable_from: Option<LogIndex>,
    outbox: Vec<Message>,
}

impl Raft {
    /// Creates a node that has never run before.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        Self::restore(config, HardState::default(), Log::new())
    }

    /// Creates a node from the state it persisted before a restart.
    ///
    /// The commit index is not persisted. It restarts at the snapshot index
    /// and catches up as soon as the node hears from a leader.
    pub fn restore(config: Config, hard_state: HardState, log: Log) -> Result<Self, ConfigError> {
        config.validate()?;
        // Mix the node id into the seed so that nodes configured with the same
        // seed still pick different election timeouts.
        let seed = config.seed ^ config.id.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let snapshot_index = log.snapshot_index();
        let mut raft = Self {
            role: Role::Follower,
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for,
            leader_id: None,
            log,
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            votes: BTreeSet::new(),
            progress: BTreeMap::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng: SplitMix64::new(seed),
            persisted: hard_state,
            unstable_from: None,
            outbox: Vec::new(),
            config,
        };
        raft.reset_election_timer();
        Ok(raft)
    }

    /// This node's identifier.
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// The node's current role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// The latest term this node has seen.
    pub fn term(&self) -> Term {
        self.current_term
    }

    /// The candidate this node voted for in the current term, if any.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// The leader of the current term, if this node knows it.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// The node's log.
    pub fn log(&self) -> &Log {
        &self.log
    }

    /// The highest log index known to be committed.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// The highest log index handed out for applying.
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// The node's current hard state.
    pub fn hard_state(&self) -> HardState {
        HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        }
    }

    /// The election timeout currently in effect, in ticks.
    pub fn election_timeout(&self) -> u64 {
        self.election_timeout
    }

    /// Advances logical time by one tick.
    ///
    /// Followers and candidates start an election once their election
    /// timeout expires. Leaders send heartbeats at the configured interval.
    pub fn tick(&mut self) {
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.campaign();
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                    self.broadcast_append();
                }
            }
        }
    }

    /// Submits a command for replication. Only the leader accepts proposals.
    ///
    /// Returns the index the command was appended at. The command is
    /// committed once an entry at that index **with the current term** shows
    /// up in [`Ready::committed_entries`]. If a different entry is committed
    /// at that index, leadership was lost and the command was dropped.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<LogIndex, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader_hint: self.leader_id,
            });
        }
        let index = self.append_as_leader(EntryPayload::Command(command));
        self.broadcast_append();
        Ok(index)
    }

    /// Handles a message from another node.
    ///
    /// Messages addressed to another node or sent by a node outside the
    /// cluster are ignored.
    pub fn step(&mut self, message: Message) {
        if message.to != self.config.id || !self.config.peers.contains(&message.from) {
            return;
        }

        // Any message from a later term means this node is out of date
        // (Raft §5.1). Only a leader sends AppendEntries, so its sender is the
        // leader of that term.
        if message.term > self.current_term {
            let leader =
                matches!(message.body, MessageBody::AppendEntries { .. }).then_some(message.from);
            self.become_follower(message.term, leader);
        }

        if message.term < self.current_term {
            self.reject_stale(&message);
            return;
        }

        let from = message.from;
        match message.body {
            MessageBody::RequestVote {
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(from, last_log_index, last_log_term),
            MessageBody::RequestVoteResponse { granted } => {
                self.handle_vote_response(from, granted);
            }
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            MessageBody::AppendEntriesResponse {
                success,
                match_index,
                conflict_index,
                conflict_term,
            } => {
                if success {
                    self.handle_append_success(from, match_index);
                } else {
                    self.handle_append_conflict(from, conflict_index, conflict_term);
                }
            }
        }
    }

    /// Takes the state to persist, the messages to send and the entries to
    /// apply. See [`Ready`] for the order in which to handle them.
    pub fn take_ready(&mut self) -> Ready {
        let current = self.hard_state();
        let hard_state = (current != self.persisted).then_some(current);
        self.persisted = current;

        let entries = self
            .unstable_from
            .take()
            .map(|from| self.log.entries_from(from).to_vec())
            .unwrap_or_default();

        let committed_entries = self
            .log
            .entries_between(self.last_applied.next(), self.commit_index)
            .to_vec();
        self.last_applied = self.commit_index;

        Ready {
            hard_state,
            entries,
            messages: std::mem::take(&mut self.outbox),
            committed_entries,
        }
    }

    /// The number of nodes that form a majority.
    fn quorum(&self) -> usize {
        let cluster_size = self.config.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.rng.gen_range(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }

    /// Starts an election for the next term (Raft §5.2).
    fn campaign(&mut self) {
        self.role = Role::Candidate;
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.config.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.config.id);
        self.reset_election_timer();

        if self.votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }

        let body = MessageBody::RequestVote {
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for peer in self.config.peers.clone() {
            self.send(peer, body.clone());
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.progress.clear();
        self.reset_election_timer();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.votes.clear();

        let next_index = self.log.last_index().next();
        self.progress = self
            .config
            .peers
            .iter()
            .map(|&peer| {
                let progress = Progress {
                    next_index,
                    match_index: LogIndex(0),
                };
                (peer, progress)
            })
            .collect();

        // A leader may only commit entries from its own term by counting
        // replicas (Raft §5.4.2). Appending an empty entry right away lets it
        // commit everything from earlier terms without waiting for a client.
        // ReadIndex reads also depend on it.
        self.append_as_leader(EntryPayload::Noop);
        // Announce leadership right away instead of waiting for the first
        // heartbeat interval, so that other candidates stop campaigning.
        self.broadcast_append();
    }

    /// Appends a new entry to the leader's own log and returns its index.
    fn append_as_leader(&mut self, payload: EntryPayload) -> LogIndex {
        let index = self.log.last_index().next();
        self.log.append(Entry {
            term: self.current_term,
            index,
            payload,
        });
        self.mark_unstable(index);
        // In a single-node cluster the leader alone is a majority.
        self.maybe_commit();
        index
    }

    /// Sends every follower the entries it is missing, or a heartbeat if it
    /// has them all.
    fn broadcast_append(&mut self) {
        self.heartbeat_elapsed = 0;
        for peer in self.config.peers.clone() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let Some(progress) = self.progress.get(&peer) else {
            return;
        };
        let prev_log_index = LogIndex(progress.next_index.0 - 1);
        let Some(prev_log_term) = self.log.term_at(prev_log_index) else {
            // The entries the follower needs were compacted into a snapshot.
            // Sending the snapshot (InstallSnapshot) comes with log compaction.
            return;
        };
        let entries: Vec<Entry> = self
            .log
            .entries_from(progress.next_index)
            .iter()
            .take(self.config.max_entries_per_append)
            .cloned()
            .collect();
        self.send(
            peer,
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    /// Grants a vote at most once per term, and only to a candidate whose log
    /// is at least as up to date as ours (Raft §5.2, §5.4.1).
    fn handle_request_vote(
        &mut self,
        candidate: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) {
        let can_vote = self.voted_for.is_none_or(|v| v == candidate);
        let log_ok =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        let granted = can_vote && log_ok;

        if granted {
            self.voted_for = Some(candidate);
            // Granting a vote counts as hearing from a possible leader, so do
            // not start a competing election right away.
            self.reset_election_timer();
        }
        self.send(candidate, MessageBody::RequestVoteResponse { granted });
    }

    fn handle_vote_response(&mut self, voter: NodeId, granted: bool) {
        if self.role != Role::Candidate || !granted {
            return;
        }
        self.votes.insert(voter);
        if self.votes.len() >= self.quorum() {
            self.become_leader();
        }
    }

    /// Follower side of log replication (Raft §5.3).
    fn handle_append_entries(
        &mut self,
        leader: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    ) {
        match self.role {
            // A candidate that hears from a leader of its own term lost the
            // election (Raft §5.2).
            Role::Candidate => self.become_follower(self.current_term, Some(leader)),
            Role::Follower => {
                self.leader_id = Some(leader);
                self.reset_election_timer();
            }
            Role::Leader => {
                // Two leaders in one term would violate election safety. The
                // simulator's invariant checks catch this; never answer.
                return;
            }
        }

        if !self.log.matches(prev_log_index, prev_log_term) {
            let (conflict_index, conflict_term) = self.conflict_hint(prev_log_index);
            self.send(
                leader,
                MessageBody::AppendEntriesResponse {
                    success: false,
                    match_index: LogIndex(0),
                    conflict_index,
                    conflict_term,
                },
            );
            return;
        }

        let last_new_index = LogIndex(prev_log_index.0 + entries.len() as u64);
        self.append_from_leader(entries);

        // Only advance to what this message proved: entries after
        // `last_new_index` may still differ from the leader's log.
        let new_commit = leader_commit.min(last_new_index);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }

        self.send(
            leader,
            MessageBody::AppendEntriesResponse {
                success: true,
                match_index: last_new_index,
                conflict_index: LogIndex(0),
                conflict_term: None,
            },
        );
    }

    /// Where a leader should continue after a failed consistency check at
    /// `prev_log_index`.
    fn conflict_hint(&self, prev_log_index: LogIndex) -> (LogIndex, Option<Term>) {
        if prev_log_index > self.log.last_index() || prev_log_index < self.log.snapshot_index() {
            // Too short, or the leader is behind our snapshot (whose entries
            // are committed and therefore match): continue after what we have
            // or after the snapshot.
            let next = if prev_log_index > self.log.last_index() {
                self.log.last_index()
            } else {
                self.log.snapshot_index()
            };
            return (next.next(), None);
        }
        let term = self.log.term_at(prev_log_index);
        let first = self
            .log
            .first_index_of_term_at(prev_log_index)
            .unwrap_or(prev_log_index);
        (first, term)
    }

    /// Stores entries the leader sent after a successful consistency check.
    ///
    /// Entries already present with the same term are kept, so a delayed or
    /// duplicated message never removes entries a newer message added. The
    /// first entry with a different term, and everything after it, is
    /// replaced (Raft §5.3).
    fn append_from_leader(&mut self, entries: Vec<Entry>) {
        let mut entries = entries.into_iter().peekable();
        while let Some(entry) = entries.peek() {
            match self.log.term_at(entry.index) {
                Some(term) if term == entry.term => {
                    entries.next();
                }
                Some(_) => {
                    assert!(
                        entry.index > self.commit_index,
                        "leader tried to overwrite committed entry {} on {}",
                        entry.index,
                        self.config.id
                    );
                    self.log.truncate_from(entry.index);
                    break;
                }
                None if entry.index <= self.log.snapshot_index() => {
                    // Already covered by our snapshot.
                    entries.next();
                }
                None => break,
            }
        }
        for entry in entries {
            self.mark_unstable(entry.index);
            self.log.append(entry);
        }
    }

    fn handle_append_success(&mut self, peer: NodeId, match_index: LogIndex) {
        if self.role != Role::Leader {
            return;
        }
        let Some(progress) = self.progress.get_mut(&peer) else {
            return;
        };
        // Responses can arrive out of order; never move backwards.
        progress.match_index = progress.match_index.max(match_index);
        progress.next_index = progress.next_index.max(progress.match_index.next());
        let caught_up = progress.next_index > self.log.last_index();

        self.maybe_commit();
        if !caught_up {
            self.send_append(peer);
        }
    }

    /// Moves `next_index` back using the follower's hint, skipping a whole
    /// conflicting term at once where possible, and retries.
    fn handle_append_conflict(
        &mut self,
        peer: NodeId,
        conflict_index: LogIndex,
        conflict_term: Option<Term>,
    ) {
        if self.role != Role::Leader {
            return;
        }
        let next = conflict_term
            .and_then(|term| self.log.last_index_of_term(term))
            .map_or(conflict_index, LogIndex::next);
        let last_index = self.log.last_index();
        let Some(progress) = self.progress.get_mut(&peer) else {
            return;
        };
        // Never go below what is known to match, and never past our log.
        progress.next_index = next
            .max(progress.match_index.next())
            .min(last_index.next())
            .max(LogIndex(1));
        self.send_append(peer);
    }

    /// Advances the commit index to the highest entry stored on a majority,
    /// if that entry is from the current term (Raft §5.4.2).
    fn maybe_commit(&mut self) {
        let mut matched: Vec<LogIndex> = self
            .progress
            .values()
            .map(|p| p.match_index)
            .chain([self.log.last_index()])
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let majority_index = matched[self.quorum() - 1];

        if majority_index > self.commit_index
            && self.log.term_at(majority_index) == Some(self.current_term)
        {
            self.commit_index = majority_index;
        }
    }

    fn mark_unstable(&mut self, index: LogIndex) {
        self.unstable_from = Some(self.unstable_from.map_or(index, |from| from.min(index)));
    }

    /// Answers a request from an older term so the sender learns it is out of
    /// date. Stale responses are dropped.
    fn reject_stale(&mut self, message: &Message) {
        let body = match message.body {
            MessageBody::RequestVote { .. } => MessageBody::RequestVoteResponse { granted: false },
            MessageBody::AppendEntries { .. } => MessageBody::AppendEntriesResponse {
                success: false,
                match_index: LogIndex(0),
                conflict_index: LogIndex(0),
                conflict_term: None,
            },
            MessageBody::RequestVoteResponse { .. } | MessageBody::AppendEntriesResponse { .. } => {
                return
            }
        };
        self.send(message.from, body);
    }

    fn send(&mut self, to: NodeId, body: MessageBody) {
        self.outbox.push(Message {
            from: self.config.id,
            to,
            term: self.current_term,
            body,
        });
    }
}
