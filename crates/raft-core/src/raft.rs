//! The Raft state machine of a single node.

use std::collections::BTreeSet;

use crate::config::{Config, ConfigError};
use crate::log::Log;
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
/// The caller **must** make `hard_state` durable before sending any of the
/// `messages`. A vote that is sent but then forgotten in a crash could let the
/// node vote twice in the same term.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[must_use = "a Ready holds state to persist and messages to send"]
pub struct Ready {
    /// The new hard state, if it changed since the last `Ready`.
    pub hard_state: Option<HardState>,
    /// Messages to send to other nodes.
    pub messages: Vec<Message>,
}

impl Ready {
    /// Whether there is nothing to persist or send.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none() && self.messages.is_empty()
    }
}

/// A single Raft node.
///
/// `Raft` performs no I/O and never reads a clock. The caller drives it:
///
/// - [`tick`](Self::tick) advances logical time by one tick,
/// - [`step`](Self::step) delivers a message from another node,
/// - [`take_ready`](Self::take_ready) collects the state to persist and the
///   messages to send that resulted from the calls above.
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
    /// Votes received in the current election, including our own.
    votes: BTreeSet<NodeId>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    rng: SplitMix64,
    /// The hard state as of the last `Ready`, to detect changes.
    persisted: HardState,
    outbox: Vec<Message>,
}

impl Raft {
    /// Creates a node that has never run before.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        Self::restore(config, HardState::default(), Log::new())
    }

    /// Creates a node from the state it persisted before a restart.
    pub fn restore(config: Config, hard_state: HardState, log: Log) -> Result<Self, ConfigError> {
        config.validate()?;
        // Mix the node id into the seed so that nodes configured with the same
        // seed still pick different election timeouts.
        let seed = config.seed ^ config.id.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut raft = Self {
            role: Role::Follower,
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for,
            leader_id: None,
            log,
            votes: BTreeSet::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng: SplitMix64::new(seed),
            persisted: hard_state,
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
                    self.broadcast_heartbeat();
                }
            }
        }
    }

    /// Handles a message from another node.
    ///
    /// Messages addressed to another node or sent by a node outside the
    /// cluster are ignored.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "log replication will move the entries out of AppendEntries"
    )]
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

        match message.body {
            MessageBody::RequestVote {
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(message.from, last_log_index, last_log_term),
            MessageBody::RequestVoteResponse { granted } => {
                self.handle_vote_response(message.from, granted);
            }
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                ..
            } => self.handle_append_entries(message.from, prev_log_index, prev_log_term),
            MessageBody::AppendEntriesResponse { .. } => {
                // Used by log replication; a heartbeat response only matters
                // for its term, which was handled above.
            }
        }
    }

    /// Takes the state to persist and the messages to send.
    pub fn take_ready(&mut self) -> Ready {
        let current = self.hard_state();
        let hard_state = (current != self.persisted).then_some(current);
        self.persisted = current;
        Ready {
            hard_state,
            messages: std::mem::take(&mut self.outbox),
        }
    }

    /// The number of votes needed to win an election.
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
        self.reset_election_timer();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.votes.clear();
        // Announce leadership right away instead of waiting for the first
        // heartbeat interval, so that other candidates stop campaigning.
        self.broadcast_heartbeat();
    }

    fn broadcast_heartbeat(&mut self) {
        self.heartbeat_elapsed = 0;
        let body = MessageBody::AppendEntries {
            prev_log_index: self.log.last_index(),
            prev_log_term: self.log.last_term(),
            entries: Vec::new(),
            leader_commit: LogIndex(0),
        };
        for peer in self.config.peers.clone() {
            self.send(peer, body.clone());
        }
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

    fn handle_append_entries(
        &mut self,
        leader: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
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

        // Appending entries and advancing the commit index are part of log
        // replication. For now only the consistency check is answered.
        let success = self.log.matches(prev_log_index, prev_log_term);
        let match_index = if success { prev_log_index } else { LogIndex(0) };
        self.send(
            leader,
            MessageBody::AppendEntriesResponse {
                success,
                match_index,
            },
        );
    }

    /// Answers a request from an older term so the sender learns it is out of
    /// date. Stale responses are dropped.
    fn reject_stale(&mut self, message: &Message) {
        let body = match message.body {
            MessageBody::RequestVote { .. } => MessageBody::RequestVoteResponse { granted: false },
            MessageBody::AppendEntries { .. } => MessageBody::AppendEntriesResponse {
                success: false,
                match_index: LogIndex(0),
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
