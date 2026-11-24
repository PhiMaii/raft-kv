//! Messages exchanged between Raft nodes.

use crate::log::Entry;
use crate::types::{LogIndex, NodeId, Term};

/// A message between two nodes. Every message carries the sender's current
/// term, which is how nodes discover that they are out of date.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    /// The sending node.
    pub from: NodeId,
    /// The receiving node.
    pub to: NodeId,
    /// The sender's current term.
    pub term: Term,
    /// The RPC request or response.
    pub body: MessageBody,
}

/// The RPC carried by a [`Message`] (Raft §5, Figure 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageBody {
    /// A candidate asks for a vote.
    RequestVote {
        /// Index of the candidate's last log entry.
        last_log_index: LogIndex,
        /// Term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// The answer to [`MessageBody::RequestVote`].
    RequestVoteResponse {
        /// Whether the vote was granted.
        granted: bool,
    },
    /// The leader replicates entries. With no entries it is a heartbeat.
    AppendEntries {
        /// Index of the entry right before `entries`.
        prev_log_index: LogIndex,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// Entries to store; empty for a heartbeat.
        entries: Vec<Entry>,
        /// The leader's commit index.
        leader_commit: LogIndex,
    },
    /// The answer to [`MessageBody::AppendEntries`].
    AppendEntriesResponse {
        /// Whether the follower's log matched at `prev_log_index`.
        success: bool,
        /// On success, the index up to which the follower's log now matches
        /// the leader's.
        match_index: LogIndex,
    },
}
