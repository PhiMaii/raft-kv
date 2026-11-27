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

impl Message {
    /// Whether the message may be sent before the [`Ready`](crate::Ready)
    /// that produced it is persisted (Raft dissertation §10.2.1).
    ///
    /// True for `AppendEntries` and `InstallSnapshot`. Only a leader sends
    /// them, and they carry its own log; they promise nothing about what is
    /// on its disk. The leader counts its own entries towards a majority
    /// only when it handles a follower's answer, and that happens after the
    /// caller has finished persisting. Votes and every answer, in contrast,
    /// promise state that must be durable first.
    ///
    /// Sending these early lets the leader's fsync overlap with the network
    /// round trip. It is only safe if the caller finishes persisting before
    /// its next call into `Raft`.
    #[must_use]
    pub fn may_send_before_persisting(&self) -> bool {
        matches!(
            self.body,
            MessageBody::AppendEntries { .. } | MessageBody::InstallSnapshot { .. }
        )
    }
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
        /// The leader's current ReadIndex confirmation round. The follower
        /// echoes it, so the leader can tell which responses prove it was
        /// still leader after a read arrived.
        read_round: u64,
    },
    /// The answer to [`MessageBody::AppendEntries`].
    AppendEntriesResponse {
        /// Whether the follower's log matched at `prev_log_index`.
        success: bool,
        /// On success, the index up to which the follower's log is now known
        /// to match the leader's. Zero on failure.
        match_index: LogIndex,
        /// On failure, where the leader should try next: the first index of
        /// `conflict_term` in the follower's log, or the follower's last
        /// index + 1 if its log is too short. Zero on success.
        conflict_index: LogIndex,
        /// On failure, the term of the follower's entry at `prev_log_index`,
        /// if it has one. Lets the leader skip a whole term at once.
        conflict_term: Option<Term>,
        /// The `read_round` of the request this answers.
        read_round: u64,
    },
    /// One chunk of the leader's snapshot, for a follower that needs entries
    /// the leader has already compacted (Raft §7). The leader sends the
    /// next chunk once the follower has acknowledged this one.
    InstallSnapshot {
        /// The last entry the snapshot includes.
        index: LogIndex,
        /// The term of that entry.
        term: Term,
        /// Where this chunk starts in the snapshot data.
        offset: u64,
        /// The snapshot's size in bytes.
        total: u64,
        /// The chunk.
        data: Vec<u8>,
        /// The leader's current ReadIndex confirmation round, as in
        /// `AppendEntries`.
        read_round: u64,
    },
    /// The answer to [`MessageBody::InstallSnapshot`].
    InstallSnapshotResponse {
        /// The `index` of the snapshot being transferred.
        index: LogIndex,
        /// How many bytes of it the follower holds: where the leader should
        /// continue. Equals the snapshot's size once it is installed.
        received: u64,
        /// Whether the follower now has everything up to `index`, either
        /// from this snapshot or because it had already committed that far.
        done: bool,
        /// The `read_round` of the request this answers.
        read_round: u64,
    },
}
