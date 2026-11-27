//! Snapshots of the replicated state machine.

use std::fmt;
use std::sync::Arc;

use crate::types::{LogIndex, Term};

/// The state machine's state after applying every entry up to `index`.
///
/// Raft never looks inside `data`: the caller produces it from its state
/// machine and restores the state machine from it. Raft keeps the latest
/// snapshot in memory and sends it to followers that are too far behind to
/// catch up from the log (`InstallSnapshot`). The data is shared, so
/// cloning a snapshot is cheap.
#[derive(Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The last log entry the snapshot includes.
    pub index: LogIndex,
    /// The term of the entry at `index`.
    pub term: Term,
    /// The serialized state machine.
    pub data: Arc<[u8]>,
}

impl fmt::Debug for Snapshot {
    // The data can be large; show its size only.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("index", &self.index)
            .field("term", &self.term)
            .field("bytes", &self.data.len())
            .finish()
    }
}

/// Why [`Raft::compact`](crate::Raft::compact) refused a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactError {
    /// The snapshot claims to include entries that have not been applied.
    NotApplied {
        /// The snapshot's index.
        index: LogIndex,
        /// The highest applied index.
        last_applied: LogIndex,
    },
    /// The node already has a snapshot at or after this index, for example
    /// one it received from the leader while this one was being written.
    Outdated {
        /// The snapshot's index.
        index: LogIndex,
        /// The index of the snapshot the node already has.
        current: LogIndex,
    },
}

impl fmt::Display for CompactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotApplied {
                index,
                last_applied,
            } => write!(
                f,
                "snapshot at {index} includes entries not applied yet (applied up to \
                 {last_applied})"
            ),
            Self::Outdated { index, current } => write!(
                f,
                "snapshot at {index} is not newer than the current one at {current}"
            ),
        }
    }
}

impl std::error::Error for CompactError {}
