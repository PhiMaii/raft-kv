//! Identifier and counter types used throughout the protocol.
//!
//! Each is a newtype over `u64` so that a term can never be passed where a log
//! index is expected, and the other way around.

use std::fmt;

/// Identifies a node in the cluster. Assigned statically from the
/// cluster configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// A Raft term: a logical clock that increases with every election.
///
/// Term 0 is the initial term before any election has happened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub u64);

/// The position of an entry in the replicated log.
///
/// The first real entry has index 1. Index 0 is the empty position before
/// the first entry and always has term 0.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(pub u64);

impl Term {
    /// The term after this one.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl LogIndex {
    /// The index after this one.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}", self.0)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "i{}", self.0)
    }
}
