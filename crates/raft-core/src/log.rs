//! The in-memory view of the replicated log.
//!
//! The log may start after a snapshot: entries up to and including
//! `snapshot_index` have been compacted away, and only their last index and
//! term are kept. Every lookup goes through that offset, so snapshots
//! (milestone 11) do not require changing how the rest of the protocol
//! addresses the log.

use std::fmt;

use crate::types::{LogIndex, Term};

/// What a log entry carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryPayload {
    /// An empty entry a new leader appends to commit entries from earlier
    /// terms (Raft §5.4.2, §8).
    Noop,
    /// An opaque command for the replicated state machine.
    Command(Vec<u8>),
}

/// A single entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The term of the leader that created the entry.
    pub term: Term,
    /// The entry's position in the log.
    pub index: LogIndex,
    /// What the entry carries.
    pub payload: EntryPayload,
}

/// Error returned when log entries do not form a valid, contiguous log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogError {
    /// The index the next entry should have had.
    pub expected_index: LogIndex,
    /// The entry that broke the sequence.
    pub found_index: LogIndex,
    /// The term of the entry that broke the sequence.
    pub found_term: Term,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid log: expected an entry at {}, found {} with term {}",
            self.expected_index, self.found_index, self.found_term
        )
    }
}

impl std::error::Error for LogError {}

/// The replicated log, possibly starting after a snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Log {
    snapshot_index: LogIndex,
    snapshot_term: Term,
    entries: Vec<Entry>,
}

impl Log {
    /// An empty log with no snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds a log from a snapshot position and the entries after it, for
    /// example when a node restarts.
    ///
    /// The entries must start right after `snapshot_index`, be contiguous, and
    /// have terms that never decrease.
    pub fn from_parts(
        snapshot_index: LogIndex,
        snapshot_term: Term,
        entries: Vec<Entry>,
    ) -> Result<Self, LogError> {
        let mut expected_index = snapshot_index.next();
        let mut previous_term = snapshot_term;
        for entry in &entries {
            if entry.index != expected_index || entry.term < previous_term {
                return Err(LogError {
                    expected_index,
                    found_index: entry.index,
                    found_term: entry.term,
                });
            }
            expected_index = expected_index.next();
            previous_term = entry.term;
        }
        Ok(Self {
            snapshot_index,
            snapshot_term,
            entries,
        })
    }

    /// The index of the last entry covered by the snapshot (0 if none).
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    /// The term of the last entry covered by the snapshot (0 if none).
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// The index of the first entry stored in this log.
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_index.next()
    }

    /// The index of the last entry, including the snapshot.
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(self.snapshot_index, |e| e.index)
    }

    /// The term of the last entry, including the snapshot.
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// The term of the entry at `index`.
    ///
    /// Returns `None` if the index is past the end of the log or was
    /// compacted into the snapshot (other than the snapshot's last index,
    /// whose term is known).
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry(index).map(|e| e.term)
    }

    /// Whether this log contains an entry at `index` with term `term`.
    ///
    /// This is the consistency check of `AppendEntries` (Raft §5.3).
    pub fn matches(&self, index: LogIndex, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// The entry at `index`, if it is stored in this log.
    pub fn entry(&self, index: LogIndex) -> Option<&Entry> {
        self.position(index).and_then(|pos| self.entries.get(pos))
    }

    /// The entries stored in this log, in order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Appends an entry to the end of the log.
    ///
    /// # Panics
    ///
    /// Panics if the entry's index is not `last_index() + 1` or its term is
    /// lower than `last_term()`. Either would be a bug in the caller.
    pub fn append(&mut self, entry: Entry) {
        assert_eq!(
            entry.index,
            self.last_index().next(),
            "log entries must be appended contiguously"
        );
        assert!(
            entry.term >= self.last_term(),
            "log entry terms must not decrease"
        );
        self.entries.push(entry);
    }

    /// Maps a log index to a position in `entries`, if it is stored here.
    fn position(&self, index: LogIndex) -> Option<usize> {
        let offset = index.0.checked_sub(self.first_index().0)?;
        usize::try_from(offset).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            term: Term(term),
            index: LogIndex(index),
            payload: EntryPayload::Noop,
        }
    }

    #[test]
    fn empty_log_has_index_and_term_zero() {
        let log = Log::new();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term(0));
        assert_eq!(log.first_index(), LogIndex(1));
        assert!(log.matches(LogIndex(0), Term(0)));
        assert_eq!(log.term_at(LogIndex(1)), None);
    }

    #[test]
    fn append_and_look_up_entries() {
        let mut log = Log::new();
        log.append(entry(1, 1));
        log.append(entry(2, 1));
        log.append(entry(3, 2));

        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.last_term(), Term(2));
        assert_eq!(log.term_at(LogIndex(2)), Some(Term(1)));
        assert!(log.matches(LogIndex(3), Term(2)));
        assert!(!log.matches(LogIndex(3), Term(1)));
        assert!(!log.matches(LogIndex(4), Term(2)));
    }

    #[test]
    fn lookups_respect_the_snapshot_offset() {
        let log = Log::from_parts(LogIndex(10), Term(3), vec![entry(11, 3), entry(12, 4)])
            .expect("valid log");

        assert_eq!(log.first_index(), LogIndex(11));
        assert_eq!(log.last_index(), LogIndex(12));
        assert_eq!(log.term_at(LogIndex(10)), Some(Term(3)));
        assert_eq!(log.term_at(LogIndex(12)), Some(Term(4)));
        assert_eq!(log.entry(LogIndex(11)), Some(&entry(11, 3)));
        // Compacted entries are gone.
        assert_eq!(log.term_at(LogIndex(9)), None);
        assert_eq!(log.entry(LogIndex(5)), None);
    }

    #[test]
    fn snapshot_only_log_reports_the_snapshot_position() {
        let log = Log::from_parts(LogIndex(7), Term(2), Vec::new()).expect("valid log");
        assert_eq!(log.last_index(), LogIndex(7));
        assert_eq!(log.last_term(), Term(2));
    }

    #[test]
    fn from_parts_rejects_gaps_and_decreasing_terms() {
        let gap = Log::from_parts(LogIndex(0), Term(0), vec![entry(1, 1), entry(3, 1)]);
        assert_eq!(
            gap,
            Err(LogError {
                expected_index: LogIndex(2),
                found_index: LogIndex(3),
                found_term: Term(1),
            })
        );

        let decreasing = Log::from_parts(LogIndex(0), Term(0), vec![entry(1, 2), entry(2, 1)]);
        assert!(decreasing.is_err());

        let wrong_start = Log::from_parts(LogIndex(5), Term(1), vec![entry(1, 1)]);
        assert!(wrong_start.is_err());
    }

    #[test]
    #[should_panic(expected = "contiguously")]
    fn append_rejects_gaps() {
        let mut log = Log::new();
        log.append(entry(2, 1));
    }
}
