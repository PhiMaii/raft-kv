//! Raft's safety properties (Raft Figure 3), checked while a run progresses.

use std::collections::{BTreeMap, BTreeSet};

use raft_core::{Entry, Log, LogIndex, NodeId, Term};

use crate::report::ViolationKind;

/// A violated invariant, before the simulator adds seed, tick and trace.
pub(crate) type Failure = (ViolationKind, String);

#[derive(Clone, Debug)]
struct Committed {
    entry: Entry,
    /// The term of the leader that first committed the entry, if a leader
    /// was seen committing it.
    commit_term: Option<Term>,
}

/// Remembers what the cluster has promised so far and checks every new
/// observation against it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Checker {
    /// The leader of each term seen so far.
    leaders: BTreeMap<Term, NodeId>,
    /// Every entry known to be committed.
    committed: BTreeMap<LogIndex, Committed>,
    /// `(term, leader)` pairs already checked for completeness.
    checked_leaders: BTreeSet<(Term, NodeId)>,
}

impl Checker {
    /// Number of distinct leaders observed.
    pub(crate) fn leaders_elected(&self) -> u64 {
        self.leaders.len() as u64
    }

    /// The highest index known to be committed.
    pub(crate) fn committed_index(&self) -> LogIndex {
        self.committed
            .last_key_value()
            .map_or(LogIndex(0), |(&index, _)| index)
    }

    /// **Election safety** and **leader completeness**: `node` is leader of
    /// `term`. No other node may lead that term, and the leader's log must
    /// contain every entry committed in an earlier term.
    pub(crate) fn on_leader(&mut self, node: NodeId, term: Term, log: &Log) -> Result<(), Failure> {
        if let Some(&other) = self.leaders.get(&term) {
            if other != node {
                return Err((
                    ViolationKind::ElectionSafety,
                    format!("{other} and {node} are both leader in {term}"),
                ));
            }
        }
        self.leaders.insert(term, node);

        if !self.checked_leaders.insert((term, node)) {
            return Ok(());
        }
        for (&index, committed) in &self.committed {
            let Some(commit_term) = committed.commit_term else {
                continue;
            };
            if commit_term >= term || index <= log.snapshot_index() {
                continue;
            }
            if log.entry(index) != Some(&committed.entry) {
                return Err((
                    ViolationKind::LeaderCompleteness,
                    format!(
                        "{node} became leader in {term} without entry {index} committed in \
                         {commit_term}; it has {:?}",
                        log.entry(index)
                    ),
                ));
            }
        }
        Ok(())
    }

    /// A leader in `term` advanced its commit index from `old` to `new`.
    pub(crate) fn on_leader_commit(
        &mut self,
        node: NodeId,
        term: Term,
        old: LogIndex,
        new: LogIndex,
        log: &Log,
    ) -> Result<(), Failure> {
        for index in log.entries_between(LogIndex(old.0 + 1), new) {
            self.record(node, index.clone(), Some(term))?;
        }
        Ok(())
    }

    /// **State machine safety**: `node` applied `entry`. It must be the same
    /// entry every other node applied, or will apply, at that index.
    pub(crate) fn on_apply(&mut self, node: NodeId, entry: &Entry) -> Result<(), Failure> {
        self.record(node, entry.clone(), None)
    }

    fn record(
        &mut self,
        node: NodeId,
        entry: Entry,
        commit_term: Option<Term>,
    ) -> Result<(), Failure> {
        match self.committed.get_mut(&entry.index) {
            Some(existing) if existing.entry != entry => Err((
                ViolationKind::StateMachineSafety,
                format!(
                    "{node} committed or applied {:?} at {}, but {:?} was committed there before",
                    entry, entry.index, existing.entry
                ),
            )),
            Some(existing) => {
                if existing.commit_term.is_none() {
                    existing.commit_term = commit_term;
                }
                Ok(())
            }
            None => {
                self.committed
                    .insert(entry.index, Committed { entry, commit_term });
                Ok(())
            }
        }
    }

    /// **Log matching**: if two logs have an entry with the same index and
    /// term, they are identical up to and including that index.
    pub(crate) fn check_log_matching(logs: &[(NodeId, &Log)]) -> Result<(), Failure> {
        for (i, &(a_id, a)) in logs.iter().enumerate() {
            for &(b_id, b) in &logs[i + 1..] {
                let first = a.first_index().max(b.first_index());
                let common = a.last_index().min(b.last_index());
                let Some(last_match) = (first.0..=common.0)
                    .rev()
                    .map(LogIndex)
                    .find(|&index| a.term_at(index) == b.term_at(index))
                else {
                    continue;
                };
                let prefix_a = a.entries_between(first, last_match);
                let prefix_b = b.entries_between(first, last_match);
                if prefix_a != prefix_b {
                    return Err((
                        ViolationKind::LogMatching,
                        format!(
                            "{a_id} and {b_id} agree on the term at {last_match} but differ \
                             before it"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use raft_core::EntryPayload;

    use super::*;

    fn entry(index: u64, term: u64, byte: u8) -> Entry {
        Entry {
            term: Term(term),
            index: LogIndex(index),
            payload: EntryPayload::Command(vec![byte]),
        }
    }

    fn log(entries: Vec<Entry>) -> Log {
        Log::from_parts(LogIndex(0), Term(0), entries).unwrap()
    }

    fn kind<T>(result: Result<T, Failure>) -> Option<ViolationKind> {
        result.err().map(|(kind, _)| kind)
    }

    #[test]
    fn two_leaders_in_one_term_are_detected() {
        let mut checker = Checker::default();
        let empty = Log::new();
        assert!(checker.on_leader(NodeId(1), Term(3), &empty).is_ok());
        assert!(checker.on_leader(NodeId(1), Term(3), &empty).is_ok());
        assert_eq!(
            kind(checker.on_leader(NodeId(2), Term(3), &empty)),
            Some(ViolationKind::ElectionSafety)
        );
    }

    #[test]
    fn diverging_applies_are_detected() {
        let mut checker = Checker::default();
        assert!(checker.on_apply(NodeId(1), &entry(1, 1, 7)).is_ok());
        assert!(checker.on_apply(NodeId(2), &entry(1, 1, 7)).is_ok());
        assert_eq!(
            kind(checker.on_apply(NodeId(3), &entry(1, 1, 8))),
            Some(ViolationKind::StateMachineSafety)
        );
    }

    #[test]
    fn a_leader_missing_a_committed_entry_is_detected() {
        let mut checker = Checker::default();
        let leader_log = log(vec![entry(1, 1, 1), entry(2, 1, 2)]);
        checker
            .on_leader_commit(NodeId(1), Term(1), LogIndex(0), LogIndex(2), &leader_log)
            .unwrap();

        let complete = log(vec![entry(1, 1, 1), entry(2, 1, 2), entry(3, 2, 3)]);
        assert!(checker.on_leader(NodeId(2), Term(2), &complete).is_ok());

        let missing = log(vec![entry(1, 1, 1)]);
        assert_eq!(
            kind(checker.on_leader(NodeId(3), Term(3), &missing)),
            Some(ViolationKind::LeaderCompleteness)
        );
    }

    #[test]
    fn completeness_only_covers_earlier_terms() {
        let mut checker = Checker::default();
        let leader_log = log(vec![entry(1, 5, 1)]);
        checker
            .on_leader_commit(NodeId(1), Term(5), LogIndex(0), LogIndex(1), &leader_log)
            .unwrap();
        // A stale leader of an older term need not have it.
        assert!(checker.on_leader(NodeId(2), Term(4), &Log::new()).is_ok());
    }

    #[test]
    fn log_matching_violations_are_detected() {
        let a = log(vec![entry(1, 1, 1), entry(2, 2, 2)]);
        let b = log(vec![entry(1, 1, 9), entry(2, 2, 2)]);
        let c = log(vec![entry(1, 1, 1), entry(2, 3, 5)]);

        assert_eq!(
            kind(Checker::check_log_matching(&[
                (NodeId(1), &a),
                (NodeId(2), &b)
            ])),
            Some(ViolationKind::LogMatching)
        );
        // Diverging after a common prefix is allowed.
        assert!(Checker::check_log_matching(&[(NodeId(1), &a), (NodeId(3), &c)]).is_ok());
    }
}
