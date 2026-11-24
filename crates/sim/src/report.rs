//! The outcome of a simulation run.

use std::fmt;

/// Counters describing what happened during a run. Useful to confirm that a
/// run actually exercised the faults it was meant to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Messages handed to the network.
    pub messages_sent: u64,
    /// Messages lost to random drops or partitions.
    pub messages_dropped: u64,
    /// Messages delivered twice.
    pub messages_duplicated: u64,
    /// Partitions started.
    pub partitions: u64,
    /// Node crashes.
    pub crashes: u64,
    /// Node pauses.
    pub pauses: u64,
    /// Distinct `(term, leader)` pairs observed.
    pub leaders_elected: u64,
    /// Proposals accepted by a leader.
    pub proposals_accepted: u64,
    /// Highest log index committed by any leader.
    pub committed_index: u64,
}

/// A successful run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// The run's seed.
    pub seed: u64,
    /// What happened during the run.
    pub stats: Stats,
    /// A hash over the run's observable behaviour. Two runs with the same
    /// seed and configuration must produce the same fingerprint.
    pub fingerprint: u64,
}

/// The invariant a failing run violated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViolationKind {
    /// Two nodes were leader in the same term (Raft §5.2).
    ElectionSafety,
    /// Two logs contained the same index and term but differed before it
    /// (Raft §5.3).
    LogMatching,
    /// A leader was missing an entry committed in an earlier term
    /// (Raft §5.4).
    LeaderCompleteness,
    /// Two nodes applied different entries at the same index, or a node
    /// applied entries out of order (Raft §5.4.3).
    StateMachineSafety,
    /// After all faults were healed, the cluster did not converge.
    Liveness,
    /// A node panicked, for example on an internal assertion.
    Panic,
}

impl fmt::Display for ViolationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ElectionSafety => "election safety",
            Self::LogMatching => "log matching",
            Self::LeaderCompleteness => "leader completeness",
            Self::StateMachineSafety => "state machine safety",
            Self::Liveness => "liveness",
            Self::Panic => "panic",
        };
        f.write_str(name)
    }
}

/// A failed run, with enough context to reproduce and debug it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// The run's seed. Running it again reproduces the failure exactly.
    pub seed: u64,
    /// The tick at which the violation was detected.
    pub tick: u64,
    /// Which invariant was violated.
    pub kind: ViolationKind,
    /// What exactly went wrong.
    pub details: String,
    /// The most recent notable events before the violation.
    pub trace: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "seed {} violated {} at tick {}: {}",
            self.seed, self.kind, self.tick, self.details
        )?;
        writeln!(f, "recent events:")?;
        for line in &self.trace {
            writeln!(f, "  {line}")?;
        }
        write!(
            f,
            "reproduce with: cargo run -p sim --release -- --seed {}",
            self.seed
        )
    }
}

impl std::error::Error for Violation {}
