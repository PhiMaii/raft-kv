//! Deterministic, seed-driven cluster simulator.
//!
//! A run places a whole Raft cluster in one process: a simulated network, a
//! virtual clock and simulated disks, all driven by a single random seed.
//! While the run injects faults (message loss, duplication, delay and
//! reordering, symmetric and asymmetric partitions, crashes and pauses), the
//! simulator checks Raft's safety properties after every tick:
//!
//! - **Election safety:** at most one leader per term.
//! - **Log matching:** logs that agree on an entry's index and term agree on
//!   everything before it.
//! - **Leader completeness:** every leader has every entry committed in an
//!   earlier term.
//! - **State machine safety:** no two nodes apply different entries at the
//!   same index.
//!
//! At the end of a run all faults are healed, and the cluster must converge:
//! one leader, identical logs, everything applied.
//!
//! The same seed always produces the same run, so any failure reproduces
//! exactly:
//!
//! ```
//! let report = sim::run(42, &sim::SimConfig::default()).expect("seed 42 passes");
//! assert_eq!(report, sim::run(42, &sim::SimConfig::default()).unwrap());
//! ```

mod config;
mod invariants;
mod network;
mod report;
mod rng;
mod simulation;

pub use config::{FaultConfig, SimConfig};
pub use report::{Report, Stats, Violation, ViolationKind};
pub use rng::Chance;

/// Runs one simulation with the given seed.
///
/// Returns a [`Report`] if every invariant held, or the first [`Violation`]
/// found.
pub fn run(seed: u64, config: &SimConfig) -> Result<Report, Violation> {
    simulation::Simulation::new(seed, config.clone()).run()
}

/// Parses a list of regression seeds: one seed per line, with `#` starting a
/// comment.
///
/// # Panics
///
/// Panics on a line that is neither empty, a comment, nor a seed.
pub fn parse_seeds(text: &str) -> Vec<u64> {
    text.lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim())
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.parse()
                .unwrap_or_else(|_| panic!("invalid seed line: {line:?}"))
        })
        .collect()
}
