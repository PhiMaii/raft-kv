//! Runs a whole cluster in one process, driven by a single seed.

use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};

use raft_core::{
    Config, Entry, HardState, Log, LogIndex, NodeId, ProposeError, Raft, Ready, Role, Term,
};

use crate::config::SimConfig;
use crate::invariants::{Checker, Failure};
use crate::network::Network;
use crate::report::{Report, Stats, Violation, ViolationKind};
use crate::rng::SimRng;

/// How many recent events a violation report includes.
const TRACE_LEN: usize = 60;

/// What a node keeps on disk. Every `Ready` is persisted in full before its
/// messages are sent, so a crash loses nothing that was persisted.
///
/// Crashes that lose data written but not yet fsynced are simulated once the
/// storage layer with its `Disk` abstraction exists.
#[derive(Clone, Debug, Default)]
struct Storage {
    hard_state: HardState,
    entries: Vec<Entry>,
}

impl Storage {
    fn persist(&mut self, ready: &Ready) {
        if let Some(hard_state) = ready.hard_state {
            self.hard_state = hard_state;
        }
        if let Some(first) = ready.entries.first() {
            let keep = usize::try_from(first.index.0 - 1).expect("index fits in usize");
            self.entries.truncate(keep);
            self.entries.extend(ready.entries.iter().cloned());
        }
    }

    fn log(&self) -> Log {
        Log::from_parts(LogIndex(0), Term(0), self.entries.clone()).expect("persisted log is valid")
    }
}

#[derive(Debug)]
enum State {
    Up {
        raft: Box<Raft>,
        /// The node is paused (receives and does nothing) until this tick.
        paused_until: Option<u64>,
    },
    Down {
        restart_at: u64,
    },
}

#[derive(Debug)]
struct Node {
    id: NodeId,
    config: Config,
    state: State,
    storage: Storage,
    /// The commit index last observed, to detect when a leader advances it.
    observed_commit: LogIndex,
    restarts: u64,
}

impl Node {
    fn raft(&self) -> Option<&Raft> {
        match &self.state {
            State::Up { raft, .. } => Some(raft),
            State::Down { .. } => None,
        }
    }

    fn is_active(&self, now: u64) -> bool {
        matches!(&self.state, State::Up { paused_until, .. } if paused_until.is_none_or(|t| now >= t))
    }
}

/// A simulated cluster.
pub(crate) struct Simulation {
    seed: u64,
    config: SimConfig,
    now: u64,
    rng: SimRng,
    nodes: Vec<Node>,
    network: Network,
    checker: Checker,
    stats: Stats,
    trace: VecDeque<String>,
    fingerprint: u64,
    next_command: u64,
}

impl Simulation {
    pub(crate) fn new(seed: u64, config: SimConfig) -> Self {
        let mut rng = SimRng::new(seed);
        let max_entries_per_append = config.max_entries_per_append.unwrap_or_else(|| {
            // Small batches make followers acknowledge a log piece by piece,
            // which opens the timing windows several classic Raft bugs need.
            [1, 2, 4, 16, 1024][rng.index(5)]
        });
        let ids: Vec<NodeId> = (1..=config.nodes).map(NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                let mut raft_config = Config::new(id, peers, rng.next_u64());
                raft_config.max_entries_per_append = max_entries_per_append;
                let raft = Raft::new(raft_config.clone()).expect("valid raft config");
                Node {
                    id,
                    config: raft_config,
                    state: State::Up {
                        raft: Box::new(raft),
                        paused_until: None,
                    },
                    storage: Storage::default(),
                    observed_commit: LogIndex(0),
                    restarts: 0,
                }
            })
            .collect();
        Self {
            seed,
            config,
            now: 0,
            rng,
            nodes,
            network: Network::default(),
            checker: Checker::default(),
            stats: Stats::default(),
            trace: VecDeque::new(),
            fingerprint: 0xCBF2_9CE4_8422_2325,
            next_command: 0,
        }
    }

    /// Runs the simulation to completion. Panics inside a node are caught
    /// and reported as violations.
    pub(crate) fn run(mut self) -> Result<Report, Violation> {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| self.run_phases()));
        match outcome {
            Ok(Ok(())) => {
                self.stats.leaders_elected = self.checker.leaders_elected();
                self.stats.committed_index = self.checker.committed_index().0;
                Ok(Report {
                    seed: self.seed,
                    stats: self.stats,
                    fingerprint: self.fingerprint,
                })
            }
            Ok(Err(failure)) => Err(self.violation(failure)),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                    .unwrap_or_else(|| "unknown panic".to_owned());
                Err(self.violation((ViolationKind::Panic, message)))
            }
        }
    }

    fn run_phases(&mut self) -> Result<(), Failure> {
        for _ in 0..self.config.ticks {
            self.inject_faults();
            self.tick(true)?;
        }
        self.heal_everything();
        for _ in 0..self.config.settle_ticks {
            self.tick(false)?;
        }
        self.check_logs()?;
        self.check_converged()
    }

    /// One unit of simulated time.
    fn tick(&mut self, with_workload: bool) -> Result<(), Failure> {
        self.now += 1;
        self.restart_due_nodes();
        self.deliver_messages();
        for node in &mut self.nodes {
            if node.is_active(self.now) {
                if let State::Up { raft, .. } = &mut node.state {
                    raft.tick();
                }
            }
        }
        if with_workload && self.rng.chance(self.config.propose) {
            self.propose();
        }
        self.process_readies()?;
        if self.now % self.config.log_check_interval == 0 {
            self.check_logs()?;
        }
        Ok(())
    }

    fn deliver_messages(&mut self) {
        for message in self
            .network
            .take_due(self.now, &mut self.rng, &mut self.stats)
        {
            let Some(node) = self.nodes.iter_mut().find(|n| n.id == message.to) else {
                continue;
            };
            match &mut node.state {
                State::Down { .. } => self.stats.messages_dropped += 1,
                State::Up {
                    paused_until: Some(until),
                    ..
                } if self.now < *until => self.network.hold(self.now, message),
                State::Up { raft, .. } => {
                    self.fingerprint = fnv(
                        self.fingerprint,
                        &[self.now, message.from.0, message.to.0, message.term.0],
                    );
                    raft.step(message);
                }
            }
        }
    }

    /// A client sends a command to a random node. If that node is not the
    /// leader but knows who is, the client retries there once.
    fn propose(&mut self) {
        let first = self.nodes[self.rng.index(self.nodes.len())].id;
        if let Err(ProposeError::NotLeader {
            leader_hint: Some(leader),
        }) = self.propose_to(first)
        {
            // Like the client library: follow the hint once.
            let _ = self.propose_to(leader);
        }
    }

    fn propose_to(&mut self, id: NodeId) -> Result<(), ProposeError> {
        let now = self.now;
        let not_reachable = ProposeError::NotLeader { leader_hint: None };
        let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) else {
            return Err(not_reachable);
        };
        if !node.is_active(now) {
            return Err(not_reachable);
        }
        let State::Up { raft, .. } = &mut node.state else {
            return Err(not_reachable);
        };
        raft.propose(self.next_command.to_le_bytes().to_vec())?;
        self.next_command += 1;
        self.stats.proposals_accepted += 1;
        Ok(())
    }

    /// Persists, sends and applies each node's `Ready`, and checks the
    /// invariants against what changed.
    fn process_readies(&mut self) -> Result<(), Failure> {
        for i in 0..self.nodes.len() {
            let node = &mut self.nodes[i];
            let State::Up { raft, .. } = &mut node.state else {
                continue;
            };
            let ready = raft.take_ready();
            node.storage.persist(&ready);

            let id = node.id;
            let (role, term, commit) = (raft.role(), raft.term(), raft.commit_index());
            if role == Role::Leader {
                self.checker.on_leader(id, term, raft.log())?;
                if commit > node.observed_commit {
                    let old = node.observed_commit;
                    self.checker
                        .on_leader_commit(id, term, old, commit, raft.log())?;
                }
            }
            node.observed_commit = node.observed_commit.max(commit);

            for entry in &ready.committed_entries {
                self.checker.on_apply(id, entry)?;
            }
            for message in ready.messages {
                self.network.send(
                    self.now,
                    message,
                    &self.config.faults,
                    &mut self.rng,
                    &mut self.stats,
                );
            }
            if role == Role::Leader && !self.trace_mentions_leader(id, term) {
                self.event(&format!("{id} is leader in {term}"));
            }
        }
        Ok(())
    }

    fn inject_faults(&mut self) {
        let faults = self.config.faults.clone();
        if self.network.is_partitioned() && self.rng.chance(faults.heal) {
            self.network.heal();
            self.event("partitions healed");
        }
        if self.rng.chance(faults.partition) {
            self.partition();
        }
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_active(self.now) {
                continue;
            }
            let is_leader = self.nodes[i]
                .raft()
                .is_some_and(|r| r.role() == Role::Leader);
            let crash = if is_leader {
                faults.leader_crash
            } else {
                faults.crash
            };
            if self.rng.chance(crash) {
                let restart_at = self.now + self.rng.between(faults.downtime.0, faults.downtime.1);
                self.nodes[i].state = State::Down { restart_at };
                self.stats.crashes += 1;
                self.event(&format!(
                    "{} crashed until tick {restart_at}",
                    self.nodes[i].id
                ));
            } else if self.rng.chance(faults.pause) {
                let until = self.now
                    + self
                        .rng
                        .between(faults.pause_length.0, faults.pause_length.1);
                if let State::Up { paused_until, .. } = &mut self.nodes[i].state {
                    *paused_until = Some(until);
                }
                self.stats.pauses += 1;
                self.event(&format!("{} paused until tick {until}", self.nodes[i].id));
            }
        }
    }

    /// Cuts the network in one of three ways: a symmetric split into two
    /// groups, one node isolated, or a single link cut in one direction.
    fn partition(&mut self) {
        let mut ids: Vec<NodeId> = self.nodes.iter().map(|n| n.id).collect();
        if ids.len() < 2 {
            return;
        }
        self.stats.partitions += 1;
        match self.rng.below(3) {
            0 => {
                self.rng.shuffle(&mut ids);
                let cut = 1 + self.rng.index(ids.len() - 1);
                let (left, right) = ids.split_at(cut);
                for &a in left {
                    for &b in right {
                        self.network.block(a, b);
                        self.network.block(b, a);
                    }
                }
                self.event(&format!("partition {left:?} | {right:?}"));
            }
            1 => {
                let victim = ids[self.rng.index(ids.len())];
                for &other in &ids {
                    if other != victim {
                        self.network.block(victim, other);
                        self.network.block(other, victim);
                    }
                }
                self.event(&format!("{victim} isolated"));
            }
            _ => {
                self.rng.shuffle(&mut ids);
                self.network.block(ids[0], ids[1]);
                self.event(&format!("link {} -> {} cut", ids[0], ids[1]));
            }
        }
    }

    fn restart_due_nodes(&mut self) {
        for i in 0..self.nodes.len() {
            let node = &mut self.nodes[i];
            match node.state {
                State::Down { restart_at } if restart_at <= self.now => {}
                _ => continue,
            }
            node.restarts += 1;
            let mut config = node.config.clone();
            config.seed ^= node.restarts;
            let raft = Raft::restore(config, node.storage.hard_state, node.storage.log())
                .expect("valid raft config");
            node.state = State::Up {
                raft: Box::new(raft),
                paused_until: None,
            };
            node.observed_commit = LogIndex(0);
            let id = node.id;
            self.event(&format!("{id} restarted"));
        }
    }

    fn heal_everything(&mut self) {
        self.network.heal();
        self.config.faults = crate::config::FaultConfig::none();
        for node in &mut self.nodes {
            match &mut node.state {
                State::Down { restart_at } => *restart_at = self.now,
                State::Up { paused_until, .. } => *paused_until = None,
            }
        }
        self.event("all faults healed; settling");
    }

    fn check_logs(&self) -> Result<(), Failure> {
        let logs: Vec<Log> = self
            .nodes
            .iter()
            .map(|n| {
                n.raft()
                    .map_or_else(|| n.storage.log(), |r| r.log().clone())
            })
            .collect();
        let named: Vec<(NodeId, &Log)> = self.nodes.iter().map(|n| n.id).zip(&logs).collect();
        Checker::check_log_matching(&named)
    }

    /// After healing, exactly one leader must exist and every node must have
    /// the leader's log and have applied all of it.
    fn check_converged(&self) -> Result<(), Failure> {
        let rafts: Vec<&Raft> = self.nodes.iter().filter_map(Node::raft).collect();
        let leaders: Vec<&&Raft> = rafts.iter().filter(|r| r.role() == Role::Leader).collect();
        let [leader] = leaders.as_slice() else {
            return Err((
                ViolationKind::Liveness,
                format!("{} leaders after settling", leaders.len()),
            ));
        };
        for raft in &rafts {
            if raft.log() != leader.log() || raft.last_applied() != leader.log().last_index() {
                return Err((
                    ViolationKind::Liveness,
                    format!(
                        "{} did not converge: last index {}, applied {}, leader {} has {}",
                        raft.id(),
                        raft.log().last_index(),
                        raft.last_applied(),
                        leader.id(),
                        leader.log().last_index()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn trace_mentions_leader(&self, id: NodeId, term: Term) -> bool {
        let line = format!("{id} is leader in {term}");
        self.trace.iter().rev().take(10).any(|l| l.ends_with(&line))
    }

    fn event(&mut self, text: &str) {
        if self.trace.len() == TRACE_LEN {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("tick {:>5}: {text}", self.now));
    }

    fn violation(&self, (kind, details): Failure) -> Violation {
        Violation {
            seed: self.seed,
            tick: self.now,
            kind,
            details,
            trace: self.trace.iter().cloned().collect(),
        }
    }
}

/// Folds values into an FNV-1a hash.
fn fnv(mut hash: u64, values: &[u64]) -> u64 {
    for value in values {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01B3);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FaultConfig;

    #[test]
    fn storage_replaces_a_suffix() {
        let entry = |index: u64, term: u64| Entry {
            term: Term(term),
            index: LogIndex(index),
            payload: raft_core::EntryPayload::Noop,
        };
        let mut storage = Storage::default();
        storage.persist(&Ready {
            entries: vec![entry(1, 1), entry(2, 1), entry(3, 1)],
            ..Ready::default()
        });
        storage.persist(&Ready {
            entries: vec![entry(2, 2)],
            ..Ready::default()
        });
        assert_eq!(storage.entries, vec![entry(1, 1), entry(2, 2)]);
    }

    #[test]
    fn fault_free_run_elects_and_commits() {
        let config = SimConfig {
            ticks: 500,
            faults: FaultConfig::none(),
            ..SimConfig::default()
        };
        let report = Simulation::new(1, config).run().expect("no violation");
        assert_eq!(report.stats.leaders_elected, 1);
        assert!(report.stats.committed_index > 50, "{report:?}");
        assert_eq!(report.stats.messages_dropped, 0);
    }
}
