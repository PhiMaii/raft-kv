//! What a simulation run looks like: cluster size, duration and faults.

use crate::rng::Chance;

/// Settings for one simulation run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimConfig {
    /// Number of nodes in the cluster.
    pub nodes: u64,
    /// How many ticks to run with faults enabled.
    pub ticks: u64,
    /// How many fault-free ticks to run at the end before checking that the
    /// cluster has recovered.
    pub settle_ticks: u64,
    /// How often, in ticks, to check log matching across all nodes. The
    /// other invariants are checked on every tick.
    pub log_check_interval: u64,
    /// The `max_entries_per_append` every node uses. `None` picks a value per
    /// seed, from 1 to 1024, so that runs also cover followers that catch up
    /// one entry at a time.
    pub max_entries_per_append: Option<usize>,
    /// Number of concurrent clients. Each has its own session and at most
    /// one request in flight.
    pub clients: usize,
    /// Number of distinct keys the clients use. Few keys mean many
    /// concurrent operations on each, which is where linearizability bugs
    /// show.
    pub keys: u64,
    /// Chance per tick that an idle client starts a new operation.
    pub request: Chance,
    /// Chance that a client forgets which node is leader before starting an
    /// operation, and sends it to a random node.
    pub forget_leader: Chance,
    /// Chance that a read goes through the log within the client's session
    /// instead of using ReadIndex, so that both paths are checked.
    pub log_reads: Chance,
    /// How long a client waits for an answer before sending the request
    /// again, in ticks.
    pub client_timeout: u64,
    /// How long a client keeps retrying an operation before it gives up, in
    /// ticks. The operation's outcome is then unknown: it may still take
    /// effect later, which the linearizability check must allow for.
    pub client_deadline: u64,
    /// Faults to inject.
    pub faults: FaultConfig,
}

/// Which faults to inject, and how often.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultConfig {
    /// Chance that a message is lost.
    pub drop: Chance,
    /// Chance that a message is delivered twice.
    pub duplicate: Chance,
    /// Minimum message delay in ticks. Must be at least 1.
    pub min_delay: u64,
    /// Maximum message delay in ticks. Different delays reorder messages.
    pub max_delay: u64,
    /// Chance that a message is held up far longer than usual, as in a
    /// latency spike. Such messages arrive long after newer ones and test
    /// how nodes handle badly outdated requests.
    pub delay_spike: Chance,
    /// How long a delayed message is held up, in ticks: `(min, max)`.
    pub spike_delay: (u64, u64),
    /// Chance per tick that a new network partition starts.
    pub partition: Chance,
    /// Chance per tick that all partitions heal.
    pub heal: Chance,
    /// Chance per tick that the current leader is cut off from every other
    /// node for a long time (`isolation_length`), unaffected by `heal`. The
    /// rest of the cluster elects a new leader and carries on, while the old
    /// one still believes it leads. Random partitions heal too quickly to
    /// produce this; it is what a leader that serves reads without
    /// confirming its leadership gets wrong.
    pub leader_isolation: Chance,
    /// How long a leader isolation lasts, in ticks: `(min, max)`.
    pub isolation_length: (u64, u64),
    /// Chance per tick, per running node, that the node crashes.
    pub crash: Chance,
    /// Chance per tick that the current leader crashes. Crashing leaders
    /// shortly after they are elected is what produces the interleavings
    /// behind Raft's subtler bugs (such as Figure 8 in the paper), which
    /// uniformly random crashes almost never hit.
    pub leader_crash: Chance,
    /// Chance that a node crashes while persisting a `Ready`, at a random
    /// point of the write sequence. Whatever was not yet synced is lost.
    pub write_crash: Chance,
    /// How long a crashed node stays down, in ticks: `(min, max)`.
    pub downtime: (u64, u64),
    /// Chance per tick, per running node, that the node pauses, as if its
    /// process were stopped or stuck in a long garbage-collection pause.
    pub pause: Chance,
    /// How long a pause lasts, in ticks: `(min, max)`.
    pub pause_length: (u64, u64),
}

impl FaultConfig {
    /// No faults at all, and messages arrive after exactly one tick.
    #[must_use]
    pub fn none() -> Self {
        Self {
            drop: Chance::NEVER,
            duplicate: Chance::NEVER,
            min_delay: 1,
            max_delay: 1,
            delay_spike: Chance::NEVER,
            spike_delay: (1, 1),
            partition: Chance::NEVER,
            heal: Chance::NEVER,
            leader_isolation: Chance::NEVER,
            isolation_length: (1, 1),
            crash: Chance::NEVER,
            leader_crash: Chance::NEVER,
            write_crash: Chance::NEVER,
            downtime: (1, 1),
            pause: Chance::NEVER,
            pause_length: (1, 1),
        }
    }
}

impl Default for FaultConfig {
    /// A hostile but not hopeless environment: with 10 ms ticks, a few
    /// partitions and crashes per simulated second across the cluster.
    fn default() -> Self {
        Self {
            drop: Chance::percent(5),
            duplicate: Chance::percent(2),
            min_delay: 1,
            max_delay: 4,
            delay_spike: Chance::percent(1),
            spike_delay: (10, 80),
            partition: Chance::percent(1),
            heal: Chance::percent(4),
            leader_isolation: Chance::per_million(2_000),
            isolation_length: (100, 300),
            crash: Chance::per_million(1_000),
            leader_crash: Chance::percent(2),
            write_crash: Chance::per_million(2_000),
            downtime: (10, 150),
            pause: Chance::per_million(1_000),
            pause_length: (5, 60),
        }
    }
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 5,
            ticks: 3_000,
            settle_ticks: 500,
            log_check_interval: 10,
            max_entries_per_append: None,
            clients: 5,
            keys: 3,
            request: Chance::percent(30),
            forget_leader: Chance::percent(10),
            log_reads: Chance::percent(25),
            client_timeout: 40,
            client_deadline: 400,
            faults: FaultConfig::default(),
        }
    }
}
