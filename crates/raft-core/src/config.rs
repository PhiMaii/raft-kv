//! Static configuration of a Raft node.

use std::collections::BTreeSet;
use std::fmt;

use crate::types::NodeId;

/// Configuration of a single Raft node.
///
/// All durations are measured in ticks. The caller decides how long a tick
/// is (for example 10 ms) and calls [`Raft::tick`](crate::Raft::tick) at that
/// rate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// This node's identifier.
    pub id: NodeId,
    /// The other members of the cluster, not including `id`.
    pub peers: Vec<NodeId>,
    /// Lower bound of the randomized election timeout, inclusive.
    pub election_timeout_min: u64,
    /// Upper bound of the randomized election timeout, exclusive.
    pub election_timeout_max: u64,
    /// How often a leader sends heartbeats.
    pub heartbeat_interval: u64,
    /// The most entries a single `AppendEntries` message may carry, so a
    /// follower that is far behind catches up in bounded steps.
    pub max_entries_per_append: usize,
    /// The most snapshot bytes a single `InstallSnapshot` message may carry.
    pub snapshot_chunk_size: usize,
    /// How many entries before a snapshot to keep in the log when it is
    /// compacted, so that followers that are only slightly behind catch up
    /// from the log instead of receiving the whole snapshot.
    pub retained_entries: u64,
    /// Seed for the node's random number generator, which only picks
    /// election timeouts. The same seed always gives the same behaviour.
    pub seed: u64,
}

impl Config {
    /// A configuration with the default timings: with a 10 ms tick, election
    /// timeouts of 150–300 ms and a heartbeat every 50 ms.
    #[must_use]
    pub fn new(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Self {
        Self {
            id,
            peers,
            election_timeout_min: 15,
            election_timeout_max: 30,
            heartbeat_interval: 5,
            max_entries_per_append: 1024,
            snapshot_chunk_size: 1 << 20,
            retained_entries: 1024,
            seed,
        }
    }

    /// Checks that the configuration is usable.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.peers.contains(&self.id) {
            return Err(ConfigError::SelfInPeers);
        }
        let unique: BTreeSet<_> = self.peers.iter().collect();
        if unique.len() != self.peers.len() {
            return Err(ConfigError::DuplicatePeer);
        }
        if self.heartbeat_interval == 0 {
            return Err(ConfigError::ZeroHeartbeatInterval);
        }
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ConfigError::EmptyElectionTimeoutRange);
        }
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err(ConfigError::HeartbeatNotShorterThanElectionTimeout);
        }
        if self.max_entries_per_append == 0 {
            return Err(ConfigError::ZeroMaxEntriesPerAppend);
        }
        if self.snapshot_chunk_size == 0 {
            return Err(ConfigError::ZeroSnapshotChunkSize);
        }
        Ok(())
    }
}

/// Why a [`Config`] was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// `peers` contains the node's own id.
    SelfInPeers,
    /// `peers` lists a node more than once.
    DuplicatePeer,
    /// `heartbeat_interval` is zero.
    ZeroHeartbeatInterval,
    /// `election_timeout_min` is not lower than `election_timeout_max`.
    EmptyElectionTimeoutRange,
    /// Heartbeats would not arrive before followers start elections.
    HeartbeatNotShorterThanElectionTimeout,
    /// `max_entries_per_append` is zero.
    ZeroMaxEntriesPerAppend,
    /// `snapshot_chunk_size` is zero.
    ZeroSnapshotChunkSize,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::SelfInPeers => "peers must not contain the node's own id",
            Self::DuplicatePeer => "peers must not contain duplicates",
            Self::ZeroHeartbeatInterval => "heartbeat interval must be at least one tick",
            Self::EmptyElectionTimeoutRange => {
                "election timeout min must be lower than election timeout max"
            }
            Self::HeartbeatNotShorterThanElectionTimeout => {
                "heartbeat interval must be shorter than the minimum election timeout"
            }
            Self::ZeroMaxEntriesPerAppend => "max entries per append must be at least one",
            Self::ZeroSnapshotChunkSize => "snapshot chunk size must be at least one byte",
        };
        f.write_str(message)
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = Config::new(NodeId(1), vec![NodeId(2), NodeId(3)], 0);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn invalid_configs_are_rejected() {
        let base = Config::new(NodeId(1), vec![NodeId(2), NodeId(3)], 0);

        let mut c = base.clone();
        c.peers.push(NodeId(1));
        assert_eq!(c.validate(), Err(ConfigError::SelfInPeers));

        let mut c = base.clone();
        c.peers.push(NodeId(2));
        assert_eq!(c.validate(), Err(ConfigError::DuplicatePeer));

        let mut c = base.clone();
        c.heartbeat_interval = 0;
        assert_eq!(c.validate(), Err(ConfigError::ZeroHeartbeatInterval));

        let mut c = base.clone();
        c.election_timeout_max = c.election_timeout_min;
        assert_eq!(c.validate(), Err(ConfigError::EmptyElectionTimeoutRange));

        let mut c = base.clone();
        c.heartbeat_interval = c.election_timeout_min;
        assert_eq!(
            c.validate(),
            Err(ConfigError::HeartbeatNotShorterThanElectionTimeout)
        );

        let mut c = base.clone();
        c.max_entries_per_append = 0;
        assert_eq!(c.validate(), Err(ConfigError::ZeroMaxEntriesPerAppend));

        let mut c = base;
        c.snapshot_chunk_size = 0;
        assert_eq!(c.validate(), Err(ConfigError::ZeroSnapshotChunkSize));
    }
}
