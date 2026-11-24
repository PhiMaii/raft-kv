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

        let mut c = base;
        c.heartbeat_interval = c.election_timeout_min;
        assert_eq!(
            c.validate(),
            Err(ConfigError::HeartbeatNotShorterThanElectionTimeout)
        );
    }
}
