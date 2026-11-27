//! The node's configuration file.

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// Configuration of one node, usually read from a TOML file:
///
/// ```toml
/// id = 1
/// listen = "127.0.0.1:7001"
/// data_dir = "data/node1"
///
/// [[peers]]
/// id = 2
/// address = "127.0.0.1:7002"
///
/// [[peers]]
/// id = 3
/// address = "127.0.0.1:7003"
/// ```
///
/// One address serves both the Raft service for other nodes and the
/// client API.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// This node's id, unique within the cluster.
    pub id: u64,
    /// The address to listen on.
    pub listen: SocketAddr,
    /// Where the node keeps its log and hard state.
    pub data_dir: PathBuf,
    /// Every other member of the cluster.
    pub peers: Vec<PeerAddress>,
    /// How long one Raft tick lasts, in milliseconds. Raft's timeouts are
    /// counted in ticks: elections time out after 15–30 ticks, and leaders
    /// send heartbeats every 5.
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    /// How long a client request may wait to be committed before the node
    /// answers `Unavailable`, in milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// How reads are made linearizable.
    #[serde(default)]
    pub read_mode: ReadMode,
    /// How many bytes of commands the node applies before it snapshots its
    /// state machine and compacts its log.
    #[serde(default = "default_snapshot_threshold_bytes")]
    pub snapshot_threshold_bytes: u64,
    /// How many log entries before a snapshot to keep, so that followers
    /// that are only slightly behind catch up from the log rather than
    /// receiving the whole snapshot.
    #[serde(default = "default_retained_entries")]
    pub retained_entries: u64,
    /// Whether the leader batches replication: while an `AppendEntries` to
    /// a follower waits for its answer, new entries wait with it. Off, every
    /// write is sent at once, with everything the follower has not
    /// acknowledged yet.
    #[serde(default = "default_true")]
    pub batch_appends: bool,
    /// About how many bytes of entries one `AppendEntries` may carry.
    #[serde(default = "default_max_append_bytes")]
    pub max_append_bytes: usize,
    /// Whether the leader sends its `AppendEntries` before its own fsync
    /// finishes, so that writing to its disk overlaps with the round trip
    /// to its followers (Raft dissertation §10.2.1).
    #[serde(default = "default_true")]
    pub send_before_persist: bool,
}

/// How the leader serves linearizable reads.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Confirm leadership with one round of heartbeats, then read local
    /// state (ReadIndex). No log entry, no fsync.
    #[default]
    ReadIndex,
    /// Append the read to the log and answer once it is applied. Slower;
    /// kept to measure what ReadIndex saves.
    Log,
}

/// Another member of the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerAddress {
    /// The member's id.
    pub id: u64,
    /// The member's address, as `host:port`.
    pub address: String,
}

fn default_tick_ms() -> u64 {
    10
}

fn default_request_timeout_ms() -> u64 {
    5_000
}

fn default_snapshot_threshold_bytes() -> u64 {
    64 << 20
}

fn default_retained_entries() -> u64 {
    1024
}

fn default_max_append_bytes() -> usize {
    1 << 20
}

fn default_true() -> bool {
    true
}

/// Why a configuration could not be used.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read.
    Read(PathBuf, std::io::Error),
    /// The file is not valid TOML for a [`ServerConfig`].
    Parse(PathBuf, toml::de::Error),
    /// The values do not describe a usable node.
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(path, error) => write!(f, "cannot read {}: {error}", path.display()),
            Self::Parse(path, error) => write!(f, "invalid config {}: {error}", path.display()),
            Self::Invalid(reason) => write!(f, "invalid config: {reason}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl ServerConfig {
    /// Reads and checks a TOML configuration file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.to_owned(), e))?;
        let config: Self =
            toml::from_str(&text).map_err(|e| ConfigError::Parse(path.to_owned(), e))?;
        config.validate()?;
        Ok(config)
    }

    /// Checks that the configuration describes a usable node.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |reason: &str| Err(ConfigError::Invalid(reason.to_owned()));
        let ids: BTreeSet<u64> = self.peers.iter().map(|peer| peer.id).collect();
        if ids.len() != self.peers.len() {
            return invalid("two peers have the same id");
        }
        if ids.contains(&self.id) {
            return invalid("the node's own id is listed among its peers");
        }
        if self.tick_ms == 0 {
            return invalid("tick_ms must be positive");
        }
        if self.request_timeout_ms == 0 {
            return invalid("request_timeout_ms must be positive");
        }
        if self.max_append_bytes == 0 {
            return invalid("max_append_bytes must be positive");
        }
        if self.snapshot_threshold_bytes == 0 {
            return invalid("snapshot_threshold_bytes must be positive");
        }
        Ok(())
    }

    /// The tick length.
    pub fn tick(&self) -> Duration {
        Duration::from_millis(self.tick_ms)
    }

    /// The request timeout.
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<ServerConfig, ConfigError> {
        let config: ServerConfig =
            toml::from_str(text).map_err(|e| ConfigError::Parse(PathBuf::new(), e))?;
        config.validate()?;
        Ok(config)
    }

    const VALID: &str = r#"
        id = 1
        listen = "127.0.0.1:7001"
        data_dir = "data/node1"

        [[peers]]
        id = 2
        address = "127.0.0.1:7002"

        [[peers]]
        id = 3
        address = "127.0.0.1:7003"
    "#;

    #[test]
    fn a_valid_file_parses_with_defaults() {
        let config = parse(VALID).expect("valid");
        assert_eq!(config.id, 1);
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.tick(), Duration::from_millis(10));
        assert_eq!(config.request_timeout(), Duration::from_secs(5));
        assert_eq!(config.read_mode, ReadMode::ReadIndex);
        assert_eq!(config.snapshot_threshold_bytes, 64 << 20);
        assert_eq!(config.retained_entries, 1024);
        assert!(config.batch_appends && config.send_before_persist);
        assert_eq!(config.max_append_bytes, 1 << 20);
        let log_reads: ServerConfig =
            toml::from_str(&format!("read_mode = \"log\"\n{VALID}")).expect("valid");
        assert_eq!(log_reads.read_mode, ReadMode::Log);
    }

    #[test]
    fn mistakes_are_reported() {
        let own_id = VALID.replace("id = 2", "id = 1");
        assert!(matches!(parse(&own_id), Err(ConfigError::Invalid(_))));
        let duplicate = VALID.replace("id = 3", "id = 2");
        assert!(matches!(parse(&duplicate), Err(ConfigError::Invalid(_))));
        let typo = format!("tick_msec = 5\n{VALID}");
        assert!(matches!(parse(&typo), Err(ConfigError::Parse(..))));
    }
}
