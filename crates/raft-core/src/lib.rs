//! Deterministic, I/O-free implementation of the Raft consensus algorithm.
//!
//! This crate contains only the protocol logic. It never touches the network,
//! the disk or the clock, and its only source of randomness is a seeded
//! generator. The caller feeds it inputs and carries out its outputs; see
//! [`Raft`] for how. This is what allows whole clusters to be tested in a
//! deterministic simulator (see ADR 0002 in the documentation).
//!
//! Implemented so far: leader election (Raft §5.2, §5.4.1) and log
//! replication with commit and apply tracking (§5.3, §5.4.2).

mod config;
mod log;
mod message;
mod raft;
mod rng;
mod types;

pub use config::{Config, ConfigError};
pub use log::{Entry, EntryPayload, Log, LogError};
pub use message::{Message, MessageBody};
pub use raft::{HardState, ProposeError, Raft, Ready, Role};
pub use types::{LogIndex, NodeId, Term};
