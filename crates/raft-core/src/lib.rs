//! Deterministic, I/O-free implementation of the Raft consensus algorithm.
//!
//! This crate contains only the protocol logic. It never touches the network,
//! the disk or the clock, and its only source of randomness is a seeded
//! generator. The caller feeds it inputs and carries out its outputs; see
//! the `Raft` type for how. This is what allows whole clusters to be tested in a
//! deterministic simulator (see ADR 0002 in the documentation).
//!
//! Implemented so far: the protocol's core types, the log and messages.

mod config;
mod log;
mod message;
mod types;

pub use config::{Config, ConfigError};
pub use log::{Entry, EntryPayload, Log, LogError};
pub use message::{Message, MessageBody};
pub use types::{LogIndex, NodeId, Term};
