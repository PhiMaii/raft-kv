//! The key-value state machine replicated by Raft.
//!
//! Clients' commands travel through the Raft log as encoded [`Envelope`]s,
//! and every node applies them to its own [`KvStore`] in log order. Because
//! applying is deterministic, all nodes end up with the same data and give
//! the same responses.
//!
//! Supported operations are put, delete, compare-and-swap and reads through
//! the log. Clients normally read with `ReadIndex` instead, which bypasses
//! the log; log reads remain for comparison in benchmarks.
//! Client sessions deduplicate retried requests, so a write is never applied
//! twice.

mod command;
mod store;

pub use command::{ClientId, Command, DecodeError, Envelope, Operation};
pub use store::{KvConfig, KvStore, Rejection, Response};
