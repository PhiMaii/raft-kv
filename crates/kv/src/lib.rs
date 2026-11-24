//! The key-value state machine replicated by Raft.
//!
//! Clients' commands travel through the Raft log as encoded [`Envelope`]s.

mod command;

pub use command::{ClientId, Command, DecodeError, Envelope, Operation};
