//! gRPC services for Raft RPCs and the client API.
//!
//! Two services run on every node, both defined in `/proto`:
//!
//! - **`Raft`** (`raft.proto`): messages between nodes. Raft messages are
//!   one-way, so each node keeps one long-lived stream to every peer and
//!   sends its messages down it in order (see ADR 0005). [`RaftService`]
//!   receives them; a [`Peer`] sends them.
//! - **`KeyValue`** (`kv.proto`): the client API. [`KvService`] translates
//!   requests into calls on a [`KvHandler`], which the server implements;
//!   [`KvClient`] is the matching typed client.
//!
//! This crate only moves messages and translates types. It knows nothing
//! about leaders, retries or replication.
//!
//! The Protocol Buffers compiler is vendored, so building needs no system
//! installation of `protoc`.

mod api;
mod convert;
mod peer;

pub mod proto {
    //! Wire types and service stubs generated from the `.proto` files.
    #![allow(missing_docs, unreachable_pub, clippy::pedantic)]

    pub mod raft {
        //! `raftkv.raft.v1`: messages between nodes.
        tonic::include_proto!("raftkv.raft.v1");
    }

    pub mod kv {
        //! `raftkv.kv.v1`: the client API.
        tonic::include_proto!("raftkv.kv.v1");
    }
}

pub use api::{
    ApiError, ClientError, KvClient, KvHandler, KvService, LeaderHint, Member, NodeStatus, Swapped,
    WriteId,
};
pub use peer::{InvalidAddress, Peer, PeerConfig, RaftService};
/// gRPC status codes, as found in [`ClientError::Rpc`].
pub use tonic::{Code, Status};

/// The largest gRPC message accepted, in bytes. Large enough for a batch of
/// log entries carrying maximum-size values.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
