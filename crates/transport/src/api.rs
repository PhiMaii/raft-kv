//! The client-facing key-value API: the gRPC service and a typed client.
//!
//! The service translates between the wire format and the types of the `kv`
//! crate and leaves the actual work to a [`KvHandler`], which the server
//! implements. The client does the reverse translation. Neither does
//! anything clever: retries and leader discovery belong to the client
//! library, and replication to the server.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use kv::{ClientId, Operation, Rejection, Response};
use raft_core::NodeId;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::proto::kv as wire;
use crate::proto::kv::key_value_client::KeyValueClient;
use crate::proto::kv::key_value_server::{KeyValue, KeyValueServer};
use crate::MAX_MESSAGE_BYTES;

/// Where a client should send its requests instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderHint {
    /// The node believed to be leader.
    pub id: NodeId,
    /// That node's client API address.
    pub address: String,
}

/// Why the server did not carry out a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiError {
    /// The node is not the leader. Retry at the hinted node, or at another
    /// node if there is no hint.
    NotLeader(Option<LeaderHint>),
    /// The session does not exist or expired. Register again.
    SessionExpired,
    /// The request is older than the session's latest request.
    StaleSequence,
    /// The key exceeds the configured size limit.
    KeyTooLarge,
    /// The value exceeds the configured size limit.
    ValueTooLarge,
    /// The cluster could not answer in time, for example because it has no
    /// leader or no majority. A write may or may not have been applied;
    /// retry it with the same sequence number.
    Unavailable(String),
    /// A bug or a corrupt log entry on the server.
    Internal(String),
}

impl ApiError {
    /// The error for a request the state machine rejected.
    pub fn from_rejection(rejection: Rejection) -> Self {
        match rejection {
            Rejection::SessionExpired => Self::SessionExpired,
            Rejection::StaleSequence => Self::StaleSequence,
            Rejection::KeyTooLarge => Self::KeyTooLarge,
            Rejection::ValueTooLarge => Self::ValueTooLarge,
            Rejection::Malformed(details) => Self::Internal(details),
        }
    }

    fn to_wire(&self) -> wire::Error {
        let (code, message) = match self {
            Self::NotLeader(_) => (wire::ErrorCode::NotLeader, String::new()),
            Self::SessionExpired => (wire::ErrorCode::SessionExpired, String::new()),
            Self::StaleSequence => (wire::ErrorCode::StaleSequence, String::new()),
            Self::KeyTooLarge => (wire::ErrorCode::KeyTooLarge, String::new()),
            Self::ValueTooLarge => (wire::ErrorCode::ValueTooLarge, String::new()),
            Self::Unavailable(details) => (wire::ErrorCode::Unavailable, details.clone()),
            Self::Internal(details) => (wire::ErrorCode::Internal, details.clone()),
        };
        let leader = match self {
            Self::NotLeader(Some(hint)) => Some(wire::LeaderHint {
                id: hint.id.0,
                address: hint.address.clone(),
            }),
            _ => None,
        };
        wire::Error {
            code: code.into(),
            message,
            leader,
        }
    }

    fn from_wire(error: wire::Error) -> Self {
        match wire::ErrorCode::try_from(error.code) {
            Ok(wire::ErrorCode::NotLeader) => {
                Self::NotLeader(error.leader.map(|hint| LeaderHint {
                    id: NodeId(hint.id),
                    address: hint.address,
                }))
            }
            Ok(wire::ErrorCode::SessionExpired) => Self::SessionExpired,
            Ok(wire::ErrorCode::StaleSequence) => Self::StaleSequence,
            Ok(wire::ErrorCode::KeyTooLarge) => Self::KeyTooLarge,
            Ok(wire::ErrorCode::ValueTooLarge) => Self::ValueTooLarge,
            Ok(wire::ErrorCode::Unavailable) => Self::Unavailable(error.message),
            Ok(wire::ErrorCode::Internal) => Self::Internal(error.message),
            Ok(wire::ErrorCode::Unspecified) | Err(_) => Self::Internal(format!(
                "unknown error code {}: {}",
                error.code, error.message
            )),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader(Some(hint)) => {
                write!(f, "not the leader; try {} at {}", hint.id, hint.address)
            }
            Self::NotLeader(None) => f.write_str("not the leader; leader unknown"),
            Self::SessionExpired => f.write_str("session expired"),
            Self::StaleSequence => f.write_str("stale sequence number"),
            Self::KeyTooLarge => f.write_str("key too large"),
            Self::ValueTooLarge => f.write_str("value too large"),
            Self::Unavailable(details) => write!(f, "unavailable: {details}"),
            Self::Internal(details) => write!(f, "internal error: {details}"),
        }
    }
}

impl std::error::Error for ApiError {}

/// Identifies a write: the client's session and the request's sequence
/// number. A retry reuses the id of the request it repeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteId {
    /// The client's session.
    pub client_id: ClientId,
    /// The request's sequence number within the session, starting at 1.
    pub sequence: u64,
}

/// What the server does with requests. Implemented by the server, which
/// runs them through Raft.
pub trait KvHandler: Send + Sync + 'static {
    /// Opens a client session.
    fn register_client(&self) -> impl Future<Output = Result<ClientId, ApiError>> + Send;

    /// Reads a key linearizably.
    fn get(&self, key: Vec<u8>) -> impl Future<Output = Result<Option<Vec<u8>>, ApiError>> + Send;

    /// Carries out a put, delete or compare-and-swap and returns the state
    /// machine's response. Never called with [`Operation::Get`].
    fn write(
        &self,
        id: WriteId,
        operation: Operation,
    ) -> impl Future<Output = Result<Response, ApiError>> + Send;
}

/// The gRPC key-value service, backed by a [`KvHandler`].
#[derive(Debug)]
pub struct KvService<H> {
    handler: Arc<H>,
}

impl<H: KvHandler> KvService<H> {
    /// Creates the service.
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }

    /// Wraps the service for a [`tonic::transport::Server`].
    pub fn into_server(self) -> KeyValueServer<Self> {
        KeyValueServer::new(self).max_decoding_message_size(MAX_MESSAGE_BYTES)
    }

    async fn write(
        &self,
        header: Option<wire::RequestHeader>,
        operation: Operation,
    ) -> Result<Result<Response, ApiError>, Status> {
        let header = header.ok_or_else(|| Status::invalid_argument("missing request header"))?;
        let id = WriteId {
            client_id: ClientId(header.client_id),
            sequence: header.sequence,
        };
        Ok(match self.handler.write(id, operation).await {
            Ok(Response::Rejected(rejection)) => Err(ApiError::from_rejection(rejection)),
            other => other,
        })
    }
}

fn unexpected(response: &Response) -> ApiError {
    ApiError::Internal(format!(
        "unexpected response from the state machine: {response:?}"
    ))
}

#[tonic::async_trait]
impl<H: KvHandler> KeyValue for KvService<H> {
    async fn register_client(
        &self,
        _request: Request<wire::RegisterClientRequest>,
    ) -> Result<tonic::Response<wire::RegisterClientResponse>, Status> {
        use wire::register_client_response::Outcome;
        let outcome = match self.handler.register_client().await {
            Ok(client_id) => Outcome::ClientId(client_id.0),
            Err(error) => Outcome::Error(error.to_wire()),
        };
        Ok(tonic::Response::new(wire::RegisterClientResponse {
            outcome: Some(outcome),
        }))
    }

    async fn get(
        &self,
        request: Request<wire::GetRequest>,
    ) -> Result<tonic::Response<wire::GetResponse>, Status> {
        use wire::get_response::Outcome;
        let outcome = match self.handler.get(request.into_inner().key).await {
            Ok(value) => Outcome::Value(wire::Value { value }),
            Err(error) => Outcome::Error(error.to_wire()),
        };
        Ok(tonic::Response::new(wire::GetResponse {
            outcome: Some(outcome),
        }))
    }

    async fn put(
        &self,
        request: Request<wire::PutRequest>,
    ) -> Result<tonic::Response<wire::PutResponse>, Status> {
        use wire::put_response::Outcome;
        let request = request.into_inner();
        let operation = Operation::Put {
            key: request.key,
            value: request.value,
        };
        let outcome = match self.write(request.header, operation).await? {
            Ok(Response::Written) => Outcome::Written(wire::Written {}),
            Ok(other) => Outcome::Error(unexpected(&other).to_wire()),
            Err(error) => Outcome::Error(error.to_wire()),
        };
        Ok(tonic::Response::new(wire::PutResponse {
            outcome: Some(outcome),
        }))
    }

    async fn delete(
        &self,
        request: Request<wire::DeleteRequest>,
    ) -> Result<tonic::Response<wire::DeleteResponse>, Status> {
        use wire::delete_response::Outcome;
        let request = request.into_inner();
        let operation = Operation::Delete { key: request.key };
        let outcome = match self.write(request.header, operation).await? {
            Ok(Response::Deleted { existed }) => Outcome::Deleted(wire::Deleted { existed }),
            Ok(other) => Outcome::Error(unexpected(&other).to_wire()),
            Err(error) => Outcome::Error(error.to_wire()),
        };
        Ok(tonic::Response::new(wire::DeleteResponse {
            outcome: Some(outcome),
        }))
    }

    async fn compare_and_swap(
        &self,
        request: Request<wire::CompareAndSwapRequest>,
    ) -> Result<tonic::Response<wire::CompareAndSwapResponse>, Status> {
        use wire::compare_and_swap_response::Outcome;
        let request = request.into_inner();
        let operation = Operation::CompareAndSwap {
            key: request.key,
            expected: request.expected,
            new: request.new,
        };
        let outcome = match self.write(request.header, operation).await? {
            Ok(Response::Swapped {
                succeeded,
                previous,
            }) => Outcome::Swapped(wire::Swapped {
                succeeded,
                previous,
            }),
            Ok(other) => Outcome::Error(unexpected(&other).to_wire()),
            Err(error) => Outcome::Error(error.to_wire()),
        };
        Ok(tonic::Response::new(wire::CompareAndSwapResponse {
            outcome: Some(outcome),
        }))
    }
}

/// Why a client call failed.
#[derive(Clone, Debug)]
pub enum ClientError {
    /// The server answered with an error.
    Api(ApiError),
    /// The call itself failed: the connection broke, the server is down, or
    /// it could not interpret the request. A write may or may not have been
    /// applied.
    Rpc(Status),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => error.fmt(f),
            Self::Rpc(status) => write!(f, "{}: {}", status.code(), status.message()),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<ApiError> for ClientError {
    fn from(error: ApiError) -> Self {
        Self::Api(error)
    }
}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        Self::Rpc(status)
    }
}

/// The outcome of a compare-and-swap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Swapped {
    /// Whether the comparison matched and the value was replaced.
    pub succeeded: bool,
    /// The key's value before the operation.
    pub previous: Option<Vec<u8>>,
}

/// A connection to one node's key-value API.
///
/// This is a thin, typed wrapper around the gRPC client: it talks to one
/// node and returns its answer, including `NotLeader`. Cheap to clone;
/// clones share the connection.
#[derive(Clone, Debug)]
pub struct KvClient {
    inner: KeyValueClient<Channel>,
}

/// A response that has neither a result nor an error.
fn missing_outcome() -> ClientError {
    ClientError::Api(ApiError::Internal("response without outcome".to_owned()))
}

impl KvClient {
    /// Connects to the node at `address`, for example
    /// `"http://10.0.0.2:7001"`.
    pub async fn connect(address: &str) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(address.to_owned())?
            .tcp_nodelay(true)
            .connect()
            .await?;
        Ok(Self {
            inner: KeyValueClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        })
    }

    /// Opens a client session.
    pub async fn register_client(&mut self) -> Result<ClientId, ClientError> {
        use wire::register_client_response::Outcome;
        let response = self
            .inner
            .register_client(wire::RegisterClientRequest {})
            .await?
            .into_inner();
        match response.outcome.ok_or_else(missing_outcome)? {
            Outcome::ClientId(id) => Ok(ClientId(id)),
            Outcome::Error(error) => Err(ApiError::from_wire(error).into()),
        }
    }

    /// Reads a key; `None` if it does not exist.
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ClientError> {
        use wire::get_response::Outcome;
        let response = self.inner.get(wire::GetRequest { key }).await?.into_inner();
        match response.outcome.ok_or_else(missing_outcome)? {
            Outcome::Value(value) => Ok(value.value),
            Outcome::Error(error) => Err(ApiError::from_wire(error).into()),
        }
    }

    /// Sets a key to a value.
    pub async fn put(
        &mut self,
        id: WriteId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ClientError> {
        use wire::put_response::Outcome;
        let response = self
            .inner
            .put(wire::PutRequest {
                header: Some(header(id)),
                key,
                value,
            })
            .await?
            .into_inner();
        match response.outcome.ok_or_else(missing_outcome)? {
            Outcome::Written(wire::Written {}) => Ok(()),
            Outcome::Error(error) => Err(ApiError::from_wire(error).into()),
        }
    }

    /// Removes a key. Returns whether it existed.
    pub async fn delete(&mut self, id: WriteId, key: Vec<u8>) -> Result<bool, ClientError> {
        use wire::delete_response::Outcome;
        let response = self
            .inner
            .delete(wire::DeleteRequest {
                header: Some(header(id)),
                key,
            })
            .await?
            .into_inner();
        match response.outcome.ok_or_else(missing_outcome)? {
            Outcome::Deleted(deleted) => Ok(deleted.existed),
            Outcome::Error(error) => Err(ApiError::from_wire(error).into()),
        }
    }

    /// Sets `key` to `new` if its current value is `expected`, where `None`
    /// means the key must not exist.
    pub async fn compare_and_swap(
        &mut self,
        id: WriteId,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<Swapped, ClientError> {
        use wire::compare_and_swap_response::Outcome;
        let response = self
            .inner
            .compare_and_swap(wire::CompareAndSwapRequest {
                header: Some(header(id)),
                key,
                expected,
                new,
            })
            .await?
            .into_inner();
        match response.outcome.ok_or_else(missing_outcome)? {
            Outcome::Swapped(swapped) => Ok(Swapped {
                succeeded: swapped.succeeded,
                previous: swapped.previous,
            }),
            Outcome::Error(error) => Err(ApiError::from_wire(error).into()),
        }
    }
}

fn header(id: WriteId) -> wire::RequestHeader {
    wire::RequestHeader {
        client_id: id.client_id.0,
        sequence: id.sequence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_error_round_trips() {
        let errors = [
            ApiError::NotLeader(None),
            ApiError::NotLeader(Some(LeaderHint {
                id: NodeId(3),
                address: "http://127.0.0.1:7003".to_owned(),
            })),
            ApiError::SessionExpired,
            ApiError::StaleSequence,
            ApiError::KeyTooLarge,
            ApiError::ValueTooLarge,
            ApiError::Unavailable("no leader".to_owned()),
            ApiError::Internal("bug".to_owned()),
        ];
        for error in errors {
            assert_eq!(ApiError::from_wire(error.to_wire()), error);
        }
    }

    #[test]
    fn unknown_error_codes_become_internal_errors() {
        let error = wire::Error {
            code: 99,
            message: "from the future".to_owned(),
            leader: None,
        };
        assert_eq!(
            ApiError::from_wire(error),
            ApiError::Internal("unknown error code 99: from the future".to_owned())
        );
    }
}
