//! The client API, carried out by sending requests to the node's loop.

use std::time::Duration;

use kv::{ClientId, Command, Operation, Response};
use tokio::sync::{mpsc, oneshot};
use transport::{ApiError, KvHandler, NodeStatus, WriteId};

use crate::node::Request;

/// Hands client requests to the node and waits for the answers.
#[derive(Clone, Debug)]
pub(crate) struct Handler {
    requests: mpsc::Sender<Request>,
    timeout: Duration,
}

impl Handler {
    pub(crate) fn new(requests: mpsc::Sender<Request>, timeout: Duration) -> Self {
        Self { requests, timeout }
    }

    /// Replicates `command` and returns the state machine's response.
    async fn propose(&self, command: Command) -> Result<Response, ApiError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Propose { command, reply })
            .await
            .map_err(|_| stopped())?;
        match tokio::time::timeout(self.timeout, answer).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) => Err(stopped()),
            Err(_) => Err(ApiError::Unavailable(
                "the request did not commit in time".to_owned(),
            )),
        }
    }
}

fn stopped() -> ApiError {
    ApiError::Unavailable("the node is shutting down".to_owned())
}

fn unexpected(response: &Response) -> ApiError {
    ApiError::Internal(format!(
        "unexpected response from the state machine: {response:?}"
    ))
}

impl KvHandler for Handler {
    async fn register_client(&self) -> Result<ClientId, ApiError> {
        match self.propose(Command::RegisterClient).await? {
            Response::Registered { client_id } => Ok(client_id),
            Response::Rejected(rejection) => Err(ApiError::from_rejection(rejection)),
            other => Err(unexpected(&other)),
        }
    }

    /// Reads through the log until `ReadIndex` is implemented: the read is
    /// answered when its entry is applied, which is linearizable because
    /// the leader only answers for entries committed in its own term.
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ApiError> {
        match self.propose(Command::Read { key }).await? {
            Response::Value(value) => Ok(value),
            Response::Rejected(rejection) => Err(ApiError::from_rejection(rejection)),
            other => Err(unexpected(&other)),
        }
    }

    async fn write(&self, id: WriteId, operation: Operation) -> Result<Response, ApiError> {
        self.propose(Command::Request {
            client_id: id.client_id,
            sequence: id.sequence,
            operation,
        })
        .await
    }

    async fn status(&self) -> Result<NodeStatus, ApiError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Status { reply })
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())
    }
}
