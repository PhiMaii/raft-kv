//! The client API, carried out by sending requests to the node's loop.

use std::time::Duration;

use kv::{ClientId, Command, Operation, Response};
use tokio::sync::{mpsc, oneshot};
use transport::{ApiError, KvHandler, NodeStatus, WriteId};

use crate::config::ReadMode;
use crate::node::Request;

/// Hands client requests to the node and waits for the answers.
#[derive(Clone, Debug)]
pub(crate) struct Handler {
    requests: mpsc::Sender<Request>,
    timeout: Duration,
    read_mode: ReadMode,
}

impl Handler {
    pub(crate) fn new(
        requests: mpsc::Sender<Request>,
        timeout: Duration,
        read_mode: ReadMode,
    ) -> Self {
        Self {
            requests,
            timeout,
            read_mode,
        }
    }

    /// Sends a request built around a reply channel and waits for the
    /// answer, up to the request timeout.
    async fn ask<T>(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<T, ApiError>>) -> Request,
    ) -> Result<T, ApiError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(request(reply))
            .await
            .map_err(|_| stopped())?;
        match tokio::time::timeout(self.timeout, answer).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) => Err(stopped()),
            Err(_) => Err(ApiError::Unavailable(
                "the request did not complete in time".to_owned(),
            )),
        }
    }

    /// Replicates `command` and returns the state machine's response.
    async fn propose(&self, command: Command) -> Result<Response, ApiError> {
        self.ask(|reply| Request::Propose { command, reply }).await
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

    /// Reads with ReadIndex, or through the log if so configured. A read
    /// through the log is answered when its entry is applied, which is
    /// linearizable because the leader only answers for entries committed
    /// in its own term.
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ApiError> {
        match self.read_mode {
            ReadMode::ReadIndex => self.ask(|reply| Request::Read { key, reply }).await,
            ReadMode::Log => match self.propose(Command::Read { key }).await? {
                Response::Value(value) => Ok(value),
                Response::Rejected(rejection) => Err(ApiError::from_rejection(rejection)),
                other => Err(unexpected(&other)),
            },
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
