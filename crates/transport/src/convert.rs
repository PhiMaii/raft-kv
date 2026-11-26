//! Conversion between `raft-core` messages and their wire form.

use std::fmt;

use raft_core::{Entry, EntryPayload, LogIndex, Message, MessageBody, NodeId, Term};

use crate::proto::raft as wire;

/// A message from the network that does not describe a valid Raft message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InvalidMessage(&'static str);

impl fmt::Display for InvalidMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid Raft message: {}", self.0)
    }
}

pub(crate) fn encode_message(message: &Message) -> wire::RaftMessage {
    let body = match &message.body {
        MessageBody::RequestVote {
            last_log_index,
            last_log_term,
        } => wire::raft_message::Body::RequestVote(wire::RequestVote {
            last_log_index: last_log_index.0,
            last_log_term: last_log_term.0,
        }),
        MessageBody::RequestVoteResponse { granted } => {
            wire::raft_message::Body::RequestVoteResponse(wire::RequestVoteResponse {
                granted: *granted,
            })
        }
        MessageBody::AppendEntries {
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            read_round,
        } => wire::raft_message::Body::AppendEntries(wire::AppendEntries {
            prev_log_index: prev_log_index.0,
            prev_log_term: prev_log_term.0,
            entries: entries.iter().map(encode_entry).collect(),
            leader_commit: leader_commit.0,
            read_round: *read_round,
        }),
        MessageBody::AppendEntriesResponse {
            success,
            match_index,
            conflict_index,
            conflict_term,
            read_round,
        } => wire::raft_message::Body::AppendEntriesResponse(wire::AppendEntriesResponse {
            success: *success,
            match_index: match_index.0,
            conflict_index: conflict_index.0,
            conflict_term: conflict_term.map(|term| term.0),
            read_round: *read_round,
        }),
    };
    wire::RaftMessage {
        from: message.from.0,
        to: message.to.0,
        term: message.term.0,
        body: Some(body),
    }
}

pub(crate) fn decode_message(message: wire::RaftMessage) -> Result<Message, InvalidMessage> {
    let body = match message.body.ok_or(InvalidMessage("no body"))? {
        wire::raft_message::Body::RequestVote(vote) => MessageBody::RequestVote {
            last_log_index: LogIndex(vote.last_log_index),
            last_log_term: Term(vote.last_log_term),
        },
        wire::raft_message::Body::RequestVoteResponse(response) => {
            MessageBody::RequestVoteResponse {
                granted: response.granted,
            }
        }
        wire::raft_message::Body::AppendEntries(append) => MessageBody::AppendEntries {
            prev_log_index: LogIndex(append.prev_log_index),
            prev_log_term: Term(append.prev_log_term),
            entries: append
                .entries
                .into_iter()
                .map(decode_entry)
                .collect::<Result<_, _>>()?,
            leader_commit: LogIndex(append.leader_commit),
            read_round: append.read_round,
        },
        wire::raft_message::Body::AppendEntriesResponse(response) => {
            MessageBody::AppendEntriesResponse {
                success: response.success,
                match_index: LogIndex(response.match_index),
                conflict_index: LogIndex(response.conflict_index),
                conflict_term: response.conflict_term.map(Term),
                read_round: response.read_round,
            }
        }
    };
    Ok(Message {
        from: NodeId(message.from),
        to: NodeId(message.to),
        term: Term(message.term),
        body,
    })
}

fn encode_entry(entry: &Entry) -> wire::Entry {
    let payload = match &entry.payload {
        EntryPayload::Noop => wire::entry::Payload::Noop(wire::Noop {}),
        EntryPayload::Command(command) => wire::entry::Payload::Command(command.clone()),
    };
    wire::Entry {
        term: entry.term.0,
        index: entry.index.0,
        payload: Some(payload),
    }
}

fn decode_entry(entry: wire::Entry) -> Result<Entry, InvalidMessage> {
    let payload = match entry
        .payload
        .ok_or(InvalidMessage("entry without payload"))?
    {
        wire::entry::Payload::Noop(wire::Noop {}) => EntryPayload::Noop,
        wire::entry::Payload::Command(command) => EntryPayload::Command(command),
    };
    Ok(Entry {
        term: Term(entry.term),
        index: LogIndex(entry.index),
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(body: MessageBody) -> Message {
        Message {
            from: NodeId(1),
            to: NodeId(2),
            term: Term(7),
            body,
        }
    }

    #[test]
    fn every_message_round_trips() {
        let bodies = [
            MessageBody::RequestVote {
                last_log_index: LogIndex(10),
                last_log_term: Term(6),
            },
            MessageBody::RequestVoteResponse { granted: true },
            MessageBody::AppendEntries {
                prev_log_index: LogIndex(9),
                prev_log_term: Term(5),
                entries: Vec::new(),
                leader_commit: LogIndex(8),
                read_round: 12,
            },
            MessageBody::AppendEntries {
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: vec![
                    Entry {
                        term: Term(7),
                        index: LogIndex(1),
                        payload: EntryPayload::Noop,
                    },
                    Entry {
                        term: Term(7),
                        index: LogIndex(2),
                        payload: EntryPayload::Command(b"put".to_vec()),
                    },
                    Entry {
                        term: Term(7),
                        index: LogIndex(3),
                        payload: EntryPayload::Command(Vec::new()),
                    },
                ],
                leader_commit: LogIndex(1),
                read_round: 0,
            },
            MessageBody::AppendEntriesResponse {
                success: true,
                match_index: LogIndex(3),
                conflict_index: LogIndex(0),
                conflict_term: None,
                read_round: 12,
            },
            MessageBody::AppendEntriesResponse {
                success: false,
                match_index: LogIndex(0),
                conflict_index: LogIndex(4),
                conflict_term: Some(Term(0)),
                read_round: 3,
            },
        ];
        for body in bodies {
            let original = message(body);
            let wire = encode_message(&original);
            // Through the actual byte encoding, not only the structs.
            let bytes = prost::Message::encode_to_vec(&wire);
            let decoded = <wire::RaftMessage as prost::Message>::decode(bytes.as_slice())
                .expect("valid protobuf");
            assert_eq!(decode_message(decoded), Ok(original));
        }
    }

    #[test]
    fn incomplete_messages_are_rejected() {
        let mut no_body = encode_message(&message(MessageBody::RequestVoteResponse {
            granted: false,
        }));
        no_body.body = None;
        assert_eq!(decode_message(no_body), Err(InvalidMessage("no body")));

        let mut no_payload = encode_message(&message(MessageBody::AppendEntries {
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: vec![Entry {
                term: Term(1),
                index: LogIndex(1),
                payload: EntryPayload::Noop,
            }],
            leader_commit: LogIndex(0),
            read_round: 0,
        }));
        if let Some(wire::raft_message::Body::AppendEntries(append)) = &mut no_payload.body {
            append.entries[0].payload = None;
        }
        assert_eq!(
            decode_message(no_payload),
            Err(InvalidMessage("entry without payload"))
        );
    }
}
