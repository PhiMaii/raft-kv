//! Commands as they are stored in the Raft log, and their binary encoding.
//!
//! Every log entry holds one [`Envelope`]: the leader's timestamp at the
//! time of the proposal, and the [`Command`]. The timestamp is what makes
//! session expiry deterministic: every node sees the same timestamps in the
//! same order, whatever its own clock says.
//!
//! The encoding is a version byte followed by tagged, length-prefixed
//! fields, all little-endian:
//!
//! ```text
//! envelope  = version:u8 (=1) | timestamp_ms:u64 | command
//! command   = 0 (register)
//!           | 1 | client_id:u64 | sequence:u64 | operation
//! operation = 0 | key (get)
//!           | 1 | key | value (put)
//!           | 2 | key (delete)
//!           | 3 | key | has_expected:u8 | [expected] | new (compare-and-swap)
//! key, value, expected, new = len:u32 | bytes
//! ```

use std::fmt;

/// Identifies a client session. Assigned by the state machine when the
/// `RegisterClient` command is applied: it is that entry's log index, which
/// is unique and the same on every node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u64);

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "client {}", self.0)
    }
}

/// A key-value operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    /// Reads a key through the log. Normal reads use `ReadIndex` instead and
    /// never reach the log; this exists to compare the two in benchmarks.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Sets a key to a value.
    Put {
        /// The key to write.
        key: Vec<u8>,
        /// The new value.
        value: Vec<u8>,
    },
    /// Removes a key.
    Delete {
        /// The key to remove.
        key: Vec<u8>,
    },
    /// Sets `key` to `new` only if its current value is `expected`, where
    /// `None` means the key must not exist.
    CompareAndSwap {
        /// The key to update.
        key: Vec<u8>,
        /// The value the key must currently have, or `None` for "absent".
        expected: Option<Vec<u8>>,
        /// The value to set if the comparison succeeds.
        new: Vec<u8>,
    },
}

impl Operation {
    /// The key the operation reads or writes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Put { key, .. }
            | Self::Delete { key }
            | Self::CompareAndSwap { key, .. } => key,
        }
    }
}

/// A command for the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Opens a new client session.
    RegisterClient,
    /// An operation from a client session.
    Request {
        /// The client's session.
        client_id: ClientId,
        /// The client's request number, starting at 1. Each new request
        /// uses a higher number than the last; a retry reuses the number of
        /// the request it repeats.
        sequence: u64,
        /// What to do.
        operation: Operation,
    },
}

/// What the leader writes into a log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The leader's wall clock when it accepted the command, in
    /// milliseconds. Only used to expire idle sessions.
    pub timestamp_ms: u64,
    /// The command itself.
    pub command: Command,
}

/// Why bytes could not be decoded into an [`Envelope`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError(String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid command encoding: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

const VERSION: u8 = 1;

impl Envelope {
    /// Encodes the envelope for a log entry.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        out.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        match &self.command {
            Command::RegisterClient => out.push(0),
            Command::Request {
                client_id,
                sequence,
                operation,
            } => {
                out.push(1);
                out.extend_from_slice(&client_id.0.to_le_bytes());
                out.extend_from_slice(&sequence.to_le_bytes());
                encode_operation(operation, &mut out);
            }
        }
        out
    }

    /// Decodes an envelope from a log entry.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader { bytes };
        let version = reader.u8()?;
        if version != VERSION {
            return Err(DecodeError(format!("unknown version {version}")));
        }
        let timestamp_ms = reader.u64()?;
        let command = match reader.u8()? {
            0 => Command::RegisterClient,
            1 => Command::Request {
                client_id: ClientId(reader.u64()?),
                sequence: reader.u64()?,
                operation: decode_operation(&mut reader)?,
            },
            tag => return Err(DecodeError(format!("unknown command tag {tag}"))),
        };
        if !reader.bytes.is_empty() {
            return Err(DecodeError(format!(
                "{} trailing bytes",
                reader.bytes.len()
            )));
        }
        Ok(Self {
            timestamp_ms,
            command,
        })
    }
}

fn encode_operation(operation: &Operation, out: &mut Vec<u8>) {
    match operation {
        Operation::Get { key } => {
            out.push(0);
            put_bytes(key, out);
        }
        Operation::Put { key, value } => {
            out.push(1);
            put_bytes(key, out);
            put_bytes(value, out);
        }
        Operation::Delete { key } => {
            out.push(2);
            put_bytes(key, out);
        }
        Operation::CompareAndSwap { key, expected, new } => {
            out.push(3);
            put_bytes(key, out);
            match expected {
                Some(expected) => {
                    out.push(1);
                    put_bytes(expected, out);
                }
                None => out.push(0),
            }
            put_bytes(new, out);
        }
    }
}

fn decode_operation(reader: &mut Reader<'_>) -> Result<Operation, DecodeError> {
    Ok(match reader.u8()? {
        0 => Operation::Get {
            key: reader.bytes()?,
        },
        1 => Operation::Put {
            key: reader.bytes()?,
            value: reader.bytes()?,
        },
        2 => Operation::Delete {
            key: reader.bytes()?,
        },
        3 => {
            let key = reader.bytes()?;
            let expected = match reader.u8()? {
                0 => None,
                1 => Some(reader.bytes()?),
                flag => return Err(DecodeError(format!("invalid expected flag {flag}"))),
            };
            Operation::CompareAndSwap {
                key,
                expected,
                new: reader.bytes()?,
            }
        }
        tag => return Err(DecodeError(format!("unknown operation tag {tag}"))),
    })
}

fn put_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(bytes.len()).expect("field shorter than 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], DecodeError> {
        if self.bytes.len() < n {
            return Err(DecodeError(format!(
                "needed {n} more bytes, found {}",
                self.bytes.len()
            )));
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes"));
        let len = usize::try_from(len).map_err(|_| DecodeError("length overflow".to_owned()))?;
        Ok(self.take(len)?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_round_trips() {
        let operations = [
            Operation::Get { key: b"k".to_vec() },
            Operation::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            Operation::Delete { key: Vec::new() },
            Operation::CompareAndSwap {
                key: b"k".to_vec(),
                expected: None,
                new: b"n".to_vec(),
            },
            Operation::CompareAndSwap {
                key: b"k".to_vec(),
                expected: Some(b"e".to_vec()),
                new: Vec::new(),
            },
        ];
        let mut commands = vec![Command::RegisterClient];
        commands.extend(operations.into_iter().map(|operation| Command::Request {
            client_id: ClientId(7),
            sequence: 3,
            operation,
        }));
        for command in commands {
            let envelope = Envelope {
                timestamp_ms: 1_700_000_000_000,
                command,
            };
            assert_eq!(Envelope::decode(&envelope.encode()), Ok(envelope));
        }
    }

    #[test]
    fn malformed_input_is_rejected() {
        let valid = Envelope {
            timestamp_ms: 1,
            command: Command::RegisterClient,
        }
        .encode();
        assert!(Envelope::decode(&[]).is_err());
        assert!(Envelope::decode(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(Envelope::decode(&trailing).is_err());
        let mut wrong_version = valid;
        wrong_version[0] = 9;
        assert!(Envelope::decode(&wrong_version).is_err());
    }
}
