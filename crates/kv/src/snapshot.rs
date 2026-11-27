//! The binary encoding of a [`KvStore`] snapshot.
//!
//! All integers are little-endian; `bytes` is `len:u32 | bytes` as in log
//! commands. Keys and sessions are written in order, so equal stores always
//! produce equal snapshots.
//!
//! ```text
//! snapshot = version:u8 (=1) | clock_ms:u64 | last_sweep_ms:u64
//!            | keys:u64 | (key:bytes | value:bytes)*
//!            | sessions:u64 | (client_id:u64 | sequence:u64 | active_at_ms:u64 | response)*
//! response = 0 | client_id:u64                     (registered)
//!          | 1 | option                            (value)
//!          | 2                                     (written)
//!          | 3 | existed:u8                        (deleted)
//!          | 4 | succeeded:u8 | option            (swapped; previous value)
//!          | 5 | rejection
//! rejection = 0 (session expired) | 1 (stale sequence) | 2 (key too large)
//!           | 3 (value too large) | 4 | reason:bytes (malformed)
//! option   = 0 | 1 | bytes
//! ```

use crate::command::{put_bytes, ClientId, DecodeError, Reader};
use crate::store::{KvConfig, KvStore, Rejection, Response, Session};

const VERSION: u8 = 1;

pub(crate) fn encode(store: &KvStore) -> Vec<u8> {
    let mut out = vec![VERSION];
    out.extend_from_slice(&store.clock_ms.to_le_bytes());
    out.extend_from_slice(&store.last_sweep_ms.to_le_bytes());
    out.extend_from_slice(&(store.data.len() as u64).to_le_bytes());
    for (key, value) in &store.data {
        put_bytes(key, &mut out);
        put_bytes(value, &mut out);
    }
    out.extend_from_slice(&(store.sessions.len() as u64).to_le_bytes());
    for (client_id, session) in &store.sessions {
        out.extend_from_slice(&client_id.0.to_le_bytes());
        out.extend_from_slice(&session.sequence.to_le_bytes());
        out.extend_from_slice(&session.active_at_ms.to_le_bytes());
        encode_response(&session.response, &mut out);
    }
    out
}

pub(crate) fn decode(config: KvConfig, bytes: &[u8]) -> Result<KvStore, DecodeError> {
    let mut reader = Reader { bytes };
    let version = reader.u8()?;
    if version != VERSION {
        return Err(DecodeError(format!("unknown snapshot version {version}")));
    }
    let mut store = KvStore::new(config);
    store.clock_ms = reader.u64()?;
    store.last_sweep_ms = reader.u64()?;
    for _ in 0..reader.u64()? {
        let key = reader.bytes()?;
        let value = reader.bytes()?;
        store.data.insert(key, value);
    }
    for _ in 0..reader.u64()? {
        let client_id = ClientId(reader.u64()?);
        let sequence = reader.u64()?;
        let active_at_ms = reader.u64()?;
        let response = decode_response(&mut reader)?;
        store.sessions.insert(
            client_id,
            Session {
                sequence,
                response,
                active_at_ms,
            },
        );
    }
    if !reader.bytes.is_empty() {
        return Err(DecodeError(format!(
            "{} trailing bytes after the snapshot",
            reader.bytes.len()
        )));
    }
    Ok(store)
}

fn encode_response(response: &Response, out: &mut Vec<u8>) {
    match response {
        Response::Registered { client_id } => {
            out.push(0);
            out.extend_from_slice(&client_id.0.to_le_bytes());
        }
        Response::Value(value) => {
            out.push(1);
            encode_option(value.as_deref(), out);
        }
        Response::Written => out.push(2),
        Response::Deleted { existed } => {
            out.push(3);
            out.push(u8::from(*existed));
        }
        Response::Swapped {
            succeeded,
            previous,
        } => {
            out.push(4);
            out.push(u8::from(*succeeded));
            encode_option(previous.as_deref(), out);
        }
        Response::Rejected(rejection) => {
            out.push(5);
            match rejection {
                Rejection::SessionExpired => out.push(0),
                Rejection::StaleSequence => out.push(1),
                Rejection::KeyTooLarge => out.push(2),
                Rejection::ValueTooLarge => out.push(3),
                Rejection::Malformed(reason) => {
                    out.push(4);
                    put_bytes(reason.as_bytes(), out);
                }
            }
        }
    }
}

fn decode_response(reader: &mut Reader<'_>) -> Result<Response, DecodeError> {
    Ok(match reader.u8()? {
        0 => Response::Registered {
            client_id: ClientId(reader.u64()?),
        },
        1 => Response::Value(decode_option(reader)?),
        2 => Response::Written,
        3 => Response::Deleted {
            existed: decode_bool(reader)?,
        },
        4 => Response::Swapped {
            succeeded: decode_bool(reader)?,
            previous: decode_option(reader)?,
        },
        5 => Response::Rejected(match reader.u8()? {
            0 => Rejection::SessionExpired,
            1 => Rejection::StaleSequence,
            2 => Rejection::KeyTooLarge,
            3 => Rejection::ValueTooLarge,
            4 => Rejection::Malformed(
                String::from_utf8(reader.bytes()?)
                    .map_err(|_| DecodeError("rejection reason is not UTF-8".to_owned()))?,
            ),
            tag => return Err(DecodeError(format!("unknown rejection tag {tag}"))),
        }),
        tag => return Err(DecodeError(format!("unknown response tag {tag}"))),
    })
}

fn encode_option(value: Option<&[u8]>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            put_bytes(value, out);
        }
        None => out.push(0),
    }
}

fn decode_option(reader: &mut Reader<'_>) -> Result<Option<Vec<u8>>, DecodeError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.bytes()?)),
        flag => Err(DecodeError(format!("invalid option flag {flag}"))),
    }
}

fn decode_bool(reader: &mut Reader<'_>) -> Result<bool, DecodeError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(DecodeError(format!("invalid boolean {flag}"))),
    }
}
