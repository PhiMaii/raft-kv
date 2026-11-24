//! On-disk encodings.
//!
//! **WAL record:** `len: u32 | crc: u32 | payload`, little-endian, where
//! `crc` is the CRC-32 (IEEE) of `payload` and `len` its length.
//!
//! **Entry payload:** `index: u64 | term: u64 | kind: u8 | command bytes`,
//! where `kind` is 0 for a no-op and 1 for a command.
//!
//! **Hard state file:** `crc: u32 | term: u64 | has_vote: u8 | vote: u64`,
//! where `crc` covers the 17 bytes after it.

use raft_core::{Entry, EntryPayload, HardState, LogIndex, NodeId, Term};

/// Bytes before a record's payload.
pub(crate) const HEADER_LEN: usize = 8;
/// The smallest valid entry payload: index, term and kind.
const MIN_ENTRY_LEN: usize = 17;
/// Records larger than this are treated as garbage, not allocated.
const MAX_RECORD_LEN: usize = 256 << 20;
const HARD_STATE_LEN: usize = 21;

/// Frames a payload as a WAL record.
pub(crate) fn encode_record(payload: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(payload.len()).expect("record shorter than 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// The result of reading one record at the start of `data`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    /// A complete record with a valid checksum, and its total size.
    Valid { payload: &'a [u8], size: usize },
    /// No valid record starts here.
    Invalid {
        /// Whether the data looks like an interrupted write at the end of a
        /// file: the record runs past the end, it is the last thing in the
        /// file, or everything from here on is zero (space allocated but
        /// never written).
        torn: bool,
    },
}

/// Reads the record at the start of `data`.
pub(crate) fn decode_record(data: &[u8]) -> Record<'_> {
    let all_zero = data.iter().all(|&b| b == 0);
    if data.len() < HEADER_LEN {
        return Record::Invalid { torn: true };
    }
    let len = u32::from_le_bytes(data[0..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(data[4..8].try_into().expect("4 bytes"));
    if !(MIN_ENTRY_LEN..=MAX_RECORD_LEN).contains(&len) {
        return Record::Invalid { torn: all_zero };
    }
    let size = HEADER_LEN + len;
    if size > data.len() {
        return Record::Invalid { torn: true };
    }
    let payload = &data[HEADER_LEN..size];
    if crc32fast::hash(payload) != crc {
        return Record::Invalid {
            torn: size == data.len() || all_zero,
        };
    }
    Record::Valid { payload, size }
}

/// Encodes a log entry as a record payload.
pub(crate) fn encode_entry(entry: &Entry, out: &mut Vec<u8>) {
    out.extend_from_slice(&entry.index.0.to_le_bytes());
    out.extend_from_slice(&entry.term.0.to_le_bytes());
    match &entry.payload {
        EntryPayload::Noop => out.push(0),
        EntryPayload::Command(bytes) => {
            out.push(1);
            out.extend_from_slice(bytes);
        }
    }
}

/// Decodes a record payload into a log entry.
pub(crate) fn decode_entry(payload: &[u8]) -> Result<Entry, String> {
    if payload.len() < MIN_ENTRY_LEN {
        return Err(format!(
            "entry payload of {} bytes is too short",
            payload.len()
        ));
    }
    let index = u64::from_le_bytes(payload[0..8].try_into().expect("8 bytes"));
    let term = u64::from_le_bytes(payload[8..16].try_into().expect("8 bytes"));
    let payload = match payload[16] {
        0 if payload.len() == MIN_ENTRY_LEN => EntryPayload::Noop,
        1 => EntryPayload::Command(payload[MIN_ENTRY_LEN..].to_vec()),
        kind => return Err(format!("unknown entry kind {kind}")),
    };
    Ok(Entry {
        term: Term(term),
        index: LogIndex(index),
        payload,
    })
}

/// Encodes the hard state file.
pub(crate) fn encode_hard_state(state: HardState) -> Vec<u8> {
    let mut body = Vec::with_capacity(HARD_STATE_LEN - 4);
    body.extend_from_slice(&state.current_term.0.to_le_bytes());
    body.push(u8::from(state.voted_for.is_some()));
    body.extend_from_slice(&state.voted_for.map_or(0, |n| n.0).to_le_bytes());
    let mut out = crc32fast::hash(&body).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Decodes the hard state file.
pub(crate) fn decode_hard_state(data: &[u8]) -> Result<HardState, String> {
    if data.len() != HARD_STATE_LEN {
        return Err(format!(
            "hard state is {} bytes, expected {HARD_STATE_LEN}",
            data.len()
        ));
    }
    let crc = u32::from_le_bytes(data[0..4].try_into().expect("4 bytes"));
    let body = &data[4..];
    if crc32fast::hash(body) != crc {
        return Err("hard state checksum mismatch".to_owned());
    }
    let term = u64::from_le_bytes(body[0..8].try_into().expect("8 bytes"));
    let vote = u64::from_le_bytes(body[9..17].try_into().expect("8 bytes"));
    let voted_for = match body[8] {
        0 => None,
        1 => Some(NodeId(vote)),
        flag => return Err(format!("invalid vote flag {flag}")),
    };
    Ok(HardState {
        current_term: Term(term),
        voted_for,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_record(payload, &mut out);
        out
    }

    #[test]
    fn records_round_trip() {
        let payload = [7u8; 20];
        let data = record(&payload);
        assert_eq!(
            decode_record(&data),
            Record::Valid {
                payload: &payload,
                size: 28
            }
        );
    }

    #[test]
    fn torn_and_corrupt_records_are_told_apart() {
        let data = record(&[7u8; 20]);

        // Cut off anywhere: torn.
        for cut in 0..data.len() {
            assert_eq!(decode_record(&data[..cut]), Record::Invalid { torn: true });
        }

        // A flipped bit in the last record of a file: torn.
        let mut flipped = data.clone();
        flipped[12] ^= 1;
        assert_eq!(decode_record(&flipped), Record::Invalid { torn: true });

        // A flipped bit with more data after it: corruption.
        flipped.extend_from_slice(&record(&[1u8; 20]));
        assert_eq!(decode_record(&flipped), Record::Invalid { torn: false });

        // Zero-filled space: torn.
        assert_eq!(decode_record(&[0u8; 64]), Record::Invalid { torn: true });
    }

    #[test]
    fn entries_round_trip() {
        for payload in [EntryPayload::Noop, EntryPayload::Command(vec![1, 2, 3])] {
            let entry = Entry {
                term: Term(9),
                index: LogIndex(42),
                payload,
            };
            let mut out = Vec::new();
            encode_entry(&entry, &mut out);
            assert_eq!(decode_entry(&out), Ok(entry));
        }
    }

    #[test]
    fn malformed_entries_are_rejected() {
        assert!(decode_entry(&[0u8; 5]).is_err());
        let mut bad_kind = vec![0u8; 17];
        bad_kind[16] = 9;
        assert!(decode_entry(&bad_kind).is_err());
    }

    #[test]
    fn hard_state_round_trips_and_detects_corruption() {
        for voted_for in [None, Some(NodeId(3))] {
            let state = HardState {
                current_term: Term(12),
                voted_for,
            };
            let mut data = encode_hard_state(state);
            assert_eq!(decode_hard_state(&data), Ok(state));
            data[10] ^= 1;
            assert!(decode_hard_state(&data).is_err());
        }
    }
}
