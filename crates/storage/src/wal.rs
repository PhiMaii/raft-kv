//! The write-ahead log: log entries in append-only segment files.
//!
//! Each segment file is named after the index of its first entry
//! (`00000000000000000001.wal`) and holds consecutive entries as checksummed
//! records. A new segment starts once the current one reaches the size
//! limit.

use raft_core::{Entry, LogIndex};

use crate::codec::{self, Record};
use crate::disk::Disk;
use crate::error::StorageError;

const SUFFIX: &str = ".wal";

#[derive(Clone, Debug)]
struct Segment {
    name: String,
    len: u64,
}

/// Where an entry's record starts.
#[derive(Clone, Copy, Debug)]
struct Position {
    segment: usize,
    offset: u64,
}

#[derive(Debug)]
pub(crate) struct Wal {
    segments: Vec<Segment>,
    /// The position of every stored entry, in index order.
    positions: Vec<Position>,
    /// The index of the first stored entry.
    first_index: LogIndex,
    segment_size: u64,
}

fn segment_name(first_index: LogIndex) -> String {
    format!("{:020}{SUFFIX}", first_index.0)
}

impl Wal {
    /// Reads all segments, truncates an interrupted write at the very end,
    /// and returns the stored entries.
    pub(crate) fn recover(
        disk: &mut impl Disk,
        segment_size: u64,
    ) -> Result<(Self, Vec<Entry>), StorageError> {
        let mut names: Vec<(LogIndex, String)> = disk
            .list()?
            .into_iter()
            .filter_map(|name| {
                let index = name.strip_suffix(SUFFIX)?.parse().ok()?;
                Some((LogIndex(index), name))
            })
            .collect();
        names.sort();

        let mut wal = Self {
            segments: Vec::new(),
            positions: Vec::new(),
            first_index: names.first().map_or(LogIndex(1), |(index, _)| *index),
            segment_size,
        };
        let mut entries = Vec::new();
        let count = names.len();

        for (number, (first_index, name)) in names.into_iter().enumerate() {
            let expected = LogIndex(wal.first_index.0 + entries.len() as u64);
            if first_index != expected {
                return Err(corrupt(
                    &name,
                    0,
                    format!("segment starts at {first_index}, expected {expected}"),
                ));
            }
            let data = disk.read(&name)?;
            let is_last = number + 1 == count;
            let len = wal.read_segment(&name, &data, is_last, &mut entries)?;
            if len < data.len() as u64 {
                // An interrupted write at the end of the log: drop it.
                disk.truncate(&name, len)?;
                disk.sync(&name)?;
            }
            wal.segments.push(Segment { name, len });
        }
        Ok((wal, entries))
    }

    /// Parses one segment's records into `entries` and returns the length
    /// of its valid part.
    fn read_segment(
        &mut self,
        name: &str,
        data: &[u8],
        is_last: bool,
        entries: &mut Vec<Entry>,
    ) -> Result<u64, StorageError> {
        let segment = self.segments.len();
        let mut offset = 0usize;
        while offset < data.len() {
            match codec::decode_record(&data[offset..]) {
                Record::Valid { payload, size } => {
                    let entry = codec::decode_entry(payload)
                        .map_err(|reason| corrupt(name, offset as u64, reason))?;
                    let expected = LogIndex(self.first_index.0 + entries.len() as u64);
                    if entry.index != expected {
                        return Err(corrupt(
                            name,
                            offset as u64,
                            format!("found entry {}, expected {expected}", entry.index),
                        ));
                    }
                    self.positions.push(Position {
                        segment,
                        offset: offset as u64,
                    });
                    entries.push(entry);
                    offset += size;
                }
                Record::Invalid { torn: true } if is_last => break,
                Record::Invalid { .. } => {
                    return Err(corrupt(
                        name,
                        offset as u64,
                        "invalid record in the middle of the log".to_owned(),
                    ));
                }
            }
        }
        Ok(offset as u64)
    }

    /// The index of the last stored entry, or the one before the first if
    /// none is stored.
    pub(crate) fn last_index(&self) -> LogIndex {
        LogIndex(self.first_index.0 + self.positions.len() as u64 - 1)
    }

    /// Appends entries and makes them durable.
    pub(crate) fn append(
        &mut self,
        disk: &mut impl Disk,
        entries: &[Entry],
    ) -> Result<(), StorageError> {
        let mut dir_changed = false;
        let mut touched: Vec<usize> = Vec::new();
        let mut buffer = Vec::new();
        let mut payload = Vec::new();

        for entry in entries {
            assert_eq!(
                entry.index,
                self.last_index().next(),
                "WAL entries must be appended contiguously"
            );
            payload.clear();
            codec::encode_entry(entry, &mut payload);
            let record_len = (codec::HEADER_LEN + payload.len()) as u64;

            // Count records still in the buffer, not just what is on disk.
            let buffered = buffer.len() as u64;
            let needs_new_segment = self.segments.last().is_none_or(|s| {
                let len = s.len + buffered;
                len > 0 && len + record_len > self.segment_size
            });
            if needs_new_segment {
                self.flush(disk, &mut buffer)?;
                self.segments.push(Segment {
                    name: segment_name(entry.index),
                    len: 0,
                });
                dir_changed = true;
            }

            let segment = self.segments.len() - 1;
            let current = &mut self.segments[segment];
            self.positions.push(Position {
                segment,
                offset: current.len + buffer.len() as u64,
            });
            codec::encode_record(&payload, &mut buffer);
            if touched.last() != Some(&segment) {
                touched.push(segment);
            }
        }
        self.flush(disk, &mut buffer)?;

        for segment in touched {
            disk.sync(&self.segments[segment].name)?;
        }
        if dir_changed {
            disk.sync_dir()?;
        }
        Ok(())
    }

    /// Writes buffered records to the current segment.
    fn flush(&mut self, disk: &mut impl Disk, buffer: &mut Vec<u8>) -> Result<(), StorageError> {
        if buffer.is_empty() {
            return Ok(());
        }
        let segment = self.segments.last_mut().expect("a segment to flush into");
        disk.append(&segment.name, buffer)?;
        segment.len += buffer.len() as u64;
        buffer.clear();
        Ok(())
    }

    /// Removes the entry at `index` and everything after it, durably.
    pub(crate) fn truncate_from(
        &mut self,
        disk: &mut impl Disk,
        index: LogIndex,
    ) -> Result<(), StorageError> {
        if index > self.last_index() {
            return Ok(());
        }
        assert!(
            index >= self.first_index,
            "cannot truncate before the WAL start"
        );
        let keep = usize::try_from(index.0 - self.first_index.0).expect("fits in usize");
        let Position { segment, offset } = self.positions[keep];

        // Remove later segments durably *before* shortening the current one.
        // In the other order, a crash in between could leave the shortened
        // segment followed by a removed segment that came back, a gap that
        // recovery rightly refuses to accept. In this order every crash
        // point leaves a prefix of the old log.
        let removed: Vec<Segment> = self.segments.drain(segment + 1..).collect();
        for later in &removed {
            disk.remove(&later.name)?;
        }
        if !removed.is_empty() {
            disk.sync_dir()?;
        }
        let current = &mut self.segments[segment];
        disk.truncate(&current.name, offset)?;
        disk.sync(&current.name)?;
        current.len = offset;
        self.positions.truncate(keep);
        Ok(())
    }
}

fn corrupt(file: &str, offset: u64, reason: String) -> StorageError {
    StorageError::Corrupt {
        file: file.to_owned(),
        offset,
        reason,
    }
}
