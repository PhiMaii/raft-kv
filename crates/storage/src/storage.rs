//! Durable storage for one Raft node: the WAL, the hard state and the
//! latest snapshot.

use raft_core::{Entry, HardState, Log, LogIndex, Ready, Snapshot, Term};

use crate::codec;
use crate::disk::Disk;
use crate::error::StorageError;
use crate::wal::Wal;

const HARD_STATE: &str = "hardstate";
const HARD_STATE_TMP: &str = "hardstate.tmp";
const SNAPSHOT: &str = "snapshot";
const SNAPSHOT_TMP: &str = "snapshot.tmp";

/// Storage settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageOptions {
    /// A new WAL segment starts once the current one would exceed this many
    /// bytes. A single record larger than this gets a segment of its own.
    pub segment_size: u64,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            segment_size: 64 << 20,
        }
    }
}

/// What a node had persisted before it stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// The persisted term and vote.
    pub hard_state: HardState,
    /// The latest snapshot, if the node has one.
    pub snapshot: Option<Snapshot>,
    /// The persisted log. If there is a snapshot, the log continues it: it
    /// contains the snapshot's last entry, or starts right after it. It may
    /// also hold entries before the snapshot.
    pub log: Log,
}

/// Durable storage for one Raft node.
///
/// [`persist`](Self::persist) writes the parts of a [`Ready`] that must be
/// durable and returns only once they are: the log entries are synced, and
/// the hard state and snapshot files are replaced atomically (written to a
/// temporary file, synced, renamed over the old file, directory synced).
/// [`save_snapshot`](Self::save_snapshot) stores a snapshot the node took
/// itself and removes the WAL segments it makes unnecessary.
#[derive(Debug)]
pub struct Storage<D: Disk> {
    disk: D,
    wal: Wal,
}

impl<D: Disk> Storage<D> {
    /// Opens storage on `disk` and recovers what was persisted.
    ///
    /// An interrupted write at the end of the log is dropped. Any other
    /// damage is reported as [`StorageError::Corrupt`].
    pub fn open(mut disk: D, options: StorageOptions) -> Result<(Self, Recovered), StorageError> {
        let names = disk.list()?;
        for tmp in [HARD_STATE_TMP, SNAPSHOT_TMP] {
            if names.iter().any(|n| n == tmp) {
                // Left over from a crash during an update; the rename never
                // happened, so the previous file is still valid.
                disk.remove(tmp)?;
                disk.sync_dir()?;
            }
        }
        let hard_state = if names.iter().any(|n| n == HARD_STATE) {
            let data = disk.read(HARD_STATE)?;
            codec::decode_hard_state(&data).map_err(|reason| corrupt(HARD_STATE, reason))?
        } else {
            HardState::default()
        };
        let snapshot = if names.iter().any(|n| n == SNAPSHOT) {
            let data = disk.read(SNAPSHOT)?;
            Some(codec::decode_snapshot(&data).map_err(|reason| corrupt(SNAPSHOT, reason))?)
        } else {
            None
        };

        let (mut wal, entries) = Wal::recover(&mut disk, options.segment_size)?;
        let log = match &snapshot {
            None => log_from(LogIndex(0), Term(0), entries)?,
            Some(snapshot) => {
                if wal.first_index() > snapshot.index.next() {
                    return Err(corrupt(
                        "wal",
                        format!(
                            "the log starts at {}, after the snapshot at {}",
                            wal.first_index(),
                            snapshot.index
                        ),
                    ));
                }
                if continues(&entries, snapshot) {
                    log_after_snapshot(snapshot, entries)?
                } else {
                    // The log is from before a snapshot received from the
                    // leader, and a crash interrupted replacing it. Finish
                    // the job: the snapshot replaces all of it.
                    wal.reset(&mut disk, snapshot.index.next())?;
                    log_from(snapshot.index, snapshot.term, Vec::new())?
                }
            }
        };
        let recovered = Recovered {
            hard_state,
            snapshot,
            log,
        };
        Ok((Self { disk, wal }, recovered))
    }

    /// Makes `ready.hard_state` and `ready.entries` durable.
    ///
    /// Entries that replace part of the stored log (their first index is at
    /// or below the last stored index) cut the stored log off first.
    ///
    /// The hard state is written first. A node that learns of a new term
    /// receives the term and that term's entries in the same `Ready`; written
    /// the other way round, a crash in between would leave entries from a
    /// term later than the node's current term.
    ///
    /// A snapshot received from the leader (`ready.snapshot`) replaces the
    /// whole stored log: the snapshot file is replaced first, then every WAL
    /// segment is removed, then `ready.entries` start the new log right
    /// after the snapshot.
    pub fn persist(&mut self, ready: &Ready) -> Result<(), StorageError> {
        if let Some(hard_state) = ready.hard_state {
            self.save_hard_state(hard_state)?;
        }
        if let Some(snapshot) = &ready.snapshot {
            self.write_snapshot_file(snapshot)?;
            self.wal.reset(&mut self.disk, snapshot.index.next())?;
        }
        if let Some(first) = ready.entries.first() {
            if first.index <= self.wal.last_index() {
                self.wal.truncate_from(&mut self.disk, first.index)?;
            }
            self.wal.append(&mut self.disk, &ready.entries)?;
        }
        Ok(())
    }

    /// Stores a snapshot the node took itself (see `Raft::compact`) and
    /// removes the WAL segments that hold only entries before `keep_from`,
    /// the start of the node's in-memory log.
    ///
    /// The snapshot is durable before any segment is removed, so a crash at
    /// any point leaves either the old state or the new one.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot covers entries that were never persisted.
    pub fn save_snapshot(
        &mut self,
        snapshot: &Snapshot,
        keep_from: LogIndex,
    ) -> Result<(), StorageError> {
        assert!(
            snapshot.index <= self.wal.last_index(),
            "a snapshot may only cover persisted entries"
        );
        self.write_snapshot_file(snapshot)?;
        self.wal
            .remove_before(&mut self.disk, keep_from.min(snapshot.index.next()))
    }

    fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), StorageError> {
        let data = codec::encode_hard_state(hard_state);
        self.replace_file(HARD_STATE_TMP, HARD_STATE, &data)
    }

    fn write_snapshot_file(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        let data = codec::encode_snapshot(snapshot);
        self.replace_file(SNAPSHOT_TMP, SNAPSHOT, &data)
    }

    /// Replaces `name` atomically: write a temporary file, sync it, rename
    /// it over the old one, sync the directory.
    fn replace_file(&mut self, tmp: &str, name: &str, data: &[u8]) -> Result<(), StorageError> {
        self.disk.write_new(tmp, data)?;
        self.disk.sync(tmp)?;
        self.disk.rename(tmp, name)?;
        self.disk.sync_dir()?;
        Ok(())
    }

    /// The underlying disk.
    pub fn disk(&self) -> &D {
        &self.disk
    }

    /// The underlying disk, mutably. Writing to it directly bypasses the
    /// storage layer's bookkeeping; this is meant for fault injection.
    pub fn disk_mut(&mut self) -> &mut D {
        &mut self.disk
    }

    /// Gives up storage and returns the disk, for example to simulate a
    /// crash and reopen it.
    pub fn into_disk(self) -> D {
        self.disk
    }
}

/// Whether the stored entries continue `snapshot`: they contain its last
/// entry with the same term, or start right after it.
fn continues(entries: &[Entry], snapshot: &Snapshot) -> bool {
    let Some(first) = entries.first() else {
        return false;
    };
    if first.index > snapshot.index {
        return first.index == snapshot.index.next();
    }
    let offset = usize::try_from(snapshot.index.0 - first.index.0).expect("fits in usize");
    entries
        .get(offset)
        .is_some_and(|entry| entry.term == snapshot.term)
}

/// The log from stored entries that continue `snapshot`, keeping the
/// entries before it that are still stored.
fn log_after_snapshot(snapshot: &Snapshot, entries: Vec<Entry>) -> Result<Log, StorageError> {
    let first = entries.first().expect("continuing entries exist");
    if first.index > snapshot.index {
        return log_from(snapshot.index, snapshot.term, entries);
    }
    if first.index == LogIndex(1) {
        return log_from(LogIndex(0), Term(0), entries);
    }
    // The entry before the first stored one is gone, and with it its term.
    // Start the log at the first stored entry instead.
    let (start_index, start_term) = (first.index, first.term);
    log_from(
        start_index,
        start_term,
        entries.into_iter().skip(1).collect(),
    )
}

fn log_from(index: LogIndex, term: Term, entries: Vec<Entry>) -> Result<Log, StorageError> {
    Log::from_parts(index, term, entries).map_err(|error| corrupt("wal", error.to_string()))
}

fn corrupt(file: &str, reason: String) -> StorageError {
    StorageError::Corrupt {
        file: file.to_owned(),
        offset: 0,
        reason,
    }
}
