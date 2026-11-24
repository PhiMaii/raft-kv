//! Durable storage for one Raft node: the WAL plus the hard state.

use raft_core::{Entry, HardState, Log, LogIndex, Ready, Term};

use crate::codec;
use crate::disk::Disk;
use crate::error::StorageError;
use crate::wal::Wal;

const HARD_STATE: &str = "hardstate";
const HARD_STATE_TMP: &str = "hardstate.tmp";

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
    /// The persisted log.
    pub log: Log,
}

/// Durable storage for one Raft node.
///
/// [`persist`](Self::persist) writes the parts of a [`Ready`] that must be
/// durable and returns only once they are: the log entries are synced, and
/// the hard state is replaced atomically (written to a temporary file,
/// synced, renamed over the old file, directory synced).
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
        if names.iter().any(|n| n == HARD_STATE_TMP) {
            // Left over from a crash during a hard state update; the rename
            // never happened, so the previous hard state is still valid.
            disk.remove(HARD_STATE_TMP)?;
            disk.sync_dir()?;
        }
        let hard_state = if names.iter().any(|n| n == HARD_STATE) {
            let data = disk.read(HARD_STATE)?;
            codec::decode_hard_state(&data).map_err(|reason| StorageError::Corrupt {
                file: HARD_STATE.to_owned(),
                offset: 0,
                reason,
            })?
        } else {
            HardState::default()
        };

        let (wal, entries) = Wal::recover(&mut disk, options.segment_size)?;
        let log = log_from(entries)?;
        let recovered = Recovered { hard_state, log };
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
    pub fn persist(&mut self, ready: &Ready) -> Result<(), StorageError> {
        if let Some(hard_state) = ready.hard_state {
            self.save_hard_state(hard_state)?;
        }
        if let Some(first) = ready.entries.first() {
            if first.index <= self.wal.last_index() {
                self.wal.truncate_from(&mut self.disk, first.index)?;
            }
            self.wal.append(&mut self.disk, &ready.entries)?;
        }
        Ok(())
    }

    fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), StorageError> {
        let data = codec::encode_hard_state(hard_state);
        self.disk.write_new(HARD_STATE_TMP, &data)?;
        self.disk.sync(HARD_STATE_TMP)?;
        self.disk.rename(HARD_STATE_TMP, HARD_STATE)?;
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

fn log_from(entries: Vec<Entry>) -> Result<Log, StorageError> {
    Log::from_parts(LogIndex(0), Term(0), entries).map_err(|error| StorageError::Corrupt {
        file: "wal".to_owned(),
        offset: 0,
        reason: error.to_string(),
    })
}
