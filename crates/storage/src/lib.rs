//! Durable storage for a Raft node: a write-ahead log, the hard state and
//! the latest snapshot, behind a pluggable [`Disk`].
//!
//! - Log entries are appended to segment files as length-prefixed,
//!   CRC-32-checksummed records and synced before [`Storage::persist`]
//!   returns.
//! - The hard state (term and vote) and the snapshot are each replaced
//!   atomically through a temporary file, a rename and a directory sync.
//! - A snapshot makes the WAL segments before it unnecessary; they are
//!   removed once the snapshot is durable. A snapshot received from the
//!   leader replaces the whole WAL.
//! - On startup, an interrupted write at the very end of the log is detected
//!   by its checksum and cut off. Damage anywhere else is an error: skipping
//!   it silently could lose acknowledged writes.
//!
//! [`FsDisk`] stores files in a real directory. [`SimDisk`] keeps them in
//! memory and forgets every unsynced change on a simulated crash, which lets
//! the simulator test recovery against realistic data loss.

mod codec;
mod disk;
mod error;
mod sim_disk;
mod storage;
mod wal;

pub use disk::{Disk, FsDisk};
pub use error::StorageError;
pub use sim_disk::SimDisk;
pub use storage::{Recovered, Storage, StorageOptions};
