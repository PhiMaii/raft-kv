//! Storage errors.

use std::{fmt, io};

/// Why storage could not be opened or written.
#[derive(Debug)]
pub enum StorageError {
    /// The disk reported an error.
    Io(io::Error),
    /// Data on disk is damaged in a way that is not an interrupted write at
    /// the end of the log. Recovery refuses to guess, because silently
    /// skipping entries could lose acknowledged writes.
    Corrupt {
        /// The damaged file.
        file: String,
        /// Byte offset of the damage within the file.
        offset: u64,
        /// What is wrong.
        reason: String,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "storage I/O error: {error}"),
            Self::Corrupt {
                file,
                offset,
                reason,
            } => write!(f, "corrupt storage in {file} at byte {offset}: {reason}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Corrupt { .. } => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
