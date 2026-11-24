//! The file operations storage needs, behind a trait so that tests and the
//! simulator can use a disk that loses unsynced data on a crash.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A flat directory of files.
///
/// The durability rules follow POSIX file systems:
///
/// - Written data survives a crash only after [`sync`](Disk::sync) on that
///   file.
/// - Creating, renaming and removing files survives a crash only after
///   [`sync_dir`](Disk::sync_dir).
pub trait Disk {
    /// Names of all files in the directory, in no particular order.
    fn list(&self) -> io::Result<Vec<String>>;

    /// The full contents of a file.
    fn read(&self, name: &str) -> io::Result<Vec<u8>>;

    /// Appends to a file, creating it if it does not exist.
    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()>;

    /// Creates or replaces a file with the given contents.
    fn write_new(&mut self, name: &str, data: &[u8]) -> io::Result<()>;

    /// Shortens a file to `len` bytes.
    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()>;

    /// Makes a file's data durable.
    fn sync(&mut self, name: &str) -> io::Result<()>;

    /// Renames a file, replacing any file named `to`. Atomic: after a crash
    /// either the old or the new name exists, never neither.
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()>;

    /// Removes a file.
    fn remove(&mut self, name: &str) -> io::Result<()>;

    /// Makes creations, renames and removals in the directory durable.
    fn sync_dir(&mut self) -> io::Result<()>;
}

/// A [`Disk`] backed by a real directory.
///
/// `sync` uses [`File::sync_all`], which on macOS and iOS issues
/// `F_FULLFSYNC`. A plain `fsync` there only hands the data to the drive,
/// whose cache may still lose it on power failure.
#[derive(Debug)]
pub struct FsDisk {
    dir: PathBuf,
}

impl FsDisk {
    /// Uses `dir` as the storage directory, creating it if needed.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// The storage directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Disk for FsDisk {
    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Ok(name) = entry.file_name().into_string() {
                    names.push(name);
                }
            }
        }
        Ok(names)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(name))
    }

    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path(name))?
            .write_all(data)
    }

    fn write_new(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        File::create(self.path(name))?.write_all(data)
    }

    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.path(name))?
            .set_len(len)
    }

    fn sync(&mut self, name: &str) -> io::Result<()> {
        File::open(self.path(name))?.sync_all()
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        fs::rename(self.path(from), self.path(to))
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        fs::remove_file(self.path(name))
    }

    fn sync_dir(&mut self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }
}
