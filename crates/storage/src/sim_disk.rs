//! An in-memory [`Disk`] that forgets unsynced changes on a crash.

use std::collections::BTreeMap;
use std::io;

use crate::disk::Disk;

#[derive(Clone, Debug, Default)]
struct FileData {
    /// What reads currently return.
    data: Vec<u8>,
    /// How much of `data` is durable, as long as `shadow` is `None`.
    synced_len: usize,
    /// The durable contents, kept only once the file was changed in a way
    /// other than appending since its last sync (truncated or replaced).
    shadow: Option<Vec<u8>>,
}

impl FileData {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            synced_len: 0,
            shadow: Some(Vec::new()),
        }
    }

    fn sync(&mut self) {
        self.synced_len = self.data.len();
        self.shadow = None;
    }

    /// Reverts to what survives a crash. Unsynced appended bytes may
    /// survive in part: `torn` gets their count and returns how many to
    /// keep, which models a write that reached the disk only partially.
    fn crash(&mut self, torn: &mut dyn FnMut(u64) -> u64) {
        if let Some(shadow) = self.shadow.take() {
            self.data = shadow;
        } else {
            let unsynced = (self.data.len() - self.synced_len) as u64;
            let keep = torn(unsynced).min(unsynced);
            let keep = usize::try_from(keep).expect("length fits in usize");
            self.data.truncate(self.synced_len + keep);
        }
        self.synced_len = self.data.len();
    }
}

/// An in-memory disk for tests and simulation.
///
/// It tracks which data and which directory changes are durable, and
/// [`crash`](Self::crash) throws away everything that is not. With
/// [`fail_after`](Self::fail_after) it can also simulate a process that
/// crashes in the middle of a sequence of writes.
#[derive(Clone, Debug, Default)]
pub struct SimDisk {
    files: BTreeMap<u64, FileData>,
    /// File names as currently visible.
    names: BTreeMap<String, u64>,
    /// File names as of the last directory sync.
    durable_names: BTreeMap<String, u64>,
    next_id: u64,
    ops_until_failure: Option<u64>,
    failed: bool,
}

impl SimDisk {
    /// An empty disk.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes every operation after the next `ops` fail, as if the process
    /// died at that point. Reads keep working.
    pub fn fail_after(&mut self, ops: u64) {
        self.ops_until_failure = Some(ops);
    }

    /// Whether an injected failure has happened since the last crash.
    pub fn has_failed(&self) -> bool {
        self.failed
    }

    /// Simulates a crash and reboot: unsynced data and unsynced directory
    /// changes are lost. `torn` decides how many unsynced appended bytes
    /// of a file survive anyway; see [`SimDisk`] for details.
    pub fn crash(&mut self, torn: &mut dyn FnMut(u64) -> u64) {
        self.names = self.durable_names.clone();
        self.files
            .retain(|id, _| self.durable_names.values().any(|v| v == id));
        for file in self.files.values_mut() {
            file.crash(torn);
        }
        self.ops_until_failure = None;
        self.failed = false;
    }

    /// Counts one write operation against an injected failure.
    fn op(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(crashed());
        }
        match self.ops_until_failure {
            Some(0) => {
                self.failed = true;
                Err(crashed())
            }
            Some(n) => {
                self.ops_until_failure = Some(n - 1);
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn file_mut(&mut self, name: &str) -> io::Result<&mut FileData> {
        let id = self.names.get(name).ok_or_else(|| not_found(name))?;
        Ok(self.files.get_mut(id).expect("named file exists"))
    }

    fn create(&mut self, name: &str, data: Vec<u8>) {
        let id = self.next_id;
        self.next_id += 1;
        self.files.insert(id, FileData::new(data));
        self.names.insert(name.to_owned(), id);
    }
}

impl Disk for SimDisk {
    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.names.keys().cloned().collect())
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let id = self.names.get(name).ok_or_else(|| not_found(name))?;
        Ok(self.files[id].data.clone())
    }

    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.op()?;
        if self.names.contains_key(name) {
            self.file_mut(name)?.data.extend_from_slice(data);
        } else {
            self.create(name, data.to_vec());
        }
        Ok(())
    }

    fn write_new(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.op()?;
        self.create(name, data.to_vec());
        Ok(())
    }

    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()> {
        self.op()?;
        let file = self.file_mut(name)?;
        let len = usize::try_from(len).expect("length fits in usize");
        if len < file.data.len() {
            if file.shadow.is_none() && len < file.synced_len {
                file.shadow = Some(file.data[..file.synced_len].to_vec());
            }
            file.data.truncate(len);
            file.synced_len = file.synced_len.min(len);
        }
        Ok(())
    }

    fn sync(&mut self, name: &str) -> io::Result<()> {
        self.op()?;
        self.file_mut(name)?.sync();
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        self.op()?;
        let id = self.names.remove(from).ok_or_else(|| not_found(from))?;
        self.names.insert(to.to_owned(), id);
        Ok(())
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.op()?;
        self.names.remove(name).ok_or_else(|| not_found(name))?;
        Ok(())
    }

    fn sync_dir(&mut self) -> io::Result<()> {
        self.op()?;
        self.durable_names = self.names.clone();
        Ok(())
    }
}

fn not_found(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no such file: {name}"))
}

fn crashed() -> io::Error {
    io::Error::other("simulated crash")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keep_none(_: u64) -> u64 {
        0
    }

    fn keep_all(n: u64) -> u64 {
        n
    }

    #[test]
    fn synced_data_survives_and_unsynced_data_is_lost() {
        let mut disk = SimDisk::new();
        disk.append("a", b"hello").unwrap();
        disk.sync("a").unwrap();
        disk.sync_dir().unwrap();
        disk.append("a", b" world").unwrap();

        disk.crash(&mut keep_none);
        assert_eq!(disk.read("a").unwrap(), b"hello");
    }

    #[test]
    fn unsynced_appends_can_survive_in_part() {
        let mut disk = SimDisk::new();
        disk.append("a", b"abc").unwrap();
        disk.sync("a").unwrap();
        disk.sync_dir().unwrap();
        disk.append("a", b"defgh").unwrap();

        disk.crash(&mut |n| {
            assert_eq!(n, 5);
            2
        });
        assert_eq!(disk.read("a").unwrap(), b"abcde");
    }

    #[test]
    fn a_new_file_needs_a_directory_sync_to_survive() {
        let mut disk = SimDisk::new();
        disk.append("a", b"data").unwrap();
        disk.sync("a").unwrap();
        disk.crash(&mut keep_all);
        assert_eq!(disk.list().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn rename_is_atomic_across_a_crash() {
        let mut disk = SimDisk::new();
        disk.write_new("state", b"old").unwrap();
        disk.sync("state").unwrap();
        disk.sync_dir().unwrap();

        disk.write_new("state.tmp", b"new").unwrap();
        disk.sync("state.tmp").unwrap();
        disk.rename("state.tmp", "state").unwrap();
        assert_eq!(disk.read("state").unwrap(), b"new");

        let mut before_dir_sync = disk.clone();
        before_dir_sync.crash(&mut keep_all);
        assert_eq!(before_dir_sync.read("state").unwrap(), b"old");

        disk.sync_dir().unwrap();
        disk.crash(&mut keep_all);
        assert_eq!(disk.read("state").unwrap(), b"new");
        assert_eq!(disk.list().unwrap(), vec!["state".to_owned()]);
    }

    #[test]
    fn unsynced_truncation_is_undone_by_a_crash() {
        let mut disk = SimDisk::new();
        disk.append("a", b"abcdef").unwrap();
        disk.sync("a").unwrap();
        disk.sync_dir().unwrap();
        disk.truncate("a", 2).unwrap();
        disk.append("a", b"XY").unwrap();
        assert_eq!(disk.read("a").unwrap(), b"abXY");

        disk.crash(&mut keep_all);
        assert_eq!(disk.read("a").unwrap(), b"abcdef");
    }

    #[test]
    fn injected_failure_stops_all_writes_until_the_crash() {
        let mut disk = SimDisk::new();
        disk.fail_after(1);
        disk.append("a", b"1").unwrap();
        assert!(disk.append("a", b"2").is_err());
        assert!(disk.sync("a").is_err());
        assert!(disk.has_failed());

        disk.crash(&mut keep_none);
        assert!(!disk.has_failed());
        disk.append("b", b"ok").unwrap();
    }
}
