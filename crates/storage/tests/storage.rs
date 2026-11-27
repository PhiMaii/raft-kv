//! Storage behaviour: persisting, recovering, torn writes, corruption, and
//! crashes at every possible point of a write.

use proptest::prelude::*;
use raft_core::{Entry, EntryPayload, HardState, LogIndex, NodeId, Ready, Snapshot, Term};
use storage::{Disk, FsDisk, Recovered, SimDisk, Storage, StorageError, StorageOptions};

fn entry(index: u64, term: u64) -> Entry {
    Entry {
        term: Term(term),
        index: LogIndex(index),
        payload: EntryPayload::Command(index.to_le_bytes().repeat(3)),
    }
}

fn entries(range: std::ops::RangeInclusive<u64>, term: u64) -> Vec<Entry> {
    range.map(|i| entry(i, term)).collect()
}

fn hard_state(term: u64, vote: Option<u64>) -> HardState {
    HardState {
        current_term: Term(term),
        voted_for: vote.map(NodeId),
    }
}

fn ready(hard_state: Option<HardState>, entries: Vec<Entry>) -> Ready {
    Ready {
        hard_state,
        entries,
        ..Ready::default()
    }
}

/// Small segments, so that a few entries already span several files.
fn small() -> StorageOptions {
    StorageOptions { segment_size: 200 }
}

fn open<D: Disk>(disk: D, options: StorageOptions) -> (Storage<D>, Recovered) {
    Storage::open(disk, options).expect("storage opens")
}

fn reopen(storage: Storage<SimDisk>, options: StorageOptions) -> (Storage<SimDisk>, Recovered) {
    let mut disk = storage.into_disk();
    disk.crash(&mut |_| 0);
    open(disk, options)
}

fn segment_files(disk: &impl Disk) -> Vec<String> {
    let mut names: Vec<_> = disk
        .list()
        .unwrap()
        .into_iter()
        .filter(|n| {
            std::path::Path::new(n)
                .extension()
                .is_some_and(|e| e == "wal")
        })
        .collect();
    names.sort();
    names
}

#[test]
fn empty_storage_recovers_nothing() {
    let (_, recovered) = open(SimDisk::new(), StorageOptions::default());
    assert_eq!(recovered.hard_state, HardState::default());
    assert_eq!(recovered.log.last_index(), LogIndex(0));
}

#[test]
fn persisted_state_survives_a_crash() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&ready(Some(hard_state(3, Some(2))), entries(1..=10, 3)))
        .unwrap();

    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.hard_state, hard_state(3, Some(2)));
    assert_eq!(recovered.log.entries(), entries(1..=10, 3).as_slice());
}

#[test]
fn large_logs_roll_over_into_new_segments() {
    let (mut storage, _) = open(SimDisk::new(), small());
    for i in 1..=50 {
        storage.persist(&ready(None, vec![entry(i, 1)])).unwrap();
    }
    let files = segment_files(storage.disk());
    assert!(files.len() > 5, "expected many segments, got {files:?}");
    assert_eq!(files[0], "00000000000000000001.wal");

    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.entries(), entries(1..=50, 1).as_slice());
}

#[test]
fn replacing_a_suffix_across_segments_is_durable() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=30, 1))).unwrap();
    let before = segment_files(storage.disk()).len();

    storage.persist(&ready(None, entries(5..=7, 2))).unwrap();
    assert!(segment_files(storage.disk()).len() < before);

    let (mut storage, recovered) = reopen(storage, small());
    let mut expected = entries(1..=4, 1);
    expected.extend(entries(5..=7, 2));
    assert_eq!(recovered.log.entries(), expected.as_slice());

    // Appending after a reopen continues the log.
    storage.persist(&ready(None, vec![entry(8, 2)])).unwrap();
    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.last_index(), LogIndex(8));
}

#[test]
fn a_torn_write_at_the_end_is_cut_off() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=12, 1))).unwrap();
    let last = segment_files(storage.disk()).pop().unwrap();

    // Half a record, as if the process died mid-write and the data was synced
    // by a later operation of the file system.
    let disk = storage.disk_mut();
    disk.append(&last, &[40, 0, 0, 0, 1, 2, 3, 4, 5]).unwrap();
    disk.sync(&last).unwrap();

    let (mut storage, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.entries(), entries(1..=12, 1).as_slice());

    // The garbage is gone for good: new entries land right after entry 12.
    storage.persist(&ready(None, vec![entry(13, 1)])).unwrap();
    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.last_index(), LogIndex(13));
}

#[test]
fn zero_filled_space_at_the_end_is_cut_off() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=3, 1))).unwrap();
    let last = segment_files(storage.disk()).pop().unwrap();
    let disk = storage.disk_mut();
    disk.append(&last, &[0; 100]).unwrap();
    disk.sync(&last).unwrap();

    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.entries(), entries(1..=3, 1).as_slice());
}

#[test]
fn corruption_in_the_middle_of_the_log_is_an_error() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=30, 1))).unwrap();
    let first = segment_files(storage.disk()).remove(0);

    let disk = storage.disk_mut();
    let mut data = disk.read(&first).unwrap();
    data[20] ^= 0xFF;
    disk.write_new(&first, &data).unwrap();
    disk.sync(&first).unwrap();
    disk.sync_dir().unwrap();

    let mut disk = storage.into_disk();
    disk.crash(&mut |_| 0);
    match Storage::open(disk, small()) {
        Err(StorageError::Corrupt { file, .. }) => assert_eq!(file, first),
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn a_missing_segment_is_an_error() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=30, 1))).unwrap();
    let files = segment_files(storage.disk());
    let disk = storage.disk_mut();
    disk.remove(&files[1]).unwrap();
    disk.sync_dir().unwrap();

    let mut disk = storage.into_disk();
    disk.crash(&mut |_| 0);
    assert!(matches!(
        Storage::open(disk, small()),
        Err(StorageError::Corrupt { .. })
    ));
}

#[test]
fn a_corrupt_hard_state_is_an_error() {
    let mut disk = SimDisk::new();
    disk.write_new("hardstate", &[1, 2, 3]).unwrap();
    disk.sync("hardstate").unwrap();
    disk.sync_dir().unwrap();
    assert!(matches!(
        Storage::open(disk, small()),
        Err(StorageError::Corrupt { .. })
    ));
}

#[test]
fn a_leftover_temporary_hard_state_is_ignored_and_removed() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&ready(Some(hard_state(2, None)), vec![]))
        .unwrap();
    let disk = storage.disk_mut();
    disk.write_new("hardstate.tmp", b"partial").unwrap();
    disk.sync("hardstate.tmp").unwrap();
    disk.sync_dir().unwrap();

    let (storage, recovered) = reopen(storage, small());
    assert_eq!(recovered.hard_state, hard_state(2, None));
    assert!(!storage
        .disk()
        .list()
        .unwrap()
        .contains(&"hardstate.tmp".to_owned()));
}

/// Crashes a write at every possible point and checks that recovery always
/// succeeds and yields either the state before or a valid partial state on
/// the way to the state after, never anything else.
#[test]
fn a_crash_at_any_point_of_a_write_recovers_a_consistent_state() {
    let old_hard_state = hard_state(1, Some(1));
    let old_log = entries(1..=30, 1);
    let new_hard_state = hard_state(2, Some(3));
    let replacement = entries(10..=14, 2);

    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&ready(Some(old_hard_state), old_log.clone()))
        .unwrap();
    let base = storage.into_disk();

    let mut completed = false;
    for ops in 0.. {
        for keep_torn_bytes in [false, true] {
            let mut disk = base.clone();
            disk.fail_after(ops);
            let (mut storage, _) = open(disk, small());
            let result = storage.persist(&ready(Some(new_hard_state), replacement.clone()));
            completed = result.is_ok();

            let mut disk = storage.into_disk();
            disk.crash(&mut |n| if keep_torn_bytes { n } else { 0 });
            let (_, recovered) = Storage::open(disk, small())
                .unwrap_or_else(|e| panic!("crash after {ops} operations: {e}"));

            let log = recovered.log.entries();
            let context =
                format!("crash after {ops} operations, torn bytes kept: {keep_torn_bytes}");
            assert!(
                log.len() >= 9,
                "{context}: lost entries before the replaced part"
            );
            assert_eq!(&log[..9], &old_log[..9], "{context}");
            let rest = &log[9..];
            assert!(
                old_log[9..].starts_with(rest) || replacement.starts_with(rest),
                "{context}: unexpected suffix {rest:?}"
            );
            assert!(
                [old_hard_state, new_hard_state].contains(&recovered.hard_state),
                "{context}"
            );
            // A node must never restart with log entries from a term later
            // than its current term: as leader it would then append entries
            // with a lower term than the ones before them.
            assert!(
                recovered.hard_state.current_term >= recovered.log.last_term(),
                "{context}: term {} but last log term {}",
                recovered.hard_state.current_term,
                recovered.log.last_term()
            );
            if completed {
                assert_eq!(rest, replacement.as_slice(), "{context}");
                assert_eq!(recovered.hard_state, new_hard_state, "{context}");
            }
        }
        if completed {
            break;
        }
    }
}

fn snapshot(index: u64, term: u64) -> Snapshot {
    Snapshot {
        index: LogIndex(index),
        term: Term(term),
        data: format!("state at {index}").into_bytes().into(),
    }
}

fn install(snapshot: Snapshot, entries: Vec<Entry>) -> Ready {
    Ready {
        snapshot: Some(snapshot),
        entries,
        ..Ready::default()
    }
}

/// What `Raft::restore_with_snapshot` requires of a recovered state.
fn assert_continues(recovered: &Recovered, context: &str) {
    if let Some(snapshot) = &recovered.snapshot {
        assert!(
            recovered.log.snapshot_index() <= snapshot.index
                && recovered.log.term_at(snapshot.index) == Some(snapshot.term),
            "{context}: log {:?} does not continue {snapshot:?}",
            recovered.log
        );
    }
}

#[test]
fn a_saved_snapshot_removes_the_segments_before_it() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=40, 1))).unwrap();
    let before = segment_files(storage.disk());

    storage
        .save_snapshot(&snapshot(30, 1), LogIndex(25))
        .unwrap();
    let after = segment_files(storage.disk());
    assert!(after.len() < before.len(), "{before:?} -> {after:?}");
    let first: u64 = after[0].trim_end_matches(".wal").parse().unwrap();
    assert!(
        first <= 25,
        "entry 25 must be kept, first segment is {first}"
    );

    let (mut storage, recovered) = reopen(storage, small());
    assert_eq!(recovered.snapshot, Some(snapshot(30, 1)));
    assert_continues(&recovered, "after reopen");
    // The first stored entry becomes the start of the log: its term is
    // known, and the leader can send everything after it.
    assert!(recovered.log.snapshot_index() <= LogIndex(25));
    assert_eq!(recovered.log.last_index(), LogIndex(40));

    // The log continues as usual.
    storage.persist(&ready(None, entries(41..=45, 2))).unwrap();
    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.last_index(), LogIndex(45));
}

#[test]
fn a_snapshot_from_the_leader_replaces_the_whole_log() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=20, 1))).unwrap();
    // The leader's log differs from entry 15 on.
    storage
        .persist(&install(snapshot(30, 3), entries(31..=33, 3)))
        .unwrap();

    let (mut storage, recovered) = reopen(storage, small());
    assert_eq!(recovered.snapshot, Some(snapshot(30, 3)));
    assert_eq!(recovered.log.snapshot_index(), LogIndex(30));
    assert_eq!(recovered.log.entries(), entries(31..=33, 3).as_slice());
    assert_eq!(segment_files(storage.disk())[0], "00000000000000000031.wal");

    storage.persist(&ready(None, entries(32..=34, 4))).unwrap();
    let (_, recovered) = reopen(storage, small());
    let mut expected = entries(31..=31, 3);
    expected.extend(entries(32..=34, 4));
    assert_eq!(recovered.log.entries(), expected.as_slice());
}

#[test]
fn a_snapshot_with_an_empty_log_after_it_recovers() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&install(snapshot(9, 2), Vec::new()))
        .unwrap();
    let (mut storage, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.last_index(), LogIndex(9));
    assert_eq!(recovered.log.last_term(), Term(2));
    storage.persist(&ready(None, entries(10..=11, 2))).unwrap();
    let (_, recovered) = reopen(storage, small());
    assert_eq!(recovered.log.entries(), entries(10..=11, 2).as_slice());
}

#[test]
fn a_corrupt_snapshot_is_an_error() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&install(snapshot(9, 2), Vec::new()))
        .unwrap();
    let mut disk = storage.into_disk();
    let mut data = disk.read("snapshot").unwrap();
    let last = data.len() - 1;
    data[last] ^= 1;
    disk.write_new("snapshot", &data).unwrap();
    disk.sync("snapshot").unwrap();
    let error = Storage::open(disk, small()).unwrap_err();
    assert!(matches!(error, StorageError::Corrupt { ref file, .. } if file == "snapshot"));
}

#[test]
fn a_log_that_starts_after_the_snapshot_is_an_error() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage
        .persist(&install(snapshot(9, 2), Vec::new()))
        .unwrap();
    storage.persist(&ready(None, entries(10..=40, 2))).unwrap();
    storage
        .save_snapshot(&snapshot(35, 2), LogIndex(35))
        .unwrap();
    // Put the older snapshot back: segments it needs are gone.
    let mut disk = storage.into_disk();
    let old = {
        let (mut other, _) = open(SimDisk::new(), small());
        other.persist(&install(snapshot(9, 2), Vec::new())).unwrap();
        other.into_disk().read("snapshot").unwrap()
    };
    disk.write_new("snapshot", &old).unwrap();
    disk.sync("snapshot").unwrap();
    let error = Storage::open(disk, small()).unwrap_err();
    assert!(
        matches!(error, StorageError::Corrupt { ref reason, .. } if reason.contains("after the snapshot")),
        "{error}"
    );
}

/// Runs `write` against a copy of `base` that fails after every possible
/// number of operations, crashes it, and hands the recovered state to
/// `check` along with whether the write completed.
fn crash_at_every_point(
    base: &SimDisk,
    write: impl Fn(&mut Storage<SimDisk>) -> Result<(), StorageError>,
    check: impl Fn(&Recovered, bool, &str),
) {
    for ops in 0.. {
        let mut disk = base.clone();
        disk.fail_after(ops);
        let (mut storage, _) = open(disk, small());
        let completed = write(&mut storage).is_ok();
        let mut disk = storage.into_disk();
        disk.crash(&mut |n| n);
        let context = format!("crash after {ops} operations");
        let (_, recovered) =
            Storage::open(disk, small()).unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_continues(&recovered, &context);
        check(&recovered, completed, &context);
        if completed {
            break;
        }
    }
}

#[test]
fn a_crash_while_saving_a_snapshot_loses_nothing() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=40, 1))).unwrap();
    let base = storage.into_disk();
    crash_at_every_point(
        &base,
        |storage| storage.save_snapshot(&snapshot(30, 1), LogIndex(30)),
        |recovered, completed, context| {
            assert_eq!(recovered.log.last_index(), LogIndex(40), "{context}");
            match &recovered.snapshot {
                None => {
                    assert!(!completed, "{context}");
                    assert_eq!(recovered.log.entries(), entries(1..=40, 1).as_slice());
                }
                Some(s) => assert_eq!(s, &snapshot(30, 1), "{context}"),
            }
        },
    );
}

#[test]
fn a_crash_while_installing_a_snapshot_never_mixes_logs() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=20, 1))).unwrap();
    let base = storage.into_disk();
    let new_entries = entries(26..=28, 2);
    crash_at_every_point(
        &base,
        |storage| storage.persist(&install(snapshot(25, 2), new_entries.clone())),
        |recovered, completed, context| match &recovered.snapshot {
            None => {
                assert!(!completed, "{context}");
                assert_eq!(recovered.log.entries(), entries(1..=20, 1).as_slice());
            }
            Some(s) => {
                assert_eq!(s, &snapshot(25, 2), "{context}");
                // Nothing of the old log may survive after the snapshot.
                let after = recovered.log.entries_from(LogIndex(26));
                assert!(new_entries.starts_with(after), "{context}: {after:?}");
                if completed {
                    assert_eq!(after, new_entries.as_slice(), "{context}");
                }
            }
        },
    );
}

#[test]
fn a_crash_while_installing_a_matching_snapshot_keeps_a_consistent_log() {
    let (mut storage, _) = open(SimDisk::new(), small());
    storage.persist(&ready(None, entries(1..=20, 1))).unwrap();
    let base = storage.into_disk();
    // The follower has the snapshot's last entry; Raft keeps 16..=20 and
    // hands them out again.
    let kept = entries(16..=20, 1);
    crash_at_every_point(
        &base,
        |storage| storage.persist(&install(snapshot(15, 1), kept.clone())),
        |recovered, completed, context| {
            let after = recovered.log.entries_from(LogIndex(16));
            assert!(kept.starts_with(after), "{context}: {after:?}");
            if completed {
                assert_eq!(after, kept.as_slice(), "{context}");
            }
        },
    );
}

#[test]
fn the_real_file_system_works_too() {
    let dir = tempfile::tempdir().unwrap();
    let options = small();
    {
        let (mut storage, _) = open(FsDisk::open(dir.path()).unwrap(), options);
        storage
            .persist(&ready(Some(hard_state(4, Some(1))), entries(1..=20, 4)))
            .unwrap();
        storage.persist(&ready(None, entries(15..=16, 5))).unwrap();
    }
    let (mut storage, recovered) = open(FsDisk::open(dir.path()).unwrap(), options);
    let mut expected = entries(1..=14, 4);
    expected.extend(entries(15..=16, 5));
    assert_eq!(recovered.log.entries(), expected.as_slice());
    assert_eq!(recovered.hard_state, hard_state(4, Some(1)));

    storage
        .save_snapshot(&snapshot(14, 4), LogIndex(15))
        .unwrap();
    drop(storage);
    let (_, recovered) = open(FsDisk::open(dir.path()).unwrap(), options);
    assert_eq!(recovered.snapshot, Some(snapshot(14, 4)));
    assert_continues(&recovered, "real file system");
    assert_eq!(recovered.log.last_index(), LogIndex(16));
}

/// One step of a random workload: append `count` entries starting at
/// `from` (clamped to a valid position), in a fresh term.
#[derive(Clone, Debug)]
struct Step {
    from: u64,
    count: u64,
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        (1..40u64, 1..8u64).prop_map(|(from, count)| Step { from, count }),
        1..20,
    )
}

proptest! {
    /// Random appends and suffix replacements, then the last segment is cut
    /// at a random byte: recovery must return a prefix of the expected log
    /// that keeps every entry before the cut segment.
    #[test]
    fn random_workloads_recover_from_a_random_cut(steps in steps(), cut in 0..10_000usize) {
        let (mut storage, _) = open(SimDisk::new(), small());
        let mut expected: Vec<Entry> = Vec::new();
        for (term, step) in (1..).zip(&steps) {
            let from = step.from.min(expected.len() as u64 + 1);
            let new = entries(from..=from + step.count - 1, term);
            storage.persist(&ready(None, new.clone())).unwrap();
            expected.truncate(usize::try_from(from - 1).unwrap());
            expected.extend(new);
        }

        let (storage, recovered) = reopen(storage, small());
        prop_assert_eq!(recovered.log.entries(), expected.as_slice());

        let files = segment_files(storage.disk());
        let last = files.last().unwrap().clone();
        let first_in_last: usize = last.trim_end_matches(".wal").parse().unwrap();
        let mut disk = storage.into_disk();
        let data = disk.read(&last).unwrap();
        let cut = cut % (data.len() + 1);
        disk.write_new(&last, &data[..cut]).unwrap();
        disk.sync(&last).unwrap();
        disk.sync_dir().unwrap();

        let (_, recovered) = open(disk, small());
        let log = recovered.log.entries();
        prop_assert!(expected.starts_with(log));
        prop_assert!(log.len() >= first_in_last - 1, "lost entries outside the cut segment");
    }
}
