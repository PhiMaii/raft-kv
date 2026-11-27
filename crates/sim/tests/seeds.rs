//! Runs the simulator over many seeds.
//!
//! `SIM_SEEDS` sets how many random seeds to run (default 200, enough for a
//! quick check in debug builds). The nightly CI job runs far more in release
//! mode through the `raft-kv-sim` binary.

use sim::{FaultConfig, SimConfig, Stats};

fn seed_count() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

fn run_all(seeds: impl IntoIterator<Item = u64>, config: &SimConfig) -> Vec<Stats> {
    let mut failures = Vec::new();
    let mut stats = Vec::new();
    for seed in seeds {
        match sim::run(seed, config) {
            Ok(report) => stats.push(report.stats),
            Err(violation) => failures.push(violation.to_string()),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
    stats
}

#[test]
fn regression_seeds_pass() {
    let seeds = sim::parse_seeds(include_str!("../regressions.txt"));
    run_all(seeds, &SimConfig::default());
}

#[test]
fn random_seeds_pass_under_faults() {
    let stats = run_all(0..seed_count(), &SimConfig::default());

    // Guard against a toothless simulator: across all seeds, every kind of
    // fault must actually have happened, and the cluster must still have
    // made progress.
    let total = |f: fn(&Stats) -> u64| stats.iter().map(f).sum::<u64>();
    assert!(total(|s| s.crashes) > 0, "no crashes injected");
    assert!(total(|s| s.write_crashes) > 0, "no crashes during writes");
    assert!(total(|s| s.bytes_lost) > 0, "no unsynced data lost");
    assert!(total(|s| s.pauses) > 0, "no pauses injected");
    assert!(total(|s| s.partitions) > 0, "no partitions injected");
    assert!(total(|s| s.messages_dropped) > 0, "no messages dropped");
    assert!(
        total(|s| s.messages_duplicated) > 0,
        "no messages duplicated"
    );
    assert!(
        stats.iter().all(|s| s.leaders_elected > 1),
        "every run should see leader changes"
    );
    let mut committed: Vec<u64> = stats.iter().map(|s| s.committed_index).collect();
    committed.sort_unstable();
    let median = committed[committed.len() / 2];
    assert!(
        committed[0] > 50 && median > 150,
        "runs should commit a meaningful number of entries: min {}, median {median}",
        committed[0]
    );

    // The linearizability check needs real histories: operations that
    // complete, and some whose outcome stays unknown.
    let mut completed: Vec<u64> = stats.iter().map(|s| s.operations_completed).collect();
    completed.sort_unstable();
    let median = completed[completed.len() / 2];
    assert!(
        completed[0] > 10 && median > 150,
        "runs should complete client operations: min {}, median {median}",
        completed[0]
    );
    assert!(total(|s| s.operations_unknown) > 0, "no unknown outcomes");
    assert!(total(|s| s.responses_lost) > 0, "no responses lost");

    // Snapshots: nodes compact their logs, and lagging followers catch up
    // through InstallSnapshot.
    assert!(total(|s| s.snapshots_taken) > 0, "no snapshots taken");
    assert!(
        total(|s| s.snapshots_installed) > 0,
        "no snapshots installed"
    );
}

#[test]
fn runs_pass_with_batching_and_early_sends_on_and_off() {
    for (batch_appends, send_before_persist) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let config = SimConfig {
            batch_appends: Some(batch_appends),
            send_before_persist: Some(send_before_persist),
            ..SimConfig::default()
        };
        run_all(4_000..4_000 + seed_count() / 8, &config);
    }
}

#[test]
fn batching_sends_fewer_entries() {
    // Entries sent per committed entry, in tenths.
    let entries_sent = |batch_appends| {
        let config = SimConfig {
            batch_appends: Some(batch_appends),
            faults: FaultConfig::none(),
            request: sim::Chance::percent(100),
            ticks: 1_000,
            ..SimConfig::default()
        };
        let stats = run_all(5_000..5_005, &config);
        let sent: u64 = stats.iter().map(|s| s.entries_sent).sum();
        let committed: u64 = stats.iter().map(|s| s.committed_index).sum();
        sent * 10 / committed
    };
    let (batched, unbatched) = (entries_sent(true), entries_sent(false));
    // Four followers: every entry must reach each of them at least once.
    // Heartbeats and ReadIndex rounds resend a few more. When every answer
    // sent more entries, this was about 600 (see the bug log).
    assert!((40..=120).contains(&batched), "{batched}");
    assert!(unbatched <= 300, "{unbatched}");
    assert!(
        batched * 3 < unbatched * 2,
        "entries sent per committed entry, in tenths: {batched} batched, {unbatched} unbatched"
    );
}

#[test]
fn long_runs_keep_the_log_bounded() {
    let config = SimConfig {
        ticks: 20_000,
        snapshot_threshold: Some(1_000),
        ..SimConfig::default()
    };
    let stats = run_all(3_000..3_004, &config);
    for s in &stats {
        // Without snapshots every node would hold every committed entry.
        assert!(
            s.committed_index > 500 && s.max_log_entries * 5 < s.committed_index,
            "log not bounded: {} entries held, {} committed",
            s.max_log_entries,
            s.committed_index
        );
    }
    assert!(stats.iter().any(|s| s.snapshots_installed > 0));
}

#[test]
fn three_node_clusters_pass() {
    let config = SimConfig {
        nodes: 3,
        ..SimConfig::default()
    };
    run_all(1_000..1_000 + seed_count() / 2, &config);
}

#[test]
fn a_single_node_cluster_passes() {
    let config = SimConfig {
        nodes: 1,
        ticks: 500,
        ..SimConfig::default()
    };
    run_all(0..20, &config);
}

#[test]
fn heavy_message_loss_is_survived() {
    let config = SimConfig {
        faults: FaultConfig {
            drop: sim::Chance::percent(30),
            ..FaultConfig::default()
        },
        settle_ticks: 1_000,
        ..SimConfig::default()
    };
    run_all(2_000..2_000 + seed_count() / 4, &config);
}

#[test]
fn runs_are_deterministic() {
    let config = SimConfig::default();
    for seed in [0, 1, 99, 12_345] {
        let first = sim::run(seed, &config).expect("seed passes");
        let second = sim::run(seed, &config).expect("seed passes");
        assert_eq!(first, second, "seed {seed}");
    }
    let a = sim::run(1, &config).unwrap();
    let b = sim::run(2, &config).unwrap();
    assert_ne!(
        a.fingerprint, b.fingerprint,
        "different seeds, different runs"
    );
}

#[test]
fn seed_files_parse_with_comments() {
    let seeds = sim::parse_seeds("# header\n\n42  # a bug\n  7\n");
    assert_eq!(seeds, vec![42, 7]);
}
