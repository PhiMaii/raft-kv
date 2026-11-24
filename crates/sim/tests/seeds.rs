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
    assert!(
        stats.iter().all(|s| s.committed_index > 100),
        "every run should commit a meaningful number of entries"
    );
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
