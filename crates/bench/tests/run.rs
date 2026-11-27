//! A short benchmark against three real nodes in this process.

use std::net::TcpListener;
use std::time::Duration;

use bench::BenchConfig;
use server::{DataDir, PeerAddress, ReadMode, ServerConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_run_measures_reads_and_writes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let addresses: Vec<String> = listeners
        .iter()
        .map(|l| l.local_addr().expect("address").to_string())
        .collect();
    drop(listeners);
    let mut servers = Vec::new();
    for id in 1..=3u64 {
        let index = usize::try_from(id - 1).expect("small");
        let config = ServerConfig {
            id,
            listen: addresses[index].parse().expect("address"),
            data_dir: dir.path().join(format!("node{id}")),
            peers: (1..=3u64)
                .filter(|&peer| peer != id)
                .map(|peer| PeerAddress {
                    id: peer,
                    address: addresses[usize::try_from(peer - 1).expect("small")].clone(),
                })
                .collect(),
            tick_ms: 10,
            request_timeout_ms: 2_000,
            read_mode: ReadMode::ReadIndex,
            snapshot_threshold_bytes: 64 << 10,
            retained_entries: 64,
            batch_appends: true,
            max_append_bytes: 1 << 20,
            send_before_persist: true,
        };
        servers.push(
            server::start(config, DataDir::CreateIfMissing)
                .await
                .expect("node starts"),
        );
    }

    let report = bench::run(BenchConfig {
        cluster: addresses,
        clients: 8,
        duration: Duration::from_secs(2),
        warmup: Duration::from_millis(300),
        read_percent: 50,
        keys: 50,
        value_size: 64,
    })
    .await
    .expect("benchmark runs");

    assert_eq!(report.errors, 0);
    assert!(
        report.reads.count > 10 && report.writes.count > 10,
        "{report:?}"
    );
    for latency in [&report.reads, &report.writes] {
        assert!(latency.p50_us > 0);
        assert!(latency.p50_us <= latency.p99_us && latency.p99_us <= latency.p999_us);
        assert!(latency.p999_us <= latency.max_us);
    }
    // About half the operations are reads.
    let reads = report.reads.count * 100 / (report.reads.count + report.writes.count);
    assert!((30..=70).contains(&reads), "{reads}% reads");

    let json = serde_json::to_value(&report).expect("serializes");
    assert_eq!(json["config"]["duration"], 2.0);
    assert!(json["environment"]["os"].is_string());
    for server in &servers {
        server.stop();
    }
}
