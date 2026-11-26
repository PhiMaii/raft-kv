//! A five-node cluster of real `raft-kv server` processes, driven through
//! the command-line client.

use std::fmt::Write as _;
use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_raft-kv");
const NODES: usize = 5;

/// A server process, killed with SIGKILL when dropped.
struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Cluster {
    dir: tempfile::TempDir,
    addresses: Vec<String>,
    processes: Vec<Option<Process>>,
}

/// One row of `raft-kv status`.
#[derive(Debug)]
struct Row {
    id: u64,
    role: String,
    applied: u64,
}

impl Cluster {
    fn start() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let listeners: Vec<_> = (0..NODES)
            .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind"))
            .collect();
        let addresses: Vec<String> = listeners
            .iter()
            .map(|l| l.local_addr().expect("address").to_string())
            .collect();
        drop(listeners);
        for id in 1..=NODES {
            let mut config = format!(
                "id = {id}\nlisten = \"{}\"\ndata_dir = \"{}\"\n",
                addresses[id - 1],
                dir.path().join(format!("node{id}")).display()
            );
            for peer in (1..=NODES).filter(|&peer| peer != id) {
                writeln!(
                    config,
                    "\n[[peers]]\nid = {peer}\naddress = \"{}\"",
                    addresses[peer - 1]
                )
                .expect("write to string");
            }
            std::fs::write(config_path(dir.path(), id), config).expect("write config");
        }
        let mut cluster = Self {
            dir,
            addresses,
            processes: (0..NODES).map(|_| None).collect(),
        };
        for id in 1..=NODES {
            cluster.spawn(id, true);
        }
        cluster
    }

    fn spawn(&mut self, id: usize, init: bool) {
        let log = File::create(self.dir.path().join(format!("node{id}.log"))).expect("log file");
        let mut command = Command::new(BIN);
        command
            .args(["server", "--config"])
            .arg(config_path(self.dir.path(), id))
            .stdout(log.try_clone().expect("log file"))
            .stderr(log);
        if init {
            command.arg("--init");
        }
        self.processes[id - 1] = Some(Process(command.spawn().expect("spawn server")));
    }

    fn kill(&mut self, id: usize) {
        self.processes[id - 1] = None;
    }

    fn cli(&self, args: &[&str]) -> Output {
        Command::new(BIN)
            .args(args)
            .env("RAFT_KV_CLUSTER", self.addresses.join(","))
            .output()
            .expect("run raft-kv")
    }

    /// Runs a command that must succeed and returns its output.
    fn ok(&self, args: &[&str]) -> String {
        let output = self.cli(args);
        assert!(
            output.status.success(),
            "raft-kv {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("UTF-8")
            .trim()
            .to_owned()
    }

    /// The reachable nodes' status rows.
    fn status(&self) -> Vec<Row> {
        self.ok(&["status"])
            .lines()
            .skip(1)
            .filter_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                (fields.len() == 8).then(|| Row {
                    id: fields[1].parse().expect("id"),
                    role: fields[2].to_owned(),
                    applied: fields[6].parse().expect("applied"),
                })
            })
            .collect()
    }

    /// Waits until `condition` holds for the status rows.
    fn wait_for(&self, what: &str, condition: impl Fn(&[Row]) -> bool) -> Vec<Row> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let rows = self.status();
            if condition(&rows) {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {rows:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

fn config_path(dir: &Path, id: usize) -> PathBuf {
    dir.join(format!("node{id}.toml"))
}

fn leaders(rows: &[Row]) -> Vec<u64> {
    rows.iter()
        .filter(|row| row.role == "leader")
        .map(|row| row.id)
        .collect()
}

#[test]
fn five_processes_form_a_cluster_and_survive_a_leader_crash() {
    let mut cluster = Cluster::start();
    let rows = cluster.wait_for("a leader among five nodes", |rows| {
        rows.len() == NODES && leaders(rows).len() == 1
    });
    let leader = usize::try_from(leaders(&rows)[0]).expect("small");

    assert_eq!(cluster.ok(&["put", "a", "1"]), "OK");
    assert_eq!(cluster.ok(&["get", "a"]), "1");
    assert_eq!(cluster.ok(&["cas", "a", "2", "--expected", "1"]), "swapped");
    let failed = cluster.cli(&["cas", "a", "3", "--absent"]);
    assert_eq!(failed.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&failed.stdout).trim(),
        "not swapped; current value: 2"
    );
    assert_eq!(cluster.ok(&["del", "a"]), "deleted");
    assert_eq!(cluster.cli(&["get", "a"]).status.code(), Some(1));
    assert_eq!(cluster.ok(&["put", "b", "before"]), "OK");

    // Kill the leader with SIGKILL. The other four elect a new one and
    // keep every acknowledged write.
    cluster.kill(leader);
    assert_eq!(cluster.ok(&["put", "c", "after"]), "OK");
    assert_eq!(cluster.ok(&["get", "b"]), "before");
    let rows = cluster.wait_for("a new leader among four nodes", |rows| {
        rows.len() == NODES - 1 && leaders(rows).len() == 1
    });
    let applied = rows.iter().map(|row| row.applied).max().expect("rows");

    // Restart it from its data directory: it rejoins as a follower and
    // catches up.
    cluster.spawn(leader, false);
    cluster.wait_for("the old leader to catch up", |rows| {
        rows.iter()
            .any(|row| row.id == leader as u64 && row.applied >= applied)
    });
    assert_eq!(cluster.ok(&["get", "c"]), "after");
}

#[test]
fn a_node_without_its_data_refuses_to_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = dir.path().join("node.toml");
    std::fs::write(
        &config,
        format!(
            "id = 1\nlisten = \"127.0.0.1:0\"\ndata_dir = \"{}\"\npeers = []\n",
            dir.path().join("lost").display()
        ),
    )
    .expect("write config");
    let output = Command::new(BIN)
        .args(["server", "--config"])
        .arg(&config)
        .stdout(Stdio::null())
        .output()
        .expect("run raft-kv");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("does not exist"), "{stderr}");
    assert!(!dir.path().join("lost").exists());
}
