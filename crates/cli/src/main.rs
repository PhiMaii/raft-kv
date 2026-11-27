//! Command-line entry point: run a node, issue requests, run benchmarks and chaos tests.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use client::{Client, ClientConfig};
use raft_core::Role;
use server::{DataDir, ServerConfig};
use transport::KvClient;

/// A replicated, linearizable key-value store built on Raft.
#[derive(Debug, Parser)]
#[command(name = "raft-kv", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a node.
    Server {
        /// The node's configuration file.
        #[arg(long)]
        config: PathBuf,
        /// Create the data directory if it does not exist. Only for a node
        /// that has never run before: a node that lost its data must not
        /// rejoin under its old id.
        #[arg(long)]
        init: bool,
    },
    /// Read a key.
    Get {
        #[command(flatten)]
        cluster: ClusterArgs,
        /// The key.
        key: String,
    },
    /// Set a key to a value.
    Put {
        #[command(flatten)]
        cluster: ClusterArgs,
        /// The key.
        key: String,
        /// The new value.
        value: String,
    },
    /// Remove a key.
    Del {
        #[command(flatten)]
        cluster: ClusterArgs,
        /// The key.
        key: String,
    },
    /// Set a key to NEW only if its current value matches.
    Cas {
        #[command(flatten)]
        cluster: ClusterArgs,
        /// The key.
        key: String,
        /// The value to set if the comparison matches.
        new: String,
        /// The value the key must currently have.
        #[arg(long, required_unless_present = "absent", conflicts_with = "absent")]
        expected: Option<String>,
        /// Require the key not to exist.
        #[arg(long)]
        absent: bool,
    },
    /// Show every node's role, term and log progress.
    Status {
        #[command(flatten)]
        cluster: ClusterArgs,
    },
    /// Measure throughput and latency against a running cluster.
    Bench {
        #[command(flatten)]
        cluster: ClusterArgs,
        #[command(flatten)]
        options: BenchArgs,
    },
}

#[derive(Debug, Args)]
struct BenchArgs {
    /// Concurrent clients, each with one request in flight.
    #[arg(long, default_value_t = 32)]
    clients: usize,
    /// Seconds to measure, after the warm-up.
    #[arg(long, default_value_t = 20.0)]
    duration: f64,
    /// Seconds to run before measuring.
    #[arg(long, default_value_t = 5.0)]
    warmup: f64,
    /// Percentage of operations that are reads; the rest are writes.
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u8).range(0..=100))]
    reads: u8,
    /// Number of distinct keys.
    #[arg(long, default_value_t = 1_000)]
    keys: u64,
    /// Size of each written value, in bytes.
    #[arg(long, default_value_t = 100)]
    value_size: usize,
    /// Also write the full report, as JSON, to this file.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ClusterArgs {
    /// The nodes' addresses, comma-separated.
    #[arg(
        long,
        env = "RAFT_KV_CLUSTER",
        value_delimiter = ',',
        default_value = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
    )]
    cluster: Vec<String>,
}

impl ClusterArgs {
    fn client(self) -> Client {
        Client::new(self.cluster, ClientConfig::default())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Commands) -> Result<ExitCode, Box<dyn std::error::Error>> {
    match command {
        Commands::Server { config, init } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .with_ansi(std::io::stdout().is_terminal())
                .init();
            let config = ServerConfig::load(&config)?;
            let data_dir = if init {
                DataDir::CreateIfMissing
            } else {
                DataDir::MustExist
            };
            let server = server::start(config, data_dir).await?;
            tokio::select! {
                result = server.wait() => result?,
                _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
            }
            Ok(ExitCode::SUCCESS)
        }
        Commands::Get { cluster, key } => {
            if let Some(value) = cluster.client().get(key.into_bytes()).await? {
                println!("{}", String::from_utf8_lossy(&value));
                Ok(ExitCode::SUCCESS)
            } else {
                eprintln!("(not found)");
                Ok(ExitCode::from(1))
            }
        }
        Commands::Put {
            cluster,
            key,
            value,
        } => {
            cluster
                .client()
                .put(key.into_bytes(), value.into_bytes())
                .await?;
            println!("OK");
            Ok(ExitCode::SUCCESS)
        }
        Commands::Del { cluster, key } => {
            let existed = cluster.client().delete(key.into_bytes()).await?;
            println!("{}", if existed { "deleted" } else { "(not found)" });
            Ok(ExitCode::SUCCESS)
        }
        Commands::Cas {
            cluster,
            key,
            new,
            expected,
            absent: _,
        } => {
            let swapped = cluster
                .client()
                .compare_and_swap(
                    key.into_bytes(),
                    expected.map(String::into_bytes),
                    new.into_bytes(),
                )
                .await?;
            if swapped.succeeded {
                println!("swapped");
                Ok(ExitCode::SUCCESS)
            } else {
                let current = swapped
                    .previous
                    .as_deref()
                    .map_or_else(|| "(not found)".into(), String::from_utf8_lossy);
                println!("not swapped; current value: {current}");
                Ok(ExitCode::from(1))
            }
        }
        Commands::Status { cluster } => {
            status(&cluster.cluster).await;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Bench { cluster, options } => run_bench(cluster, options).await,
    }
}

async fn run_bench(
    cluster: ClusterArgs,
    options: BenchArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let config = bench::BenchConfig {
        cluster: cluster.cluster,
        clients: options.clients,
        duration: Duration::try_from_secs_f64(options.duration)?,
        warmup: Duration::try_from_secs_f64(options.warmup)?,
        read_percent: options.reads,
        keys: options.keys,
        value_size: options.value_size,
    };
    eprintln!(
        "{} clients, {}% reads, {} keys, {} B values: {:.0} s warm-up, then {:.0} s measured",
        config.clients,
        config.read_percent,
        config.keys,
        config.value_size,
        config.warmup.as_secs_f64(),
        config.duration.as_secs_f64()
    );
    let report = bench::run(config).await?;
    print_report(&report);
    if let Some(path) = options.output {
        std::fs::write(&path, serde_json::to_string_pretty(&report)? + "\n")?;
        eprintln!("report written to {}", path.display());
    }
    Ok(ExitCode::SUCCESS)
}

fn print_report(report: &bench::Report) {
    println!("throughput: {:.0} ops/s", report.throughput);
    println!(
        "{:<6} {:>9} {:>10} {:>9} {:>9} {:>9} {:>9}",
        "", "ops", "ops/s", "p50 ms", "p99 ms", "p99.9 ms", "max ms"
    );
    for (name, latency) in [("reads", &report.reads), ("writes", &report.writes)] {
        let ms = |us: u64| Duration::from_micros(us).as_secs_f64() * 1000.0;
        println!(
            "{name:<6} {:>9} {:>10.0} {:>9.2} {:>9.2} {:>9.2} {:>9.2}",
            latency.count,
            latency.throughput,
            ms(latency.p50_us),
            ms(latency.p99_us),
            ms(latency.p999_us),
            ms(latency.max_us)
        );
    }
    if report.errors > 0 {
        println!("errors: {}", report.errors);
    }
}

/// Prints one line per node.
async fn status(addresses: &[String]) {
    println!(
        "{:<22} {:>4} {:<10} {:>6} {:>7} {:>8} {:>8} {:>8}",
        "address", "id", "role", "term", "leader", "commit", "applied", "last"
    );
    for address in addresses {
        let result = match KvClient::connect(&format!("http://{address}")).await {
            Ok(mut client) => client.status().await.map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match result {
            Ok(s) => {
                let role = match s.role {
                    Role::Follower => "follower",
                    Role::Candidate => "candidate",
                    Role::Leader => "leader",
                };
                let leader = s
                    .leader
                    .map_or_else(|| "-".to_owned(), |id| id.0.to_string());
                println!(
                    "{address:<22} {:>4} {role:<10} {:>6} {leader:>7} {:>8} {:>8} {:>8}",
                    s.id.0, s.term.0, s.commit_index.0, s.last_applied.0, s.last_log_index.0
                );
            }
            Err(_) => println!("{address:<22} unreachable"),
        }
    }
}
