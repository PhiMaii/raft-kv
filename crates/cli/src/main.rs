//! Command-line entry point: run a node, issue requests, run benchmarks and chaos tests.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;

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
