//! Load generator for throughput and latency benchmarks.
//!
//! [`run`] starts a number of clients against a running cluster. Each
//! client has its own session and one request in flight at a time (a
//! closed loop), and picks reads or writes at random in the configured mix,
//! over a fixed set of keys. After a warm-up period whose results are thrown
//! away, every request's latency is recorded in an HDR histogram, one per
//! operation type. The [`Report`] serializes to JSON together with the
//! configuration and a description of the machine it ran on.
//!
//! With a closed loop, throughput is bounded by the number of clients
//! divided by the latency: more clients mean more requests in flight, and
//! with batching, larger batches.

mod environment;

use std::time::{Duration, Instant};

use client::{Client, ClientConfig};
use hdrhistogram::Histogram;
use serde::Serialize;

pub use environment::Environment;

/// What to run.
#[derive(Clone, Debug, Serialize)]
pub struct BenchConfig {
    /// The nodes' addresses.
    pub cluster: Vec<String>,
    /// Concurrent clients, each with one request in flight.
    pub clients: usize,
    /// How long to measure, after the warm-up.
    #[serde(serialize_with = "seconds")]
    pub duration: Duration,
    /// How long to run before measuring.
    #[serde(serialize_with = "seconds")]
    pub warmup: Duration,
    /// Percentage of operations that are reads; the rest are writes.
    pub read_percent: u8,
    /// Number of distinct keys. Every key is written once before the
    /// warm-up, so reads find a value.
    pub keys: u64,
    /// Size of every written value, in bytes.
    pub value_size: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            cluster: Vec::new(),
            clients: 32,
            duration: Duration::from_secs(20),
            warmup: Duration::from_secs(5),
            read_percent: 50,
            keys: 1_000,
            value_size: 100,
        }
    }
}

fn seconds<S: serde::Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64(duration.as_secs_f64())
}

/// Latency statistics for one kind of operation, in microseconds.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Latency {
    /// Completed operations.
    pub count: u64,
    /// Completed operations per second.
    pub throughput: f64,
    /// Mean latency.
    pub mean_us: f64,
    /// Median latency.
    pub p50_us: u64,
    /// 99th percentile.
    pub p99_us: u64,
    /// 99.9th percentile.
    pub p999_us: u64,
    /// Slowest operation.
    pub max_us: u64,
}

/// The result of a run.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// What was run.
    pub config: BenchConfig,
    /// Where it ran.
    pub environment: Environment,
    /// Operations per second, reads and writes together.
    pub throughput: f64,
    /// Reads.
    pub reads: Latency,
    /// Writes.
    pub writes: Latency,
    /// Requests that failed after the client library's retries, during the
    /// measured period.
    pub errors: u64,
}

/// Why a run could not start.
#[derive(Debug)]
pub enum BenchError {
    /// The configuration cannot be run.
    Config(String),
    /// Writing the initial values failed.
    Setup(client::Error),
}

impl std::fmt::Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(reason) => write!(f, "invalid benchmark configuration: {reason}"),
            Self::Setup(error) => write!(f, "could not write the initial values: {error}"),
        }
    }
}

impl std::error::Error for BenchError {}

/// What one client measured.
struct Measured {
    reads: Histogram<u64>,
    writes: Histogram<u64>,
    errors: u64,
}

/// Runs a benchmark against a running cluster.
///
/// # Panics
///
/// Panics if a client task panics, which would be a bug in this crate.
pub async fn run(config: BenchConfig) -> Result<Report, BenchError> {
    if config.cluster.is_empty() || config.clients == 0 || config.keys == 0 {
        return Err(BenchError::Config(
            "needs at least one node, one client and one key".to_owned(),
        ));
    }
    if config.read_percent > 100 || config.duration.is_zero() {
        return Err(BenchError::Config(
            "read_percent must be at most 100 and duration positive".to_owned(),
        ));
    }
    preload(&config).await?;

    let start = Instant::now();
    let measure_from = start + config.warmup;
    let until = measure_from + config.duration;
    let tasks: Vec<_> = (0..config.clients)
        .map(|id| tokio::spawn(client_loop(config.clone(), id as u64, measure_from, until)))
        .collect();
    let mut reads = new_histogram();
    let mut writes = new_histogram();
    let mut errors = 0;
    for task in tasks {
        let measured = task.await.expect("a client task panicked");
        reads.add(&measured.reads).expect("same bounds");
        writes.add(&measured.writes).expect("same bounds");
        errors += measured.errors;
    }
    let seconds = config.duration.as_secs_f64();
    let reads = latency(&reads, seconds);
    let writes = latency(&writes, seconds);
    Ok(Report {
        throughput: reads.throughput + writes.throughput,
        reads,
        writes,
        errors,
        environment: Environment::detect(),
        config,
    })
}

/// Writes every key once, so that reads find values.
async fn preload(config: &BenchConfig) -> Result<(), BenchError> {
    let writers = config.clients.min(16) as u64;
    let tasks: Vec<_> = (0..writers)
        .map(|writer| {
            let config = config.clone();
            tokio::spawn(async move {
                let mut client = Client::new(config.cluster.clone(), ClientConfig::default());
                let value = vec![b'x'; config.value_size];
                for key in (writer..config.keys).step_by(writers as usize) {
                    client.put(key_name(key), value.clone()).await?;
                }
                Ok(())
            })
        })
        .collect();
    for task in tasks {
        task.await
            .expect("a preload task panicked")
            .map_err(BenchError::Setup)?;
    }
    Ok(())
}

async fn client_loop(
    config: BenchConfig,
    id: u64,
    measure_from: Instant,
    until: Instant,
) -> Measured {
    let mut client = Client::new(config.cluster.clone(), ClientConfig::default());
    let mut rng = Xorshift::new(id);
    let mut measured = Measured {
        reads: new_histogram(),
        writes: new_histogram(),
        errors: 0,
    };
    let mut value = vec![0u8; config.value_size];
    loop {
        let started = Instant::now();
        if started >= until {
            return measured;
        }
        let key = key_name(rng.next() % config.keys);
        let read = rng.next() % 100 < u64::from(config.read_percent);
        let result = if read {
            client.get(key).await.map(drop)
        } else {
            // Different bytes every time, so no layer can skip a write.
            for chunk in value.chunks_mut(8) {
                chunk.copy_from_slice(&rng.next().to_le_bytes()[..chunk.len()]);
            }
            client.put(key, value.clone()).await
        };
        let finished = Instant::now();
        // Requests that started during the warm-up do not count, and
        // neither do those still running at the end.
        if started < measure_from || finished > until {
            continue;
        }
        if result.is_err() {
            measured.errors += 1;
            continue;
        }
        let micros = u64::try_from((finished - started).as_micros()).unwrap_or(u64::MAX);
        let histogram = if read {
            &mut measured.reads
        } else {
            &mut measured.writes
        };
        histogram.saturating_record(micros.max(1));
    }
}

fn key_name(key: u64) -> Vec<u8> {
    format!("key{key:08}").into_bytes()
}

/// Latencies from 1 µs to 1 min, with three significant digits.
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid bounds")
}

fn latency(histogram: &Histogram<u64>, seconds: f64) -> Latency {
    let count = histogram.len();
    #[allow(clippy::cast_precision_loss)] // counts far below 2^52
    let throughput = count as f64 / seconds;
    Latency {
        count,
        throughput,
        mean_us: if count == 0 { 0.0 } else { histogram.mean() },
        p50_us: histogram.value_at_quantile(0.50),
        p99_us: histogram.value_at_quantile(0.99),
        p999_us: histogram.value_at_quantile(0.999),
        max_us: histogram.max(),
    }
}

/// A tiny, seeded generator for picking keys and operations; the workload
/// does not need more.
struct Xorshift(u64);

impl Xorshift {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
