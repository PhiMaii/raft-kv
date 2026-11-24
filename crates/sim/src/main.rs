//! `raft-kv-sim`: runs many simulation seeds and reports any that fail.
//!
//! ```text
//! raft-kv-sim [--seeds N] [--start S] [--ticks T]   run seeds S..S+N
//! raft-kv-sim --seed S [--ticks T]                  run one seed and print its report
//! ```
//!
//! Build with `--release`: a run is thousands of ticks across five nodes.

use std::process::ExitCode;
use std::time::Instant;

use sim::SimConfig;

struct Args {
    start: u64,
    count: u64,
    single: bool,
    config: SimConfig,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        start: 0,
        count: 1_000,
        single: false,
        config: SimConfig::default(),
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || -> Result<u64, String> {
            let raw = iter.next().ok_or_else(|| format!("{flag} needs a value"))?;
            raw.parse()
                .map_err(|_| format!("{flag}: not a number: {raw}"))
        };
        match flag.as_str() {
            "--seeds" => args.count = value()?,
            "--start" => args.start = value()?,
            "--seed" => {
                args.start = value()?;
                args.count = 1;
                args.single = true;
            }
            "--ticks" => args.config.ticks = value()?,
            "--nodes" => args.config.nodes = value()?,
            "-h" | "--help" => {
                return Err("usage: raft-kv-sim [--seeds N] [--start S] [--seed S] \
                     [--ticks T] [--nodes N]"
                    .to_owned())
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(args)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };

    let started = Instant::now();
    let mut failures = Vec::new();
    let mut committed = 0;
    let mut crashes = 0;
    let mut partitions = 0;

    for seed in args.start..args.start + args.count {
        match sim::run(seed, &args.config) {
            Ok(report) => {
                committed += report.stats.committed_index;
                crashes += report.stats.crashes;
                partitions += report.stats.partitions;
                if args.single {
                    println!("{report:#?}");
                }
            }
            Err(violation) => {
                eprintln!("{violation}\n");
                failures.push(seed);
            }
        }
    }

    println!(
        "{} seeds ({}..{}) in {:.1?}: {} failed; {committed} entries committed, \
         {crashes} crashes, {partitions} partitions",
        args.count,
        args.start,
        args.start + args.count,
        started.elapsed(),
        failures.len(),
    );
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        println!("failing seeds: {failures:?}");
        ExitCode::FAILURE
    }
}
