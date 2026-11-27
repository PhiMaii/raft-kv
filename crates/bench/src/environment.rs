//! A description of the machine a benchmark ran on, for its report.

use std::process::Command;

use serde::Serialize;

/// Where a benchmark ran. Gathered on a best-effort basis: fields that
/// cannot be determined are `None`.
#[derive(Clone, Debug, Serialize)]
pub struct Environment {
    /// Operating system, as Rust names it (`macos`, `linux`, …).
    pub os: &'static str,
    /// CPU architecture.
    pub arch: &'static str,
    /// The CPU model.
    pub cpu: Option<String>,
    /// Logical CPUs available to the process.
    pub cpus: Option<usize>,
    /// Physical memory, in GiB.
    pub memory_gib: Option<u64>,
    /// The commit the benchmark binary was built from, if run in a
    /// checkout.
    pub git_commit: Option<String>,
    /// When the run finished, in seconds since the Unix epoch.
    pub timestamp: u64,
}

impl Environment {
    /// Looks at the current machine.
    pub fn detect() -> Self {
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpu: cpu_model(),
            cpus: std::thread::available_parallelism().ok().map(Into::into),
            memory_gib: memory_bytes().map(|bytes| bytes >> 30),
            git_commit: output("git", &["rev-parse", "--short", "HEAD"]),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        }
    }
}

fn cpu_model() -> Option<String> {
    if cfg!(target_os = "macos") {
        return output("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split(':').nth(1))
        .map(|model| model.trim().to_owned())
}

fn memory_bytes() -> Option<u64> {
    if cfg!(target_os = "macos") {
        return output("sysctl", &["-n", "hw.memsize"])?.parse().ok();
    }
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = info
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(kib * 1024)
}

/// The trimmed standard output of a command, if it succeeds.
fn output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!text.is_empty()).then_some(text)
}
