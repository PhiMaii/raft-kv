#!/usr/bin/env python3
"""Turns repeated benchmark runs into history records. Called by
`scripts/bench.sh record`; not meant to be run by hand.

Usage: scripts/bench-record.py RUNS_DIR HISTORY_DIR LABEL POWER

RUNS_DIR holds, for every scenario NAME, the reports NAME.run1.json,
NAME.run2.json, ... and NAME.scenario.json (its server settings and cluster
size), plus fsync.json (the calibration). For each scenario, the run with
the median throughput is kept whole, so its percentiles and throughput come
from the same run, and one JSON line is appended to
HISTORY_DIR/SERIES.jsonl. SERIES names the machine: results from another
machine start another file and are never mixed in.
"""

import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone


def git(*args):
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def os_version():
    if sys.platform == "darwin":
        return platform.mac_ver()[0]
    return platform.release()


def series_name(environment):
    """A stable name for the machine: CPU, memory and OS."""
    parts = [environment.get("cpu") or "unknown-cpu"]
    if environment.get("memory_gib"):
        parts.append(f"{environment['memory_gib']}gib")
    parts.append(environment.get("os") or "unknown-os")
    return re.sub(r"[^a-z0-9]+", "-", " ".join(parts).lower()).strip("-")


def main():
    runs_dir, history_dir, label, power = sys.argv[1:5]
    with open(os.path.join(runs_dir, "fsync.json")) as f:
        fsync = json.load(f)
    commit = git("rev-parse", "HEAD")
    date = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    scenarios = sorted(
        name[: -len(".scenario.json")]
        for name in os.listdir(runs_dir)
        if name.endswith(".scenario.json")
    )
    lines_by_series = {}
    for scenario in scenarios:
        with open(os.path.join(runs_dir, f"{scenario}.scenario.json")) as f:
            setup = json.load(f)
        reports = []
        run = 1
        while os.path.exists(path := os.path.join(runs_dir, f"{scenario}.run{run}.json")):
            with open(path) as f:
                reports.append(json.load(f))
            run += 1
        if not reports:
            sys.exit(f"no runs for scenario {scenario}")
        by_throughput = sorted(reports, key=lambda r: r["throughput"])
        median = by_throughput[(len(by_throughput) - 1) // 2]
        environment = median["environment"]
        config = {k: v for k, v in median["config"].items() if k != "cluster"}
        record = {
            "series": series_name(environment),
            "label": label,
            "commit": commit,
            "date": date,
            "machine": {
                "cpu": environment.get("cpu"),
                "cpus": environment.get("cpus"),
                "memory_gib": environment.get("memory_gib"),
                "os": environment.get("os"),
                "os_version": os_version(),
                "arch": environment.get("arch"),
            },
            "power": power,
            "fsync": fsync,
            "scenario": scenario,
            "nodes": setup["nodes"],
            "settings": setup["settings"],
            "config": config,
            "runs": [round(r["throughput"], 1) for r in reports],
            "throughput": median["throughput"],
            "reads": median["reads"],
            "writes": median["writes"],
            "errors": median["errors"],
        }
        lines_by_series.setdefault(record["series"], []).append(
            json.dumps(record, sort_keys=True)
        )

    os.makedirs(history_dir, exist_ok=True)
    for series, lines in lines_by_series.items():
        path = os.path.join(history_dir, f"{series}.jsonl")
        with open(path, "a") as f:
            f.write("\n".join(lines) + "\n")
        print(f"appended {len(lines)} records to {path}")


if __name__ == "__main__":
    main()
