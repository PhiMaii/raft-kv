#!/usr/bin/env python3
"""Measures how long it takes to make a small append durable.

Usage: scripts/fsync-probe.py [--processes N] [--iterations N] [--dir DIR]

Each process appends 200 bytes to a file of its own and syncs it, over and
over, the way a node syncs its WAL. On macOS the sync is F_FULLFSYNC, which
is what the storage crate uses there; elsewhere it is fsync. Running
several processes at once shows what nodes sharing one disk pay.
"""

import argparse
import fcntl
import multiprocessing
import os
import statistics
import tempfile
import time


def probe(path, iterations, results):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    full_fsync = getattr(fcntl, "F_FULLFSYNC", None)
    times = []
    try:
        for _ in range(iterations):
            os.write(fd, b"x" * 200)
            start = time.perf_counter()
            if full_fsync is not None:
                fcntl.fcntl(fd, full_fsync)
            else:
                os.fsync(fd)
            times.append((time.perf_counter() - start) * 1000)
    finally:
        os.close(fd)
    results.put(sorted(times))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--dir", help="where to write; default: a temporary directory")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(dir=args.dir) as directory:
        results = multiprocessing.Queue()
        workers = [
            multiprocessing.Process(
                target=probe,
                args=(os.path.join(directory, f"probe{i}"), args.iterations, results),
            )
            for i in range(args.processes)
        ]
        for worker in workers:
            worker.start()
        times = sorted(t for _ in workers for t in results.get())
        for worker in workers:
            worker.join()

    sync = "F_FULLFSYNC" if hasattr(fcntl, "F_FULLFSYNC") else "fsync"
    print(
        f"{args.processes} process(es), {sync}: median {statistics.median(times):.2f} ms, "
        f"p99 {times[int(len(times) * 0.99)]:.2f} ms"
    )


if __name__ == "__main__":
    main()
