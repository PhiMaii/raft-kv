# Introduction

raft-kv is a replicated, linearizable key-value store built from scratch on
the Raft consensus algorithm, written in Rust with Tokio and gRPC.

This book collects everything needed to understand and check the system:

- the **[specification](spec.md)**: requirements, guarantees and milestones;
- the **[design overview](design.md)** and the
  **[architecture decision records](adr/README.md)** that explain why the
  system is built the way it is;
- the **[testing methodology](testing.md)**: deterministic simulation,
  linearizability checking and real-process fault injection;
- the **[benchmarks](benchmarks.md)**, each with the command and hardware
  needed to reproduce it;
- the **[bug log](bugs.md)**: every real bug the tests found, with its root
  cause, fix and regression test.

The project is a work in progress. Pages describe the current state of the
code; planned work is marked as such.
