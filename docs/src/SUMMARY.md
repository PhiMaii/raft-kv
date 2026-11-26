# Summary

[Introduction](introduction.md)

# Design

- [Specification](spec.md)
- [Design overview](design.md)
- [Architecture decision records](adr/README.md)
  - [0001: Record architecture decisions](adr/0001-record-architecture-decisions.md)
  - [0002: Deterministic, I/O-free Raft core](adr/0002-io-free-raft-core.md)
  - [0003: ReadIndex instead of leader leases](adr/0003-readindex-over-leases.md)
  - [0004: Expire client sessions by leader timestamps](adr/0004-session-expiry-by-leader-timestamps.md)
  - [0005: Raft messages as one-way gRPC streams](adr/0005-one-way-message-streams.md)

# Verification

- [Testing methodology](testing.md)
- [Benchmarks](benchmarks.md)
- [Bug log](bugs.md)
