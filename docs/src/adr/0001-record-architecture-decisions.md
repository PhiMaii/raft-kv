# 0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

Many choices in a consensus system look arbitrary without their reasoning:
why reads skip the log, why the core does no I/O, why replication is not
pipelined. That reasoning gets lost if it only lives in commit messages.

## Decision

Record each significant design decision as a numbered ADR in `docs/src/adr/`,
with its context, the decision and its consequences.

## Consequences

- Reviewers can follow why the system is built the way it is.
- Changing a recorded decision means writing a new ADR that supersedes the
  old one, which makes reversals visible.
