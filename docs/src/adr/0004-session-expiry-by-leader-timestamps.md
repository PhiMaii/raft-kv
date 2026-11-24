# 0004: Expire client sessions by leader timestamps

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

Client sessions deduplicate retried requests: the state machine remembers
each client's latest request number and response. Without a limit, the
session table grows forever, because clients come and go without saying
goodbye. Sessions have to expire.

Expiry changes the state machine (a request from an expired session is
rejected instead of applied), so every node must expire exactly the same
sessions at exactly the same point in the log. Using each node's own clock
would break that: nodes apply entries at different times and their clocks
drift, so one node could still accept a request that another rejects, and
their states would diverge.

## Decision

The leader stamps every command with its wall-clock time when it accepts the
proposal. The timestamp is part of the log entry. The state machine keeps a
logical clock: the highest timestamp seen so far, which never moves
backwards, even if a new leader's clock is behind the previous one. A session
expires when this clock passes its last activity plus the session timeout
(5 minutes by default). Expiry is checked while applying entries, so it
happens at the same log position on every node.

This follows section 6.3 of Diego Ongaro's dissertation.

## Consequences

- All nodes agree on which sessions exist at every log index.
- Expiry is only as accurate as the leaders' clocks, which is fine for a
  timeout measured in minutes. Correctness does not depend on clock
  accuracy: a wrong clock can only expire a session too early or too late.
- A client whose session expired gets `SessionExpired` and must register
  again. It cannot tell whether its last request was applied; it has to
  treat that request's outcome as unknown.
- An idle cluster expires nothing until the next command arrives, because
  the logical clock only moves with new entries.
