# Bug log

Every real bug found by the tests is recorded here, newest first. A bug only
counts once it has a regression test.

## 2026-11-27: Replication answers multiplied into a message storm

- **Found by:** the simulator's new `entries_sent` counter, added to measure
  batching: on some seeds every committed entry was sent about 600 times,
  and a five-node cluster exchanged 375 messages per tick. Present since
  log replication was first written; found before batching was merged.
- **Symptom:** no wrong results, but far more traffic than needed whenever
  a follower was behind: hundreds of thousands of messages in a 15-second
  simulated run instead of about 7,000.
- **Root cause:** the leader answered every successful
  `AppendEntriesResponse` from a follower that was still behind by sending
  it the next entries. Heartbeats and ReadIndex rounds are answered too, so
  each of them started another send-and-answer chain. Chains only end once
  the follower catches up, so under steady load they piled up. Small
  `max_entries_per_append` values made followers stay behind longest.
- **Fix:** the leader sends more only when an answer shows progress (a
  higher match index). An answer that tells it nothing new ends its chain;
  a lost message is resent by the next heartbeat. The same rule already
  applied to `InstallSnapshot` chunks for the same reason.
- **Regression test:** `only_answers_that_show_progress_send_more` in
  `crates/raft-core/tests/batching.rs`, and an upper bound on entries sent
  per committed entry in the simulator test `batching_sends_fewer_entries`.

## 2026-11-26: Every follower restart deposed a healthy leader

- **Found by:** manual testing of the local cluster script: after
  `scripts/cluster.sh kill 1` and `start 1`, the term went up every time.
  Five restarts of a follower caused five elections. Found during
  development, before merge.
- **Symptom:** restarting a follower caused an election, so the cluster
  briefly had no leader and rejected requests, although the leader was
  healthy throughout.
- **Root cause:** while the follower was down, the leader's connection to
  it backed off up to 1 s between reconnection attempts. The restarted
  follower heard nothing for up to a second, longer than its election
  timeout (150–300 ms), so it started an election with a higher term, which
  made the leader step down. Not a safety problem, but a needless loss of
  availability.
- **Fix:** cap the reconnection backoff at 100 ms, below the shortest
  election timeout. Afterwards, five restarts caused no election. A node
  that is slow to start can still trigger one; Raft's PreVote extension
  (dissertation §9.6) would prevent that and is listed as future work.
- **Regression test:** `peer_reconnects_quickly_after_a_long_outage` in
  `crates/transport/tests/localhost.rs`: after a 1.6 s outage the peer must
  reconnect within 150 ms. With the old cap it took 972 ms.

## 2026-11-24: Removed WAL segments could come back after a crash

- **Found by:** code review while writing the storage crash-point test, and
  confirmed by that test
  (`a_crash_at_any_point_of_a_write_recovers_a_consistent_state`): a crash
  after 7 disk operations made recovery fail with *segment starts at i13,
  expected i10*. Found during development, before the code was merged.
- **Symptom:** after a crash in the middle of replacing a log suffix, the
  node refused to start because its WAL had a gap.
- **Root cause:** `Wal::truncate_from` removed the later segments, then
  shortened and synced the current segment, and only then synced the
  directory. A crash before that directory sync brought the removed segments
  back behind an already shortened segment.
- **Fix:** remove later segments and sync the directory first, then shorten
  the current segment. Every crash point now leaves a prefix of the old log.
- **Regression test:** the crash-point test above; the simulator catches the
  old order too (3 of 1,000 seeds, see `scripts/sim-mutations.sh`).

## 2026-11-24: A crash could leave entries from a term later than the node's term

- **Found by:** suspicion during review, confirmed by adding the invariant
  *current term ≥ last log term* to the storage crash-point test, which then
  failed after 9 disk operations. Found during development, before merge.
- **Symptom:** after a crash, a node's log ended with an entry from term 2
  while its hard state said term 1.
- **Root cause:** `Storage::persist` wrote the entries before the hard state.
  A follower that learns of a new term receives the term and that term's
  entries in the same `Ready`.
- **Fix:** persist the hard state first.
- **Impact:** not reachable as a safety violation in Raft, because the stale
  node cannot win an election in any term up to the one it fell behind on;
  the simulator never detects the old order. Fixed anyway, because the
  invariant is cheap to keep and the opposite state is easy to misuse later.
- **Regression test:** the term assertion in the crash-point test.

## 2026-11-24: One large write did not roll over into new segments

- **Found by:** unit tests (`a_missing_segment_is_an_error`,
  `replacing_a_suffix_across_segments_is_durable`), during development.
- **Symptom:** persisting many entries at once put all of them into one
  segment, however large.
- **Root cause:** the rollover check compared only the bytes already written
  to the segment with the size limit, ignoring records still in the write
  buffer.
- **Fix:** count buffered bytes in the rollover check.
- **Regression test:** the two tests above.

## Template

```markdown
## YYYY-MM-DD: <short title>

- **Found by:** simulation seed `<seed>` / chaos run `<id>` / unit test `<name>`
- **Symptom:** what went wrong, as observed
- **Root cause:** why it happened
- **Fix:** what changed (link to the commit or PR)
- **Regression test:** the test or seed that now covers it
```
