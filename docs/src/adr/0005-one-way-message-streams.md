# 0005: Raft messages as one-way gRPC streams

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Raft is usually described as two RPCs, `RequestVote` and `AppendEntries`,
each with a request and a response. The natural gRPC mapping would be one
unary call per RPC, where the response comes back as the call's result.

`raft-core` does not work that way (see ADR 0002). It consumes messages and
produces messages; a response is just another message, produced by a later
`step` on the receiving node and sent back through `take_ready`. The core
never waits for an answer and does not know which response belongs to which
request, and it does not need to, because every response carries enough
information on its own (term, success, match index).

Mapping messages onto unary calls would mean the transport has to hold every
call open until the core happens to produce the matching response, and has
to match the two up. It would also open a new HTTP/2 stream per message,
which adds overhead and does not keep messages to one peer in order.

## Decision

The Raft service has a single client-streaming method, `Send`. Each node
opens one long-lived stream to every peer and writes its messages for that
peer into it, in order. The receiver passes each message to its local node
and returns nothing per message. A response travels back on the
receiver's own stream to the original sender.

The sending side never blocks the node. Messages wait in a bounded queue
per peer. When a peer is unreachable and the queue fills up, new messages
are dropped. When a connection breaks, messages in flight on it are lost,
and the sender reconnects with exponential backoff.

## Consequences

- The transport is as simple as the core's interface: it moves messages and
  translates types, nothing more.
- Messages to one peer arrive in order within a connection, which avoids
  needless rejections from reordered `AppendEntries`.
- Delivery is best effort. That is safe because Raft is designed for lossy
  networks: a lost message is repaired by the next heartbeat or retry. The
  simulator checks exactly this under far worse conditions (loss,
  duplication, reordering).
- A slow or dead peer cannot stall the leader: its queue fills and further
  messages to it are dropped, while the other peers are unaffected.
- `InstallSnapshot` (milestone 11) carries large chunked payloads and will
  get a stream of its own, so that snapshot chunks do not delay heartbeats.
