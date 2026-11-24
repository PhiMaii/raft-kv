//! A simulated network that loses, duplicates, delays, reorders and
//! partitions messages.

use std::collections::BTreeSet;

use raft_core::{Message, NodeId};

use crate::config::FaultConfig;
use crate::report::Stats;
use crate::rng::SimRng;

#[derive(Clone, Debug)]
struct InFlight {
    deliver_at: u64,
    message: Message,
}

/// Messages in transit, plus the set of links that are currently cut.
#[derive(Clone, Debug, Default)]
pub(crate) struct Network {
    in_flight: Vec<InFlight>,
    /// Directed links `(from, to)` that drop every message. A cut in one
    /// direction only models an asymmetric partition.
    blocked: BTreeSet<(NodeId, NodeId)>,
}

impl Network {
    /// Puts a message on the wire, subject to loss, duplication and delay.
    pub(crate) fn send(
        &mut self,
        now: u64,
        message: Message,
        faults: &FaultConfig,
        rng: &mut SimRng,
        stats: &mut Stats,
    ) {
        stats.messages_sent += 1;
        if self.is_blocked(message.from, message.to) || rng.chance(faults.drop) {
            stats.messages_dropped += 1;
            return;
        }
        if rng.chance(faults.duplicate) {
            stats.messages_duplicated += 1;
            let deliver_at = now + Self::delay(faults, rng);
            self.in_flight.push(InFlight {
                deliver_at,
                message: message.clone(),
            });
        }
        let deliver_at = now + Self::delay(faults, rng);
        self.in_flight.push(InFlight {
            deliver_at,
            message,
        });
    }

    fn delay(faults: &FaultConfig, rng: &mut SimRng) -> u64 {
        if rng.chance(faults.delay_spike) {
            rng.between(faults.spike_delay.0, faults.spike_delay.1)
        } else {
            rng.between(faults.min_delay, faults.max_delay)
        }
    }

    /// Removes and returns the messages due at `now`, in a random order.
    /// Messages across a link that was cut while they were in transit are
    /// lost.
    pub(crate) fn take_due(
        &mut self,
        now: u64,
        rng: &mut SimRng,
        stats: &mut Stats,
    ) -> Vec<Message> {
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.in_flight)
            .into_iter()
            .partition(|m| m.deliver_at <= now);
        self.in_flight = later;

        let mut due: Vec<Message> = due
            .into_iter()
            .map(|m| m.message)
            .filter(|m| {
                let blocked = self.is_blocked(m.from, m.to);
                if blocked {
                    stats.messages_dropped += 1;
                }
                !blocked
            })
            .collect();
        rng.shuffle(&mut due);
        due
    }

    /// Holds a message back until the next tick, for a receiver that is
    /// paused.
    pub(crate) fn hold(&mut self, now: u64, message: Message) {
        self.in_flight.push(InFlight {
            deliver_at: now + 1,
            message,
        });
    }

    pub(crate) fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to))
    }

    pub(crate) fn block(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from, to));
    }

    pub(crate) fn heal(&mut self) {
        self.blocked.clear();
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        !self.blocked.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use raft_core::{MessageBody, Term};

    use super::*;
    use crate::rng::Chance;

    fn message(from: u64, to: u64) -> Message {
        Message {
            from: NodeId(from),
            to: NodeId(to),
            term: Term(1),
            body: MessageBody::RequestVoteResponse { granted: true },
        }
    }

    #[test]
    fn messages_arrive_within_the_delay_window() {
        let mut network = Network::default();
        let mut rng = SimRng::new(1);
        let mut stats = Stats::default();
        let faults = FaultConfig {
            min_delay: 2,
            max_delay: 3,
            ..FaultConfig::none()
        };
        network.send(10, message(1, 2), &faults, &mut rng, &mut stats);

        assert_eq!(network.take_due(11, &mut rng, &mut stats), vec![]);
        let delivered = [12, 13]
            .iter()
            .map(|&t| network.take_due(t, &mut rng, &mut stats).len())
            .sum::<usize>();
        assert_eq!(delivered, 1);
    }

    #[test]
    fn blocked_links_drop_messages_in_one_direction_only() {
        let mut network = Network::default();
        let mut rng = SimRng::new(1);
        let mut stats = Stats::default();
        let faults = FaultConfig::none();
        network.block(NodeId(1), NodeId(2));

        network.send(0, message(1, 2), &faults, &mut rng, &mut stats);
        network.send(0, message(2, 1), &faults, &mut rng, &mut stats);
        let due = network.take_due(1, &mut rng, &mut stats);
        assert_eq!(due, vec![message(2, 1)]);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn a_cut_drops_messages_already_in_transit() {
        let mut network = Network::default();
        let mut rng = SimRng::new(1);
        let mut stats = Stats::default();
        network.send(0, message(1, 2), &FaultConfig::none(), &mut rng, &mut stats);
        network.block(NodeId(1), NodeId(2));
        assert_eq!(network.take_due(1, &mut rng, &mut stats), vec![]);
    }

    #[test]
    fn duplication_delivers_twice() {
        let mut network = Network::default();
        let mut rng = SimRng::new(1);
        let mut stats = Stats::default();
        let faults = FaultConfig {
            duplicate: Chance::percent(100),
            ..FaultConfig::none()
        };
        network.send(0, message(1, 2), &faults, &mut rng, &mut stats);
        assert_eq!(network.take_due(1, &mut rng, &mut stats).len(), 2);
    }
}
