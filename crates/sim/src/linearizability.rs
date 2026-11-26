//! Linearizability checking for recorded operation histories.
//!
//! A history is linearizable if every operation can be placed at a single
//! point in time between its invocation and its completion, so that the
//! operations, executed one after another in that order, produce exactly the
//! results that clients observed.
//!
//! [`check`] implements the algorithm of Wing and Gong with the memoization
//! by Lowe ("Testing for linearizability", 2017), the same approach as the
//! Porcupine checker. It searches for a valid order depth first, and caches
//! every `(set of placed operations, model state)` pair it has already
//! explored, so that a state reached along two different orders is only
//! explored once.
//!
//! Operations whose outcome a client never learned (the request timed out,
//! or its session expired) may or may not have taken effect. They are
//! treated as completing at the end of time: the search may place them
//! anywhere after their invocation, including after everything else, which
//! is the same as never.
//!
//! The search is exponential in the worst case. Callers keep it small by
//! splitting the history into independent parts first; for a key-value
//! store, every key is its own register (see [`crate::check_kv_history`]).

use std::collections::HashSet;
use std::hash::Hash;

/// A sequential specification of the system under test.
pub trait Model {
    /// The model's state.
    type State: Clone + Eq + Hash;
    /// What a client asked for.
    type Input;
    /// What the client was told.
    type Output;

    /// The state before any operation.
    fn init(&self) -> Self::State;

    /// Applies `input` to `state`. Returns the new state if `output` is what
    /// the operation returns in `state`, or `None` if it is not. An `output`
    /// of `None` means the result is unknown and any result is accepted.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Option<Self::State>;
}

/// How an operation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<O> {
    /// The client received `output` at time `at`.
    Returned {
        /// When the client received the result. Must not be earlier than
        /// the operation's invocation.
        at: u64,
        /// The result.
        output: O,
    },
    /// The client never learned the result. The operation may or may not
    /// have taken effect.
    Unknown,
}

/// One operation in a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op<I, O> {
    /// The client that issued the operation. Only used in reports.
    pub client: u64,
    /// When the client invoked the operation.
    pub call: u64,
    /// What the client asked for.
    pub input: I,
    /// How the operation ended.
    pub outcome: Outcome<O>,
}

/// The result of [`check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckResult {
    /// A valid order exists.
    Linearizable,
    /// No valid order exists.
    NotLinearizable(Counterexample),
    /// The search gave up after its step budget without an answer.
    Inconclusive,
}

/// Where the search got stuck, to help explain why a history is not
/// linearizable.
///
/// It is the longest partial order the search found: `linearized` lists
/// operations (as indices into the history) in an order that is valid so
/// far, and `blocked` is an operation that must come next, because it
/// completed before any remaining operation was invoked, but cannot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counterexample {
    /// The longest valid prefix of a linearization.
    pub linearized: Vec<usize>,
    /// The operation that could not be placed after that prefix.
    pub blocked: usize,
}

const NIL: usize = usize::MAX;

#[derive(Clone, Copy, Debug)]
struct Event {
    op: usize,
    /// For a call event, the position of the matching return event.
    /// `None` for return events.
    ret: Option<usize>,
}

/// The history's call and return events in time order, as a doubly linked
/// list. Placing an operation unlinks its two events; backtracking links them
/// again in reverse order ("dancing links").
struct EventList {
    events: Vec<Event>,
    next: Vec<usize>,
    prev: Vec<usize>,
    head: usize,
}

impl EventList {
    fn new<I, O>(history: &[Op<I, O>]) -> Self {
        // (time, is_return, op): at equal times calls come first, so that
        // operations that touch at a single instant count as concurrent.
        let mut order: Vec<(u64, bool, usize)> = Vec::with_capacity(history.len() * 2);
        for (i, op) in history.iter().enumerate() {
            let ret = match op.outcome {
                Outcome::Returned { at, .. } => {
                    debug_assert!(at >= op.call, "operation {i} returns before its call");
                    at
                }
                Outcome::Unknown => u64::MAX,
            };
            order.push((op.call, false, i));
            order.push((ret, true, i));
        }
        order.sort_unstable();

        let mut ret_position = vec![0; history.len()];
        for (position, &(_, is_return, op)) in order.iter().enumerate() {
            if is_return {
                ret_position[op] = position;
            }
        }
        let events: Vec<Event> = order
            .iter()
            .map(|&(_, is_return, op)| Event {
                op,
                ret: (!is_return).then(|| ret_position[op]),
            })
            .collect();

        // Event i links to i + 1 and the last one to NIL. The head sentinel
        // sits at index n and links to event 0.
        let n = events.len();
        let head = n;
        let mut next: Vec<usize> = (1..n).chain([NIL]).collect();
        next.truncate(n);
        next.push(if n == 0 { NIL } else { 0 });
        let mut prev: Vec<usize> = (0..n).map(|i| if i == 0 { head } else { i - 1 }).collect();
        prev.push(NIL);

        Self {
            events,
            next,
            prev,
            head,
        }
    }

    fn first(&self) -> usize {
        self.next[self.head]
    }

    fn unlink(&mut self, x: usize) {
        let (prev, next) = (self.prev[x], self.next[x]);
        self.next[prev] = next;
        if next != NIL {
            self.prev[next] = prev;
        }
    }

    /// Links `x` back in where it was. Only valid in reverse unlink order.
    fn relink(&mut self, x: usize) {
        let (prev, next) = (self.prev[x], self.next[x]);
        self.next[prev] = x;
        if next != NIL {
            self.prev[next] = x;
        }
    }

    /// Removes a call event and its return event.
    fn lift(&mut self, call: usize, ret: usize) {
        self.unlink(call);
        self.unlink(ret);
    }

    fn unlift(&mut self, call: usize, ret: usize) {
        self.relink(ret);
        self.relink(call);
    }
}

/// A fixed-size set of operation indices.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Bitset(Vec<u64>);

impl Bitset {
    fn new(len: usize) -> Self {
        Self(vec![0; len.div_ceil(64)])
    }

    fn set(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }

    fn clear(&mut self, i: usize) {
        self.0[i / 64] &= !(1 << (i % 64));
    }
}

/// Checks whether `history` is linearizable with respect to `model`.
///
/// `budget` limits how many search steps may be taken; past it the result
/// is [`CheckResult::Inconclusive`].
pub fn check<M: Model>(model: &M, history: &[Op<M::Input, M::Output>], budget: u64) -> CheckResult {
    let mut list = EventList::new(history);
    let mut state = model.init();
    let mut placed = Bitset::new(history.len());
    let mut cache: HashSet<(Bitset, M::State)> = HashSet::new();
    // Placed operations: (call event, return event, state before it).
    let mut stack: Vec<(usize, usize, M::State)> = Vec::new();
    let mut best = Counterexample {
        linearized: Vec::new(),
        blocked: 0,
    };
    let mut steps = 0;

    let mut entry = list.first();
    while list.first() != NIL {
        steps += 1;
        if steps > budget {
            return CheckResult::Inconclusive;
        }
        let event = list.events[entry];
        if let Some(ret) = event.ret {
            // A call: try to place this operation next.
            let op = &history[event.op];
            let output = match &op.outcome {
                Outcome::Returned { output, .. } => Some(output),
                Outcome::Unknown => None,
            };
            if let Some(next_state) = model.step(&state, &op.input, output) {
                let mut next_placed = placed.clone();
                next_placed.set(event.op);
                if cache.insert((next_placed.clone(), next_state.clone())) {
                    stack.push((entry, ret, std::mem::replace(&mut state, next_state)));
                    placed = next_placed;
                    list.lift(entry, ret);
                    entry = list.first();
                    continue;
                }
            }
            entry = list.next[entry];
        } else {
            // A return: this operation must have been placed by now, so the
            // current prefix is a dead end. Undo the last placement and try
            // the operation after it instead.
            if stack.len() >= best.linearized.len() {
                best = Counterexample {
                    linearized: stack.iter().map(|&(e, _, _)| list.events[e].op).collect(),
                    blocked: event.op,
                };
            }
            let Some((call, ret, previous)) = stack.pop() else {
                return CheckResult::NotLinearizable(best);
            };
            state = previous;
            placed.clear(list.events[call].op);
            list.unlift(call, ret);
            entry = list.next[call];
        }
    }
    CheckResult::Linearizable
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single integer register: `Some(v)` writes `v`, `None` reads.
    struct Register;

    impl Model for Register {
        type State = u64;
        type Input = Option<u64>;
        type Output = u64;

        fn init(&self) -> u64 {
            0
        }

        fn step(&self, state: &u64, input: &Option<u64>, output: Option<&u64>) -> Option<u64> {
            match input {
                Some(value) => Some(*value),
                None => output.is_none_or(|o| o == state).then_some(*state),
            }
        }
    }

    fn write(client: u64, call: u64, ret: u64, value: u64) -> Op<Option<u64>, u64> {
        Op {
            client,
            call,
            input: Some(value),
            outcome: Outcome::Returned { at: ret, output: 0 },
        }
    }

    fn read(client: u64, call: u64, ret: u64, value: u64) -> Op<Option<u64>, u64> {
        Op {
            client,
            call,
            input: None,
            outcome: Outcome::Returned {
                at: ret,
                output: value,
            },
        }
    }

    fn unknown_write(client: u64, call: u64, value: u64) -> Op<Option<u64>, u64> {
        Op {
            client,
            call,
            input: Some(value),
            outcome: Outcome::Unknown,
        }
    }

    fn is_linearizable(history: &[Op<Option<u64>, u64>]) -> bool {
        match check(&Register, history, 1_000_000) {
            CheckResult::Linearizable => true,
            CheckResult::NotLinearizable(_) => false,
            CheckResult::Inconclusive => panic!("budget exceeded"),
        }
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert!(is_linearizable(&[]));
    }

    #[test]
    fn sequential_history_follows_the_model() {
        assert!(is_linearizable(&[
            write(1, 0, 1, 5),
            read(2, 2, 3, 5),
            write(1, 4, 5, 7),
            read(2, 6, 7, 7),
        ]));
        assert!(!is_linearizable(&[write(1, 0, 1, 5), read(2, 2, 3, 0)]));
    }

    #[test]
    fn concurrent_operations_may_take_effect_in_either_order() {
        // The read overlaps the write, so it may see the old or new value.
        assert!(is_linearizable(&[write(1, 0, 10, 5), read(2, 1, 2, 0)]));
        assert!(is_linearizable(&[write(1, 0, 10, 5), read(2, 1, 2, 5)]));
    }

    #[test]
    fn a_read_cannot_go_back_in_time() {
        // Once one read saw the new value, a later read must not see the
        // old one, even though both overlap the write.
        assert!(!is_linearizable(&[
            write(1, 0, 10, 5),
            read(2, 1, 2, 5),
            read(3, 3, 4, 0),
        ]));
        assert!(is_linearizable(&[
            write(1, 0, 10, 5),
            read(2, 1, 2, 0),
            read(3, 3, 4, 5),
        ]));
    }

    #[test]
    fn a_completed_write_must_be_visible() {
        // A stale read after the write completed: the classic stale-leader
        // bug.
        assert!(!is_linearizable(&[
            write(1, 0, 1, 1),
            write(1, 2, 3, 2),
            read(2, 4, 5, 1),
        ]));
    }

    #[test]
    fn writes_overlapping_two_reads_are_ordered_consistently() {
        // Two concurrent writes, and two later reads that disagree about
        // which came last: not linearizable.
        assert!(!is_linearizable(&[
            write(1, 0, 5, 1),
            write(2, 0, 5, 2),
            read(3, 6, 7, 1),
            read(4, 8, 9, 2),
        ]));
        // Reads that overlap the writes may disagree.
        assert!(is_linearizable(&[
            write(1, 0, 5, 1),
            write(2, 0, 5, 2),
            read(3, 1, 2, 1),
            read(4, 3, 4, 2),
        ]));
    }

    #[test]
    fn unknown_operations_may_or_may_not_take_effect() {
        // The write's result was never seen. It may have happened...
        assert!(is_linearizable(&[
            unknown_write(1, 0, 5),
            read(2, 10, 11, 5)
        ]));
        // ...or not.
        assert!(is_linearizable(&[
            unknown_write(1, 0, 5),
            read(2, 10, 11, 0)
        ]));
        // But not before it was invoked.
        assert!(!is_linearizable(&[
            read(2, 0, 1, 5),
            unknown_write(1, 2, 5)
        ]));
        // And once observed, it cannot be undone.
        assert!(!is_linearizable(&[
            unknown_write(1, 0, 5),
            read(2, 10, 11, 5),
            read(2, 12, 13, 0),
        ]));
    }

    #[test]
    fn counterexample_names_the_blocked_operation() {
        let history = [write(1, 0, 1, 1), read(2, 2, 3, 9)];
        let CheckResult::NotLinearizable(counterexample) = check(&Register, &history, 1_000) else {
            panic!("history is not linearizable");
        };
        assert_eq!(
            counterexample,
            Counterexample {
                linearized: vec![0],
                blocked: 1,
            }
        );
    }

    #[test]
    fn many_concurrent_clients_finish_quickly() {
        // 8 clients, each writing its own values and reading back, all
        // overlapping. Without memoization this would take far too long.
        let mut history = Vec::new();
        let mut time = 0;
        let mut last = 0;
        for round in 0..30 {
            for client in 0..8 {
                let value = round * 8 + client + 1;
                history.push(write(client, time, time + 20, value));
                time += 1;
                last = value;
            }
            history.push(read(99, time + 20, time + 21, last));
            time += 22;
        }
        assert_eq!(
            check(&Register, &history, 10_000_000),
            CheckResult::Linearizable
        );
    }

    /// The definition, checked literally: some permutation of all operations
    /// respects real-time order and follows the model. Unknown operations
    /// complete at the end of time.
    fn brute_force(history: &[Op<Option<u64>, u64>]) -> bool {
        fn ret(op: &Op<Option<u64>, u64>) -> u64 {
            match op.outcome {
                Outcome::Returned { at, .. } => at,
                Outcome::Unknown => u64::MAX,
            }
        }
        fn search(history: &[Op<Option<u64>, u64>], used: &mut Vec<bool>, state: u64) -> bool {
            if used.iter().all(|&u| u) {
                return true;
            }
            for i in 0..history.len() {
                // `i` may go next only if no remaining operation returned
                // before `i` was called.
                let blocked = (0..history.len())
                    .any(|j| j != i && !used[j] && ret(&history[j]) < history[i].call);
                if used[i] || blocked {
                    continue;
                }
                let output = match &history[i].outcome {
                    Outcome::Returned { output, .. } => Some(output),
                    Outcome::Unknown => None,
                };
                if let Some(next) = Register.step(&state, &history[i].input, output) {
                    used[i] = true;
                    if search(history, used, next) {
                        return true;
                    }
                    used[i] = false;
                }
            }
            false
        }
        search(history, &mut vec![false; history.len()], 0)
    }

    #[test]
    fn agrees_with_brute_force_on_random_histories() {
        let mut rng = crate::rng::SimRng::new(7);
        let (mut linearizable, mut not) = (0, 0);
        for _ in 0..3_000 {
            let len = 1 + rng.index(6);
            let history: Vec<_> = (0..len)
                .map(|client| {
                    let call = rng.below(12);
                    let ret = call + rng.below(6);
                    // Values from a small range, so that reads often match.
                    let value = rng.below(3);
                    match rng.below(5) {
                        0 => unknown_write(client as u64, call, value),
                        1 | 2 => write(client as u64, call, ret, value),
                        _ => read(client as u64, call, ret, value),
                    }
                })
                .collect();
            let expected = brute_force(&history);
            assert_eq!(is_linearizable(&history), expected, "{history:#?}");
            if expected {
                linearizable += 1;
            } else {
                not += 1;
            }
        }
        // Both answers must be well represented for the comparison to mean
        // anything.
        assert!(linearizable > 500 && not > 500, "{linearizable} vs {not}");
    }

    #[test]
    fn the_budget_is_respected() {
        let history = [write(1, 0, 1, 1), read(2, 2, 3, 1)];
        assert_eq!(check(&Register, &history, 1), CheckResult::Inconclusive);
    }
}
