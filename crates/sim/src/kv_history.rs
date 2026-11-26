//! Linearizability of key-value histories.
//!
//! Keys are independent: an operation on one key never affects another. A
//! history is therefore linearizable exactly if the sub-history of every key
//! is (Herlihy and Wing's locality property), and each key is checked on its
//! own as a single register. This keeps every search small.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};

use kv::{Operation, Response};

use crate::linearizability::{self, CheckResult, Model, Op, Outcome};

/// One operation on the key-value store, as a client recorded it.
pub type KvOp = Op<Operation, Response>;

/// The sequential specification of a single key: a register that may be
/// absent.
#[derive(Clone, Copy, Debug, Default)]
pub struct KvModel;

impl Model for KvModel {
    type State = Option<Vec<u8>>;
    type Input = Operation;
    type Output = Response;

    fn init(&self) -> Self::State {
        None
    }

    fn step(
        &self,
        state: &Self::State,
        input: &Operation,
        output: Option<&Response>,
    ) -> Option<Self::State> {
        let (next, expected) = match input {
            Operation::Get { .. } => (state.clone(), Response::Value(state.clone())),
            Operation::Put { value, .. } => (Some(value.clone()), Response::Written),
            Operation::Delete { .. } => (
                None,
                Response::Deleted {
                    existed: state.is_some(),
                },
            ),
            Operation::CompareAndSwap { expected, new, .. } => {
                let succeeded = state == expected;
                let next = if succeeded {
                    Some(new.clone())
                } else {
                    state.clone()
                };
                (
                    next,
                    Response::Swapped {
                        succeeded,
                        previous: state.clone(),
                    },
                )
            }
        };
        match output {
            Some(output) if *output != expected => None,
            _ => Some(next),
        }
    }
}

/// Why a key-value history failed the check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvCheckError {
    /// The key whose history failed.
    pub key: Vec<u8>,
    /// What went wrong, including the operations involved.
    pub details: String,
}

impl fmt::Display for KvCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key {}: {}", show(&self.key), self.details)
    }
}

impl std::error::Error for KvCheckError {}

/// Checks a key-value history for linearizability, one key at a time.
///
/// `budget` bounds the search steps per key; a key that exceeds it is
/// reported as an error too, so that a run never passes without an answer.
pub fn check_kv_history(history: &[KvOp], budget: u64) -> Result<(), KvCheckError> {
    let mut by_key: BTreeMap<&[u8], Vec<KvOp>> = BTreeMap::new();
    for op in history {
        by_key.entry(op.input.key()).or_default().push(op.clone());
    }
    for (key, ops) in by_key {
        let details = match linearizability::check(&KvModel, &ops, budget) {
            CheckResult::Linearizable => continue,
            CheckResult::Inconclusive => format!(
                "gave up after {budget} search steps on {} operations",
                ops.len()
            ),
            CheckResult::NotLinearizable(counterexample) => {
                let mut text = format!(
                    "not linearizable ({} operations). Longest valid order found \
                     (last 8 shown):",
                    ops.len()
                );
                let shown = counterexample.linearized.len().saturating_sub(8);
                for &i in &counterexample.linearized[shown..] {
                    let _ = write!(text, "\n    {}", describe(&ops[i]));
                }
                let _ = write!(
                    text,
                    "\n  cannot be followed by:\n    {}",
                    describe(&ops[counterexample.blocked])
                );
                text
            }
        };
        return Err(KvCheckError {
            key: key.to_vec(),
            details,
        });
    }
    Ok(())
}

/// One line per operation, for reports: who, what, when, and the result.
fn describe(op: &KvOp) -> String {
    let what = match &op.input {
        Operation::Get { .. } => "get".to_owned(),
        Operation::Put { value, .. } => format!("put {}", show(value)),
        Operation::Delete { .. } => "delete".to_owned(),
        Operation::CompareAndSwap { expected, new, .. } => format!(
            "cas {} -> {}",
            expected
                .as_deref()
                .map_or_else(|| "absent".to_owned(), show),
            show(new)
        ),
    };
    let client = if op.client == u64::MAX {
        "final read".to_owned()
    } else {
        format!("client {}", op.client)
    };
    match &op.outcome {
        Outcome::Returned { at, output } => {
            format!("{client}: {what} [{}..{at}] = {}", op.call, result(output))
        }
        Outcome::Unknown => format!("{client}: {what} [{}..?] = unknown", op.call),
    }
}

fn result(response: &Response) -> String {
    let value = |v: &Option<Vec<u8>>| v.as_deref().map_or_else(|| "absent".to_owned(), show);
    match response {
        Response::Value(v) => value(v),
        Response::Written => "ok".to_owned(),
        Response::Deleted { existed } => format!("deleted (existed: {existed})"),
        Response::Swapped {
            succeeded,
            previous,
        } => format!(
            "{} (was {})",
            if *succeeded { "swapped" } else { "not swapped" },
            value(previous)
        ),
        other => format!("{other:?}"),
    }
}

fn show(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(call: u64, ret: u64, input: Operation, output: Response) -> KvOp {
        Op {
            client: 1,
            call,
            input,
            outcome: Outcome::Returned { at: ret, output },
        }
    }

    fn put(key: &str, value: &str) -> Operation {
        Operation::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    fn get(key: &str) -> Operation {
        Operation::Get { key: key.into() }
    }

    fn value(v: Option<&str>) -> Response {
        Response::Value(v.map(Into::into))
    }

    #[test]
    fn the_model_follows_the_store() {
        let model = KvModel;
        let a = Some(b"a".to_vec());
        let cas = |expected: Option<&str>| Operation::CompareAndSwap {
            key: b"k".to_vec(),
            expected: expected.map(Into::into),
            new: b"b".to_vec(),
        };
        let swapped = |succeeded| Response::Swapped {
            succeeded,
            previous: a.clone(),
        };
        let delete = Operation::Delete { key: b"k".to_vec() };

        assert_eq!(
            model.step(&a, &get("k"), Some(&value(Some("a")))),
            Some(a.clone())
        );
        assert_eq!(model.step(&a, &get("k"), Some(&value(None))), None);
        assert_eq!(
            model.step(&None, &put("k", "a"), Some(&Response::Written)),
            Some(a.clone())
        );
        assert_eq!(
            model.step(&a, &delete, Some(&Response::Deleted { existed: true })),
            Some(None)
        );
        assert_eq!(
            model.step(&a, &delete, Some(&Response::Deleted { existed: false })),
            None
        );
        assert_eq!(
            model.step(&a, &cas(Some("a")), Some(&swapped(true))),
            Some(Some(b"b".to_vec()))
        );
        assert_eq!(
            model.step(&a, &cas(None), Some(&swapped(false))),
            Some(a.clone())
        );
        assert_eq!(model.step(&a, &cas(None), Some(&swapped(true))), None);
        // An unknown result is accepted; the state still follows the input.
        assert_eq!(
            model.step(&a, &cas(Some("a")), None),
            Some(Some(b"b".to_vec()))
        );
    }

    #[test]
    fn keys_are_checked_independently() {
        // Interleaved operations on two keys, each fine on its own.
        let history = [
            op(0, 1, put("x", "1"), Response::Written),
            op(2, 3, put("y", "2"), Response::Written),
            op(4, 5, get("x"), value(Some("1"))),
            op(6, 7, get("y"), value(Some("2"))),
        ];
        assert_eq!(check_kv_history(&history, 1_000), Ok(()));
    }

    #[test]
    fn a_lost_write_is_reported_with_its_key() {
        let history = [
            op(0, 1, put("x", "1"), Response::Written),
            op(2, 3, put("y", "1"), Response::Written),
            op(4, 5, get("y"), value(None)),
        ];
        let error = check_kv_history(&history, 1_000).unwrap_err();
        assert_eq!(error.key, b"y");
        assert!(error.details.contains("not linearizable"), "{error}");
        assert!(error.details.contains("client 1: get [4..5]"), "{error}");
    }

    #[test]
    fn running_out_of_budget_is_an_error() {
        let history = [
            op(0, 1, put("x", "1"), Response::Written),
            op(2, 3, get("x"), value(Some("1"))),
        ];
        let error = check_kv_history(&history, 1).unwrap_err();
        assert!(error.details.contains("gave up"), "{error}");
    }
}
