//! Behaviour of the key-value state machine: operations, sessions,
//! deduplication, expiry, limits and determinism.

use std::collections::BTreeMap;

use kv::{ClientId, Command, Envelope, KvConfig, KvStore, Operation, Rejection, Response};
use proptest::prelude::*;

/// Drives a store like the Raft log would: consecutive indices, one
/// timestamp per entry.
struct Log {
    store: KvStore,
    next_index: u64,
    now_ms: u64,
}

impl Log {
    fn new() -> Self {
        Self::with_config(KvConfig::default())
    }

    fn with_config(config: KvConfig) -> Self {
        Self {
            store: KvStore::new(config),
            next_index: 1,
            now_ms: 1_000,
        }
    }

    fn apply(&mut self, command: Command) -> Response {
        let bytes = Envelope {
            timestamp_ms: self.now_ms,
            command,
        }
        .encode();
        let index = self.next_index;
        self.next_index += 1;
        self.store.apply(index, &bytes)
    }

    fn register(&mut self) -> ClientId {
        match self.apply(Command::RegisterClient) {
            Response::Registered { client_id } => client_id,
            other => panic!("expected a registration, got {other:?}"),
        }
    }

    fn request(&mut self, client_id: ClientId, sequence: u64, operation: Operation) -> Response {
        self.apply(Command::Request {
            client_id,
            sequence,
            operation,
        })
    }
}

fn put(key: &str, value: &str) -> Operation {
    Operation::Put {
        key: key.into(),
        value: value.into(),
    }
}

fn cas(key: &str, expected: Option<&str>, new: &str) -> Operation {
    Operation::CompareAndSwap {
        key: key.into(),
        expected: expected.map(Into::into),
        new: new.into(),
    }
}

fn get(key: &str) -> Operation {
    Operation::Get { key: key.into() }
}

#[test]
fn registration_uses_the_log_index_as_client_id() {
    let mut log = Log::new();
    log.next_index = 41;
    assert_eq!(log.register(), ClientId(41));
    assert_eq!(log.register(), ClientId(42));
    assert_eq!(log.store.session_count(), 2);
}

#[test]
fn put_get_and_delete() {
    let mut log = Log::new();
    let c = log.register();
    assert_eq!(log.request(c, 1, put("a", "1")), Response::Written);
    assert_eq!(log.store.get(b"a"), Some(&b"1"[..]));
    assert_eq!(
        log.request(c, 2, get("a")),
        Response::Value(Some(b"1".to_vec()))
    );
    assert_eq!(
        log.request(c, 3, Operation::Delete { key: b"a".to_vec() }),
        Response::Deleted { existed: true }
    );
    assert_eq!(
        log.request(c, 4, Operation::Delete { key: b"a".to_vec() }),
        Response::Deleted { existed: false }
    );
    assert_eq!(log.request(c, 5, get("a")), Response::Value(None));
    assert!(log.store.is_empty());
}

#[test]
fn compare_and_swap_succeeds_only_on_a_match() {
    let mut log = Log::new();
    let c = log.register();

    assert_eq!(
        log.request(c, 1, cas("k", None, "1")),
        Response::Swapped {
            succeeded: true,
            previous: None
        },
        "absent key matches None"
    );
    assert_eq!(
        log.request(c, 2, cas("k", None, "2")),
        Response::Swapped {
            succeeded: false,
            previous: Some(b"1".to_vec())
        }
    );
    assert_eq!(
        log.request(c, 3, cas("k", Some("1"), "3")),
        Response::Swapped {
            succeeded: true,
            previous: Some(b"1".to_vec())
        }
    );
    assert_eq!(log.store.get(b"k"), Some(&b"3"[..]));
}

#[test]
fn a_retried_request_is_answered_again_but_not_applied_again() {
    let mut log = Log::new();
    let c = log.register();
    let first = log.request(c, 1, cas("k", None, "x"));
    assert_eq!(
        first,
        Response::Swapped {
            succeeded: true,
            previous: None
        }
    );

    // Applied a second time, this CAS would fail. As a retry it must return
    // the original answer, or the client would think its write was lost.
    let retry = log.request(c, 1, cas("k", None, "x"));
    assert_eq!(retry, first);
    assert_eq!(log.store.get(b"k"), Some(&b"x"[..]));
}

#[test]
fn duplicate_writes_from_retries_do_not_overwrite_newer_data() {
    let mut log = Log::new();
    let a = log.register();
    let b = log.register();
    log.request(a, 1, put("k", "from a"));
    log.request(b, 1, put("k", "from b"));
    // A's retry arrives late.
    assert_eq!(log.request(a, 1, put("k", "from a")), Response::Written);
    assert_eq!(log.store.get(b"k"), Some(&b"from b"[..]));
}

#[test]
fn delayed_older_requests_are_rejected() {
    let mut log = Log::new();
    let c = log.register();
    log.request(c, 1, put("k", "1"));
    log.request(c, 2, put("k", "2"));
    assert_eq!(
        log.request(c, 1, put("k", "1")),
        Response::Rejected(Rejection::StaleSequence)
    );
    assert_eq!(log.store.get(b"k"), Some(&b"2"[..]));
}

#[test]
fn requests_from_unknown_sessions_are_rejected() {
    let mut log = Log::new();
    assert_eq!(
        log.request(ClientId(99), 1, put("k", "v")),
        Response::Rejected(Rejection::SessionExpired)
    );
    assert!(log.store.is_empty());
}

#[test]
fn idle_sessions_expire_and_active_ones_do_not() {
    let config = KvConfig {
        session_timeout_ms: 10_000,
        ..KvConfig::default()
    };
    let mut log = Log::with_config(config);
    let idle = log.register();
    let busy = log.register();

    for step in 1..=30 {
        log.now_ms += 1_000;
        log.request(busy, step, put("k", "v"));
    }
    assert_eq!(
        log.request(idle, 1, put("k", "late")),
        Response::Rejected(Rejection::SessionExpired)
    );
    assert_eq!(
        log.request(busy, 31, get("k")),
        Response::Value(Some(b"v".to_vec()))
    );
    assert_eq!(log.store.session_count(), 1);
}

#[test]
fn a_leader_clock_behind_the_previous_one_does_not_move_time_backwards() {
    let config = KvConfig {
        session_timeout_ms: 10_000,
        ..KvConfig::default()
    };
    let mut log = Log::with_config(config);
    let idle = log.register();
    let busy = log.register();
    log.now_ms += 20_000;
    log.request(busy, 1, put("k", "v"));

    // A new leader whose clock is far behind proposes the next entries.
    log.now_ms -= 15_000;
    log.request(busy, 2, put("k", "w"));
    assert_eq!(
        log.request(idle, 1, put("k", "x")),
        Response::Rejected(Rejection::SessionExpired),
        "the idle session expired at the later time and stays expired"
    );
}

#[test]
fn oversized_keys_and_values_are_rejected() {
    let config = KvConfig {
        max_key_len: 4,
        max_value_len: 8,
        ..KvConfig::default()
    };
    let mut log = Log::with_config(config);
    let c = log.register();
    assert_eq!(
        log.request(c, 1, put("toolong", "v")),
        Response::Rejected(Rejection::KeyTooLarge)
    );
    assert_eq!(
        log.request(c, 2, put("k", "much too long")),
        Response::Rejected(Rejection::ValueTooLarge)
    );
    assert_eq!(
        log.request(c, 3, cas("k", Some("much too long"), "v")),
        Response::Rejected(Rejection::ValueTooLarge)
    );
    assert!(log.store.is_empty());
}

#[test]
fn malformed_entries_are_rejected_without_changing_state() {
    let mut store = KvStore::new(KvConfig::default());
    let before = store.clone();
    assert!(matches!(
        store.apply(1, &[1, 2, 3]),
        Response::Rejected(Rejection::Malformed(_))
    ));
    assert_eq!(store, before);
}

// --- Property tests ----------------------------------------------------------

#[derive(Clone, Debug)]
enum Step {
    Register,
    Request {
        client: usize,
        retry: bool,
        operation: Operation,
    },
    AdvanceClock(u64),
}

fn small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0..3u8, 0..3)
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        small_bytes().prop_map(|key| Operation::Get { key }),
        (small_bytes(), small_bytes()).prop_map(|(key, value)| Operation::Put { key, value }),
        small_bytes().prop_map(|key| Operation::Delete { key }),
        (
            small_bytes(),
            prop::option::of(small_bytes()),
            small_bytes()
        )
            .prop_map(|(key, expected, new)| Operation::CompareAndSwap {
                key,
                expected,
                new
            }),
    ]
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        1 => Just(Step::Register),
        8 => (0..4usize, prop::bool::weighted(0.2), operation())
            .prop_map(|(client, retry, operation)| Step::Request { client, retry, operation }),
        1 => (0..5_000u64).prop_map(Step::AdvanceClock),
    ];
    prop::collection::vec(step, 1..200)
}

/// The obvious model of a key-value map, with no sessions.
fn model_apply(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, operation: &Operation) -> Response {
    match operation {
        Operation::Get { key } => Response::Value(model.get(key).cloned()),
        Operation::Put { key, value } => {
            model.insert(key.clone(), value.clone());
            Response::Written
        }
        Operation::Delete { key } => Response::Deleted {
            existed: model.remove(key).is_some(),
        },
        Operation::CompareAndSwap { key, expected, new } => {
            let previous = model.get(key).cloned();
            let succeeded = &previous == expected;
            if succeeded {
                model.insert(key.clone(), new.clone());
            }
            Response::Swapped {
                succeeded,
                previous,
            }
        }
    }
}

/// What the test remembers about one client.
struct Client {
    id: ClientId,
    sequence: u64,
    /// The latest request and the response the model gave for it.
    last: Option<(Operation, Response)>,
}

impl Client {
    /// The next command for this client, and the response it must get.
    fn next(
        &mut self,
        retry: bool,
        operation: Operation,
        model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> (Command, Response) {
        if let (true, Some((operation, response))) = (retry, &self.last) {
            let command = Command::Request {
                client_id: self.id,
                sequence: self.sequence,
                operation: operation.clone(),
            };
            return (command, response.clone());
        }
        self.sequence += 1;
        let response = model_apply(model, &operation);
        self.last = Some((operation.clone(), response.clone()));
        let command = Command::Request {
            client_id: self.id,
            sequence: self.sequence,
            operation,
        };
        (command, response)
    }
}

proptest! {
    /// Random workloads with retries: the store must behave exactly like a
    /// plain map in which every request is applied once, and two stores fed
    /// the same log must stay identical.
    #[test]
    fn matches_a_plain_map_and_is_deterministic(steps in steps()) {
        let config = KvConfig { session_timeout_ms: 1_000_000, ..KvConfig::default() };
        let mut log = Log::with_config(config);
        let mut replica = KvStore::new(config);
        let mut model = BTreeMap::new();
        let mut clients: Vec<Client> = Vec::new();

        for step in steps {
            let (command, expected) = match step {
                Step::Register => (Command::RegisterClient, None),
                Step::AdvanceClock(ms) => {
                    log.now_ms += ms;
                    continue;
                }
                Step::Request { client, retry, operation } => {
                    let Some(client) = clients.get_mut(client) else { continue };
                    let (command, response) = client.next(retry, operation, &mut model);
                    (command, Some(response))
                }
            };
            let bytes = Envelope { timestamp_ms: log.now_ms, command: command.clone() }.encode();
            let index = log.next_index;
            let response = log.apply(command);
            prop_assert_eq!(&replica.apply(index, &bytes), &response);
            if let Some(expected) = expected {
                prop_assert_eq!(response, expected);
            } else {
                let Response::Registered { client_id } = response else {
                    panic!("registration failed: {response:?}");
                };
                clients.push(Client { id: client_id, sequence: 0, last: None });
            }
        }

        prop_assert_eq!(&log.store, &replica);
        for (key, value) in &model {
            prop_assert_eq!(log.store.get(key), Some(value.as_slice()));
        }
        prop_assert_eq!(log.store.len(), model.len());
    }

    /// Arbitrary bytes never crash the state machine or change its data.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let mut store = KvStore::new(KvConfig::default());
        let _ = store.apply(1, &bytes);
        prop_assert!(store.is_empty());
    }

    /// Every valid command survives encoding.
    #[test]
    fn commands_round_trip(
        timestamp_ms in any::<u64>(),
        client in any::<u64>(),
        sequence in any::<u64>(),
        operation in operation(),
    ) {
        let envelope = Envelope {
            timestamp_ms,
            command: Command::Request { client_id: ClientId(client), sequence, operation },
        };
        prop_assert_eq!(Envelope::decode(&envelope.encode()), Ok(envelope));
    }
}
