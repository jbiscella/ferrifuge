//! Block 33: closing a client, on the core.

use super::*;
use crate::{CallRequest, ServerSubscriptionEvent, SubscriptionState};

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

fn opened(actions: &[Action]) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt")
}

/// A machine connected with a server-side subscription to "alerts" and a
/// subscribed client-side subscription to "news".
fn connected() -> (Machine, TransportId, SubscriptionId) {
    let mut machine = Machine::new(Config::default());
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{}}}}"#,
    ));
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(id));
    machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));
    (machine, transport, id)
}

/// The events `actions` emit, by name and code, in order.
fn emitted(actions: &[Action]) -> Vec<(String, u32)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSubscription(_, _, SubscriptionEvent::Unsubscribed { code, .. }) => {
                Some(("unsubscribed".to_owned(), *code))
            }
            Action::Emit(
                _,
                ClientEvent::ServerSubscription {
                    event: ServerSubscriptionEvent::Unsubscribed { code, .. },
                    ..
                },
            ) => Some(("server unsubscribed".to_owned(), *code)),
            Action::Emit(_, ClientEvent::Disconnected { code, .. }) => {
                Some(("disconnected".to_owned(), *code))
            }
            _ => None,
        })
        .collect()
}

fn settled_codes(actions: &[Action]) -> Vec<u32> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::CallSettled {
                result: Err(error), ..
            } => Some(error.code),
            _ => None,
        })
        .collect()
}

// D2: unsubscribed code 2, client-side then server-side, then
// disconnected code 0; no unsubscribe command.
#[test]
fn close_unsubscribes_every_subscription_before_disconnected() {
    let (mut machine, _, id) = connected();

    let closed = machine.handle(Input::Close);

    assert_eq!(
        emitted(&closed),
        [
            (
                "unsubscribed".to_owned(),
                codes::unsubscribed::CLIENT_CLOSED
            ),
            (
                "server unsubscribed".to_owned(),
                codes::unsubscribed::CLIENT_CLOSED
            ),
            (
                "disconnected".to_owned(),
                codes::disconnected::DISCONNECT_CALLED
            ),
        ]
    );
    assert!(
        !closed
            .iter()
            .any(|action| matches!(action, Action::Send { .. })),
        "{closed:?}"
    );
    assert!(
        closed
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_)))
    );
    // Client-side kept, server-side gone (D2, D5).
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
    assert!(machine.server_subscriptions().is_empty());
    assert_eq!(machine.statistics().subscriptions, 1);
}

// D2: every call not settled fails with code 4, whatever its phase.
#[test]
fn close_fails_every_call_with_client_closed() {
    let (mut machine, _, id) = connected();
    let sent = machine.add_call(CallRequest::Rpc {
        method: "slow".into(),
        data: Vec::new(),
    });
    machine.handle(Input::Call(sent));
    let on_subscription = machine.add_subscription_call(
        id,
        CallRequest::History {
            channel: "news".into(),
            options: crate::HistoryOptions::default(),
        },
    );
    machine.handle(Input::Call(on_subscription));
    let added = machine.add_call(CallRequest::Rpc {
        method: "later".into(),
        data: Vec::new(),
    });

    let closed = machine.handle(Input::Close);

    assert_eq!(settled_codes(&closed), [codes::error::CLIENT_CLOSED; 3]);
    let _ = added;
}

// D2: a disconnected client emits no disconnected; its subscribing
// subscriptions are unsubscribed with code 2; not a failed attempt.
#[test]
fn close_on_a_disconnected_client_unsubscribes_its_subscribing_subscriptions() {
    let mut machine = Machine::new(Config::default());
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(id));

    let closed = machine.handle(Input::Close);

    assert_eq!(
        emitted(&closed),
        [(
            "unsubscribed".to_owned(),
            codes::unsubscribed::CLIENT_CLOSED
        )]
    );
    assert_eq!(machine.statistics().failed_attempts, 0);
}

#[test]
fn closing_an_attempt_is_not_a_failed_attempt() {
    let mut machine = Machine::new(Config::default());
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));

    machine.handle(Input::Close);

    assert_eq!(machine.statistics().failed_attempts, 0);
}

// D4: the driver's end closes the same way, with its own codes.
#[test]
fn driver_dropped_closes_with_its_own_disconnect_code() {
    for (panicked, code) in [
        (false, codes::disconnected::RUNTIME_SHUT_DOWN),
        (true, codes::disconnected::BAD_PROTOCOL),
    ] {
        let (mut machine, _, _) = connected();
        let sent = machine.add_call(CallRequest::Rpc {
            method: "slow".into(),
            data: Vec::new(),
        });
        machine.handle(Input::Call(sent));

        let closed = machine.handle(Input::DriverDropped { panicked });

        assert_eq!(
            emitted(&closed),
            [
                (
                    "unsubscribed".to_owned(),
                    codes::unsubscribed::CLIENT_CLOSED
                ),
                (
                    "server unsubscribed".to_owned(),
                    codes::unsubscribed::CLIENT_CLOSED
                ),
                ("disconnected".to_owned(), code),
            ],
            "panicked {panicked}"
        );
        assert_eq!(settled_codes(&closed), [codes::error::CLIENT_CLOSED]);
    }
}

#[test]
fn close_debug_output_names_it() {
    assert_eq!(format!("{:?}", Input::Close), "Close");
}
