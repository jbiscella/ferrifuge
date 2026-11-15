//! Block 53, D1 and D2: each setting of a timer carries a number, and the
//! core ignores a firing whose number is not the one running for its timer.

use super::*;
use crate::CallRequest;

/// A machine connected over websocket, its server pinging every 25 s, with
/// a subscription whose subscribe reply and an RPC whose reply are awaited:
/// one timer of each family runs.
fn running() -> (Machine, Vec<Action>, SubscriptionId, CallId) {
    let mut machine = Machine::new(Config::default());
    let mut actions = machine.handle(Input::Connect);
    let transport = actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport to open");
    actions.extend(machine.handle(Input::TransportOpened(transport)));
    actions.extend(machine.handle(Input::Frame {
        transport,
        data: br#"{"id":1,"connect":{"client":"c","ping":25}}"#.to_vec(),
    }));
    let id = machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    actions.extend(machine.handle(Input::Subscribe(id)));
    let call = machine.add_call(CallRequest::Rpc {
        method: "echo".to_owned(),
        data: b"{}".to_vec(),
    });
    actions.extend(machine.handle(Input::Call(call)));
    (machine, actions, id, call)
}

/// The setting of the last set action in `actions` that `pick` selects.
fn last_setting(actions: &[Action], pick: impl Fn(&Action) -> bool) -> TimerSetting {
    actions
        .iter()
        .rev()
        .filter(|action| pick(action))
        .find_map(|action| match action {
            Action::SetTimer(
                TimerKey::Client(_) | TimerKey::Subscription(_, _) | TimerKey::Call(_, _),
                _,
                setting,
            ) => Some(*setting),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no such setting in {actions:?}"))
}

#[test]
fn a_second_firing_of_a_taken_setting_returns_no_action() {
    let (mut machine, _, _, _) = running();
    let transport = machine.transport.expect("a transport");
    let closed = machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".to_owned(),
    });
    let reconnect = last_setting(&closed, |action| {
        matches!(
            action,
            Action::SetTimer(TimerKey::Client(Timer::Reconnect), ..)
        )
    });

    let first = machine.handle(Input::TimerFired(
        TimerKey::Client(Timer::Reconnect),
        reconnect,
    ));
    assert!(
        first
            .iter()
            .any(|action| matches!(action, Action::OpenTransport { .. })),
        "{first:?}"
    );
    let second = machine.handle(Input::TimerFired(
        TimerKey::Client(Timer::Reconnect),
        reconnect,
    ));
    assert!(second.is_empty(), "{second:?}");
}

#[test]
fn a_firing_of_each_family_with_another_timers_number_returns_no_action() {
    let (mut machine, actions, id, call) = running();
    let ping = last_setting(&actions, |action| {
        matches!(
            action,
            Action::SetTimer(TimerKey::Client(Timer::ServerPing), ..)
        )
    });
    let subscribe = last_setting(&actions, |action| {
        matches!(
            action,
            Action::SetTimer(TimerKey::Subscription(of, SubscriptionTimer::SubscribeReply), ..) if *of == id
        )
    });
    let reply = last_setting(
        &actions,
        |action| matches!(action, Action::SetTimer(TimerKey::Call(of, CallTimer::Reply), ..) if *of == call),
    );
    assert_eq!(machine.timer_setting(Timer::ServerPing), Some(ping));
    assert_eq!(
        machine.subscription_timer_setting(id, SubscriptionTimer::SubscribeReply),
        Some(subscribe)
    );
    assert_eq!(
        machine.call_timer_setting(call, CallTimer::Reply),
        Some(reply)
    );

    for fired in [
        Input::TimerFired(TimerKey::Client(Timer::ServerPing), subscribe),
        Input::TimerFired(
            TimerKey::Subscription(id, SubscriptionTimer::SubscribeReply),
            reply,
        ),
        Input::TimerFired(TimerKey::Call(call, CallTimer::Reply), ping),
    ] {
        let returned = machine.handle(fired);
        assert!(returned.is_empty(), "{returned:?}");
    }
    // Each timer still runs with its own setting, which the core acts on.
    assert_eq!(machine.state(), State::Connected);
    assert_eq!(machine.timer_setting(Timer::ServerPing), Some(ping));
    let timed_out = machine.handle(Input::TimerFired(
        TimerKey::Call(call, CallTimer::Reply),
        reply,
    ));
    assert!(
        timed_out
            .iter()
            .any(|action| matches!(action, Action::CallSettled { call: of, .. } if *of == call)),
        "{timed_out:?}"
    );
}

// Block 61, D4: a second firing of a taken setting returns no action, for
// the subscription and call families too.
#[test]
fn a_second_firing_of_a_taken_subscription_or_call_setting_returns_no_action() {
    for subscription in [true, false] {
        // A machine each: a subscribe reply's timeout reconnects.
        let (mut machine, _, id, call) = running();
        let key = if subscription {
            TimerKey::Subscription(id, SubscriptionTimer::SubscribeReply)
        } else {
            TimerKey::Call(call, CallTimer::Reply)
        };
        let setting = machine.key_setting(key).expect("the timer runs");

        let first = machine.handle(Input::TimerFired(key, setting));
        let second = machine.handle(Input::TimerFired(key, setting));

        assert!(!first.is_empty(), "{key:?} fired");
        assert!(second.is_empty(), "{key:?}: {second:?}");
    }
}

/// The names of the variants of `pub enum <name>` in `src/sans_io.rs` that
/// name a timer.
fn timer_variants(name: &str) -> Vec<String> {
    // Line endings normalised: a Windows checkout has CRLF.
    let text =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/sans_io.rs")).replace("\r\n", "\n");
    let start = text
        .find(&format!("\npub enum {name} {{\n"))
        .unwrap_or_else(|| panic!("pub enum {name}"));
    let body = &text[start..];
    let body = &body[..body.find("\n}\n").expect("the enum's end")];
    body.lines()
        .filter_map(|line| {
            let variant = line.strip_prefix("    ")?;
            let first = variant.chars().next()?;
            first.is_ascii_uppercase().then(|| {
                variant
                    .split(|c: char| !c.is_ascii_alphanumeric())
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
        })
        .filter(|variant| variant.contains("Timer"))
        .collect()
}

// Block 61, D4: one timer family, so `Action` has exactly three timer
// variants and `Input` one.
#[test]
fn action_and_input_have_exactly_the_keyed_timer_variants() {
    assert_eq!(
        timer_variants("Action"),
        ["SetTimer", "CancelTimer", "ShortenTimer"]
    );
    assert_eq!(timer_variants("Input"), ["TimerFired"]);
}
