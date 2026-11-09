//! Unit tests of Block 46's core: windows, followers and every way out of
//! them.

use std::time::Duration;

use super::calls::CallWait;
use super::{
    Action, CallId, DebounceTimer, Input, Machine, SubscriptionId, SubscriptionOptions,
    TransportId, codes,
};
use crate::{CallError, CallReply, CallRequest, Config};

const INTERVAL: Duration = Duration::from_millis(100);

fn config() -> Config {
    Config {
        timeout: Duration::from_secs(5),
        ..Config::default()
    }
}

/// A connected machine with the debounce on, unless `off`.
fn connected(config: Config, off: bool) -> (Machine, TransportId) {
    let machine = Machine::new(config);
    let mut machine = if off {
        machine
    } else {
        machine.with_publish_debounce()
    };
    let transport = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    (machine, transport)
}

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// A subscription to "news" on a connected machine, its subscribe reply
/// (id 2) advising `interval` ms.
fn subscribed(interval: u32) -> (Machine, TransportId, SubscriptionId) {
    subscribed_with(config(), interval, false)
}

fn subscribed_with(
    config: Config,
    interval: u32,
    off: bool,
) -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected(config, off);
    let subscription = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(subscription));
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":2,"subscribe":{{"publish_debounce":{interval}}}}}"#),
    ));
    (machine, transport, subscription)
}

/// A publish of `data` on `subscription`, its call and its actions.
fn publish(
    machine: &mut Machine,
    subscription: SubscriptionId,
    data: &str,
) -> (CallId, Vec<Action>) {
    let id = machine.add_subscription_call(
        subscription,
        CallRequest::Publish {
            channel: String::new(),
            data: data.as_bytes().to_vec(),
        },
    );
    let actions = machine.handle(Input::Call(id));
    (id, actions)
}

fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8(frame.clone()).unwrap()),
            _ => None,
        })
        .collect()
}

fn settled(actions: &[Action]) -> Vec<(CallId, Result<CallReply, CallError>)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::CallSettled { call, result } => Some((*call, result.clone())),
            _ => None,
        })
        .collect()
}

/// Publishes `"a"` on `subscription`, then opens the window's timer: the
/// timer it set.
fn timer_of(actions: &[Action]) -> DebounceTimer {
    actions
        .iter()
        .find_map(|action| match action {
            Action::SetDebounceTimer(timer, _) => Some(*timer),
            _ => None,
        })
        .expect("a debounce timer")
}

fn timers_set(actions: &[Action]) -> Vec<(DebounceTimer, Duration)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::SetDebounceTimer(timer, delay) => Some((*timer, *delay)),
            _ => None,
        })
        .collect()
}

fn timers_cancelled(actions: &[Action]) -> Vec<DebounceTimer> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::CancelDebounceTimer(timer) => Some(*timer),
            _ => None,
        })
        .collect()
}

fn publish_frame(id: u32, data: &str) -> String {
    format!(r#"{{"id":{id},"publish":{{"channel":"news","data":{data}}}}}"#)
}

fn publish_reply(id: u32) -> String {
    format!(r#"{{"id":{id},"publish":{{}}}}"#)
}

#[test]
fn without_the_opt_in_an_interval_is_ignored_and_every_publish_sent() {
    let (mut machine, _, subscription) = subscribed_with(config(), 100, true);

    let (_, first) = publish(&mut machine, subscription, "1");
    let (_, second) = publish(&mut machine, subscription, "2");

    assert_eq!(sent(&first).len(), 1);
    assert_eq!(sent(&second).len(), 1);
    assert_eq!(timers_set(&first), []);
    assert_eq!(timers_set(&second), []);
}

#[test]
fn an_interval_of_zero_means_no_debounce() {
    let (mut machine, _, subscription) = subscribed(0);

    let (_, first) = publish(&mut machine, subscription, "1");
    let (_, second) = publish(&mut machine, subscription, "2");

    assert_eq!(sent(&first).len(), 1);
    assert_eq!(sent(&second).len(), 1);
    assert_eq!(timers_set(&first), []);
}

#[test]
fn a_client_level_publish_never_meets_the_gate() {
    let (mut machine, _, _) = subscribed(100);

    let id = machine.add_call(CallRequest::Publish {
        channel: "news".into(),
        data: b"1".to_vec(),
    });
    let first = machine.handle(Input::Call(id));
    let id = machine.add_call(CallRequest::Publish {
        channel: "news".into(),
        data: b"2".to_vec(),
    });
    let second = machine.handle(Input::Call(id));

    assert_eq!(sent(&first).len(), 1);
    assert_eq!(sent(&second).len(), 1);
    assert_eq!(timers_set(&first), []);
}

#[test]
fn only_a_publish_of_a_subscription_meets_the_gate() {
    let (mut machine, _, subscription) = subscribed(100);

    let id = machine.add_subscription_call(
        subscription,
        CallRequest::History {
            channel: String::new(),
            options: crate::HistoryOptions::default(),
        },
    );
    let actions = machine.handle(Input::Call(id));

    assert_eq!(sent(&actions).len(), 1);
    assert_eq!(timers_set(&actions), []);
}

#[test]
fn the_first_publish_opens_a_window_of_the_interval_and_is_sent() {
    let (mut machine, _, subscription) = subscribed(100);

    let (_, actions) = publish(&mut machine, subscription, "1");

    assert_eq!(sent(&actions), [publish_frame(3, "1")]);
    assert_eq!(timers_set(&actions).len(), 1);
    assert_eq!(timers_set(&actions)[0].1, INTERVAL);
}

#[test]
fn a_publish_in_a_window_is_held_without_a_timer_of_its_own() {
    let (mut machine, _, subscription) = subscribed(100);
    publish(&mut machine, subscription, "1");

    let (id, actions) = publish(&mut machine, subscription, "2");

    // No send, no reply deadline: only the window bounds it (D6).
    assert_eq!(actions, []);
    assert_eq!(machine.call_wait(id), Some(CallWait::Other));
}

#[test]
fn the_timer_is_not_reset_by_later_publishes() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let timer = timer_of(&first);

    let (_, second) = publish(&mut machine, subscription, "2");
    let (_, third) = publish(&mut machine, subscription, "3");

    assert_eq!(timers_set(&second), []);
    assert_eq!(timers_set(&third), []);
    assert_eq!(timers_cancelled(&second), []);
    assert_eq!(timers_cancelled(&third), []);
    let fired = machine.handle(Input::DebounceTimerFired(timer));
    assert_eq!(sent(&fired), [publish_frame(4, "3")]);
}

#[test]
fn the_window_keeps_only_the_latest_value() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (a, _) = publish(&mut machine, subscription, "2");
    let (b, _) = publish(&mut machine, subscription, "3");

    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    // The earlier latest was freed at once, its publish waits with the rest.
    assert_eq!(sent(&fired), [publish_frame(4, "3")]);
    assert_eq!(machine.call_wait(a), Some(CallWait::Reply));
    assert_eq!(machine.call_wait(b), Some(CallWait::Reply));
}

#[test]
fn an_empty_window_closes_and_leaves_no_timer() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    machine.handle(frame(transport, &publish_reply(3)));

    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    assert_eq!(fired, []);
    let (_, next) = publish(&mut machine, subscription, "2");
    assert_eq!(sent(&next), [publish_frame(4, "2")]);
    assert_eq!(timers_set(&next).len(), 1);
}

#[test]
fn a_trailing_send_goes_past_the_gate_and_opens_a_new_window() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    publish(&mut machine, subscription, "2");

    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    assert_eq!(sent(&fired), [publish_frame(4, "2")]);
    let timers = timers_set(&fired);
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].1, INTERVAL);
    assert_ne!(timers[0].0, timer_of(&first));
}

#[test]
fn a_fired_timer_number_is_never_reused() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (_, _) = publish(&mut machine, subscription, "2");
    let original = timer_of(&first);
    machine.handle(Input::DebounceTimerFired(original));

    assert_eq!(machine.handle(Input::DebounceTimerFired(original)), []);
}

#[test]
fn a_debounce_timer_the_machine_does_not_know_does_nothing() {
    let (mut machine, _, _) = subscribed(100);

    assert_eq!(
        machine.handle(Input::DebounceTimerFired(DebounceTimer(1234))),
        []
    );
}

#[test]
fn set_debounce_timers_count_the_hand_offs() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    machine.handle(frame(transport, &publish_reply(3)));
    machine.handle(frame(transport, &publish_reply(4)));
    let closed = machine.handle(Input::DebounceTimerFired(timer_of(&fired)));

    assert_eq!(timers_set(&first).len(), 1);
    assert_eq!(timers_set(&fired).len(), 1);
    assert_eq!(timers_set(&closed), []);
}

#[test]
fn a_later_reply_without_an_interval_turns_the_debounce_off() {
    let (mut machine, transport, subscription) = subscribed(100);
    machine.handle(Input::Unsubscribe(subscription));
    machine.handle(frame(transport, r#"{"id":3,"unsubscribe":{}}"#));
    machine.handle(Input::Subscribe(subscription));
    machine.handle(frame(transport, r#"{"id":4,"subscribe":{}}"#));

    let (_, first) = publish(&mut machine, subscription, "1");
    let (_, second) = publish(&mut machine, subscription, "2");

    assert_eq!(sent(&first).len(), 1);
    assert_eq!(sent(&second).len(), 1);
    assert_eq!(timers_set(&first), []);
}

#[test]
fn an_interval_is_clamped_to_the_call_timeout() {
    let (mut machine, _, subscription) = subscribed_with(config(), 60_000, false);

    let (_, actions) = publish(&mut machine, subscription, "1");

    assert_eq!(timers_set(&actions)[0].1, Duration::from_secs(5));
}

#[test]
fn a_zero_call_timeout_makes_every_interval_zero() {
    let config = Config {
        timeout: Duration::ZERO,
        ..config()
    };
    let (mut machine, _, subscription) = subscribed_with(config, 100, false);

    let (_, first) = publish(&mut machine, subscription, "1");
    let (_, second) = publish(&mut machine, subscription, "2");

    assert_eq!(timers_set(&first), []);
    assert_eq!(sent(&second).len(), 1);
}

#[test]
fn publishes_made_while_subscribing_meet_the_gate_when_subscribed() {
    let (mut machine, transport) = connected(config(), false);
    let subscription = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(subscription));
    let (first, _) = publish(&mut machine, subscription, "1");
    let (second, _) = publish(&mut machine, subscription, "2");
    let (third, _) = publish(&mut machine, subscription, "3");

    let actions = machine.handle(frame(
        transport,
        r#"{"id":2,"subscribe":{"publish_debounce":100}}"#,
    ));

    assert_eq!(sent(&actions), [publish_frame(3, "1")]);
    assert_eq!(timers_set(&actions).len(), 1);
    // Their Subscription deadlines end: the window bounds them now.
    assert!(actions.contains(&Action::CancelCallTimer(
        second,
        super::CallTimer::Subscription
    )));
    assert!(actions.contains(&Action::CancelCallTimer(
        third,
        super::CallTimer::Subscription
    )));
    assert_eq!(machine.call_wait(first), Some(CallWait::Reply));
    assert_eq!(machine.call_wait(second), Some(CallWait::Other));
}

#[test]
fn a_leading_publish_released_by_room_is_not_debounced_twice() {
    let (mut machine, _, subscription) = subscribed_with(
        Config {
            write_queue_bytes: 10,
            ..config()
        },
        100,
        false,
    );
    // Fills the queue: the publish waits for room, its window open.
    let filler = machine.add_call(CallRequest::Rpc {
        method: "fill".into(),
        data: b"\"xxxxxxxxxxxxxxxxxxxx\"".to_vec(),
    });
    let filled = machine.handle(Input::Call(filler));
    let (id, waiting) = publish(&mut machine, subscription, "1");
    assert_eq!(sent(&waiting), Vec::<String>::new());
    assert_eq!(machine.call_wait(id), Some(CallWait::Other));

    let Action::Send {
        transport,
        id: frame_id,
        ..
    } = filled
        .iter()
        .find(|action| matches!(action, Action::Send { .. }))
        .expect("the filler's frame")
    else {
        unreachable!();
    };
    let admitted = machine.handle(Input::FrameWritten {
        transport: *transport,
        frame: *frame_id,
    });

    // Sent, not parked again in the window it opened.
    assert_eq!(sent(&admitted).len(), 1);
    assert_eq!(machine.call_wait(id), Some(CallWait::Reply));
}

#[test]
fn the_replies_settle_the_leading_and_the_waiting_publishes_in_order() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (a, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let (c, _) = publish(&mut machine, subscription, "3");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    let replied_leading = machine.handle(frame(transport, &publish_reply(3)));
    let replied_trailing = machine.handle(frame(transport, &publish_reply(4)));

    assert_eq!(settled(&replied_leading), [(a, Ok(CallReply::Publish))]);
    assert_eq!(
        settled(&replied_trailing),
        [(b, Ok(CallReply::Publish)), (c, Ok(CallReply::Publish))]
    );
}

#[test]
fn the_trailing_error_reaches_every_waiter_with_its_code_message_and_temporary_flag() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let (c, _) = publish(&mut machine, subscription, "3");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    let replied = machine.handle(frame(
        transport,
        r#"{"id":4,"error":{"code":103,"message":"no","temporary":true}}"#,
    ));

    assert_eq!(
        settled(&replied),
        [
            (b, Err(CallError::new(103, "no").temporary(true))),
            (c, Err(CallError::new(103, "no").temporary(true)))
        ]
    );
}

#[test]
fn a_leading_publish_keeps_its_own_error() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (a, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    let refused = machine.handle(frame(
        transport,
        r#"{"id":3,"error":{"code":103,"message":"no"}}"#,
    ));

    assert_eq!(settled(&refused), [(a, Err(CallError::new(103, "no")))]);
    assert_eq!(machine.call_wait(b), Some(CallWait::Reply));
}

#[test]
fn a_trailing_send_never_settles_under_its_own_id() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    // Its own timer, fed back as for any call.
    let trailing = fired
        .iter()
        .find_map(|action| match action {
            Action::SetCallTimer(id, super::CallTimer::Reply, _) => Some(*id),
            _ => None,
        })
        .expect("the trailing send's deadline");

    let replied = machine.handle(frame(transport, &publish_reply(4)));

    assert_ne!(trailing, b);
    assert!(replied.contains(&Action::CancelCallTimer(trailing, super::CallTimer::Reply)));
    assert_eq!(settled(&replied), [(b, Ok(CallReply::Publish))]);
}

#[test]
fn a_trailing_deadline_fails_its_followers() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    let trailing = fired
        .iter()
        .find_map(|action| match action {
            Action::SetCallTimer(id, super::CallTimer::Reply, _) => Some(*id),
            _ => None,
        })
        .expect("the trailing send's deadline");

    let timed_out = machine.handle(Input::CallTimerFired(trailing, super::CallTimer::Reply));

    assert_eq!(
        settled(&timed_out),
        [(b, Err(CallError::new(codes::error::TIMEOUT, "timeout")))]
    );
}

fn window_with_a_waiter() -> (Machine, TransportId, SubscriptionId, CallId, Vec<Action>) {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (waiter, _) = publish(&mut machine, subscription, "2");
    (machine, transport, subscription, waiter, first)
}

fn assert_cancelled_with(actions: &[Action], first: &[Action], waiter: CallId, code: u32) {
    assert_eq!(timers_cancelled(actions), [timer_of(first)], "{actions:?}");
    assert_eq!(
        settled(actions)
            .into_iter()
            .map(|(call, result)| (call, result.err().map(|error| error.code)))
            .collect::<Vec<_>>(),
        [(waiter, Some(code))]
    );
}

#[test]
fn unsubscribing_fails_a_waiting_publish_with_code_7() {
    let (mut machine, _, subscription, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(Input::Unsubscribe(subscription));

    assert_cancelled_with(
        &actions,
        &first,
        waiter,
        codes::error::SUBSCRIPTION_UNSUBSCRIBED,
    );
    assert_eq!(sent(&actions).len(), 1, "only the unsubscribe command");
}

#[test]
fn losing_the_transport_fails_a_waiting_publish_with_code_17() {
    let (mut machine, transport, _, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(Input::TransportClosed {
        transport,
        code: Some(3005),
        reason: String::new(),
    });

    assert_eq!(timers_cancelled(&actions), [timer_of(&first)]);
    let waiter_code = settled(&actions)
        .into_iter()
        .find(|(call, _)| *call == waiter)
        .and_then(|(_, result)| result.err().map(|error| error.code));
    assert_eq!(waiter_code, Some(codes::error::PUBLISH_DROPPED));
    assert_eq!(codes::error::PUBLISH_DROPPED, 17);
}

#[test]
fn a_dropped_publish_is_not_resent_after_the_reconnect() {
    let (mut machine, transport, subscription, _, _) = window_with_a_waiter();
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3005),
        reason: String::new(),
    });

    let (reconnect, new_transport) = {
        let actions = machine.handle(Input::TimerFired(super::Timer::Reconnect));
        let transport = actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a new attempt");
        (actions, transport)
    };
    machine.handle(Input::TransportOpened(new_transport));
    let connected = machine.handle(frame(new_transport, r#"{"id":4,"connect":{"client":"c"}}"#));
    let subscribed = machine.handle(frame(new_transport, r#"{"id":5,"subscribe":{}}"#));

    assert!(!reconnect.is_empty());
    assert_eq!(sent(&connected).len(), 1, "only the subscribe command");
    assert_eq!(sent(&subscribed), Vec::<String>::new());
    assert_eq!(
        machine.subscription_state(subscription),
        Some(crate::SubscriptionState::Subscribed)
    );
}

#[test]
fn a_server_unsubscribe_of_code_2500_or_more_fails_a_waiting_publish_with_code_17() {
    let (mut machine, transport, _, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"x"}}}"#,
    ));

    assert_cancelled_with(&actions, &first, waiter, codes::error::PUBLISH_DROPPED);
}

#[test]
fn a_server_unsubscribe_below_2500_fails_a_waiting_publish_with_code_7() {
    let (mut machine, transport, _, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","unsubscribe":{"code":2000,"reason":"x"}}}"#,
    ));

    assert_cancelled_with(
        &actions,
        &first,
        waiter,
        codes::error::SUBSCRIPTION_UNSUBSCRIBED,
    );
}

#[test]
fn a_delta_failure_fails_a_waiting_publish_with_code_17() {
    let (mut machine, transport) = connected(config(), false);
    let subscription = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(subscription));
    machine.handle(frame(
        transport,
        r#"{"id":2,"subscribe":{"publish_debounce":100,"delta":true}}"#,
    ));
    let (_, first) = publish(&mut machine, subscription, "1");
    let (waiter, _) = publish(&mut machine, subscription, "2");

    // A delta with no base to apply to fails.
    let actions = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":"bm8=","delta":true}}}"#,
    ));

    assert_cancelled_with(&actions, &first, waiter, codes::error::PUBLISH_DROPPED);
}

#[test]
fn disconnecting_fails_a_waiting_publish_with_code_3() {
    let (mut machine, _, _, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(Input::Disconnect);

    assert_cancelled_with(&actions, &first, waiter, codes::error::DISCONNECTED);
}

#[test]
fn closing_fails_a_waiting_publish_with_code_4() {
    let (mut machine, _, _, waiter, first) = window_with_a_waiter();

    let actions = machine.handle(Input::Close);

    assert_cancelled_with(&actions, &first, waiter, codes::error::CLIENT_CLOSED);
}

#[test]
fn no_timer_of_a_window_survives_its_subscription_leaving_subscribed() {
    let (mut machine, _, subscription, _, first) = window_with_a_waiter();

    machine.handle(Input::Unsubscribe(subscription));

    assert_eq!(
        machine.handle(Input::DebounceTimerFired(timer_of(&first))),
        []
    );
}

/// A window whose trailing send was handed over: the trailing send's
/// phase, the follower, the machine.
fn handed_over() -> (
    Machine,
    TransportId,
    SubscriptionId,
    CallId,
    CallId,
    Vec<Action>,
) {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (follower, _) = publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    let trailing = fired
        .iter()
        .find_map(|action| match action {
            Action::SetCallTimer(id, super::CallTimer::Reply, _) => Some(*id),
            _ => None,
        })
        .expect("the trailing send's deadline");
    (machine, transport, subscription, follower, trailing, fired)
}

#[test]
fn a_follower_waits_on_the_phase_of_its_trailing_send() {
    let (machine, _, _, follower, _, _) = handed_over();

    assert_eq!(machine.call_wait(follower), Some(CallWait::Reply));
}

#[test]
fn a_follower_of_a_trailing_send_waiting_for_the_connection_waits_for_a_message() {
    let (mut machine, transport) = connected(config(), false);
    let subscription = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    // A stream that takes nothing: the `subscribed` event, not taken, holds
    // the close read after it, so the subscription stays `subscribed`.
    machine.handle(Input::StreamOpened(Some(subscription)));
    machine.handle(Input::Subscribe(subscription));
    machine.handle(frame(
        transport,
        r#"{"id":2,"subscribe":{"publish_debounce":100}}"#,
    ));
    let (_, first) = publish(&mut machine, subscription, "1");
    let (follower, _) = publish(&mut machine, subscription, "2");
    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });

    // The close is held: the timer fires with the transport closing.
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    assert_eq!(sent(&fired), Vec::<String>::new());
    assert_eq!(machine.call_wait(follower), Some(CallWait::Message));
}

#[test]
fn a_follower_of_a_trailing_send_waiting_for_room_is_other() {
    let (mut machine, _, subscription) = subscribed_with(
        Config {
            write_queue_bytes: 10,
            ..config()
        },
        100,
        false,
    );
    let filler = machine.add_call(CallRequest::Rpc {
        method: "fill".into(),
        data: b"\"xxxxxxxxxxxxxxxxxxxx\"".to_vec(),
    });
    machine.handle(Input::Call(filler));
    let (_, first) = publish(&mut machine, subscription, "1");
    let (follower, _) = publish(&mut machine, subscription, "2");

    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    assert_eq!(machine.call_wait(follower), Some(CallWait::Other));
}

#[test]
fn a_trailing_send_lost_with_its_transport_fails_its_followers_with_code_11() {
    let (mut machine, transport, _, follower, _, _) = handed_over();

    let lost = machine.handle(Input::TransportClosed {
        transport,
        code: Some(3005),
        reason: String::new(),
    });

    assert_eq!(
        settled(&lost)
            .into_iter()
            .find(|(call, _)| *call == follower)
            .map(|(_, result)| result.map_err(|error| error.code)),
        Some(Err(codes::error::CONNECTION_CLOSED))
    );
}

#[test]
fn a_trailing_send_waiting_for_room_fails_with_code_7_on_unsubscribe() {
    let (mut machine, _, subscription) = subscribed_with(
        Config {
            write_queue_bytes: 10,
            ..config()
        },
        100,
        false,
    );
    let filler = machine.add_call(CallRequest::Rpc {
        method: "fill".into(),
        data: b"\"xxxxxxxxxxxxxxxxxxxx\"".to_vec(),
    });
    machine.handle(Input::Call(filler));
    let (_, first) = publish(&mut machine, subscription, "1");
    let (follower, _) = publish(&mut machine, subscription, "2");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    let unsubscribed = machine.handle(Input::Unsubscribe(subscription));

    assert_eq!(
        settled(&unsubscribed)
            .into_iter()
            .find(|(call, _)| *call == follower)
            .map(|(_, result)| result.map_err(|error| error.code)),
        Some(Err(codes::error::SUBSCRIPTION_UNSUBSCRIBED))
    );
}

#[test]
fn a_window_ending_while_the_previous_trailing_send_waits_for_room_makes_one_more() {
    let (mut machine, _, subscription) = subscribed_with(
        Config {
            write_queue_bytes: 10,
            ..config()
        },
        100,
        false,
    );
    let filler = machine.add_call(CallRequest::Rpc {
        method: "fill".into(),
        data: b"\"xxxxxxxxxxxxxxxxxxxx\"".to_vec(),
    });
    let filled = machine.handle(Input::Call(filler));
    let (_, first) = publish(&mut machine, subscription, "1");
    publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    publish(&mut machine, subscription, "3");
    let fired_again = machine.handle(Input::DebounceTimerFired(timer_of(&fired)));

    // Two trailing sends waiting for room, the window keeps its own timing.
    assert_eq!(sent(&fired_again), Vec::<String>::new());
    assert_eq!(timers_set(&fired_again).len(), 1);

    let Action::Send {
        transport,
        id: frame_id,
        ..
    } = filled
        .iter()
        .find(|action| matches!(action, Action::Send { .. }))
        .expect("the filler's frame")
    else {
        unreachable!();
    };
    let admitted = machine.handle(Input::FrameWritten {
        transport: *transport,
        frame: *frame_id,
    });

    assert_eq!(
        sent(&admitted),
        [
            publish_frame(3, "1"),
            publish_frame(4, "2"),
            publish_frame(5, "3")
        ]
    );
}

#[test]
fn abandoning_the_only_waiter_of_a_window_drops_its_data() {
    let (mut machine, _, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (waiter, _) = publish(&mut machine, subscription, "2");

    let abandoned = machine.handle(Input::CallAbandoned(waiter));
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    assert_eq!(abandoned, []);
    assert_eq!(fired, [], "the window goes on with nothing to send");
    assert_eq!(machine.call_wait(waiter), None);
}

#[test]
fn abandoning_the_last_of_several_waiters_still_sends_its_value_for_the_others() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let (c, _) = publish(&mut machine, subscription, "3");

    machine.handle(Input::CallAbandoned(c));
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    let replied = machine.handle(frame(transport, &publish_reply(4)));

    assert_eq!(sent(&fired), [publish_frame(4, "3")]);
    assert_eq!(settled(&replied), [(b, Ok(CallReply::Publish))]);
    assert_eq!(machine.call_wait(c), None);
}

#[test]
fn abandoning_an_earlier_waiter_keeps_the_latest_for_the_others() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let (c, _) = publish(&mut machine, subscription, "3");

    machine.handle(Input::CallAbandoned(b));
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    let replied = machine.handle(frame(transport, &publish_reply(4)));

    assert_eq!(sent(&fired), [publish_frame(4, "3")]);
    assert_eq!(settled(&replied), [(c, Ok(CallReply::Publish))]);
}

#[test]
fn abandoning_a_follower_keeps_the_trailing_send_for_the_others() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    let (c, _) = publish(&mut machine, subscription, "3");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    machine.handle(Input::CallAbandoned(b));
    let replied = machine.handle(frame(transport, &publish_reply(4)));

    assert_eq!(settled(&replied), [(c, Ok(CallReply::Publish))]);
}

#[test]
fn abandoning_every_follower_of_a_trailing_send_not_yet_out_withdraws_it() {
    let (mut machine, _, subscription) = subscribed_with(
        Config {
            write_queue_bytes: 10,
            ..config()
        },
        100,
        false,
    );
    let filler = machine.add_call(CallRequest::Rpc {
        method: "fill".into(),
        data: b"\"xxxxxxxxxxxxxxxxxxxx\"".to_vec(),
    });
    let filled = machine.handle(Input::Call(filler));
    let (_, first) = publish(&mut machine, subscription, "1");
    let (b, _) = publish(&mut machine, subscription, "2");
    machine.handle(Input::DebounceTimerFired(timer_of(&first)));

    let abandoned = machine.handle(Input::CallAbandoned(b));
    let Action::Send {
        transport,
        id: frame_id,
        ..
    } = filled
        .iter()
        .find(|action| matches!(action, Action::Send { .. }))
        .expect("the filler's frame")
    else {
        unreachable!();
    };
    let admitted = machine.handle(Input::FrameWritten {
        transport: *transport,
        frame: *frame_id,
    });

    assert_eq!(settled(&abandoned), []);
    assert_eq!(
        sent(&admitted),
        [publish_frame(3, "1")],
        "the trailing send was never written"
    );
}

#[test]
fn abandoning_every_follower_of_a_trailing_send_already_out_ignores_its_reply() {
    let (mut machine, transport, _, follower, _, _) = handed_over();

    let abandoned = machine.handle(Input::CallAbandoned(follower));
    let replied = machine.handle(frame(transport, &publish_reply(4)));

    assert_eq!(settled(&abandoned), []);
    assert_eq!(settled(&replied), []);
    assert_eq!(machine.call_wait(follower), None);
}

#[test]
fn closed_windows_leave_nothing_behind() {
    let (mut machine, transport, subscription) = subscribed(100);
    let (_, first) = publish(&mut machine, subscription, "1");
    publish(&mut machine, subscription, "2");
    let fired = machine.handle(Input::DebounceTimerFired(timer_of(&first)));
    machine.handle(frame(transport, &publish_reply(3)));
    machine.handle(frame(transport, &publish_reply(4)));
    machine.handle(Input::DebounceTimerFired(timer_of(&fired)));

    assert_eq!(machine.debounce_window_count(), 0);
    assert_eq!(machine.subscription_debounce_windows(subscription), 0);
}
