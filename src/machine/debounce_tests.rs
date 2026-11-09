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

/// An rpc whose frame is larger than the 200 bytes of [`queue_config`]: it
/// fits alone (Block 13), and fills the queue.
fn filler() -> CallRequest {
    CallRequest::Rpc {
        method: "fill".into(),
        data: format!("\"{}\"", "x".repeat(300)).into_bytes(),
    }
}

/// A write queue that a filler fills, and three small publishes fit.
fn queue_config() -> Config {
    Config {
        write_queue_bytes: 200,
        ..config()
    }
}

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
    let (mut machine, _, subscription) = subscribed_with(queue_config(), 100, false);
    // A frame larger than the queue: the publish waits for room, its
    // window open.
    let filling_call = machine.add_call(filler());
    let filled = machine.handle(Input::Call(filling_call));
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
            .find(|(call, _)| *call == waiter)
            .map(|(_, result)| result.err().map(|error| error.code)),
        Some(Some(code)),
        "{actions:?}"
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

    let new_transport = machine
        .handle(Input::TimerFired(super::Timer::Reconnect))
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a new attempt");
    let opened = machine.handle(Input::TransportOpened(new_transport));
    // Whichever frame carries the subscribe command (Block 36, D6).
    let connected = machine.handle(frame(new_transport, r#"{"id":4,"connect":{"client":"c"}}"#));
    let subscribed = machine.handle(frame(new_transport, r#"{"id":5,"subscribe":{}}"#));

    for frame in sent(&opened)
        .iter()
        .chain(&sent(&connected))
        .chain(&sent(&subscribed))
    {
        assert!(!frame.contains("publish"), "{frame}");
    }
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
    let (mut machine, _, subscription) = subscribed_with(queue_config(), 100, false);
    let filler = machine.add_call(filler());
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
    let (mut machine, _, subscription) = subscribed_with(queue_config(), 100, false);
    let filler = machine.add_call(filler());
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
    let (mut machine, _, subscription) = subscribed_with(queue_config(), 100, false);
    let filling_call = machine.add_call(filler());
    let filled = machine.handle(Input::Call(filling_call));
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
            publish_frame(4, "1"),
            publish_frame(5, "2"),
            publish_frame(6, "3")
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
    let (mut machine, _, subscription) = subscribed_with(queue_config(), 100, false);
    let filling_call = machine.add_call(filler());
    let filled = machine.handle(Input::Call(filling_call));
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
        [publish_frame(4, "1")],
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

/// Block 46b: map keys (`unstable`, D12).
#[cfg(feature = "unstable")]
mod maps {
    use serde_json::Value;

    use super::{
        Action, CallError, CallId, CallReply, CallRequest, CallWait, Config, DebounceTimer,
        Duration, INTERVAL, Input, Machine, SubscriptionId, TransportId, codes, config, connected,
        frame, settled, timers_cancelled, timers_set,
    };
    use crate::MapKind;
    use crate::machine::{MapSubscriptionOptions, Timer};

    fn map_options() -> MapSubscriptionOptions {
        MapSubscriptionOptions::new(MapKind::Map)
    }

    /// A map subscription on a connected machine, its live reply (id 2)
    /// advising `interval` ms.
    fn subscribed_map(interval: u32) -> (Machine, TransportId, SubscriptionId) {
        let (mut machine, transport) = connected(config(), false);
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        machine.handle(frame(
            transport,
            &format!(
                r#"{{"id":2,"subscribe":{{"type":1,"epoch":"e","offset":1,"publish_debounce":{interval}}}}}"#
            ),
        ));
        (machine, transport, subscription)
    }

    /// The commands sent among `actions`, decoded.
    fn commands(actions: &[Action]) -> Vec<Value> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(frame),
                _ => None,
            })
            .flat_map(|frame| {
                std::str::from_utf8(frame)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The publish commands among `actions`, as `(key, data)`, `None` for
    /// a removal.
    fn map_writes(actions: &[Action]) -> Vec<(String, Option<i64>)> {
        commands(actions)
            .iter()
            .filter_map(|command| {
                let publish = command.get("publish")?;
                assert_eq!(publish["type"], 1, "{command}");
                let key = publish["key"].as_str().unwrap_or_default().to_owned();
                if publish["removed"] == true {
                    return Some((key, None));
                }
                Some((key, publish["data"].as_i64()))
            })
            .collect()
    }

    fn command_id(command: &Value) -> u32 {
        u32::try_from(command["id"].as_u64().expect("an id")).expect("a command id")
    }

    fn publish(
        machine: &mut Machine,
        subscription: SubscriptionId,
        key: &str,
        data: &str,
    ) -> (CallId, Vec<Action>) {
        let id = machine.add_subscription_call(
            subscription,
            CallRequest::MapPublish {
                channel: String::new(),
                key: key.to_owned(),
                data: data.as_bytes().to_vec(),
            },
        );
        let actions = machine.handle(Input::Call(id));
        (id, actions)
    }

    fn remove(
        machine: &mut Machine,
        subscription: SubscriptionId,
        key: &str,
    ) -> (CallId, Vec<Action>) {
        let id = machine.add_subscription_call(
            subscription,
            CallRequest::MapRemove {
                channel: String::new(),
                key: key.to_owned(),
            },
        );
        let actions = machine.handle(Input::Call(id));
        (id, actions)
    }

    fn ok() -> Result<CallReply, CallError> {
        Ok(CallReply::Publish)
    }

    fn timer(actions: &[Action]) -> DebounceTimer {
        super::timer_of(actions)
    }

    /// The reply to the command `id` of a map write.
    fn reply(machine: &mut Machine, transport: TransportId, id: u32) -> Vec<Action> {
        machine.handle(frame(
            transport,
            &format!(r#"{{"id":{id},"publish":{{}}}}"#),
        ))
    }

    /// A subscribed map with one publish of `"k"` in flight (leading) and
    /// a later one parked in its window.
    fn window_on_k() -> (Machine, TransportId, SubscriptionId, CallId, DebounceTimer) {
        let (mut machine, transport, subscription) = subscribed_map(100);
        let (_, first) = publish(&mut machine, subscription, "k", "1");
        let (waiter, parked) = publish(&mut machine, subscription, "k", "2");
        assert_eq!(map_writes(&parked), []);
        (machine, transport, subscription, waiter, timer(&first))
    }

    // D2: the interval of a map subscription's replies.

    #[test]
    fn the_map_live_reply_sets_the_interval() {
        let (mut machine, _, subscription) = subscribed_map(100);

        let (_, first) = publish(&mut machine, subscription, "k", "1");

        assert_eq!(timers_set(&first), [(timer(&first), INTERVAL)]);
    }

    #[test]
    fn a_state_page_advising_an_interval_is_ignored_and_the_live_reply_decides() {
        let (mut machine, transport) = connected(config(), false);
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        // A page, with no cursor: the stream page is asked next.
        machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"phase":2,"epoch":"e","offset":1,"publish_debounce":500}}"#,
        ));
        // The live reply, advising nothing.
        machine.handle(frame(
            transport,
            r#"{"id":3,"subscribe":{"type":1,"epoch":"e","offset":1}}"#,
        ));

        let (_, first) = publish(&mut machine, subscription, "k", "1");
        let (_, second) = publish(&mut machine, subscription, "k", "2");

        assert_eq!(timers_set(&first), []);
        assert_eq!(map_writes(&first).len(), 1);
        assert_eq!(map_writes(&second).len(), 1);
    }

    #[test]
    fn a_map_publish_without_the_opt_in_ignores_the_interval() {
        let (mut machine, transport) = connected(config(), true);
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"epoch":"e","offset":1,"publish_debounce":100}}"#,
        ));

        let (_, first) = publish(&mut machine, subscription, "k", "1");
        let (_, second) = publish(&mut machine, subscription, "k", "2");

        assert_eq!(timers_set(&first), []);
        assert_eq!(map_writes(&second).len(), 1);
    }

    /// A later reply replaces the interval, 0 included (D2).
    #[test]
    fn a_later_live_reply_without_an_interval_turns_it_off() {
        let (mut machine, transport, subscription) = subscribed_map(100);
        machine.handle(Input::Unsubscribe(subscription));
        let actions = machine.handle(Input::Subscribe(subscription));
        let subscribe = commands(&actions)
            .into_iter()
            .find(|command| command.get("subscribe").is_some())
            .expect("a subscribe");
        machine.handle(frame(
            transport,
            &format!(
                r#"{{"id":{},"subscribe":{{"type":1,"epoch":"e","offset":1}}}}"#,
                command_id(&subscribe)
            ),
        ));

        let (_, first) = publish(&mut machine, subscription, "k", "1");
        let (_, second) = publish(&mut machine, subscription, "k", "2");

        assert_eq!(timers_set(&first), []);
        assert_eq!(map_writes(&second).len(), 1);
    }

    /// The map flow's second live-phase reply (`map_recovered`, D2).
    #[test]
    fn a_recovered_live_reply_sets_the_interval() {
        let (mut machine, transport) = connected(config(), false);
        let id = machine
            .add_map_subscription("scores".into(), map_options())
            .unwrap();
        machine.handle(Input::StreamOpened(Some(id)));
        machine.handle(Input::Subscribe(id));
        let live = machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"recoverable":true,"epoch":"e1","offset":7,"state":[{"key":"a","data":{"v":1},"offset":7}]}}"#,
        ));
        for action in &live {
            if let Action::EmitMapSubscription(event, ..) = action {
                machine.handle(Input::EventTaken(*event));
            }
        }
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let transport = match machine
            .handle(Input::TimerFired(Timer::Reconnect))
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            }) {
            Some(transport) => transport,
            None => panic!("a new transport"),
        };
        let mut opened = machine.handle(Input::TransportOpened(transport));
        let connect = commands(&opened)
            .into_iter()
            .find(|command| command.get("connect").is_some())
            .expect("a connect");
        opened.extend(machine.handle(frame(
            transport,
            &format!(
                r#"{{"id":{},"connect":{{"client":"c"}}}}"#,
                command_id(&connect)
            ),
        )));
        let subscribe = commands(&opened)
            .into_iter()
            .find(|command| command.get("subscribe").is_some())
            .expect("a recovering subscribe");
        assert_eq!(subscribe["subscribe"]["recover"], true, "{subscribe}");
        machine.handle(frame(
            transport,
            &format!(
                r#"{{"id":{},"subscribe":{{"type":1,"recoverable":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":9,"publish_debounce":250}}}}"#,
                command_id(&subscribe)
            ),
        ));

        let (_, first) = publish(&mut machine, id, "k", "1");

        assert_eq!(
            timers_set(&first).first().map(|(_, delay)| *delay),
            Some(Duration::from_millis(250))
        );
    }

    /// The interval is read before the calls waiting for `subscribed`
    /// start, in the map flow too (D2).
    #[test]
    fn a_map_publish_made_while_subscribing_is_debounced_by_the_live_reply_of_that_load() {
        let (mut machine, transport) = connected(config(), false);
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        let (_, first) = publish(&mut machine, subscription, "k", "1");
        let (_, second) = publish(&mut machine, subscription, "k", "2");
        assert_eq!(map_writes(&first), []);
        assert_eq!(map_writes(&second), []);

        let live = machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"epoch":"e","offset":1,"publish_debounce":100}}"#,
        ));

        assert_eq!(map_writes(&live), [("k".into(), Some(1))]);
        assert_eq!(timers_set(&live).len(), 1);
    }

    // D12: per key.

    #[test]
    fn each_key_has_its_own_window() {
        let (mut machine, _, subscription) = subscribed_map(100);

        let (_, a1) = publish(&mut machine, subscription, "a", "1");
        let (_, a2) = publish(&mut machine, subscription, "a", "2");
        let (_, b1) = publish(&mut machine, subscription, "b", "1");
        let (_, b2) = publish(&mut machine, subscription, "b", "2");

        assert_eq!(map_writes(&a1), [("a".into(), Some(1))]);
        assert_eq!(map_writes(&a2), []);
        assert_eq!(map_writes(&b1), [("b".into(), Some(1))]);
        assert_eq!(map_writes(&b2), []);
        assert_ne!(timer(&a1), timer(&b1));
        assert_eq!(machine.subscription_debounce_windows(subscription), 2);

        let fired = machine.handle(Input::DebounceTimerFired(timer(&a1)));

        assert_eq!(map_writes(&fired), [("a".into(), Some(2))]);
        let fired = machine.handle(Input::DebounceTimerFired(timer(&b1)));
        assert_eq!(map_writes(&fired), [("b".into(), Some(2))]);
    }

    #[test]
    fn the_empty_key_on_a_map_is_a_key_like_another() {
        let (mut machine, _, subscription) = subscribed_map(100);

        let (_, empty) = publish(&mut machine, subscription, "", "1");
        let (_, parked) = publish(&mut machine, subscription, "", "2");
        let (_, other) = publish(&mut machine, subscription, "b", "1");

        assert_eq!(map_writes(&empty), [(String::new(), Some(1))]);
        assert_eq!(map_writes(&parked), []);
        assert_eq!(map_writes(&other), [("b".into(), Some(1))]);
    }

    #[test]
    fn the_trailing_map_send_carries_its_key_and_data() {
        let (mut machine, _, subscription) = subscribed_map(100);
        let (_, first) = publish(&mut machine, subscription, "a", "1");
        publish(&mut machine, subscription, "a", "2");

        let fired = machine.handle(Input::DebounceTimerFired(timer(&first)));

        assert_eq!(map_writes(&fired), [("a".into(), Some(2))]);
        assert_eq!(timers_set(&fired).len(), 1);
    }

    #[test]
    fn a_window_of_one_key_ends_alone() {
        let (mut machine, _, subscription) = subscribed_map(100);
        let (_, a1) = publish(&mut machine, subscription, "a", "1");
        let (_, b1) = publish(&mut machine, subscription, "b", "1");
        publish(&mut machine, subscription, "b", "2");

        let fired = machine.handle(Input::DebounceTimerFired(timer(&a1)));
        assert_eq!(map_writes(&fired), []);
        assert_eq!(timers_set(&fired), []);
        assert_eq!(machine.subscription_debounce_windows(subscription), 1);

        let fired = machine.handle(Input::DebounceTimerFired(timer(&b1)));
        assert_eq!(map_writes(&fired), [("b".into(), Some(2))]);
    }

    #[test]
    fn client_level_map_writes_never_meet_the_gate() {
        let (mut machine, _, _) = subscribed_map(100);
        let first = machine.add_call(CallRequest::MapPublish {
            channel: "m".into(),
            key: "k".into(),
            data: b"1".to_vec(),
        });
        let second = machine.add_call(CallRequest::MapPublish {
            channel: "m".into(),
            key: "k".into(),
            data: b"2".to_vec(),
        });
        let remove = machine.add_call(CallRequest::MapRemove {
            channel: "m".into(),
            key: "k".into(),
        });

        let one = machine.handle(Input::Call(first));
        let two = machine.handle(Input::Call(second));
        let three = machine.handle(Input::Call(remove));

        assert_eq!(map_writes(&one), [("k".into(), Some(1))]);
        assert_eq!(map_writes(&two), [("k".into(), Some(2))]);
        assert_eq!(map_writes(&three), [("k".into(), None)]);
        assert_eq!(timers_set(&one), []);
        assert_eq!(timers_set(&three), []);
    }

    // D12: a remove.

    #[test]
    fn a_remove_cancels_the_window_and_its_latest_and_is_sent_at_once() {
        let (mut machine, _, subscription, _, window) = window_on_k();

        let (_, removed) = remove(&mut machine, subscription, "k");

        assert_eq!(map_writes(&removed), [("k".into(), None)]);
        assert_eq!(timers_cancelled(&removed), [window]);
        assert_eq!(timers_set(&removed), []);
        assert_eq!(machine.subscription_debounce_windows(subscription), 0);
        assert_eq!(machine.debounce_window_count(), 0);
    }

    #[test]
    fn the_timer_of_a_cancelled_window_is_ignored() {
        let (mut machine, _, subscription, _, window) = window_on_k();
        remove(&mut machine, subscription, "k");

        let fired = machine.handle(Input::DebounceTimerFired(window));

        assert_eq!(fired, []);
    }

    #[test]
    fn the_publishes_a_remove_replaced_follow_its_success_in_order() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (other, _) = publish(&mut machine, subscription, "k", "3");
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let command = command_id(&commands(&removed)[0]);

        let settled_actions = reply(&mut machine, transport, command);

        assert_eq!(
            settled(&settled_actions),
            [(waiter, ok()), (other, ok()), (remove_id, ok())]
        );
    }

    #[test]
    fn the_publishes_a_remove_replaced_fail_with_its_error() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let command = command_id(&commands(&removed)[0]);

        let settled_actions = machine.handle(frame(
            transport,
            &format!(
                r#"{{"id":{command},"error":{{"code":103,"message":"permission denied","temporary":true}}}}"#
            ),
        ));

        let error = Err(CallError::new(103, "permission denied").temporary(true));
        assert_eq!(
            settled(&settled_actions),
            [(waiter, error.clone()), (remove_id, error)]
        );
    }

    #[test]
    fn a_remove_with_no_waiters_closes_the_window_and_a_later_publish_is_sent_at_once() {
        let (mut machine, _, subscription) = subscribed_map(100);
        let (_, first) = publish(&mut machine, subscription, "k", "1");

        let (_, removed) = remove(&mut machine, subscription, "k");
        assert_eq!(timers_cancelled(&removed), [timer(&first)]);

        let (_, later) = publish(&mut machine, subscription, "k", "2");

        assert_eq!(map_writes(&later), [("k".into(), Some(2))]);
        assert_eq!(timers_set(&later).len(), 1);
    }

    #[test]
    fn a_remove_opens_no_window_and_does_not_park_a_later_publish() {
        let (mut machine, _, subscription) = subscribed_map(100);

        let (_, removed) = remove(&mut machine, subscription, "k");

        assert_eq!(map_writes(&removed), [("k".into(), None)]);
        assert_eq!(timers_set(&removed), []);
        assert_eq!(timers_cancelled(&removed), []);
        assert_eq!(machine.debounce_window_count(), 0);
        let (_, later) = publish(&mut machine, subscription, "k", "1");
        assert_eq!(map_writes(&later), [("k".into(), Some(1))]);
        assert_eq!(timers_set(&later).len(), 1);
    }

    #[test]
    fn a_remove_of_another_key_leaves_the_window_alone() {
        let (mut machine, _, subscription, waiter, window) = window_on_k();

        let (_, removed) = remove(&mut machine, subscription, "other");

        assert_eq!(timers_cancelled(&removed), []);
        assert_eq!(machine.debounce_window_count(), 1);
        let fired = machine.handle(Input::DebounceTimerFired(window));
        assert_eq!(map_writes(&fired), [("k".into(), Some(2))]);
        assert!(machine.call_wait(waiter).is_some());
    }

    #[test]
    fn a_publish_in_flight_keeps_its_reply_as_a_remove_follows() {
        let (mut machine, transport, subscription) = subscribed_map(100);
        let (leading, first) = publish(&mut machine, subscription, "k", "1");
        let command = command_id(&commands(&first)[0]);
        let (_, removed) = remove(&mut machine, subscription, "k");

        let replied = reply(&mut machine, transport, command);

        assert_eq!(settled(&replied), [(leading, ok())]);
        assert_eq!(map_writes(&removed), [("k".into(), None)]);
    }

    #[test]
    fn a_trailing_send_handed_over_keeps_its_followers_as_a_remove_follows() {
        let (mut machine, transport, subscription, waiter, window) = window_on_k();
        let fired = machine.handle(Input::DebounceTimerFired(window));
        let trailing = command_id(&commands(&fired)[0]);
        // The window the trailing send opened has nothing in it.
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let remove_command = command_id(&commands(&removed)[0]);

        let of_trailing = reply(&mut machine, transport, trailing);
        assert_eq!(settled(&of_trailing), [(waiter, ok())]);

        let of_remove = reply(&mut machine, transport, remove_command);
        assert_eq!(settled(&of_remove), [(remove_id, ok())]);
    }

    /// A remove made while subscribing meets the gate once subscribed, in
    /// call order, after the publishes before it (D3, D12).
    #[test]
    fn a_remove_made_while_subscribing_meets_the_gate_after_the_publishes_before_it() {
        let (mut machine, transport) = connected(config(), false);
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        publish(&mut machine, subscription, "k", "1");
        let (waiter, _) = publish(&mut machine, subscription, "k", "2");
        let (remove_id, _) = remove(&mut machine, subscription, "k");

        let live = machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"epoch":"e","offset":1,"publish_debounce":100}}"#,
        ));

        assert_eq!(
            map_writes(&live),
            [("k".into(), Some(1)), ("k".into(), None)]
        );
        assert_eq!(timers_cancelled(&live).len(), 1);
        assert_eq!(machine.debounce_window_count(), 0);
        let remove_command = command_id(
            commands(&live)
                .iter()
                .find(|command| command["publish"]["removed"] == true)
                .expect("the remove"),
        );
        let replied = reply(&mut machine, transport, remove_command);
        assert_eq!(settled(&replied), [(waiter, ok()), (remove_id, ok())]);
    }

    #[test]
    fn a_follower_of_a_remove_waits_on_the_removes_phase() {
        let (mut machine, _, subscription, waiter, _) = window_on_k();
        assert_eq!(machine.call_wait(waiter), Some(CallWait::Other));

        remove(&mut machine, subscription, "k");

        assert_eq!(machine.call_wait(waiter), Some(CallWait::Reply));
    }

    #[test]
    fn a_remove_lost_with_its_transport_fails_its_followers_with_code_11() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (remove_id, _) = remove(&mut machine, subscription, "k");

        let lost = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let settled = settled(&lost);
        for id in [waiter, remove_id] {
            let (_, result) = settled
                .iter()
                .find(|(call, _)| *call == id)
                .unwrap_or_else(|| panic!("{id:?} did not settle: {settled:?}"));
            assert_eq!(
                result.as_ref().err().map(|error| error.code),
                Some(codes::error::CONNECTION_CLOSED)
            );
        }
    }

    // D11, for a remove: the follower clauses and the remove's own owner.

    #[test]
    fn a_dropped_remove_with_followers_is_still_sent_and_settles_only_them() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let command = command_id(&commands(&removed)[0]);

        let abandoned = machine.handle(Input::CallAbandoned(remove_id));
        assert_eq!(settled(&abandoned), []);
        assert_eq!(machine.call_wait(waiter), Some(CallWait::Reply));

        let replied = reply(&mut machine, transport, command);

        assert_eq!(settled(&replied), [(waiter, ok())]);
    }

    #[test]
    fn a_dropped_remove_waiting_to_be_written_still_goes_for_its_followers() {
        // A filler keeps the write queue full: the remove waits for room.
        let (mut machine, transport) = connected(
            Config {
                write_queue_bytes: 200,
                ..config()
            },
            false,
        );
        let subscription = machine
            .add_map_subscription("m".into(), map_options())
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        machine.handle(frame(
            transport,
            r#"{"id":2,"subscribe":{"type":1,"epoch":"e","offset":1,"publish_debounce":100}}"#,
        ));
        let filler = machine.add_call(super::filler());
        let filled = machine.handle(Input::Call(filler));
        let (frame_id, _) = filled
            .iter()
            .find_map(|action| match action {
                Action::Send { id, frame, .. } => Some((*id, frame.clone())),
                _ => None,
            })
            .expect("the filling frame");
        let (_, first) = publish(&mut machine, subscription, "k", "1");
        assert_eq!(map_writes(&first), []);
        let (waiter, _) = publish(&mut machine, subscription, "k", "2");
        let (remove_id, _) = remove(&mut machine, subscription, "k");

        machine.handle(Input::CallAbandoned(remove_id));
        let drained = machine.handle(Input::FrameWritten {
            transport,
            frame: frame_id,
        });

        assert_eq!(
            map_writes(&drained),
            [("k".into(), Some(1)), ("k".into(), None)]
        );
        assert_eq!(machine.call_wait(waiter), Some(CallWait::Reply));
    }

    #[test]
    fn a_dropped_remove_with_no_follower_left_is_abandoned() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let command = command_id(&commands(&removed)[0]);

        machine.handle(Input::CallAbandoned(remove_id));
        let abandoned = machine.handle(Input::CallAbandoned(waiter));
        assert_eq!(settled(&abandoned), []);

        // The reply is ignored: nothing settles.
        let replied = reply(&mut machine, transport, command);
        assert_eq!(settled(&replied), []);
    }

    #[test]
    fn a_dropped_follower_leaves_the_remove_with_its_owner() {
        let (mut machine, transport, subscription, waiter, _) = window_on_k();
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        let command = command_id(&commands(&removed)[0]);

        let dropped = machine.handle(Input::CallAbandoned(waiter));
        assert_eq!(settled(&dropped), []);

        let replied = reply(&mut machine, transport, command);

        assert_eq!(settled(&replied), [(remove_id, ok())]);
    }

    #[test]
    fn a_remove_with_no_followers_abandoned_is_never_sent_if_queued() {
        let (mut machine, _, subscription) = subscribed_map(100);
        // A batch holds the remove: abandoning it withdraws it.
        machine.handle(Input::StartBatching);
        let (remove_id, removed) = remove(&mut machine, subscription, "k");
        assert_eq!(map_writes(&removed), []);

        machine.handle(Input::CallAbandoned(remove_id));
        let stopped = machine.handle(Input::StopBatching);

        assert_eq!(map_writes(&stopped), []);
    }

    // D10: map subscriptions leave `subscribed` through the same functions.

    #[test]
    fn losing_the_transport_drops_a_map_window_with_code_17() {
        let (mut machine, transport, _, waiter, window) = window_on_k();

        let lost = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        assert_eq!(timers_cancelled(&lost), [window]);
        let settled = settled(&lost);
        let (_, result) = settled
            .iter()
            .find(|(call, _)| *call == waiter)
            .expect("the waiter settled");
        assert_eq!(
            result.as_ref().err().map(|error| error.code),
            Some(codes::error::PUBLISH_DROPPED)
        );
    }

    #[test]
    fn unsubscribing_a_map_drops_its_windows_with_code_7() {
        let (mut machine, _, subscription, waiter, window) = window_on_k();

        let unsubscribed = machine.handle(Input::Unsubscribe(subscription));

        assert_eq!(timers_cancelled(&unsubscribed), [window]);
        let settled = settled(&unsubscribed);
        let (_, result) = settled
            .iter()
            .find(|(call, _)| *call == waiter)
            .expect("the waiter settled");
        assert_eq!(
            result.as_ref().err().map(|error| error.code),
            Some(codes::error::SUBSCRIPTION_UNSUBSCRIBED)
        );
    }
}
