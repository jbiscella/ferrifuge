//! Block 36b: subscribe commands in the frame of the connect command, on
//! the core.

use super::*;
use crate::SubscriptionState;

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// The frames among `actions`, as text.
fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
            _ => None,
        })
        .collect()
}

/// The channels of the subscribe commands each frame among `actions`
/// carries, `connect` standing for a connect command.
fn commands(actions: &[Action]) -> Vec<Vec<String>> {
    sent(actions)
        .iter()
        .map(|frame| {
            frame
                .lines()
                .map(|line| {
                    let command: serde_json::Value = serde_json::from_str(line).unwrap();
                    match command["subscribe"]["channel"].as_str() {
                        Some(channel) => channel.to_owned(),
                        None if command.get("connect").is_some() => "connect".to_owned(),
                        None => line.to_owned(),
                    }
                })
                .collect()
        })
        .collect()
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

/// A machine with subscriptions to `channels` subscribed before connect,
/// each with the options `options` gives it.
fn subscribing(
    mut machine: Machine,
    channels: &[&str],
    options: impl Fn(&str) -> SubscriptionOptions,
) -> (Machine, Vec<SubscriptionId>) {
    let ids = channels
        .iter()
        .map(|channel| {
            let id = machine
                .add_subscription((*channel).to_owned(), options(channel))
                .unwrap();
            machine.handle(Input::Subscribe(id));
            id
        })
        .collect();
    (machine, ids)
}

fn plain(channels: &[&str]) -> (Machine, Vec<SubscriptionId>) {
    subscribing(Machine::new(Config::default()), channels, |_| {
        SubscriptionOptions::default()
    })
}

fn subscribe_deadline(actions: &[Action], id: SubscriptionId) -> Option<Duration> {
    actions.iter().find_map(|action| match action {
        Action::SetSubscriptionTimer(sub, SubscriptionTimer::SubscribeReply, delay)
            if *sub == id =>
        {
            Some(*delay)
        }
        _ => None,
    })
}

#[test]
fn subscribes_go_in_the_connect_frame_in_registry_order() {
    let (mut machine, _) = plain(&["a", "b", "c"]);
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

    assert_eq!(commands(&opened), [["connect", "a", "b", "c"]]);
    assert_eq!(sent(&replied), Vec::<String>::new());
}

#[test]
fn an_emulated_transport_sends_its_subscribes_after_the_connect_reply() {
    let (machine, _) = plain(&["a"]);
    let mut machine = machine
        .with_transports(vec![TransportKind::HttpStream])
        .unwrap();
    let connect = machine.handle(Input::Connect);
    let transport = opened(&connect);

    let opened = machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
    ));

    let initial = connect.iter().find_map(|action| match action {
        Action::OpenTransport { initial_frame, .. } => initial_frame.clone(),
        _ => None,
    });
    assert_eq!(
        initial.map(|frame| String::from_utf8(frame).unwrap().lines().count()),
        Some(1)
    );
    assert_eq!(sent(&opened), Vec::<String>::new());
    assert_eq!(sent(&replied).len(), 1, "{replied:?}");
    assert!(
        sent(&replied)[0].contains(r#"\"channel\":\"a\""#),
        "{replied:?}"
    );
}

/// A subscription that needs a callback first starts it after the connect
/// reply, as before (Block 36, D5).
#[test]
fn a_subscription_needing_a_callback_waits_for_the_connect_reply() {
    let (mut machine, ids) = subscribing(
        Machine::new(Config::default()),
        &["news", "token", "data", "state"],
        |channel| match channel {
            "token" => SubscriptionOptions::default().token_source(),
            "data" => SubscriptionOptions::default().data_source(),
            "state" => SubscriptionOptions::default().state_source(),
            _ => SubscriptionOptions::default(),
        },
    );
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

    assert_eq!(commands(&opened), [["connect", "news"]]);
    let callbacks = |actions: &[Action]| -> Vec<SubscriptionId> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::FetchSubscriptionToken { subscription, .. }
                | Action::FetchSubscriptionData { subscription, .. }
                | Action::FetchSubscriptionState { subscription, .. } => Some(*subscription),
                _ => None,
            })
            .collect()
    };
    assert_eq!(callbacks(&opened), []);
    assert_eq!(callbacks(&replied), ids[1..]);
}

/// A subscription whose resubscribe backoff is pending subscribes when it
/// elapses, not with the connect command.
#[test]
fn a_pending_resubscribe_backoff_keeps_a_subscription_out_of_the_connect_frame() {
    let mut machine = Machine::new(Config::default());
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    let (mut machine, _) = subscribing(machine, &["a", "b"], |_| SubscriptionOptions::default());
    machine.handle(frame(
        transport,
        r#"{"id":2,"error":{"code":100,"message":"busy","temporary":true}}"#,
    ));
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".into(),
    });
    let again = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)));

    let opened = machine.handle(Input::TransportOpened(again));

    assert_eq!(commands(&opened), [["connect", "b"]]);
}

#[test]
fn only_the_subscribes_that_fit_the_cap_go_with_the_connect_command() {
    let config = Config::default().batch_frame_bytes(160);
    let (mut machine, _) = subscribing(Machine::new(config), &["a", "b", "c"], |_| {
        SubscriptionOptions::default()
    });
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

    assert!(sent(&opened)[0].len() <= 160, "{opened:?}");
    let with = commands(&opened)[0].len() - 1;
    assert!((1..3).contains(&with), "{opened:?}");
    // The others subscribe after the connect reply, in creation order.
    let after: Vec<String> = commands(&replied).concat();
    assert_eq!(
        [&commands(&opened)[0][1..], &after[..]].concat(),
        ["a", "b", "c"]
    );
}

#[test]
fn a_connect_command_over_the_cap_goes_alone() {
    let config = Config::default().batch_frame_bytes(10);
    let (mut machine, _) = subscribing(Machine::new(config), &["a", "b"], |_| {
        SubscriptionOptions::default()
    });
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

    assert_eq!(commands(&opened), [["connect"]]);
    assert_eq!(commands(&replied), [["a"], ["b"]]);
}

/// The deadline of a subscribe sent with the connect command starts when
/// the connect reply is read, and stops when its own reply is read
/// (Block 36, D6).
#[test]
fn an_optimistic_subscribe_s_deadline_starts_at_the_connect_reply() {
    let config = Config::default().timeout(Duration::from_secs(5));
    let (mut machine, ids) = subscribing(Machine::new(config), &["a"], |_| {
        SubscriptionOptions::default()
    });
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let connected = machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    let subscribed = machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));

    assert_eq!(subscribe_deadline(&opened, ids[0]), None, "{opened:?}");
    assert_eq!(
        subscribe_deadline(&connected, ids[0]),
        Some(Duration::from_secs(5))
    );
    assert!(subscribed.contains(&Action::CancelSubscriptionTimer(
        ids[0],
        SubscriptionTimer::SubscribeReply
    )));
}

/// A subscribe reply read with the connect reply, after it, needs no
/// deadline.
#[test]
fn a_subscribe_reply_read_with_the_connect_reply_arms_no_deadline() {
    let (mut machine, ids) = plain(&["a"]);
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));

    let replied = machine.handle(frame(
        transport,
        concat!(
            r#"{"id":1,"connect":{"client":"c"}}"#,
            "\n",
            r#"{"id":2,"subscribe":{}}"#
        ),
    ));

    let set = replied
        .iter()
        .position(|action| {
            matches!(action, Action::SetSubscriptionTimer(sub, SubscriptionTimer::SubscribeReply, _) if *sub == ids[0])
        })
        .expect("the deadline starts");
    let cancelled = replied.iter().position(|action| {
        *action == Action::CancelSubscriptionTimer(ids[0], SubscriptionTimer::SubscribeReply)
    });
    assert!(cancelled > Some(set), "{replied:?}");
    assert_eq!(
        machine.subscription_state(ids[0]),
        Some(SubscriptionState::Subscribed)
    );
}

#[test]
fn a_subscribe_reply_before_the_connect_reply_breaks_the_protocol() {
    let (mut machine, ids) = plain(&["a"]);
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));

    let replied = machine.handle(frame(
        transport,
        concat!(
            r#"{"id":2,"subscribe":{}}"#,
            "\n",
            r#"{"id":1,"connect":{"client":"c"}}"#
        ),
    ));

    assert!(
        replied.contains(&Action::Anomaly(Anomaly::ReplyBeforeConnect {
            transport,
            command: "subscribe",
        })),
        "{replied:?}"
    );
    // Handled as a lost transport: the client, still connecting, closes it
    // and reconnects (Block 21, D4).
    assert!(
        replied.contains(&Action::CloseTransport(transport)),
        "{replied:?}"
    );
    assert!(
        replied
            .iter()
            .any(|action| matches!(action, Action::SetTimer(Timer::Reconnect, _))),
        "{replied:?}"
    );
    assert!(
        !replied.iter().any(|action| matches!(
            action,
            Action::EmitSubscription(_, _, SubscriptionEvent::Subscribed(_))
        )),
        "{replied:?}"
    );
    assert_eq!(
        machine.subscription_state(ids[0]),
        Some(SubscriptionState::Subscribing)
    );
}

/// A failed connect loses the optimistic subscribes as a teardown does:
/// the subscription stays subscribing and goes with the next connect
/// command (Block 36, D7).
#[test]
fn a_failed_connect_loses_the_optimistic_subscribes() {
    let (mut machine, ids) = plain(&["a"]);
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));

    let failed = machine.handle(frame(
        transport,
        r#"{"id":1,"error":{"code":100,"message":"internal","temporary":true}}"#,
    ));
    let again = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)));
    let reopened = machine.handle(Input::TransportOpened(again));

    assert!(
        !failed
            .iter()
            .any(|action| matches!(action, Action::EmitSubscription(..))),
        "{failed:?}"
    );
    assert_eq!(
        machine.subscription_state(ids[0]),
        Some(SubscriptionState::Subscribing)
    );
    assert_eq!(commands(&reopened), [["connect", "a"]]);
}

/// With automatic batching the connect command and its subscribes go into
/// the output buffer, then out in one frame.
#[test]
fn with_automatic_batching_the_connect_frame_is_flushed_whole() {
    let (machine, _) = plain(&["a", "b"]);
    let mut machine = machine.with_automatic_batching();
    let transport = opened(&machine.handle(Input::Connect));

    let opened = machine.handle(Input::TransportOpened(transport));
    let flushed = machine.handle(Input::Flush);

    assert_eq!(sent(&opened), Vec::<String>::new());
    assert_eq!(commands(&flushed), [["connect", "a", "b"]]);
}

#[test]
fn the_debug_output_of_the_anomaly_names_it() {
    let anomaly = Anomaly::ReplyBeforeConnect {
        transport: TransportId(3),
        command: "subscribe",
    };

    assert_eq!(
        format!("{anomaly:?}"),
        r#"ReplyBeforeConnect { transport: TransportId(3), command: "subscribe" }"#
    );
}
