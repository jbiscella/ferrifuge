//! Steps of Block 37a: the test constructors, checked against what the
//! client core yields from the wire (@E).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, SubscriptionOptions, TransportKind};
use ferrifuge::{
    CallReply, CallRequest, ClientEvent, ClientInfo, Config, HistoryOptions, HistoryResult,
    PresenceResult, PresenceStatsResult, Publication, StreamPosition, Subscribed,
    SubscriptionEvent,
};
use serde_json::{Value, json};

use super::ClientWorld;
use super::batching::{BatchRun, run};

/// What a 37a scenario's core yielded, and the value the steps build.
#[derive(Debug, Default)]
pub(crate) struct TestSupport {
    built: Option<Yielded>,
}

#[derive(Debug, Clone, PartialEq)]
enum Yielded {
    Connected(ClientEvent),
    Subscribed(Subscribed),
    Publication(Publication),
    History(HistoryResult),
    Presence(PresenceResult),
    PresenceStats(PresenceStatsResult),
}

#[given(
    regex = r#"^a client core with the JSON protocol over websocket, with a subscription to "news" subscribed before connect$"#
)]
fn a_core(world: &mut ClientWorld) {
    let mut run = BatchRun::of(
        Machine::new(Config::default())
            .with_transports(vec![TransportKind::WebSocket])
            .expect("one transport"),
    );
    let id = run
        .machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    run.handle(Input::Subscribe(id));
    run.subscriptions.push(id);
    world.batching.run = Some(run);
}

/// The id of the last `method` command sent.
fn command_id(run: &BatchRun, method: &str) -> u64 {
    run.frames_since(0)
        .into_iter()
        .flat_map(|(_, commands)| commands)
        .filter(|command| command[method].is_object())
        .filter_map(|command| command["id"].as_u64())
        .next_back()
        .unwrap_or_else(|| panic!("a {method} command"))
}

fn connect_reply(run: &mut BatchRun, result: &Value) {
    let id = command_id(run, "connect");
    run.mark = run.actions.len();
    run.frame(&json!({"id": id, "connect": result}));
}

fn subscribe_reply(run: &mut BatchRun, result: &Value) {
    let id = command_id(run, "subscribe");
    run.frame(&json!({"id": id, "subscribe": result}));
}

/// Calls `request`, then answers its command with `result` under `method`.
fn call_answered(run: &mut BatchRun, request: CallRequest, method: &str, result: &Value) {
    let call = run.machine.add_call(request);
    run.handle(Input::Call(call));
    let id = command_id(run, method);
    run.frame(&json!({"id": id, method: result}));
}

#[when(regex = r#"^the core reads the connect reply with client id "c1" and data \{"a":1\}$"#)]
fn connect_with_data(world: &mut ClientWorld) {
    connect_reply(run(world), &json!({"client": "c1", "data": {"a": 1}}));
}

#[when(
    regex = r#"^the core reads the connect reply, then the subscribe reply for "news", recoverable and positioned, at offset 3 with epoch "e"$"#
)]
fn subscribed_at_three(world: &mut ClientWorld) {
    let run = run(world);
    connect_reply(run, &json!({"client": "c1"}));
    subscribe_reply(
        run,
        &json!({"recoverable": true, "positioned": true, "offset": 3, "epoch": "e"}),
    );
}

#[when(
    regex = r#"^the core reads the connect reply and the subscribe reply, then a publication of "news" with data \{"n":1\}, offset 4, tag "k" "v" and info of client "c1", user "u1"$"#
)]
fn a_publication(world: &mut ClientWorld) {
    let run = run(world);
    connect_reply(run, &json!({"client": "c1"}));
    subscribe_reply(run, &json!({}));
    run.frame(&json!({"push": {"channel": "news", "pub": {
        "data": {"n": 1},
        "offset": 4,
        "tags": {"k": "v"},
        "info": {"client": "c1", "user": "u1"},
    }}}));
}

#[when(
    regex = r#"^the core reads the connect reply, the application requests the history of "news" with limit -1, and the core reads publications \{"n":1\} at 1 and \{"n":2\} at 2, epoch "e", offset 2$"#
)]
fn a_history(world: &mut ClientWorld) {
    let run = run(world);
    connect_reply(run, &json!({"client": "c1"}));
    call_answered(
        run,
        CallRequest::History {
            channel: "news".to_owned(),
            options: HistoryOptions::default().limit(-1),
        },
        "history",
        &json!({
            "publications": [{"data": {"n": 1}, "offset": 1}, {"data": {"n": 2}, "offset": 2}],
            "epoch": "e",
            "offset": 2,
        }),
    );
}

#[when(
    regex = r#"^the core reads the connect reply, the application requests the presence of "news", and the core reads one client "c1" of user "u1"$"#
)]
fn a_presence(world: &mut ClientWorld) {
    let run = run(world);
    connect_reply(run, &json!({"client": "c1"}));
    call_answered(
        run,
        CallRequest::Presence {
            channel: "news".to_owned(),
        },
        "presence",
        &json!({"presence": {"c1": {"client": "c1", "user": "u1"}}}),
    );
}

#[when(
    regex = r#"^the core reads the connect reply, the application requests the presence stats of "news", and the core reads 2 clients and 1 user$"#
)]
fn presence_stats(world: &mut ClientWorld) {
    let run = run(world);
    connect_reply(run, &json!({"client": "c1"}));
    call_answered(
        run,
        CallRequest::PresenceStats {
            channel: "news".to_owned(),
        },
        "presence_stats",
        &json!({"num_clients": 2, "num_users": 1}),
    );
}

/// What the core yielded of `kind` since the mark.
fn yielded(run: &BatchRun, kind: &str) -> Yielded {
    let since = &run.actions[run.mark..];
    since
        .iter()
        .find_map(|action| match (kind, action) {
            ("connected", Action::Emit(_, event @ ClientEvent::Connected { .. })) => {
                Some(Yielded::Connected(event.clone()))
            }
            ("subscribed", Action::EmitSubscription(_, _, SubscriptionEvent::Subscribed(s))) => {
                Some(Yielded::Subscribed(s.clone()))
            }
            ("publication", Action::EmitSubscription(_, _, SubscriptionEvent::Publication(p))) => {
                Some(Yielded::Publication(p.clone()))
            }
            (
                "history",
                Action::CallSettled {
                    result: Ok(CallReply::History(h)),
                    ..
                },
            ) => Some(Yielded::History(h.clone())),
            (
                "presence",
                Action::CallSettled {
                    result: Ok(CallReply::Presence(p)),
                    ..
                },
            ) => Some(Yielded::Presence(p.clone())),
            (
                "presence stats",
                Action::CallSettled {
                    result: Ok(CallReply::PresenceStats(s)),
                    ..
                },
            ) => Some(Yielded::PresenceStats(*s)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the core yielded no {kind}: {since:?}"))
}

/// The value the row builds with the test constructors.
fn built(kind: &str) -> Yielded {
    match kind {
        "connected" => Yielded::Connected(ClientEvent::connected(
            "c1",
            "websocket",
            Some(br#"{"a":1}"#.to_vec()),
        )),
        "subscribed" => Yielded::Subscribed(
            Subscribed::default()
                .recoverable(true)
                .positioned(true)
                .stream_position(StreamPosition {
                    offset: 3,
                    epoch: "e".to_owned(),
                }),
        ),
        "publication" => Yielded::Publication(
            Publication::new(br#"{"n":1}"#.to_vec())
                .offset(4)
                .tags([("k", "v")])
                .info(ClientInfo::new("c1", "u1")),
        ),
        "history" => Yielded::History(
            HistoryResult::default()
                .publications([
                    Publication::new(br#"{"n":1}"#.to_vec()).offset(1),
                    Publication::new(br#"{"n":2}"#.to_vec()).offset(2),
                ])
                .epoch("e")
                .offset(2),
        ),
        "presence" => {
            Yielded::Presence(PresenceResult::default().clients([ClientInfo::new("c1", "u1")]))
        }
        _ => Yielded::PresenceStats(PresenceStatsResult::default().num_clients(2).num_users(1)),
    }
}

#[then(
    regex = r#"^the core yields (the client's "connected" event|the "subscribed" event of "news"|the publication event of "news"|the history result|the presence result|the presence stats result), equal to .+$"#
)]
fn yields_equal(world: &mut ClientWorld, what: String) {
    let kind = match what.as_str() {
        r#"the client's "connected" event"# => "connected",
        r#"the "subscribed" event of "news""# => "subscribed",
        r#"the publication event of "news""# => "publication",
        "the history result" => "history",
        "the presence result" => "presence",
        _ => "presence stats",
    };
    let got = yielded(run(world), kind);
    let built = built(kind);
    assert_eq!(got, built);
    world.test_support.built = Some(built);
}

#[then("the built value holds every value the row names")]
fn built_holds_the_row(world: &mut ClientWorld) {
    let built = world.test_support.built.clone().expect("a built value");
    match built {
        Yielded::Connected(ClientEvent::Connected {
            client_id,
            transport,
            data,
            ..
        }) => assert_eq!(
            (client_id.as_str(), transport.as_str(), data.as_deref()),
            ("c1", "websocket", Some(&br#"{"a":1}"#[..]))
        ),
        Yielded::Connected(other) => panic!("not a connected event: {other:?}"),
        Yielded::Subscribed(subscribed) => {
            assert!(
                subscribed.recoverable && subscribed.positioned,
                "{subscribed:?}"
            );
            assert_eq!(
                subscribed.stream_position,
                Some(StreamPosition {
                    offset: 3,
                    epoch: "e".to_owned()
                })
            );
        }
        Yielded::Publication(publication) => {
            assert_eq!(publication.data, br#"{"n":1}"#);
            assert_eq!(publication.offset, Some(4));
            assert_eq!(publication.tags.get("k").map(String::as_str), Some("v"));
            let info = publication.info.expect("an info");
            assert_eq!((info.client.as_str(), info.user.as_str()), ("c1", "u1"));
        }
        Yielded::History(history) => {
            let offsets: Vec<Option<u64>> = history.publications.iter().map(|p| p.offset).collect();
            assert_eq!(offsets, [Some(1), Some(2)]);
            assert_eq!(history.publications[1].data, br#"{"n":2}"#);
            assert_eq!((history.epoch.as_str(), history.offset), ("e", 2));
        }
        Yielded::Presence(presence) => {
            assert_eq!(presence.clients.len(), 1);
            assert_eq!(presence.clients["c1"].user, "u1");
        }
        Yielded::PresenceStats(stats) => {
            assert_eq!((stats.num_clients, stats.num_users), (2, 1));
        }
    }
}
