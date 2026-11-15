//! No `Debug` output of the crate shows a secret (Block 60, D2; Block 21).
//!
//! Every token, signature, payload, data, header value, tag value,
//! credential and cookie in the samples carries a distinct sentinel, and
//! the formatted output must show none of them, neither as text nor as the
//! list of its bytes. A wildcard-free `match` names every `Input` and
//! `Action` variant and field, a check against the core's source finds any
//! variant without a sample, and an inventory of the crate's hand-written
//! `Debug` impls names each as sampled here or as holding no secret.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::time::Duration;

use super::*;
use crate::call::{HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, RpcResult};
use crate::subscription::{ClientInfo, Publication};
use crate::{CallError, ClientEvent, SubscriptionEvent, TagFilter};

/// The path of the sans-IO core's source from the manifest directory,
/// followed by `$rest`: the one place naming it, for a rename.
macro_rules! core_path {
    ($rest:literal) => {
        concat!("src/sans_io", $rest)
    };
}

/// The sentinels the samples carry: each a distinct string.
#[derive(Default)]
struct Sentinels(Vec<String>);

impl Sentinels {
    fn text(&mut self, what: &str) -> String {
        let sentinel = format!("SeNtInEl{}x{what}", self.0.len());
        self.0.push(sentinel.clone());
        sentinel
    }

    fn bytes(&mut self, what: &str) -> Vec<u8> {
        self.text(what).into_bytes()
    }

    /// Fails when `value`'s `Debug` output, plain or pretty, shows one of
    /// the sentinels, as text or as the `Debug` list of its bytes.
    fn assert_hidden(&self, what: &str, value: &dyn Debug) {
        for shown in [format!("{value:?}"), format!("{value:#?}")] {
            let compact: String = shown.chars().filter(|c| !c.is_whitespace()).collect();
            for sentinel in &self.0 {
                assert!(
                    !shown.contains(sentinel.as_str()),
                    "{what} shows {sentinel}: {shown}"
                );
                let bytes = sentinel
                    .bytes()
                    .map(|byte| byte.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                assert!(
                    !compact.contains(&bytes),
                    "{what} shows the bytes of {sentinel}: {shown}"
                );
            }
        }
    }
}

const TRANSPORT: TransportId = TransportId(1);
const FETCH: FetchId = FetchId(2);
const SUBSCRIPTION: SubscriptionId = SubscriptionId(3);
const EVENT: EventId = EventId(4);
const CALL: CallId = CallId(5);
const SETTING: TimerSetting = TimerSetting(6);

fn tags(s: &mut Sentinels) -> HashMap<String, String> {
    HashMap::from([("kind".to_owned(), s.text("tag-value"))])
}

fn client_info(s: &mut Sentinels) -> ClientInfo {
    ClientInfo {
        client: "client-id".to_owned(),
        user: "user-id".to_owned(),
        conn_info: Some(s.bytes("conn-info")),
        chan_info: Some(s.bytes("chan-info")),
    }
}

fn publication(s: &mut Sentinels) -> Publication {
    Publication {
        data: s.bytes("publication-data"),
        offset: Some(1),
        tags: tags(s),
        info: Some(client_info(s)),
        epoch: Some("epoch".to_owned()),
        channel: Some("news".to_owned()),
        ..Publication::default()
    }
}

fn tag_filter(s: &mut Sentinels) -> TagFilter {
    TagFilter::eq("region", s.text("tag-filter-value"))
        .and(TagFilter::eq("tier", s.text("tag-filter-value")))
}

/// One value of every `Input` variant, each secret field a sentinel.
fn inputs(s: &mut Sentinels) -> Vec<Input> {
    let mut inputs = Vec::new();
    inputs.extend([
        Input::Connect,
        Input::Reconnect,
        Input::Disconnect,
        Input::Close,
        Input::DriverDropped { panicked: true },
        Input::TransportOpened(TRANSPORT),
        Input::TransportUnsupported(TRANSPORT),
        Input::TransportFailed {
            transport: TRANSPORT,
            message: "connection refused".to_owned(),
        },
        Input::MessageTooLarge(TRANSPORT),
        Input::InboxOverflow(TRANSPORT),
        Input::Offline,
        Input::PageCached,
        Input::TransportClosed {
            transport: TRANSPORT,
            code: Some(3001),
            reason: "reconnect".to_owned(),
        },
        Input::Frame {
            transport: TRANSPORT,
            data: s.bytes("frame"),
        },
        Input::SendRejected {
            transport: TRANSPORT,
            frame: s.bytes("rejected-frame"),
        },
        Input::FrameWritten {
            transport: TRANSPORT,
            frame: FrameId(7),
        },
        Input::TimerFired(Timer::Refresh, SETTING),
        Input::DebounceTimerFired(DebounceTimer(8)),
        Input::TokenFetched {
            fetch: FETCH,
            result: Ok(s.text("token")),
        },
        Input::StartBatching,
        Input::StopBatching,
        Input::Flush,
        Input::SetToken(s.text("set-token")),
        Input::SetData(s.bytes("set-data")),
        Input::SetSubscriptionData(SUBSCRIPTION, s.bytes("subscription-data")),
        Input::SetSubscriptionTagFilter(SUBSCRIPTION, Some(tag_filter(s))),
        Input::SetEmulatedHeaders(BTreeMap::from([(
            "authorization".to_owned(),
            s.text("emulated-header"),
        )])),
        Input::ConnectDataFetched {
            fetch: FETCH,
            result: Ok(s.bytes("connect-data")),
        },
    ]);
    inputs.extend(subscription_and_call_inputs(s));
    #[cfg(feature = "unstable")]
    inputs.extend(shared_poll_inputs(s));
    inputs
}

/// The subscription, event and call inputs of [`inputs`].
fn subscription_and_call_inputs(s: &mut Sentinels) -> Vec<Input> {
    vec![
        Input::Subscribe(SUBSCRIPTION),
        Input::Unsubscribe(SUBSCRIPTION),
        Input::RemoveSubscription(SUBSCRIPTION),
        Input::SubscriptionTimerFired(SUBSCRIPTION, SubscriptionTimer::Refresh, SETTING),
        Input::SubscriptionDataFetched {
            fetch: FETCH,
            result: Ok(s.bytes("fetched-subscription-data")),
        },
        Input::SubscriptionTokenFetched {
            fetch: FETCH,
            result: Ok(s.text("subscription-token")),
        },
        Input::SubscriptionStateFetched {
            fetch: FETCH,
            result: Ok(StreamPosition::new(9, "epoch")),
        },
        Input::EventTaken(EVENT),
        Input::EventHanded(EVENT),
        Input::HandlerPanicked {
            event: EVENT,
            message: "handler panicked".to_owned(),
        },
        Input::StreamOpened(Some(SUBSCRIPTION)),
        Input::StreamClosed(None),
        Input::Call(CALL),
        Input::CallTimerFired(CALL, CallTimer::Reply, SETTING),
        Input::CallAbandoned(CALL),
        Input::Slept {
            gap: Duration::from_secs(30),
        },
    ]
}

/// The shared poll inputs of [`inputs`].
#[cfg(feature = "unstable")]
fn shared_poll_inputs(s: &mut Sentinels) -> Vec<Input> {
    vec![
        Input::TrackKeys(SUBSCRIPTION, vec!["a".to_owned()]),
        Input::TrackKeysWithSignature(
            SUBSCRIPTION,
            vec![crate::SharedPollTrackItem::new("a", 1)],
            s.text("track-signature"),
        ),
        Input::UntrackKeys(SUBSCRIPTION, vec!["a".to_owned()]),
        Input::SharedPollTimerFired(SharedPollTimer(10)),
        Input::SharedPollRefreshFired(SUBSCRIPTION),
        Input::SignatureFetched {
            subscription: SUBSCRIPTION,
            request: FETCH,
            result: Ok(crate::SharedPollSignature::new(
                vec!["a".to_owned()],
                s.text("fetched-signature"),
            )),
        },
    ]
}

/// One value of every `Action` variant, each secret field a sentinel. The
/// events inside carry sentinels too; every event variant keeps the test of
/// `src/event.rs`.
fn actions(s: &mut Sentinels) -> Vec<Action> {
    let mut actions = Vec::new();
    actions.extend([
        Action::OpenTransport {
            transport: TRANSPORT,
            kind: TransportKind::WebSocket,
            endpoint: 0,
            initial_frame: Some(s.bytes("initial-frame")),
        },
        Action::CloseTransport(TRANSPORT),
        Action::Send {
            transport: TRANSPORT,
            id: FrameId(7),
            frame: s.bytes("sent-frame"),
            frame_type: FrameType::Text,
        },
        Action::SetTimer(Timer::Refresh, Duration::from_secs(1), SETTING),
        Action::CancelTimer(Timer::Refresh),
        Action::Emit(
            EVENT,
            ClientEvent::Message(crate::MessageContext {
                data: s.bytes("message"),
            }),
        ),
        Action::FetchToken(FETCH),
        Action::FetchConnectData(FETCH),
        Action::CancelConnectData(FETCH),
        Action::SetSubscriptionTimer(
            SUBSCRIPTION,
            SubscriptionTimer::Refresh,
            Duration::from_secs(1),
            SETTING,
        ),
        Action::CancelSubscriptionTimer(SUBSCRIPTION, SubscriptionTimer::Refresh),
        Action::EmitSubscription(
            EVENT,
            SUBSCRIPTION,
            SubscriptionEvent::Publication(publication(s)),
        ),
        Action::Withdraw(vec![EVENT]),
        Action::FetchSubscriptionData {
            subscription: SUBSCRIPTION,
            channel: "news".to_owned(),
            fetch: FETCH,
        },
        Action::CancelSubscriptionData(FETCH),
        Action::FetchSubscriptionToken {
            subscription: SUBSCRIPTION,
            channel: "news".to_owned(),
            fetch: FETCH,
        },
        Action::CancelSubscriptionToken(FETCH),
        Action::FetchSubscriptionState {
            subscription: SUBSCRIPTION,
            channel: "news".to_owned(),
            fetch: FETCH,
        },
        Action::CancelSubscriptionState(FETCH),
        Action::SetDebounceTimer(DebounceTimer(8), Duration::from_secs(1)),
        Action::CancelDebounceTimer(DebounceTimer(8)),
        Action::SetCallTimer(CALL, CallTimer::Reply, Duration::from_secs(1), SETTING),
        Action::CancelCallTimer(CALL, CallTimer::Reply),
        Action::CallSettled {
            call: CALL,
            result: Ok(CallReply::Rpc(s.bytes("call-result"))),
        },
        Action::Anomaly(Anomaly::UndecodableFrame {
            transport: TRANSPORT,
            bytes: 3,
        }),
        Action::ShortenTimer(Timer::Refresh, Duration::from_secs(1)),
        Action::ShortenSubscriptionTimer(
            SUBSCRIPTION,
            SubscriptionTimer::Refresh,
            Duration::from_secs(1),
        ),
    ]);
    #[cfg(feature = "unstable")]
    actions.extend([
        Action::EmitMapSubscription(
            EVENT,
            SUBSCRIPTION,
            crate::MapSubscriptionEvent::Update(map_update(s)),
        ),
        Action::EmitSharedPollSubscription(
            EVENT,
            SUBSCRIPTION,
            crate::SharedPollSubscriptionEvent::Update(shared_poll_update(s)),
        ),
        Action::SetSharedPollTimer(SharedPollTimer(10), Duration::from_secs(1)),
        Action::CancelSharedPollTimer(SharedPollTimer(10)),
        Action::SetSharedPollRefresh(SUBSCRIPTION, Duration::from_secs(1)),
        Action::CancelSharedPollRefresh(SUBSCRIPTION),
        Action::FetchSignature {
            subscription: SUBSCRIPTION,
            request: FETCH,
            keys: vec!["a".to_owned()],
        },
    ]);
    actions
}

#[cfg(feature = "unstable")]
fn map_update(s: &mut Sentinels) -> crate::MapUpdate {
    crate::MapUpdate {
        key: "k".to_owned(),
        data: s.bytes("map-update-data"),
        tags: tags(s),
        info: Some(client_info(s)),
        ..crate::MapUpdate::default()
    }
}

#[cfg(feature = "unstable")]
fn shared_poll_update(s: &mut Sentinels) -> crate::SharedPollUpdate {
    crate::SharedPollUpdate {
        key: "k".to_owned(),
        data: s.bytes("shared-poll-update-data"),
        ..crate::SharedPollUpdate::default()
    }
}

/// The variant's name, naming every variant and field: a new variant or
/// field fails to compile until a sample covers it.
fn input_name(input: &Input) -> &'static str {
    match input {
        Input::Connect => "Connect",
        Input::Reconnect => "Reconnect",
        Input::Disconnect => "Disconnect",
        Input::Close => "Close",
        Input::DriverDropped { panicked: _ } => "DriverDropped",
        Input::TransportOpened(_) => "TransportOpened",
        Input::TransportUnsupported(_) => "TransportUnsupported",
        Input::TransportFailed {
            transport: _,
            message: _,
        } => "TransportFailed",
        Input::MessageTooLarge(_) => "MessageTooLarge",
        Input::InboxOverflow(_) => "InboxOverflow",
        Input::Offline => "Offline",
        Input::PageCached => "PageCached",
        Input::TransportClosed {
            transport: _,
            code: _,
            reason: _,
        } => "TransportClosed",
        Input::Frame {
            transport: _,
            data: _,
        } => "Frame",
        Input::SendRejected {
            transport: _,
            frame: _,
        } => "SendRejected",
        Input::FrameWritten {
            transport: _,
            frame: _,
        } => "FrameWritten",
        Input::TimerFired(_, _) => "TimerFired",
        Input::DebounceTimerFired(_) => "DebounceTimerFired",
        Input::TokenFetched {
            fetch: _,
            result: _,
        } => "TokenFetched",
        Input::StartBatching => "StartBatching",
        Input::StopBatching => "StopBatching",
        Input::Flush => "Flush",
        Input::SetToken(_) => "SetToken",
        Input::SetData(_) => "SetData",
        Input::SetSubscriptionData(_, _) => "SetSubscriptionData",
        Input::SetSubscriptionTagFilter(_, _) => "SetSubscriptionTagFilter",
        Input::SetEmulatedHeaders(_) => "SetEmulatedHeaders",
        Input::ConnectDataFetched {
            fetch: _,
            result: _,
        } => "ConnectDataFetched",
        Input::Subscribe(_) => "Subscribe",
        Input::Unsubscribe(_) => "Unsubscribe",
        Input::RemoveSubscription(_) => "RemoveSubscription",
        Input::SubscriptionTimerFired(_, _, _) => "SubscriptionTimerFired",
        Input::SubscriptionDataFetched {
            fetch: _,
            result: _,
        } => "SubscriptionDataFetched",
        Input::SubscriptionTokenFetched {
            fetch: _,
            result: _,
        } => "SubscriptionTokenFetched",
        Input::SubscriptionStateFetched {
            fetch: _,
            result: _,
        } => "SubscriptionStateFetched",
        Input::EventTaken(_) => "EventTaken",
        Input::EventHanded(_) => "EventHanded",
        Input::HandlerPanicked {
            event: _,
            message: _,
        } => "HandlerPanicked",
        Input::StreamOpened(_) => "StreamOpened",
        Input::StreamClosed(_) => "StreamClosed",
        Input::Call(_) => "Call",
        Input::CallTimerFired(_, _, _) => "CallTimerFired",
        Input::CallAbandoned(_) => "CallAbandoned",
        Input::Slept { gap: _ } => "Slept",
        #[cfg(feature = "unstable")]
        Input::TrackKeys(_, _) => "TrackKeys",
        #[cfg(feature = "unstable")]
        Input::TrackKeysWithSignature(_, _, _) => "TrackKeysWithSignature",
        #[cfg(feature = "unstable")]
        Input::UntrackKeys(_, _) => "UntrackKeys",
        #[cfg(feature = "unstable")]
        Input::SharedPollTimerFired(_) => "SharedPollTimerFired",
        #[cfg(feature = "unstable")]
        Input::SharedPollRefreshFired(_) => "SharedPollRefreshFired",
        #[cfg(feature = "unstable")]
        Input::SignatureFetched {
            subscription: _,
            request: _,
            result: _,
        } => "SignatureFetched",
    }
}

/// As [`input_name`], for `Action`.
fn action_name(action: &Action) -> &'static str {
    match action {
        Action::OpenTransport {
            transport: _,
            kind: _,
            endpoint: _,
            initial_frame: _,
        } => "OpenTransport",
        Action::CloseTransport(_) => "CloseTransport",
        Action::Send {
            transport: _,
            id: _,
            frame: _,
            frame_type: _,
        } => "Send",
        Action::SetTimer(_, _, _) => "SetTimer",
        Action::CancelTimer(_) => "CancelTimer",
        Action::Emit(_, _) => "Emit",
        Action::FetchToken(_) => "FetchToken",
        Action::FetchConnectData(_) => "FetchConnectData",
        Action::CancelConnectData(_) => "CancelConnectData",
        Action::SetSubscriptionTimer(_, _, _, _) => "SetSubscriptionTimer",
        Action::CancelSubscriptionTimer(_, _) => "CancelSubscriptionTimer",
        Action::EmitSubscription(_, _, _) => "EmitSubscription",
        #[cfg(feature = "unstable")]
        Action::EmitMapSubscription(_, _, _) => "EmitMapSubscription",
        #[cfg(feature = "unstable")]
        Action::EmitSharedPollSubscription(_, _, _) => "EmitSharedPollSubscription",
        #[cfg(feature = "unstable")]
        Action::SetSharedPollTimer(_, _) => "SetSharedPollTimer",
        #[cfg(feature = "unstable")]
        Action::CancelSharedPollTimer(_) => "CancelSharedPollTimer",
        #[cfg(feature = "unstable")]
        Action::SetSharedPollRefresh(_, _) => "SetSharedPollRefresh",
        #[cfg(feature = "unstable")]
        Action::CancelSharedPollRefresh(_) => "CancelSharedPollRefresh",
        #[cfg(feature = "unstable")]
        Action::FetchSignature {
            subscription: _,
            request: _,
            keys: _,
        } => "FetchSignature",
        Action::Withdraw(_) => "Withdraw",
        Action::FetchSubscriptionData {
            subscription: _,
            channel: _,
            fetch: _,
        } => "FetchSubscriptionData",
        Action::CancelSubscriptionData(_) => "CancelSubscriptionData",
        Action::FetchSubscriptionToken {
            subscription: _,
            channel: _,
            fetch: _,
        } => "FetchSubscriptionToken",
        Action::CancelSubscriptionToken(_) => "CancelSubscriptionToken",
        Action::FetchSubscriptionState {
            subscription: _,
            channel: _,
            fetch: _,
        } => "FetchSubscriptionState",
        Action::CancelSubscriptionState(_) => "CancelSubscriptionState",
        Action::SetDebounceTimer(_, _) => "SetDebounceTimer",
        Action::CancelDebounceTimer(_) => "CancelDebounceTimer",
        Action::SetCallTimer(_, _, _, _) => "SetCallTimer",
        Action::CancelCallTimer(_, _) => "CancelCallTimer",
        Action::CallSettled { call: _, result: _ } => "CallSettled",
        Action::Anomaly(_) => "Anomaly",
        Action::ShortenTimer(_, _) => "ShortenTimer",
        Action::ShortenSubscriptionTimer(_, _, _) => "ShortenSubscriptionTimer",
    }
}

/// The variant names of `pub enum <name>` in the core's root file, those
/// compiled out of this build left out.
fn declared_variants(name: &str) -> Vec<String> {
    let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", core_path!(".rs")));
    let start = source
        .find(&format!("\npub enum {name} {{\n"))
        .unwrap_or_else(|| panic!("pub enum {name} in {}", core_path!(".rs")));
    let body = &source[start..];
    let end = body.find("\n}\n").expect("the enum's end");
    let mut variants = Vec::new();
    // Whether the next variant is compiled out of this build.
    let mut compiled_out = false;
    for line in body[..end].lines() {
        let Some(rest) = line.strip_prefix("    ") else {
            continue;
        };
        if rest == r#"#[cfg(feature = "unstable")]"# {
            compiled_out = !cfg!(feature = "unstable");
        } else if rest.starts_with(|first: char| first.is_ascii_uppercase()) {
            if !compiled_out {
                variants.push(
                    rest.chars()
                        .take_while(char::is_ascii_alphanumeric)
                        .collect(),
                );
            }
            compiled_out = false;
        }
    }
    assert!(variants.len() > 20, "the variants of {name} found");
    variants
}

#[test]
fn every_input_and_action_variant_has_a_sample() {
    let mut s = Sentinels::default();
    let sampled: Vec<&str> = inputs(&mut s).iter().map(input_name).collect();
    for variant in declared_variants("Input") {
        assert!(
            sampled.contains(&variant.as_str()),
            "Input::{variant} has no sample"
        );
    }
    let sampled: Vec<&str> = actions(&mut s).iter().map(action_name).collect();
    for variant in declared_variants("Action") {
        assert!(
            sampled.contains(&variant.as_str()),
            "Action::{variant} has no sample"
        );
    }
}

#[test]
fn no_input_or_action_shows_a_secret() {
    let mut s = Sentinels::default();
    let inputs = inputs(&mut s);
    let actions = actions(&mut s);
    for input in &inputs {
        s.assert_hidden(input_name(input), input);
    }
    for action in &actions {
        s.assert_hidden(action_name(action), action);
    }
}

#[test]
fn no_call_request_or_reply_shows_a_secret() {
    let mut s = Sentinels::default();
    let mut requests = Vec::new();
    requests.extend([
        CallRequest::Rpc {
            method: "m".to_owned(),
            data: s.bytes("rpc-data"),
        },
        CallRequest::Publish {
            channel: "news".to_owned(),
            data: s.bytes("publish-data"),
        },
        CallRequest::History {
            channel: "news".to_owned(),
            options: HistoryOptions::default(),
        },
        CallRequest::Presence {
            channel: "news".to_owned(),
        },
        CallRequest::PresenceStats {
            channel: "news".to_owned(),
        },
        CallRequest::Send {
            data: s.bytes("send-data"),
        },
    ]);
    #[cfg(feature = "unstable")]
    requests.extend([
        CallRequest::MapPublish {
            channel: "map:m".to_owned(),
            key: "k".to_owned(),
            data: s.bytes("map-publish-data"),
        },
        CallRequest::MapRemove {
            channel: "map:m".to_owned(),
            key: "k".to_owned(),
        },
    ]);
    let replies = vec![
        CallReply::Rpc(s.bytes("rpc-result")),
        CallReply::Publish,
        CallReply::History(HistoryResult {
            publications: vec![publication(&mut s)],
            ..HistoryResult::default()
        }),
        CallReply::Presence(PresenceResult {
            clients: HashMap::from([("client-id".to_owned(), client_info(&mut s))]),
        }),
        CallReply::PresenceStats(PresenceStatsResult::default()),
        CallReply::Send,
    ];
    let error: Result<CallReply, CallError> = Err(CallError::new(1, "timeout"));
    for request in &requests {
        s.assert_hidden("CallRequest", request);
    }
    for reply in &replies {
        s.assert_hidden("CallReply", reply);
    }
    let rpc = RpcResult {
        data: s.bytes("rpc-result-data"),
    };
    s.assert_hidden("CallError", &error);
    s.assert_hidden("RpcResult", &rpc);
}

#[test]
fn no_option_or_value_shows_a_secret() {
    let mut s = Sentinels::default();
    let config = Config::default()
        .token(s.text("config-token"))
        .data(s.bytes("config-data"));
    let options = SubscriptionOptions::default()
        .token(s.text("subscription-token"))
        .data(s.bytes("subscription-options-data"))
        .tag_filter(tag_filter(&mut s));
    let connect_data = ConnectData {
        source: true,
        fetch: Some(FETCH),
        attempt: Some(s.bytes("connect-data-attempt")),
    };
    let headers = EmulatedHeaders(BTreeMap::from([(
        "authorization".to_owned(),
        s.text("emulated-header"),
    )]));
    let filter = tag_filter(&mut s);
    s.assert_hidden("Config", &config);
    s.assert_hidden("SubscriptionOptions", &options);
    s.assert_hidden("ConnectData", &connect_data);
    s.assert_hidden("EmulatedHeaders", &headers);
    s.assert_hidden("TagFilter", &filter);
    let publication = publication(&mut s);
    let info = client_info(&mut s);
    s.assert_hidden("Publication", &publication);
    s.assert_hidden("ClientInfo", &info);
}

#[cfg(feature = "unstable")]
#[test]
fn no_map_or_shared_poll_value_shows_a_secret() {
    let mut s = Sentinels::default();
    let entry = crate::MapEntry {
        key: "k".to_owned(),
        data: s.bytes("map-entry-data"),
        tags: tags(&mut s),
        info: Some(client_info(&mut s)),
        ..crate::MapEntry::default()
    };
    let update = map_update(&mut s);
    let signature = crate::SharedPollSignature::new(vec!["a".to_owned()], s.text("signature"));
    let shared = shared_poll_update(&mut s);
    s.assert_hidden("MapEntry", &entry);
    s.assert_hidden("MapUpdate", &update);
    s.assert_hidden("SharedPollSignature", &signature);
    s.assert_hidden("SharedPollUpdate", &shared);
}

#[cfg(all(feature = "test-util", adapter_tokio))]
#[test]
fn no_test_util_command_shows_a_secret() {
    use crate::test_util::Command;

    let mut s = Sentinels::default();
    let commands = [
        Command::Connect {
            token: s.text("command-token"),
            data: Some(s.bytes("command-connect-data")),
        },
        Command::Publish {
            channel: "news".to_owned(),
            data: s.bytes("command-publish-data"),
        },
        Command::Rpc {
            method: "m".to_owned(),
            data: s.bytes("command-rpc-data"),
        },
        Command::Send {
            data: s.bytes("command-send-data"),
        },
    ];
    for command in &commands {
        s.assert_hidden("test_util::Command", command);
    }
}

/// The builder with headers, a proxy with credentials, a jar holding a
/// cookie, an endpoint with userinfo and a query, and secret options; the
/// handles built from it; the adapter's header set.
#[cfg(adapter_tokio)]
#[tokio::test]
async fn no_builder_or_handle_shows_a_secret() {
    use crate::{Client, Proxy, ProxyTarget};

    let mut s = Sentinels::default();
    let password = s.text("endpoint-password");
    let query = s.text("endpoint-query");
    let endpoint = format!("wss://user:{password}@example.com/connection/websocket?token={query}");
    let proxy_password = s.text("proxy-password");
    let target = ProxyTarget::new(&format!("http://user:{proxy_password}@proxy.invalid:3128"))
        .expect("a proxy target");
    let builder = Client::builder(endpoint)
        .config(
            Config::default()
                .token(s.text("builder-token"))
                .data(s.bytes("builder-data")),
        )
        .http_headers([("authorization", s.text("http-header"))])
        .emulated_headers([("x-token", s.text("builder-emulated-header"))])
        .proxy(Proxy::all(target.clone()));
    #[cfg(feature = "cookies")]
    let builder = {
        use std::sync::Arc;

        use crate::{CookieJar, MemoryJar};

        let jar = Arc::new(MemoryJar::new());
        let cookie: reqwest::header::HeaderValue = format!("session={}", s.text("cookie"))
            .parse()
            .expect("a cookie header");
        jar.set_cookies(
            &mut std::iter::once(&cookie),
            &"https://example.com/".parse().expect("a URL"),
        );
        s.assert_hidden("MemoryJar", &*jar);
        builder.cookie_jar(jar)
    };
    s.assert_hidden("ClientBuilder", &builder);
    s.assert_hidden("ProxyTarget", &target);
    s.assert_hidden("Proxy", &Proxy::all(target));
    let header = s.text("header-set");
    let headers = crate::adapter::header_set_sample("authorization", &header);
    s.assert_hidden("HeaderSet", &headers);

    let client = builder.build().expect("a client");
    let subscription = client
        .subscription_builder("news")
        .token(s.text("subscription-builder-token"))
        .data(s.bytes("subscription-builder-data"));
    s.assert_hidden("SubscriptionBuilder", &subscription);
    let subscription = subscription.build().expect("a subscription");
    s.assert_hidden("Subscription", &subscription);
    s.assert_hidden("Client", &client);
    s.assert_hidden("WeakClient", &client.downgrade());
    #[cfg(feature = "unstable")]
    {
        let map = client
            .map_subscription_builder("map:m", crate::MapKind::Map)
            .token(s.text("map-builder-token"))
            .data(s.bytes("map-builder-data"));
        s.assert_hidden("MapSubscriptionBuilder", &map);
        s.assert_hidden("MapSubscription", &map.build().expect("a map subscription"));
        let shared = client
            .shared_poll_subscription_builder("poll:p")
            .token(s.text("shared-poll-builder-token"))
            .data(s.bytes("shared-poll-builder-data"));
        s.assert_hidden("SharedPollSubscriptionBuilder", &shared);
        s.assert_hidden(
            "SharedPollSubscription",
            &shared.build().expect("a shared poll subscription"),
        );
    }
}

/// How a hand-written `Debug` impl of the crate is covered.
enum Covered {
    /// Formatted with sentinels by a test of this file.
    Sampled,
    /// Formatted inside a sampled type's `Debug`, named here.
    Inside(&'static str),
    /// The event `Debug`s keep their own test (Block 21).
    EventTest,
    /// It holds no secret, or prints none of what it holds: why.
    NoSecret(&'static str),
}

/// Every hand-written `impl … Debug for` in `src/` outside test code, by
/// file and type.
const INVENTORY: &[(&str, &str, Covered)] = &[
    (
        "src/adapter/body.rs",
        "Decoder",
        Covered::NoSecret("prints the decoder's kind only"),
    ),
    (
        "src/adapter/browser/events.rs",
        "Events",
        Covered::NoSecret("prints the number of listeners and whether the page is cached"),
    ),
    (
        "src/adapter/browser/shims.rs",
        "Shims",
        Covered::NoSecret("prints whether each shim is set, never the shim"),
    ),
    ("src/adapter/client.rs", "ClientBuilder", Covered::Sampled),
    ("src/adapter/client.rs", "Client", Covered::Sampled),
    (
        "src/adapter/client.rs",
        "Endpoints<'_>",
        Covered::Inside("ClientBuilder"),
    ),
    (
        "src/adapter/client.rs",
        "HeaderNames<'_>",
        Covered::Inside("ClientBuilder"),
    ),
    (
        "src/adapter/client.rs",
        "SubscriptionCallbacks",
        Covered::NoSecret("prints the subscription ids only, never the callbacks"),
    ),
    ("src/adapter/client.rs", "WeakClient", Covered::Sampled),
    (
        "src/adapter/handler.rs",
        "Handlers",
        Covered::NoSecret(
            "prints whether a client handler is set and the number of subscription handlers",
        ),
    ),
    (
        "src/adapter/map_subscription.rs",
        "MapSubscription",
        Covered::Sampled,
    ),
    (
        "src/adapter/map_subscription.rs",
        "MapSubscriptionBuilder",
        Covered::Sampled,
    ),
    (
        "src/adapter/shared_poll_subscription.rs",
        "SharedPollSubscription",
        Covered::Sampled,
    ),
    (
        "src/adapter/shared_poll_subscription.rs",
        "SharedPollSubscriptionBuilder",
        Covered::Sampled,
    ),
    (
        "src/adapter/subscription.rs",
        "Subscription",
        Covered::Sampled,
    ),
    (
        "src/adapter/subscription.rs",
        "SubscriptionBuilder",
        Covered::Sampled,
    ),
    (
        "src/adapter/tokio/connector.rs",
        "Shared",
        Covered::NoSecret("prints a fixed name"),
    ),
    (
        "src/adapter/tokio/connector.rs",
        "ConnectedStream",
        Covered::NoSecret("prints a fixed name"),
    ),
    (
        "src/adapter/tokio/cookies.rs",
        "MemoryJar",
        Covered::Sampled,
    ),
    ("src/adapter/tokio/http.rs", "HeaderSet", Covered::Sampled),
    (
        "src/adapter/tokio/net.rs",
        "Jar",
        Covered::NoSecret("prints whether a jar is set, never the jar"),
    ),
    ("src/adapter/tokio/proxy.rs", "Proxy", Covered::Sampled),
    (
        "src/adapter/tokio/proxy.rs",
        "ProxyTarget",
        Covered::Sampled,
    ),
    (
        "src/adapter/tokio/tls.rs",
        "Trust",
        Covered::NoSecret(
            "prints the number of extra roots, or whether a raw configuration is set",
        ),
    ),
    (
        "src/adapter/tokio/tls.rs",
        "Guarded<V>",
        Covered::NoSecret("prints its name only"),
    ),
    (
        "src/call.rs",
        "JsonCallError",
        Covered::NoSecret("prints the decode error and the result data by its length only"),
    ),
    ("src/config.rs", "Config", Covered::Sampled),
    ("src/config.rs", "Token<'_>", Covered::Inside("Config")),
    ("src/call.rs", "RpcResult", Covered::Sampled),
    ("src/event.rs", "ClientEvent", Covered::EventTest),
    ("src/event.rs", "ConnectedContext", Covered::EventTest),
    ("src/event.rs", "MessageContext", Covered::EventTest),
    (core_path!(".rs"), "SubscriptionOptions", Covered::Sampled),
    (core_path!(".rs"), "Input", Covered::Sampled),
    (core_path!(".rs"), "Action", Covered::Sampled),
    (core_path!(".rs"), "ConnectData", Covered::Sampled),
    (core_path!(".rs"), "EmulatedHeaders", Covered::Sampled),
    (core_path!("/call.rs"), "CallRequest", Covered::Sampled),
    (core_path!("/call.rs"), "CallReply", Covered::Sampled),
    ("src/map.rs", "MapEntry", Covered::Sampled),
    ("src/map.rs", "MapUpdate", Covered::Sampled),
    (
        "src/shared_poll.rs",
        "SharedPollSignature",
        Covered::Sampled,
    ),
    ("src/shared_poll.rs", "SharedPollUpdate", Covered::Sampled),
    (
        "src/subscription.rs",
        "SubscribedContext",
        Covered::EventTest,
    ),
    ("src/subscription.rs", "Publication", Covered::Sampled),
    ("src/subscription.rs", "ClientInfo", Covered::Sampled),
    ("src/tag_filter.rs", "TagFilter", Covered::Sampled),
    (
        "src/test_util/server.rs",
        "Server",
        Covered::NoSecret("prints the numbers of connections and channels"),
    ),
    (
        "src/test_util/server.rs",
        "Length<'_>",
        Covered::Inside("test_util::Command"),
    ),
    ("src/test_util/server.rs", "Command", Covered::Sampled),
    ("src/token.rs", "Secret", Covered::Inside("Input")),
    ("src/token.rs", "Bytes<'_>", Covered::Inside("MapEntry")),
];

/// The hand-written `impl … Debug for` of `src/` outside test code: files
/// named like tests, and inline `#[cfg(test)]` modules at a file's top
/// level, are left out.
fn debug_impls() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("a source directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                if path
                    .file_name()
                    .is_some_and(|name| name != "tests" && name != "protocol")
                {
                    walk(&path, files);
                }
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk(&root.join("src"), &mut files);
    let mut found = Vec::new();
    for path in files {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if name.ends_with("_tests.rs") || name == "tests.rs" {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .expect("under the manifest directory")
            .to_string_lossy()
            .replace('\\', "/");
        let source = std::fs::read_to_string(&path).expect("a source file");
        let mut lines = source.lines().peekable();
        while let Some(line) = lines.next() {
            let trimmed = line.trim_start();
            if trimmed == "#[cfg(test)]"
                && lines
                    .peek()
                    .is_some_and(|next| next.starts_with("mod ") && next.trim_end().ends_with('{'))
            {
                // An inline test module at the top level: skipped up to
                // its closing brace.
                lines.by_ref().find(|line| *line == "}");
                continue;
            }
            let Some(rest) = trimmed.strip_prefix("impl") else {
                continue;
            };
            let Some((_, after)) = rest.split_once("Debug for ") else {
                continue;
            };
            if !(rest.starts_with(' ') || rest.starts_with('<')) {
                continue;
            }
            let ty = after.trim_end_matches('{').trim().to_owned();
            found.push((relative.clone(), ty));
        }
    }
    found.sort();
    found
}

#[test]
fn every_hand_written_debug_is_sampled_or_holds_no_secret() {
    let found = debug_impls();
    for (file, ty) in &found {
        assert!(
            INVENTORY
                .iter()
                .any(|(listed, name, _)| listed == file && name == ty),
            "{file}: `impl Debug for {ty}` is neither sampled by the redaction test nor listed as holding no secret"
        );
    }
    for (file, ty, covered) in INVENTORY {
        assert!(
            found
                .iter()
                .any(|(listed, name)| listed == file && name == ty),
            "the inventory lists {file}: {ty}, which has no hand-written Debug"
        );
        if let Covered::NoSecret(reason) | Covered::Inside(reason) = covered {
            assert!(!reason.is_empty(), "{file}: {ty} needs its reason");
        }
    }
}
