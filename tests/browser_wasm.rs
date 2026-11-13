//! The @W scenarios of the websocket transport, of the network and page
//! events, of `http_stream`, of sse, and of the shims and credentials
//! (Blocks 44b to 44f): the browser
//! client against the Centrifugo that `tests/browser.rs` starts, and the
//! relay in front of it, run by `wasm-bindgen-test-runner` in a browser
//! (Block 44, D18). The server's and the relay's `host:port` come at compile
//! time in `FERRIFUGE_W_SERVER` and `FERRIFUGE_W_RELAY`.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::time::{Duration, UNIX_EPOCH};

use ferrifuge::machine::TransportKind;
use ferrifuge::{
    Client, ClientEvent, ClientHandler, Config, EventDelivery, Protocol, SubscriptionEvent,
};
use futures_util::future::{Either, select};
use futures_util::{Stream, StreamExt};
use gloo_timers::future::TimeoutFuture;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::{Closure, JsValue};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// `host:port` of the server; a placeholder when the tests are only built.
const SERVER: &str = match option_env!("FERRIFUGE_W_SERVER") {
    Some(server) => server,
    None => "127.0.0.1:8000",
};

/// `host:port` of the relay in front of the server, `tests/support/http_proxy.rs`.
const RELAY: &str = match option_env!("FERRIFUGE_W_RELAY") {
    Some(relay) => relay,
    None => "127.0.0.1:8001",
};

/// How long a step waits for what it expects.
const WAIT: u32 = 10_000;

fn endpoint() -> String {
    format!("ws://{SERVER}/connection/websocket")
}

fn client(config: Config) -> Client {
    Client::builder(endpoint())
        .config(config)
        .build()
        .expect("a valid client")
}

/// A channel of its own for one test, so that each browser's run starts at
/// offset 1 on the same server.
fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", js_sys::Math::random().to_bits())
}

async fn within<T>(millis: u32, future: impl Future<Output = T>) -> T {
    match select(std::pin::pin!(future), TimeoutFuture::new(millis)).await {
        Either::Left((output, _)) => output,
        Either::Right(_) => panic!("nothing within {millis} ms"),
    }
}

async fn next<S: Stream + Unpin>(stream: &mut S) -> S::Item {
    within(WAIT, stream.next())
        .await
        .expect("the stream not to end")
}

/// The next event of `stream` that is not an `error` event.
async fn next_event(stream: &mut (impl Stream<Item = ClientEvent> + Unpin)) -> ClientEvent {
    loop {
        match next(stream).await {
            ClientEvent::Error(_) => {}
            event => return event,
        }
    }
}

async fn connected_over(events: &mut (impl Stream<Item = ClientEvent> + Unpin)) -> String {
    loop {
        match next_event(events).await {
            ClientEvent::Connected { transport, .. } => return transport,
            ClientEvent::Connecting { .. } => {}
            event => panic!("not connecting or connected: {event:?}"),
        }
    }
}

async fn subscribed(
    events: &mut (impl Stream<Item = SubscriptionEvent> + Unpin),
) -> ferrifuge::Subscribed {
    loop {
        match next(events).await {
            SubscriptionEvent::Subscribed(subscribed) => return subscribed,
            SubscriptionEvent::Subscribing { .. } | SubscriptionEvent::Error(_) => {}
            event => panic!("not subscribing or subscribed: {event:?}"),
        }
    }
}

async fn publication(
    events: &mut (impl Stream<Item = SubscriptionEvent> + Unpin),
) -> ferrifuge::Publication {
    match next(events).await {
        SubscriptionEvent::Publication(publication) => publication,
        event => panic!("not a publication: {event:?}"),
    }
}

/// A connected client publishing for the test, its streams not taken.
async fn publisher(config: Config) -> Client {
    let publisher = client(config);
    publisher.connect();
    within(WAIT, publisher.ready())
        .await
        .expect("the publisher connected");
    publisher
}

fn sleep(millis: u32) -> TimeoutFuture {
    TimeoutFuture::new(millis)
}

// @W scenario: A browser client connects over websocket and receives a publication
#[wasm_bindgen_test]
async fn a_browser_client_connects_over_websocket_and_receives_a_publication() {
    let client = client(Config::default());
    let subscription = client.new_subscription("news").unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();

    client.connect();
    subscription.subscribe();

    assert!(matches!(
        next_event(&mut events).await,
        ClientEvent::Connecting { .. }
    ));
    assert_eq!(connected_over(&mut events).await, "websocket");
    assert!(matches!(
        next(&mut subscription_events).await,
        SubscriptionEvent::Subscribing { .. }
    ));
    subscribed(&mut subscription_events).await;
    within(WAIT, client.publish("news", br#""hello""#.to_vec()))
        .await
        .expect("published");
    assert_eq!(
        publication(&mut subscription_events).await.data,
        br#""hello""#
    );
}

// @W scenario: A browser client speaks Protobuf over websocket
#[wasm_bindgen_test]
async fn a_browser_client_speaks_protobuf_over_websocket() {
    let config = Config::default().protocol(Protocol::Protobuf);
    let client = client(config.clone());
    let subscription = client.new_subscription("news").unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();

    client.connect();
    subscription.subscribe();

    assert_eq!(connected_over(&mut events).await, "websocket");
    subscribed(&mut subscription_events).await;
    let publisher = publisher(config).await;
    within(WAIT, publisher.publish("news", vec![0x00, 0xff, 0x7f]))
        .await
        .expect("published");
    assert_eq!(
        publication(&mut subscription_events).await.data,
        [0x00, 0xff, 0x7f]
    );
}

// @W scenario: Publications a browser client had not taken before reconnect() are recovered once
#[wasm_bindgen_test]
async fn publications_not_taken_before_reconnect_are_recovered_once() {
    let channel = unique("history:news");
    let client = client(Config::default());
    let subscription = client.new_subscription(&channel).unwrap();
    let mut subscription_events = subscription.events();
    client.connect();
    subscription.subscribe();
    subscribed(&mut subscription_events).await;
    let publisher = publisher(Config::default()).await;
    for n in 1..=3 {
        within(
            WAIT,
            publisher.publish(&channel, format!("{n}").into_bytes()),
        )
        .await
        .expect("published");
    }
    assert_eq!(publication(&mut subscription_events).await.offset, Some(1));
    // The other two reach the client before reconnect() (Block 15: they
    // wait, emitted, for the stream).
    sleep(500).await;

    client.reconnect();

    assert!(matches!(
        next(&mut subscription_events).await,
        SubscriptionEvent::Subscribing { .. }
    ));
    let resubscribed = subscribed(&mut subscription_events).await;
    assert!(resubscribed.recovered, "{resubscribed:?}");
    let offsets = [
        publication(&mut subscription_events).await.offset,
        publication(&mut subscription_events).await.offset,
    ];
    assert_eq!(offsets, [Some(2), Some(3)]);
    // No publication is yielded twice.
    let more = select(subscription_events.next(), sleep(1000)).await;
    assert!(
        !matches!(
            more,
            Either::Left((Some(SubscriptionEvent::Publication(_)), _))
        ),
        "a publication yielded again"
    );
}

// @W scenario: A browser client whose stream is not read reconnects once its inbox overflows, losing no publication
#[wasm_bindgen_test]
async fn an_unread_stream_overflows_the_inbox_and_loses_no_publication() {
    let channel = unique("history:news");
    let client = client(Config::default().read_ahead_messages(16));
    let subscription = client.new_subscription(&channel).unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();
    client.connect();
    subscription.subscribe();
    connected_over(&mut events).await;
    subscribed(&mut subscription_events).await;
    let publisher = publisher(Config::default()).await;

    let publishing = async {
        for n in 1..=100 {
            publisher
                .publish(&channel, format!("{n}").into_bytes())
                .await
                .expect("published");
        }
    };
    let overflowed = async {
        loop {
            if let ClientEvent::Connecting { code, reason } = next_event(&mut events).await {
                return (code, reason);
            }
        }
    };
    let ((), connecting) =
        within(2 * WAIT, futures_util::future::join(publishing, overflowed)).await;

    assert_eq!(connecting, (7, "read-ahead overflow".to_owned()));
    // The client's stream is read on: its events would otherwise hold
    // the next connection's messages back (Block 15).
    wasm_bindgen_futures::spawn_local(async move { while events.next().await.is_some() {} });
    assert!(matches!(
        next(&mut subscription_events).await,
        SubscriptionEvent::Subscribing { .. }
    ));
    let resubscribed = subscribed(&mut subscription_events).await;
    assert!(resubscribed.recovered, "{resubscribed:?}");
    for offset in 1..=100 {
        assert_eq!(
            publication(&mut subscription_events).await.offset,
            Some(offset)
        );
    }
}

/// The events the handlers of a client and its subscription received, as
/// names, and whether two handler calls overlapped.
#[derive(Default)]
struct Record {
    events: RefCell<Vec<String>>,
    running: Cell<bool>,
    overlapped: Cell<bool>,
}

impl Record {
    fn add(&self, name: &str) {
        if self.running.replace(true) {
            self.overlapped.set(true);
        }
        self.events.borrow_mut().push(name.to_owned());
        self.running.set(false);
    }

    fn has(&self, name: &str) -> bool {
        self.events.borrow().iter().any(|event| event == name)
    }
}

struct Recording(Rc<Record>);

impl ClientHandler for Recording {
    fn on_event(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::Connecting { .. } => self.0.add("connecting"),
            ClientEvent::Connected { .. } => self.0.add("connected"),
            _ => {}
        }
    }
}

// @W scenario: Handlers receive a browser client's events in order
async fn handlers_receive_the_events_in_order(delivery: EventDelivery) {
    let record = Rc::new(Record::default());
    let client = Client::builder(endpoint())
        .event_delivery(delivery)
        .handler(Recording(Rc::clone(&record)))
        .build()
        .unwrap();
    let of_subscription = Rc::clone(&record);
    let subscription = client
        .subscription_builder("news")
        .handler(move |event: SubscriptionEvent| match event {
            SubscriptionEvent::Subscribing { .. } => of_subscription.add("subscribing"),
            SubscriptionEvent::Subscribed(_) => of_subscription.add("subscribed"),
            SubscriptionEvent::Publication(_) => of_subscription.add("publication"),
            _ => {}
        })
        .build()
        .unwrap();

    subscription.subscribe();
    client.connect();
    within(WAIT, async {
        while !record.has("subscribed") {
            sleep(20).await;
        }
    })
    .await;
    within(WAIT, client.publish("news", b"{}".to_vec()))
        .await
        .expect("published");
    within(WAIT, async {
        while !record.has("publication") {
            sleep(20).await;
        }
    })
    .await;

    assert_eq!(
        *record.events.borrow(),
        [
            "subscribing",
            "connecting",
            "connected",
            "subscribed",
            "publication"
        ]
    );
    assert!(!record.overlapped.get());
}

#[wasm_bindgen_test]
async fn task_handlers_receive_the_events_in_order() {
    handlers_receive_the_events_in_order(EventDelivery::Task).await;
}

#[wasm_bindgen_test]
async fn inline_handlers_receive_the_events_in_order() {
    handlers_receive_the_events_in_order(EventDelivery::Inline).await;
}

/// The page's monotonic clock, in milliseconds.
fn now() -> f64 {
    js_sys::Reflect::get(&js_sys::global(), &"performance".into())
        .expect("a performance object")
        .unchecked_into::<web_sys::Performance>()
        .now()
}

// @W scenario: close() ends a browser client
#[wasm_bindgen_test]
async fn close_ends_a_browser_client() {
    let config = Config::default();
    let timeout = config.timeout;
    let client = client(config);
    let mut events = client.events();
    client.connect();
    connected_over(&mut events).await;

    let start = now();
    client.close().await;
    let took = Duration::from_secs_f64((now() - start) / 1000.0);

    assert!(took <= timeout + Duration::from_secs(1), "{took:?}");
    assert!(matches!(
        next_event(&mut events).await,
        ClientEvent::Disconnected { code: 0, .. }
    ));
    assert!(within(WAIT, events.next()).await.is_none());
    assert!(client.is_closed());
}

// @W scenario: A browser client's statistics tell when it connected
#[wasm_bindgen_test]
async fn statistics_tell_when_a_browser_client_connected() {
    let before = js_sys::Date::now();
    let client = client(Config::default());
    let mut events = client.events();

    client.connect();
    connected_over(&mut events).await;
    let after = js_sys::Date::now();

    let since = client
        .statistics()
        .connected_since
        .expect("connected")
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs_f64()
        * 1000.0;
    // The clock's milliseconds, through a float of seconds.
    assert!(
        before - 1.0 <= since && since <= after + 1.0,
        "{before} {since} {after}"
    );
}

/// Messages of the page's uncaught errors and of `console.error` from now
/// on.
fn captured_errors() -> Rc<RefCell<Vec<String>>> {
    let captured = Rc::new(RefCell::new(Vec::new()));
    let global = js_sys::global();
    let of_errors = Rc::clone(&captured);
    let on_error = Closure::<dyn FnMut(JsValue)>::new(move |event: JsValue| {
        let message = js_sys::Reflect::get(&event, &"message".into())
            .ok()
            .and_then(|message| message.as_string())
            .unwrap_or_default();
        of_errors.borrow_mut().push(message);
    })
    .into_js_value();
    // A page's errors; Node has no global `addEventListener`.
    if let Ok(add) = js_sys::Reflect::get(&global, &"addEventListener".into())
        .and_then(JsCast::dyn_into::<js_sys::Function>)
    {
        add.call2(&global, &"error".into(), &on_error)
            .expect("an error listener");
    }
    let console = js_sys::Reflect::get(&global, &"console".into()).expect("a console");
    let of_console = Rc::clone(&captured);
    let console_error = Closure::<dyn FnMut(JsValue)>::new(move |message: JsValue| {
        of_console.borrow_mut().push(
            message
                .as_string()
                .unwrap_or_else(|| format!("{message:?}")),
        );
    })
    .into_js_value();
    js_sys::Reflect::set(&console, &"error".into(), &console_error)
        .expect("console.error replaced");
    captured
}

// Block 44, D6: the listeners are removed before their closures are
// dropped, so the close event of a dropped client calls none.
#[wasm_bindgen_test]
async fn dropping_a_connected_client_calls_no_dropped_closure() {
    let client = client(Config::default());
    let mut events = client.events();
    client.connect();
    connected_over(&mut events).await;
    let captured = captured_errors();

    drop(events);
    drop(client);
    sleep(2000).await;

    let captured = captured.borrow();
    assert!(
        !captured
            .iter()
            .any(|message| message.contains("closure invoked")),
        "{captured:?}"
    );
}

// Block 44, D6: an attempt dropped while its websocket opens removes its
// listeners before dropping their closures, so the browser's later error
// and close events call none.
#[wasm_bindgen_test]
async fn dropping_a_client_whose_websocket_opens_calls_no_dropped_closure() {
    let captured = captured_errors();
    let client = client(Config::default());
    client.connect();
    // The driver runs and opens the websocket.
    sleep(0).await;

    drop(client);
    sleep(2000).await;

    let captured = captured.borrow();
    assert!(
        !captured
            .iter()
            .any(|message| message.contains("closure invoked")),
        "{captured:?}"
    );
}

/// An `EventTarget` of the test, which counts the listeners added to it
/// and not removed (Block 44, D10, D11).
fn recording_target() -> web_sys::EventTarget {
    let target = web_sys::EventTarget::new().expect("an EventTarget");
    let record = js_sys::Function::new_with_args(
        "target",
        "const add = target.addEventListener.bind(target);
         const remove = target.removeEventListener.bind(target);
         const held = [];
         target.addEventListener = (type, listener, options) => {
           held.push([type, listener]);
           add(type, listener, options);
         };
         target.removeEventListener = (type, listener, options) => {
           const at = held.findIndex(([t, l]) => t === type && l === listener);
           if (at >= 0) held.splice(at, 1);
           remove(type, listener, options);
         };
         target.held = () => held.length;",
    );
    record
        .call1(&JsValue::NULL, &target)
        .expect("the recording wrapper");
    target
}

/// How many listeners a [`recording_target`] holds.
fn held(target: &web_sys::EventTarget) -> u32 {
    let held: js_sys::Function = js_sys::Reflect::get(target, &JsValue::from_str("held"))
        .expect("a recording target")
        .unchecked_into();
    let count = held
        .call0(target)
        .expect("a count")
        .as_f64()
        .expect("a number");
    // A count of listeners.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let count = count as u32;
    count
}

/// Dispatches the event `name` on `target`, with `persisted` when given,
/// as a `PageTransitionEvent` has it.
fn dispatch(target: &web_sys::EventTarget, name: &str, persisted: Option<bool>) {
    let event = web_sys::Event::new(name).expect("an Event");
    if let Some(persisted) = persisted {
        js_sys::Reflect::set(
            &event,
            &JsValue::from_str("persisted"),
            &JsValue::from_bool(persisted),
        )
        .expect("persisted set");
    }
    target.dispatch_event(&event).expect("dispatched");
}

/// A client built by `build` from the test's websocket endpoint, connected,
/// its stream read up to `connected`.
async fn connected_with(
    config: Config,
    build: impl FnOnce(ferrifuge::ClientBuilder) -> ferrifuge::ClientBuilder,
) -> (Client, impl Stream<Item = ClientEvent> + Unpin) {
    let client = build(Client::builder(endpoint()).config(config))
        .build()
        .expect("a valid client");
    let mut events = client.events();
    client.connect();
    assert_eq!(connected_over(&mut events).await, "websocket");
    (client, events)
}

/// Whether `events` yields nothing within `millis`.
async fn yields_nothing(events: &mut (impl Stream<Item = ClientEvent> + Unpin), millis: u32) {
    match select(events.next(), sleep(millis)).await {
        Either::Left((event, _)) => panic!("yielded {event:?}"),
        Either::Right(_) => {}
    }
}

/// The next event of `events`, `connecting` with `code` and `reason`.
async fn connecting_with(
    events: &mut (impl Stream<Item = ClientEvent> + Unpin),
    code: u32,
    reason: &str,
) {
    match next_event(events).await {
        ClientEvent::Connecting {
            code: got,
            reason: why,
        } => {
            assert_eq!((got, why.as_str()), (code, reason));
        }
        event => panic!("not connecting: {event:?}"),
    }
}

fn slow_reconnects() -> Config {
    Config::default().min_reconnect_delay(Duration::from_secs(20))
}

// @W scenario: Going offline closes a browser client's connection, and coming online reconnects it at once
#[wasm_bindgen_test]
async fn going_offline_closes_the_connection_and_coming_online_reconnects_it() {
    let network = web_sys::EventTarget::new().expect("an EventTarget");
    let (client, mut events) = connected_with(slow_reconnects(), |builder| {
        builder.network_event_target(network.clone())
    })
    .await;

    dispatch(&network, "offline", None);

    connecting_with(&mut events, 1, "transport closed").await;
    sleep(2000).await;
    assert_eq!(client.state(), ferrifuge::State::Connecting);
    dispatch(&network, "online", None);
    assert_eq!(within(2000, connected_over(&mut events)).await, "websocket");
}

// @W scenario: Coming online while connected changes nothing
#[wasm_bindgen_test]
async fn coming_online_while_connected_changes_nothing() {
    let network = web_sys::EventTarget::new().expect("an EventTarget");
    let (client, mut events) = connected_with(Config::default(), |builder| {
        builder.network_event_target(network.clone())
    })
    .await;

    dispatch(&network, "online", None);

    sleep(1000).await;
    assert_eq!(client.state(), ferrifuge::State::Connected);
    yields_nothing(&mut events, 0).await;
}

// @W scenario: A page entering the back/forward cache closes the connection, and its restore reconnects at once
#[wasm_bindgen_test]
async fn the_back_forward_cache_closes_the_connection_and_its_restore_reconnects() {
    let page = web_sys::EventTarget::new().expect("an EventTarget");
    let (_client, mut events) = connected_with(slow_reconnects(), |builder| {
        builder.page_event_target(page.clone())
    })
    .await;

    dispatch(&page, "pagehide", Some(true));

    connecting_with(&mut events, 1, "page entered back/forward cache").await;
    dispatch(&page, "pageshow", Some(true));
    assert_eq!(within(2000, connected_over(&mut events)).await, "websocket");
}

// @W scenario: A page hidden without entering the back/forward cache keeps the connection
#[wasm_bindgen_test]
async fn a_page_hidden_without_the_cache_keeps_the_connection() {
    let page = web_sys::EventTarget::new().expect("an EventTarget");
    let (client, mut events) = connected_with(Config::default(), |builder| {
        builder.page_event_target(page.clone())
    })
    .await;

    dispatch(&page, "pagehide", Some(false));

    sleep(1000).await;
    assert_eq!(client.state(), ferrifuge::State::Connected);
    yields_nothing(&mut events, 0).await;
}

// @W scenario: A disconnected browser client stops listening to network and page events
#[wasm_bindgen_test]
async fn a_disconnected_client_stops_listening() {
    let network = recording_target();
    let page = recording_target();
    let (client, _events) = connected_with(Config::default(), |builder| {
        builder
            .network_event_target(network.clone())
            .page_event_target(page.clone())
    })
    .await;
    assert_eq!((held(&network), held(&page)), (2, 2));

    client.disconnect();

    assert_eq!((held(&network), held(&page)), (0, 0));
    dispatch(&network, "online", None);
    sleep(1000).await;
    assert_eq!(client.state(), ferrifuge::State::Disconnected);
}

// Block 44, D10, D11: closing or dropping a client removes its listeners
// too.
#[wasm_bindgen_test]
async fn a_closed_or_dropped_client_holds_no_listener() {
    for close in [true, false] {
        let network = recording_target();
        let page = recording_target();
        let (client, _events) = connected_with(Config::default(), |builder| {
            builder
                .network_event_target(network.clone())
                .page_event_target(page.clone())
        })
        .await;

        if close {
            within(WAIT, client.close()).await;
        } else {
            drop(client);
        }

        assert_eq!((held(&network), held(&page)), (0, 0), "close: {close}");
    }
}

// Block 44, D10: with no target given, the client listens on `globalThis`.
// Node's has no `addEventListener`: there the client listens to nothing.
#[wasm_bindgen_test]
async fn with_no_target_an_offline_on_the_global_closes_the_connection() {
    let global = js_sys::global();
    if !js_sys::Reflect::get(&global, &JsValue::from_str("addEventListener"))
        .is_ok_and(|add| add.is_function())
    {
        return;
    }
    let (_client, mut events) = connected_with(slow_reconnects(), |builder| builder).await;

    dispatch(global.unchecked_ref(), "offline", None);

    connecting_with(&mut events, 1, "transport closed").await;
}

/// An endpoint nothing listens on.
const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

fn http_stream_endpoint() -> String {
    format!("http://{SERVER}/connection/http_stream")
}

/// A client trying the unreachable websocket endpoint, then the server's
/// `http_stream` endpoint, then its sse one unless `protobuf`: the Protobuf
/// protocol is refused with sse at build (Block 27, D3).
fn falling_back(config: Config, protobuf: bool) -> Client {
    let mut endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE.to_owned()),
        (TransportKind::HttpStream, http_stream_endpoint()),
    ];
    if !protobuf {
        endpoints.push((
            TransportKind::Sse,
            format!("http://{SERVER}/connection/sse"),
        ));
    }
    Client::builder_with_transports(endpoints)
        .config(config)
        .build()
        .expect("a valid client")
}

/// A client over the server's `http_stream` endpoint only.
fn over_http_stream(config: Config) -> Client {
    Client::builder_with_transports([(TransportKind::HttpStream, http_stream_endpoint())])
        .config(config)
        .build()
        .expect("a valid client")
}

// @W scenario: A browser client falls back to http_stream when websocket is unreachable
async fn falls_back_to_http_stream(protocol: Protocol) {
    let protobuf = protocol == Protocol::Protobuf;
    let client = falling_back(Config::default().protocol(protocol), protobuf);
    let mut events = client.events();

    client.connect();

    assert!(matches!(
        next_event(&mut events).await,
        ClientEvent::Connecting { .. }
    ));
    assert_eq!(connected_over(&mut events).await, "http_stream");
}

#[wasm_bindgen_test]
async fn a_json_client_falls_back_to_http_stream() {
    falls_back_to_http_stream(Protocol::Json).await;
}

#[wasm_bindgen_test]
async fn a_protobuf_client_falls_back_to_http_stream() {
    falls_back_to_http_stream(Protocol::Protobuf).await;
}

// @W scenario: Commands work over http_stream from a browser client
#[wasm_bindgen_test]
async fn commands_work_over_http_stream() {
    let channel = unique("news");
    let client = over_http_stream(Config::default());
    let subscription = client.new_subscription(&channel).unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();
    client.connect();
    subscription.subscribe();
    assert_eq!(connected_over(&mut events).await, "http_stream");
    subscribed(&mut subscription_events).await;

    within(WAIT, client.publish(&channel, br#""hello""#.to_vec()))
        .await
        .expect("the publish succeeds");

    assert_eq!(
        publication(&mut subscription_events).await.data,
        br#""hello""#
    );
}

// @W scenario: A stream not read does not stop a browser client reading its http_stream response
#[wasm_bindgen_test]
async fn a_stream_not_read_does_not_stop_the_client_reading_its_http_stream_response() {
    const COUNT: usize = 1000;
    // 100 bytes of JSON: a string of 98 digits.
    let data = |n: usize| format!("\"{n:098}\"").into_bytes();
    let channel = unique("news");
    let client = over_http_stream(Config::default().read_ahead_messages(600));
    let subscription = client.new_subscription(&channel).unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();
    client.connect();
    subscription.subscribe();
    assert_eq!(connected_over(&mut events).await, "http_stream");
    subscribed(&mut subscription_events).await;
    let publisher = publisher(Config::default()).await;

    for n in 1..=COUNT {
        within(WAIT, publisher.publish(&channel, data(n)))
            .await
            .expect("published");
    }

    within(WAIT, async {
        while client.read_ahead().0 != 600 {
            sleep(20).await;
        }
    })
    .await;
    sleep(500).await;
    assert_eq!(client.read_ahead().0, 600);
    yields_nothing(&mut events, 1000).await;
    assert_eq!(client.read_ahead().0, 600);
    for n in 1..=COUNT {
        // Names the publication a stall stops at (Safari: the job's log).
        let event = match select(
            subscription_events.next(),
            std::pin::pin!(TimeoutFuture::new(WAIT)),
        )
        .await
        {
            Either::Left((event, _)) => event,
            Either::Right(_) => {
                let read_ahead = client.read_ahead();
                wasm_bindgen_test::console_log!(
                    "stalled before publication {n} of {COUNT}, read-ahead {read_ahead:?}"
                );
                panic!("publication {n} of {COUNT} not within {WAIT} ms");
            }
        };
        match event {
            Some(SubscriptionEvent::Publication(publication)) => {
                assert_eq!(publication.data, data(n));
            }
            other => panic!("publication {n} of {COUNT}: {other:?}"),
        }
    }
    match select(subscription_events.next(), sleep(500)).await {
        Either::Left((event, _)) => panic!("yielded {event:?} after the publications"),
        Either::Right(_) => {}
    }
}

// The server API is out of a page's reach (no CORS): a client writes the map.
// @W scenario: A browser client syncs a map and receives its updates
#[cfg(feature = "unstable")]
#[wasm_bindgen_test]
async fn a_browser_client_syncs_a_map_and_receives_its_updates() {
    use ferrifuge::{MapKind, MapSubscriptionEvent};

    let channel = unique("map:w1");
    let writer = publisher(Config::default()).await;
    for key in ["a", "b"] {
        within(WAIT, writer.map_publish(&channel, key, b"{}".to_vec()))
            .await
            .expect("key written");
    }
    let client = client(Config::default());
    let map = client.new_map_subscription(&channel, MapKind::Map).unwrap();
    let mut events = map.events();

    client.connect();
    map.subscribe();

    assert_eq!(next(&mut events).await.name(), "subscribing");
    assert_eq!(next(&mut events).await.name(), "subscribed");
    match next(&mut events).await {
        MapSubscriptionEvent::Sync(entries) => {
            let keys: Vec<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
            assert_eq!(keys, ["a", "b"]);
        }
        event => panic!("not a sync: {event:?}"),
    }
    within(WAIT, writer.map_publish(&channel, "c", b"{}".to_vec()))
        .await
        .expect("key written");
    match next(&mut events).await {
        MapSubscriptionEvent::Update(update) => assert_eq!(update.key, "c"),
        event => panic!("not an update: {event:?}"),
    }
}

// @W scenario: A browser client whose stream is not read reconnects once its inbox overflows over http_stream, losing no publication
#[wasm_bindgen_test]
async fn an_unread_stream_overflows_the_inbox_over_http_stream_and_loses_no_publication() {
    let channel = unique("history:news");
    let client = over_http_stream(Config::default().read_ahead_messages(16));
    let subscription = client.new_subscription(&channel).unwrap();
    let mut events = client.events();
    let mut subscription_events = subscription.events();
    client.connect();
    subscription.subscribe();
    assert_eq!(connected_over(&mut events).await, "http_stream");
    subscribed(&mut subscription_events).await;
    let publisher = publisher(Config::default()).await;

    let publishing = async {
        for n in 1..=100 {
            publisher
                .publish(&channel, format!("{n}").into_bytes())
                .await
                .expect("published");
        }
    };
    let overflowed = async {
        loop {
            if let ClientEvent::Connecting { code, reason } = next_event(&mut events).await {
                return (code, reason);
            }
        }
    };
    let ((), connecting) =
        within(2 * WAIT, futures_util::future::join(publishing, overflowed)).await;

    assert_eq!(connecting, (7, "read-ahead overflow".to_owned()));
    // The client's stream is read on: its events would otherwise hold
    // the next connection's messages back (Block 15).
    wasm_bindgen_futures::spawn_local(async move { while events.next().await.is_some() {} });
    assert!(matches!(
        next(&mut subscription_events).await,
        SubscriptionEvent::Subscribing { .. }
    ));
    let resubscribed = subscribed(&mut subscription_events).await;
    assert!(resubscribed.recovered, "{resubscribed:?}");
    for offset in 1..=100 {
        assert_eq!(
            publication(&mut subscription_events).await.offset,
            Some(offset)
        );
    }
}

// The relay signs the key and calls the server API, out of a page's reach
// (no CORS; Block 47, D17).
// @W scenario: A browser client tracks a key and receives its updates
#[cfg(feature = "unstable")]
#[wasm_bindgen_test]
async fn a_browser_client_tracks_a_key_and_receives_its_updates() {
    use ferrifuge::{SharedPollSubscriptionEvent, SharedPollTrackItem};

    let channel = unique("poll_v:w1");
    let client = client(Config::default());
    let subscription = client.new_shared_poll_subscription(&channel).unwrap();
    let mut events = subscription.events();

    client.connect();
    subscription.subscribe();
    assert_eq!(next(&mut events).await.name(), "subscribing");
    assert_eq!(next(&mut events).await.name(), "subscribed");

    let signature = relay(&format!("shared-poll-signature/{channel}/a")).await;
    let read = client.statistics().messages_in;
    subscription.track_with_signature([SharedPollTrackItem::new("a", 0)], signature);
    // The track reply is read before the key is written.
    within(WAIT, async {
        while client.statistics().messages_in == read {
            sleep(20).await;
        }
    })
    .await;
    assert_eq!(
        relay(&format!("shared-poll-publish/{channel}/a/2")).await,
        "ok"
    );

    match next(&mut events).await {
        SharedPollSubscriptionEvent::Update(update) => {
            assert_eq!(update.key, "a");
            assert_eq!(update.version, Some(2));
            assert_eq!(update.data, br#"{"v":2}"#);
        }
        event => panic!("not an update: {event:?}"),
    }
}

fn sse_endpoint(server: &str) -> String {
    format!("http://{server}/connection/sse")
}

/// What the relay answers to its control request `command`
/// (`tests/support/http_proxy.rs`, `CONTROL`).
async fn relay(command: &str) -> String {
    let global = js_sys::global();
    let call = |object: &JsValue, method: &str, argument: Option<&JsValue>| {
        let function: js_sys::Function = js_sys::Reflect::get(object, &JsValue::from_str(method))
            .expect("a method")
            .unchecked_into();
        let promise = match argument {
            Some(argument) => function.call1(object, argument),
            None => function.call0(object),
        }
        .expect("a promise");
        wasm_bindgen_futures::JsFuture::from(js_sys::Promise::from(promise))
    };
    let url = JsValue::from_str(&format!("http://{RELAY}/_relay/{command}"));
    let response = within(WAIT, call(&global, "fetch", Some(&url)))
        .await
        .expect("the relay answers");
    within(WAIT, call(&response, "text", None))
        .await
        .expect("the relay's answer")
        .as_string()
        .expect("text")
}

/// How many sse stream requests the relay has received.
async fn sse_streams_relayed() -> u32 {
    relay("requests/connection/sse")
        .await
        .parse()
        .expect("a count")
}

// @W scenario: A browser client falls back to sse when websocket and http_stream are unreachable
#[wasm_bindgen_test]
async fn a_browser_client_falls_back_to_sse() {
    let client = Client::builder_with_transports([
        (TransportKind::WebSocket, UNREACHABLE.to_owned()),
        (
            TransportKind::HttpStream,
            "http://127.0.0.1:1/connection/http_stream".to_owned(),
        ),
        (TransportKind::Sse, sse_endpoint(SERVER)),
    ])
    .build()
    .expect("a valid client");
    let mut events = client.events();

    client.connect();

    assert!(matches!(
        next_event(&mut events).await,
        ClientEvent::Connecting { .. }
    ));
    assert_eq!(connected_over(&mut events).await, "sse");
}

// @W scenario: An sse stream that ends makes a browser client reconnect, not the browser's EventSource
#[wasm_bindgen_test]
async fn an_sse_stream_that_ends_makes_the_client_reconnect() {
    let before = sse_streams_relayed().await;
    let client = Client::builder_with_transports([(TransportKind::Sse, sse_endpoint(RELAY))])
        .build()
        .expect("a valid client");
    let mut events = client.events();
    client.connect();
    assert_eq!(connected_over(&mut events).await, "sse");

    relay("end-streams").await;

    connecting_with(&mut events, 1, "transport closed").await;
    assert_eq!(connected_over(&mut events).await, "sse");
    sleep(10_000).await;
    assert_eq!(sse_streams_relayed().await - before, 2);
}

/// What a test changed on the page, undone when it ends, also when it
/// fails: a failed assertion aborts the module before any `Drop` runs, so
/// the panic hook undoes it too (Block 44, D18: a test that removes a
/// global restores it).
struct Restore;

thread_local! {
    static UNDO: RefCell<Vec<Box<dyn FnOnce()>>> = RefCell::new(Vec::new());
}

fn undo_all() {
    let undo = UNDO.with(|undo| std::mem::take(&mut *undo.borrow_mut()));
    for undo in undo.into_iter().rev() {
        undo();
    }
}

impl Restore {
    fn new(undo: impl FnOnce() + 'static) -> Self {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                undo_all();
                previous(info);
            }));
        });
        UNDO.with(|pending| pending.borrow_mut().push(Box::new(undo)));
        Self
    }
}

impl Drop for Restore {
    fn drop(&mut self) {
        undo_all();
    }
}

/// Removes the global `name` until the guard is dropped.
fn without_global(name: &'static str) -> Restore {
    let global = js_sys::global();
    let key = JsValue::from_str(name);
    let saved = js_sys::Reflect::get(&global, &key).expect("the global");
    js_sys::Reflect::delete_property(&global, &key).expect("removed");
    Restore::new(move || {
        js_sys::Reflect::set(&js_sys::global(), &key, &saved).expect("restored");
    })
}

/// A shim calling the global `name` as `call` says (`new` for a
/// constructor), counting its calls in `count`.
fn counting_wrapper(name: &str, call: &str) -> (js_sys::Function, js_sys::Object) {
    let count = js_sys::Object::new();
    let make = js_sys::Function::new_with_args(
        "count, name, call",
        "count.calls = 0;
         return function (...args) {
           count.calls += 1;
           const target = globalThis[name];
           return call === 'new' ? new target(...args) : target(...args);
         };",
    );
    let shim = make
        .call3(
            &JsValue::NULL,
            &count,
            &JsValue::from_str(name),
            &JsValue::from_str(call),
        )
        .expect("a shim")
        .unchecked_into();
    (shim, count)
}

fn calls(count: &js_sys::Object) -> f64 {
    js_sys::Reflect::get(count, &JsValue::from_str("calls"))
        .expect("a count")
        .as_f64()
        .expect("a number")
}

// @W scenario: A constructor given to a browser client replaces the global
async fn a_shim_replaces_the_global(shim: &str, kind: TransportKind) {
    let (global, call) = match shim {
        "websocket" => ("WebSocket", "new"),
        "fetch" => ("fetch", "call"),
        "readable_stream" => ("ReadableStream", "new"),
        "event_source" => ("EventSource", "new"),
        _ => unreachable!("a shim"),
    };
    let (wrapper, count) = counting_wrapper(global, call);
    let endpoint = match kind {
        TransportKind::WebSocket => endpoint(),
        TransportKind::HttpStream => http_stream_endpoint(),
        _ => sse_endpoint(SERVER),
    };
    let builder = Client::builder_with_transports([(kind, endpoint)]);
    let builder = match shim {
        "websocket" => builder.websocket(wrapper),
        "fetch" => builder.fetch(wrapper),
        "readable_stream" => builder.readable_stream(wrapper),
        _ => builder.event_source(wrapper),
    };
    let client = builder.build().expect("a valid client");
    let mut events = client.events();

    client.connect();

    assert_eq!(connected_over(&mut events).await, kind.name());
    assert!(calls(&count) >= 1.0, "the {shim} shim was not called");
}

#[wasm_bindgen_test]
async fn a_websocket_shim_replaces_the_global() {
    a_shim_replaces_the_global("websocket", TransportKind::WebSocket).await;
}

#[wasm_bindgen_test]
async fn a_fetch_shim_replaces_the_global() {
    a_shim_replaces_the_global("fetch", TransportKind::HttpStream).await;
}

#[wasm_bindgen_test]
async fn a_readable_stream_shim_replaces_the_global() {
    a_shim_replaces_the_global("readable_stream", TransportKind::HttpStream).await;
}

#[wasm_bindgen_test]
async fn an_event_source_shim_replaces_the_global() {
    a_shim_replaces_the_global("event_source", TransportKind::Sse).await;
}

// @W scenario: A browser client skips a transport the page has no constructor for
#[wasm_bindgen_test]
async fn a_transport_without_its_constructor_is_skipped() {
    let _restore = without_global("EventSource");
    let client = Client::builder_with_transports([
        (TransportKind::WebSocket, UNREACHABLE.to_owned()),
        (TransportKind::Sse, sse_endpoint(SERVER)),
        (TransportKind::HttpStream, http_stream_endpoint()),
    ])
    .build()
    .expect("a valid client");
    let mut events = client.events();

    client.connect();

    let transport = loop {
        match next(&mut events).await {
            ClientEvent::Connected { transport, .. } => break transport,
            ClientEvent::Error(error) => {
                assert_ne!(error.transport.as_deref(), Some("sse"), "{error:?}");
            }
            ClientEvent::Connecting { .. } => {}
            event => panic!("not connecting or connected: {event:?}"),
        }
    };
    assert_eq!(transport, "http_stream");
    // The websocket's only: no sse attempt was made.
    assert_eq!(client.statistics().failed_attempts, 1);
}

// @W scenario: Building a browser client with no usable transport fails
#[wasm_bindgen_test]
fn a_client_with_no_usable_transport_is_refused() {
    let _restore = without_global("EventSource");

    let built =
        Client::builder_with_transports([(TransportKind::Sse, sse_endpoint(SERVER))]).build();

    let error = built.expect_err("refused");
    assert_eq!(error.kind, ferrifuge::ErrorKind::Configuration);
    assert!(
        error.message.contains("sse") && error.message.contains("event_source"),
        "{error:?}"
    );
}

/// `document.cookie` set to `cookie`, until the guard is dropped.
fn page_cookie(name: &'static str, value: &str) -> Restore {
    let document =
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("document")).expect("a page");
    let set = move |cookie: &str| {
        js_sys::Reflect::set(
            &document,
            &JsValue::from_str("cookie"),
            &JsValue::from_str(cookie),
        )
        .expect("a cookie set");
    };
    set(&format!("{name}={value}; path=/"));
    Restore::new(move || set(&format!("{name}=; path=/; max-age=0")))
}

/// The relay's `host:port`, on the page's own host: another origin of the
/// same site, whose cookies the page holds.
fn relay_on_page_host() -> String {
    let location =
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("location")).expect("a page");
    let host = js_sys::Reflect::get(&location, &JsValue::from_str("hostname"))
        .expect("a host")
        .as_string()
        .expect("text");
    let port = RELAY.rsplit_once(':').expect("host:port").1;
    format!("{host}:{port}")
}

/// The Cookie values of each request the relay received at `path`, CORS
/// preflights left out.
async fn cookies_relayed(path: &str) -> Vec<String> {
    let answer = relay(&format!("cookies{path}")).await;
    answer
        .lines()
        .map(|line| line.strip_prefix('>').expect("a request").to_owned())
        .collect()
}

// @W scenario: With credentials included, a browser client's HTTP requests carry the page's cookies to the server's origin
async fn credentials_carry_the_page_cookies(kind: TransportKind, include: bool) {
    let stream = format!("/connection/{}", kind.name());
    let streams_before = cookies_relayed(&stream).await.len();
    let emulations_before = cookies_relayed("/emulation").await.len();
    let _cookie = page_cookie("affinity", "a1");
    let host = relay_on_page_host();
    let endpoint = format!("http://{host}{stream}");
    let client = Client::builder_with_transports([(kind, endpoint)])
        .include_credentials(include)
        .build()
        .expect("a valid client");
    let mut events = client.events();
    client.connect();
    assert_eq!(connected_over(&mut events).await, kind.name());

    within(WAIT, client.publish(&unique("news"), br#""hi""#.to_vec()))
        .await
        .expect("the publish succeeds");

    let streams = cookies_relayed(&stream).await.split_off(streams_before);
    let emulations = cookies_relayed("/emulation")
        .await
        .split_off(emulations_before);
    assert_eq!(streams.len(), 1, "the stream request: {streams:?}");
    assert!(!emulations.is_empty(), "an emulation request");
    let expected = if include { "affinity=a1" } else { "" };
    for cookie in streams.iter().chain(&emulations) {
        assert_eq!(
            cookie, expected,
            "streams {streams:?}, emulations {emulations:?}"
        );
    }
}

#[wasm_bindgen_test]
async fn included_credentials_carry_the_cookies_over_http_stream() {
    credentials_carry_the_page_cookies(TransportKind::HttpStream, true).await;
}

#[wasm_bindgen_test]
async fn credentials_not_included_carry_no_cookie_over_http_stream() {
    credentials_carry_the_page_cookies(TransportKind::HttpStream, false).await;
}

#[wasm_bindgen_test]
async fn included_credentials_carry_the_cookies_over_sse() {
    credentials_carry_the_page_cookies(TransportKind::Sse, true).await;
}

#[wasm_bindgen_test]
async fn credentials_not_included_carry_no_cookie_over_sse() {
    credentials_carry_the_page_cookies(TransportKind::Sse, false).await;
}

/// The WebTransport relay's endpoint, `tests/support/webtransport_relay.rs`
/// (Block 48, D19): none when the harness runs no WebTransport server, as
/// with a Centrifugo binary on hosted macOS, and the WebTransport tests are
/// skipped.
#[cfg(feature = "unstable")]
const WEBTRANSPORT: Option<&str> = option_env!("FERRIFUGE_W_WEBTRANSPORT");

/// The relay's endpoint, or none after saying why the test is skipped.
#[cfg(feature = "unstable")]
fn webtransport_relay() -> Option<&'static str> {
    if WEBTRANSPORT.is_none() {
        wasm_bindgen_test::console_log!("no WebTransport relay in this run: skipped");
    }
    WEBTRANSPORT
}

#[cfg(feature = "unstable")]
fn webtransport_then_websocket(relay: &str) -> Client {
    Client::builder_with_transports([
        (TransportKind::WebTransport, relay.to_owned()),
        (TransportKind::WebSocket, endpoint()),
    ])
    .build()
    .expect("a valid client")
}

/// Sessions the WebTransport relay accepted so far.
#[cfg(feature = "unstable")]
async fn webtransport_sessions() -> u32 {
    relay("webtransport-sessions")
        .await
        .parse()
        .expect("a count")
}

// @W scenario: A browser client connects over WebTransport and receives a publication
#[cfg(feature = "unstable")]
#[wasm_bindgen_test]
async fn a_browser_client_connects_over_webtransport_and_receives_a_publication() {
    let Some(endpoint) = webtransport_relay() else {
        return;
    };
    for protocol in [Protocol::Json, Protocol::Protobuf] {
        let channel = unique("news");
        let client =
            Client::builder_with_transports([(TransportKind::WebTransport, endpoint.to_owned())])
                .config(Config::default().protocol(protocol))
                .build()
                .expect("a valid client");
        let subscription = client.new_subscription(channel.clone()).unwrap();
        let mut events = client.events();
        let mut subscription_events = subscription.events();

        client.connect();
        subscription.subscribe();
        let transport = connected_over(&mut events).await;
        subscribed(&mut subscription_events).await;
        let published = relay(&format!("webtransport-publish/{channel}")).await;
        let received = publication(&mut subscription_events).await;

        assert_eq!(transport, "webtransport", "{protocol:?}");
        assert_eq!(published, "ok", "{protocol:?}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&received.data).expect("JSON data"),
            serde_json::json!({"from": "api"}),
            "{protocol:?}"
        );
        client.disconnect();
    }
}

// @W scenario: A browser without WebTransport skips the transport
#[cfg(feature = "unstable")]
#[wasm_bindgen_test]
async fn a_browser_without_webtransport_skips_the_transport() {
    let Some(endpoint) = webtransport_relay() else {
        return;
    };
    let global = js_sys::global();
    let key = JsValue::from_str("WebTransport");
    let saved = js_sys::Reflect::get(&global, &key).expect("the global");
    js_sys::Reflect::delete_property(&global, &key).expect("removed");
    let before = webtransport_sessions().await;
    let client = webtransport_then_websocket(endpoint);
    let mut events = client.events();

    client.connect();
    // Skipped, not tried (Block 44, D12): no error event names it.
    let mut failed = None;
    let transport = loop {
        match next(&mut events).await {
            ClientEvent::Error(error) => failed = failed.or(error.transport.clone()),
            ClientEvent::Connected { transport, .. } => break transport,
            ClientEvent::Connecting { .. } => {}
            event => panic!("not connecting or connected: {event:?}"),
        }
    };
    let after = webtransport_sessions().await;
    client.disconnect();
    js_sys::Reflect::set(&global, &key, &saved).expect("restored");

    assert_eq!(transport, "websocket");
    assert_eq!(after, before);
    assert_eq!(failed, None);
}

// @W scenario: A browser client falls back from WebTransport to websocket when the session is refused
#[cfg(feature = "unstable")]
#[wasm_bindgen_test]
async fn a_browser_client_falls_back_from_webtransport_when_the_session_is_refused() {
    let Some(endpoint) = webtransport_relay() else {
        return;
    };
    relay("webtransport-refuse/on").await;
    let client = webtransport_then_websocket(endpoint);
    let mut events = client.events();

    client.connect();
    let mut failed = None;
    let transport = loop {
        match next(&mut events).await {
            ClientEvent::Error(error) => failed = failed.or(error.transport.clone()),
            ClientEvent::Connected { transport, .. } => break transport,
            ClientEvent::Connecting { .. } => {}
            event => panic!("not connecting or connected: {event:?}"),
        }
    };
    client.disconnect();
    relay("webtransport-refuse/off").await;

    assert_eq!(failed.as_deref(), Some("webtransport"));
    assert_eq!(transport, "websocket");
}
