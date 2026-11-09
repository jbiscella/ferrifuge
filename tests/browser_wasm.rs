//! The @W scenarios of the websocket transport, of the network and page
//! events and of `http_stream` (Blocks 44b, 44c, 44d): the browser client
//! against the Centrifugo that `tests/browser.rs` starts, run by
//! `wasm-bindgen-test-runner` in a browser (Block 44, D18). The server's
//! `host:port` comes at compile time in `FERRIFUGE_W_SERVER`.

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

// @W scenario: A stream not read stops a browser client reading its http_stream response
#[wasm_bindgen_test]
async fn a_stream_not_read_stops_the_client_reading_its_http_stream_response() {
    const COUNT: usize = 1000;
    // 100 bytes of JSON: a string of 98 digits.
    let data = |n: usize| format!("\"{n:098}\"").into_bytes();
    let channel = unique("news");
    let client = over_http_stream(Config::default().read_ahead_messages(16));
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
        while client.read_ahead().0 != 16 {
            sleep(20).await;
        }
    })
    .await;
    sleep(500).await;
    assert_eq!(client.read_ahead().0, 16);
    yields_nothing(&mut events, 1000).await;
    assert_eq!(client.read_ahead().0, 16);
    for n in 1..=COUNT {
        assert_eq!(publication(&mut subscription_events).await.data, data(n));
    }
    match select(subscription_events.next(), sleep(500)).await {
        Either::Left((event, _)) => panic!("yielded {event:?} after the publications"),
        Either::Right(_) => {}
    }
}
