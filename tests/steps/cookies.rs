//! Steps of Block 43: the cookie jar on the websocket upgrade (@D, 43a)
//! and on the emulated transports (@B, 43b).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;

use cucumber::{given, then, when};
use ferrifuge::{Client, MemoryJar, State};

use super::ClientWorld;
use super::delivery::wait_until;

/// The scenario's client for the fake server, with a memory cookie jar
/// when `jar`, and `headers`.
fn build(world: &mut ClientWorld, jar: bool, headers: &[(&str, &str)]) {
    let mut builder = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .http_headers(headers.iter().copied());
    if jar {
        builder = builder.cookie_jar(Arc::new(MemoryJar::new()));
    }
    world.build_client(builder);
}

async fn connected(world: &mut ClientWorld) {
    world.client().connect();
    let client = world.client().clone();
    wait_until("connected", || client.state() == State::Connected).await;
}

#[given(
    regex = r#"^a client with a memory cookie jar connected to the fake server, whose upgrade response sets the cookie "([^"]+)"$"#
)]
async fn a_client_with_a_jar(world: &mut ClientWorld, cookie: String) {
    world.fake().set_cookies_on_upgrades(&[&cookie]);
    build(world, true, &[]);
    connected(world).await;
}

#[given(
    regex = r#"^a client without a cookie jar connected to the fake server, whose upgrade response sets the cookie "([^"]+)"$"#
)]
async fn a_client_without_a_jar(world: &mut ClientWorld, cookie: String) {
    world.fake().set_cookies_on_upgrades(&[&cookie]);
    build(world, false, &[]);
    connected(world).await;
}

#[given(
    regex = r#"^a client with a memory cookie jar and the fake server answering its next upgrade with HTTP status (\d+) and the cookie "([^"]+)"$"#
)]
fn a_client_refused_once(world: &mut ClientWorld, status: u16, cookie: String) {
    world.fake().refuse_next_upgrade(status, &cookie);
    build(world, true, &[]);
}

#[given(
    regex = r#"^a client with a memory cookie jar and the HTTP header "Cookie: ([^"]+)", connected to the fake server, whose upgrade response sets the cookies "([^"]+)" and "([^"]+)"$"#
)]
async fn a_client_with_an_app_cookie(
    world: &mut ClientWorld,
    app: String,
    first: String,
    second: String,
) {
    world.fake().set_cookies_on_upgrades(&[&first, &second]);
    build(world, true, &[("Cookie", &app)]);
    connected(world).await;
}

#[given(
    regex = r#"^a client with a memory cookie jar whose log records are captured, connected to the fake server, whose upgrade response sets the cookie "([^"]+)"$"#
)]
async fn a_captured_client_with_a_jar(world: &mut ClientWorld, cookie: String) {
    world.fake().set_cookies_on_upgrades(&[&cookie]);
    build(world, true, &[]);
    super::logging::captured(world);
    connected(world).await;
}

/// The Cookie headers of upgrade request `index`, once there is one.
async fn upgrade_cookies(world: &ClientWorld, index: usize) -> Vec<String> {
    let fake = world.fake();
    wait_until(&format!("upgrade request {}", index + 1), || {
        fake.upgrade_cookies().len() > index
    })
    .await;
    fake.upgrade_cookies()[index].clone()
}

#[then(regex = r#"^the next upgrade request carries one Cookie header "([^"]+)"$"#)]
async fn the_next_upgrade_carries(world: &mut ClientWorld, value: String) {
    assert_eq!(upgrade_cookies(world, 1).await, [value]);
}

#[then(
    regex = r#"^the upgrade request after the refused one carries one Cookie header "([^"]+)"$"#
)]
async fn the_upgrade_after_the_refused_one_carries(world: &mut ClientWorld, value: String) {
    assert_eq!(upgrade_cookies(world, 0).await, Vec::<String>::new());
    assert_eq!(upgrade_cookies(world, 1).await, [value]);
}

#[then("the next upgrade request carries no Cookie header")]
async fn the_next_upgrade_carries_none(world: &mut ClientWorld) {
    assert_eq!(upgrade_cookies(world, 1).await, Vec::<String>::new());
}

/// The emulation requests the HTTP proxy relayed, with their Cookie
/// headers.
fn emulation_cookies(world: &ClientWorld) -> Vec<Vec<String>> {
    world
        .http_proxy
        .as_ref()
        .expect("an HTTP proxy")
        .requests_with_header_values("cookie")
        .into_iter()
        .filter(|(path, _)| path == "/emulation")
        .map(|(_, values)| values)
        .collect()
}

#[given(
    regex = r#"^a client with a memory cookie jar connected over transport "(\w+)" through the HTTP proxy, whose (stream response sets|emulation responses set) the cookie "([^"]+)"$"#
)]
async fn a_client_with_a_jar_behind_the_http_proxy(
    world: &mut ClientWorld,
    transport: String,
    response: String,
    cookie: String,
) {
    let kind = super::transports::kind(&transport);
    let http_proxy = super::transports::pinging_server_behind_an_http_proxy(world).await;
    if response.starts_with("stream") {
        http_proxy.set_cookie_on_stream(&cookie);
    } else {
        http_proxy.set_cookie_on_emulation(&cookie);
    }
    let url = format!("http://{}/connection/{}", http_proxy.address(), kind.name());
    world.transports = vec![kind];
    let builder = Client::builder_with_transports(vec![(kind, url)])
        .config(world.client_config())
        .cookie_jar(Arc::new(MemoryJar::new()));
    world.build_client(builder);
    world.http_proxy = Some(http_proxy);
    super::transports::connect_and_wait(world).await;
}

#[then("the HTTP proxy relayed at least one emulation request")]
fn the_proxy_relayed_an_emulation_request(world: &mut ClientWorld) {
    assert!(!emulation_cookies(world).is_empty(), "no emulation request");
}

#[then(regex = r#"^every emulation request it relayed carried one Cookie header "([^"]+)"$"#)]
fn every_emulation_request_carried(world: &mut ClientWorld, cookie: String) {
    let relayed = emulation_cookies(world);
    assert!(
        relayed
            .iter()
            .all(|values| values == std::slice::from_ref(&cookie)),
        "{relayed:?}"
    );
}

#[when("the client sends a command and, once it is answered, another")]
async fn the_client_sends_two_commands(world: &mut ClientWorld) {
    // Its commands are its pongs: one per server ping (Block 8).
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    wait_until("an emulation request answered", || {
        proxy.emulation_answered() >= 1
    })
    .await;
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    wait_until("a second emulation request", || {
        proxy
            .requests_with_header_values("cookie")
            .iter()
            .filter(|(path, _)| path == "/emulation")
            .count()
            >= 2
    })
    .await;
}

#[then(
    regex = r#"^the emulation request of the second command carried one Cookie header "([^"]+)"$"#
)]
fn the_second_emulation_request_carried(world: &mut ClientWorld, cookie: String) {
    let relayed = emulation_cookies(world);
    assert_eq!(relayed.get(1), Some(&vec![cookie]), "{relayed:?}");
}
