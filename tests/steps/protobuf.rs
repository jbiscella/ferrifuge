//! Steps of Block 7: Protobuf encoding.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, Protocol};

use super::codes::a_client_connected_to_the_fake_server;
use super::ping::connected_announcing_pong;
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::protobuf;

/// Bytes that do not decode as length-delimited Protobuf: a length varint
/// whose continuation bit never ends.
const MALFORMED_FRAME: [u8; 3] = [0xff, 0xff, 0xff];

#[given("a new client configured with the Protobuf protocol")]
fn a_new_protobuf_client(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
}

#[given("a client configured with the Protobuf protocol connected to the fake server")]
async fn a_protobuf_client_connected_to_the_fake_server(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    a_client_connected_to_the_fake_server(world).await;
}

#[given(
    "a client configured with the Protobuf protocol connected to the fake server that announces pong is expected"
)]
async fn a_protobuf_client_expected_to_pong(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    connected_announcing_pong(world, "expected".to_owned()).await;
}

#[when("the fake server sends a Protobuf ping")]
fn the_fake_server_sends_a_protobuf_ping(world: &mut ClientWorld) {
    world.fake().send_protobuf(&[protobuf::Reply::default()]);
}

#[when("the fake server sends two Protobuf pings in one frame")]
fn the_fake_server_sends_two_protobuf_pings(world: &mut ClientWorld) {
    world
        .fake()
        .send_protobuf(&[protobuf::Reply::default(), protobuf::Reply::default()]);
}

#[when("the fake server sends a binary frame that is not valid Protobuf")]
fn the_fake_server_sends_a_malformed_frame(world: &mut ClientWorld) {
    world.fake().send_binary(MALFORMED_FRAME.to_vec());
}

/// The fake server records a binary frame's commands only when it decodes
/// as Protobuf, and text frames apart as JSON.
#[then("the fake server received the connect command as binary Protobuf")]
fn fake_server_received_protobuf_connect(world: &mut ClientWorld) {
    let fake = world.fake();
    let received = fake.received_protobuf();
    assert!(
        received
            .first()
            .is_some_and(|first| first.connect.is_some()),
        "{received:?}"
    );
    assert!(
        fake.received().is_empty(),
        "JSON received: {:?}",
        fake.received()
    );
}

#[then("the client sends a Protobuf pong")]
async fn the_client_sends_a_protobuf_pong(world: &mut ClientWorld) {
    wait_for_pongs(world, 1).await;
}

#[then("the client sends two Protobuf pongs")]
async fn the_client_sends_two_protobuf_pongs(world: &mut ClientWorld) {
    wait_for_pongs(world, 2).await;
}

#[then(regex = r#"^the client emits "connecting" with code (\d+)$"#)]
async fn the_client_emits_connecting_with_code(world: &mut ClientWorld, code: u32) {
    let event = world.next_event().await;
    let ClientEvent::Connecting { code: got, .. } = event else {
        panic!("expected connecting, got {event:?}");
    };
    assert_eq!(got, code);
}

/// How long a step watches for a pong beyond the expected ones.
const NO_MORE_PONGS: Duration = Duration::from_millis(300);

/// Waits until the fake server has received `count` pongs (empty Protobuf
/// commands), and no more.
async fn wait_for_pongs(world: &ClientWorld, count: usize) {
    let pongs = || {
        world
            .fake()
            .received_protobuf()
            .iter()
            .filter(|command| **command == protobuf::Command::default())
            .count()
    };
    let arrived = tokio::time::timeout(EVENT_TIMEOUT, async {
        while pongs() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        arrived.is_ok(),
        "{} of {count} pongs: {:?}",
        pongs(),
        world.fake().received_protobuf()
    );
    // "And no more": a pong too many would come right after the others
    // (review item D3).
    tokio::time::sleep(NO_MORE_PONGS).await;
    assert_eq!(pongs(), count);
}
