//! Steps of Block 41a: clones of a client and weak handles.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{Client, WeakClient};

use super::connection::a_new_client;
use super::{ClientWorld, test_config};

/// Beyond the configured timeout, for a close to reach the fake server.
const TOLERANCE: Duration = Duration::from_secs(1);

/// What a Block 41a scenario holds besides the world's client.
#[derive(Debug, Default)]
pub(crate) struct Clones {
    clone: Option<Client>,
    weak: Option<WeakClient>,
}

#[given("a clone of the client")]
fn a_clone_of_the_client(world: &mut ClientWorld) {
    world.clones.clone = Some(world.client().clone());
}

#[given("a new client whose stream was taken")]
fn a_new_client_whose_stream_was_taken(world: &mut ClientWorld) {
    // The world records a new client's stream from the start.
    a_new_client(world);
}

#[given("a weak handle downgraded from it")]
fn a_weak_handle(world: &mut ClientWorld) {
    world.clones.weak = Some(world.client().downgrade());
}

#[then("1 s later the fake server still sees the connection open")]
async fn still_open(world: &mut ClientWorld) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let fake = world.fake();
    assert_eq!(
        (fake.connections(), fake.closed_connections()),
        (1, 0),
        "connections opened and closed"
    );
}

#[then(regex = r#"^the clone's state is "(\w+)"$"#)]
fn the_clones_state(world: &mut ClientWorld, state: String) {
    let clone = world.clones.clone.as_ref().expect("a clone");
    assert_eq!(format!("{:?}", clone.state()).to_lowercase(), state);
}

#[then(
    "when the clone is dropped, the fake server sees the connection closed within the configured timeout plus tolerance"
)]
async fn the_last_drop_closes(world: &mut ClientWorld) {
    drop(world.clones.clone.take().expect("a clone"));
    let limit = test_config().timeout + TOLERANCE;
    let fake = world.fake();
    let closed = tokio::time::timeout(limit, async {
        while fake.closed_connections() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "the connection is still open after {limit:?}"
    );
}

#[then("upgrading the weak handle gives a client that is not closed")]
fn upgrades(world: &mut ClientWorld) {
    let weak = world.clones.weak.as_ref().expect("a weak handle");
    let client = weak.upgrade().expect("a clone is alive");
    assert!(!client.is_closed());
}

#[then("when the clone is dropped, upgrading the weak handle gives nothing")]
fn upgrades_to_nothing(world: &mut ClientWorld) {
    drop(world.clones.clone.take().expect("a clone"));
    let weak = world.clones.weak.as_ref().expect("a weak handle");
    assert!(weak.upgrade().is_none(), "the client is still alive");
}

#[when("close is awaited on the clone")]
async fn close_on_the_clone(world: &mut ClientWorld) {
    world.clones.clone.as_ref().expect("a clone").close().await;
}
