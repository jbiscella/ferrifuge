//! Steps of Block 28: the tokio runtime handle.

use std::fmt;

use cucumber::{given, then, when};
use ferrifuge::{Client, ClientBuilder, ErrorKind, SubscriptionError, codes};

use super::{
    ClientWorld, EVENT_TIMEOUT, UNREACHABLE_ENDPOINT, expect_event, recorded, test_config,
};
use crate::support::fake_server::Rpc;

/// The separate runtime of a Block 28 scenario, shut down without waiting
/// when the scenario ends: dropping a runtime in asynchronous code panics.
#[derive(Default)]
pub(crate) struct Separate(Option<tokio::runtime::Runtime>);

impl Separate {
    fn handle(&self) -> tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("a separate runtime: see \"a separate multi-thread tokio runtime\"")
            .handle()
            .clone()
    }
}

impl Drop for Separate {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

impl fmt::Debug for Separate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Separate").field(&self.0.is_some()).finish()
    }
}

/// Builds on a thread of its own, where no tokio runtime is current: a
/// `spawn_blocking` thread would be inside the scenario's runtime.
fn build_outside_any_runtime(builder: ClientBuilder) -> Result<Client, ferrifuge::ClientError> {
    std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        builder.build()
    })
    .join()
    .expect("the building thread")
}

fn keep(world: &mut ClientWorld, client: Client) {
    world.events = Some(recorded(client.events()));
    world.client = Some(client);
}

#[given("client options without a runtime handle")]
fn client_options_without_a_runtime_handle(world: &mut ClientWorld) {
    world.pending_builder = Some(Client::builder(UNREACHABLE_ENDPOINT).config(test_config()));
}

#[when("the client is built outside any tokio runtime")]
fn the_client_is_built_outside_any_runtime(world: &mut ClientWorld) {
    let builder = world.pending_builder.take().expect("client options");
    world.build_error = build_outside_any_runtime(builder).err();
}

#[then("building fails with a configuration error naming the runtime option")]
fn building_fails_naming_the_runtime_option(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert_eq!(error.code, codes::error::BAD_CONFIGURATION, "{error:?}");
    assert!(
        error.message.contains("ClientBuilder::runtime"),
        "{error:?}"
    );
}

#[given("a separate multi-thread tokio runtime")]
fn a_separate_runtime(world: &mut ClientWorld) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    world.runtime = Separate(Some(runtime));
}

#[given("a client built outside any tokio runtime with that runtime's handle")]
fn a_client_built_outside_any_runtime(world: &mut ClientWorld) {
    let builder = world
        .connect_data
        .build(Client::builder(world.server_endpoint()).config(world.client_config()))
        .runtime(world.runtime.handle());
    let client = build_outside_any_runtime(builder).expect("a valid client configuration");
    keep(world, client);
}

#[given(
    "a client built with that runtime's handle and connected to the fake server that never replies to commands"
)]
async fn a_client_on_the_runtime_connected_to_the_fake_server(world: &mut ClientWorld) {
    world.fake().answer_rpc(Rpc::Ignore);
    let client = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .runtime(world.runtime.handle())
        .build()
        .expect("a valid client configuration");
    keep(world, client);
    world.client().connect();
    expect_event(world, "connecting").await;
    expect_event(world, "connected").await;
}

#[when("that runtime shuts down")]
fn that_runtime_shuts_down(world: &mut ClientWorld) {
    let runtime = world.runtime.0.take().expect("a separate runtime");
    runtime.shutdown_background();
}

#[then("the client's stream ends")]
async fn the_clients_stream_ends(world: &mut ClientWorld) {
    use futures_util::StreamExt;
    let events = world.events.as_mut().expect("no client created yet");
    let next = tokio::time::timeout(EVENT_TIMEOUT, events.next())
        .await
        .unwrap_or_else(|_| panic!("the stream did not end within {EVENT_TIMEOUT:?}"));
    assert!(next.is_none(), "expected the end, got {next:?}");
}

#[then("creating a subscription fails because the client is closed")]
fn creating_a_subscription_fails(world: &mut ClientWorld) {
    let created = world.client().new_subscription("news");
    assert_eq!(created.err(), Some(SubscriptionError::ClientClosed));
}
