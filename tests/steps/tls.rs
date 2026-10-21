//! Steps of Block 6: TLS crypto provider.

use cucumber::given;

use super::ClientWorld;
use crate::SERVER_A;
use crate::support::centrifugo::Centrifugo;
use crate::support::tls;

/// Replaces the @A server the scenario started with one serving TLS. The
/// client connects to it directly: the server certificate names `localhost`.
#[given("a server serving TLS with a certificate from the test CA")]
async fn a_tls_server(world: &mut ClientWorld) {
    let (cert, key) = tls::server_pem();
    world.relay = None;
    world.server = Some(Centrifugo::start_tls(SERVER_A, &cert, &key).await);
}

#[given("a client configured with the server's wss endpoint")]
fn a_client_with_the_wss_endpoint(world: &mut ClientWorld) {
    let endpoint = world
        .server
        .as_ref()
        .expect("this step needs a server: tag the scenario @A")
        .websocket_url();
    world.new_client(endpoint);
}
