//! Block 68: refusing endpoints that are not TLS.

use cucumber::{given, then};
use ferrifuge::machine::TransportKind;
use ferrifuge::{Client, ErrorKind, codes};

use super::ClientWorld;

fn kind(name: &str) -> TransportKind {
    match name {
        "websocket" => TransportKind::WebSocket,
        "http_stream" => TransportKind::HttpStream,
        "sse" => TransportKind::Sse,
        _ => panic!("unknown transport {name}"),
    }
}

/// The endpoints of a phrase such as `the websocket endpoint "…", the
/// http_stream endpoint "…" and the emulation endpoint "…"`, in order.
fn endpoints(phrase: &str) -> (Vec<(TransportKind, String)>, Option<String>) {
    let mut transports = Vec::new();
    let mut emulation = None;
    let parts: Vec<&str> = phrase.split('"').collect();
    for pair in parts.chunks(2) {
        let [before, url] = pair else { break };
        let name = before
            .trim_end()
            .strip_suffix("endpoint")
            .expect("an endpoint phrase")
            .split_whitespace()
            .last()
            .expect("a transport name");
        if name == "emulation" {
            emulation = Some((*url).to_owned());
        } else {
            transports.push((kind(name), (*url).to_owned()));
        }
    }
    (transports, emulation)
}

fn pending(world: &mut ClientWorld, phrase: &str, tls_only: bool) {
    let (transports, emulation) = endpoints(phrase);
    let mut builder = Client::builder_with_transports(transports);
    if let Some(emulation) = emulation {
        builder = builder.emulation_endpoint(emulation);
    }
    if tls_only {
        builder = builder.tls_only(true);
    }
    world.pending_builder = Some(builder);
    world.build_error = None;
}

#[given(
    regex = r#"^client options with tls_only(?:,| and) (the (?:websocket|http_stream|sse) endpoint ".+)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn client_options_with_tls_only(world: &mut ClientWorld, phrase: String) {
    pending(world, &phrase, true);
}

#[given(regex = r#"^client options with (the websocket endpoint ".+)$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn client_options_without_tls_only(world: &mut ClientWorld, phrase: String) {
    pending(world, &phrase, false);
}

#[given("client options with tls_only and the double's endpoint and connector")]
fn client_options_with_the_double(world: &mut ClientWorld) {
    let double = world
        .doubles
        .server
        .as_ref()
        .expect("a server double")
        .clone();
    world.pending_builder = Some(
        Client::builder(double.endpoint())
            .connector(double.connector())
            .tls_only(true),
    );
    world.build_error = None;
}

#[then(
    regex = r#"^building fails with a configuration error of (?:transport (\w+)|no transport) with message "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn building_fails_with(world: &mut ClientWorld, transport: String, message: String) {
    let error = world.build_error.as_ref().expect("building succeeded");
    let transport = (!transport.is_empty()).then(|| kind(&transport));
    assert_eq!(
        (
            error.kind,
            error.code,
            error.transport,
            error.message.as_str()
        ),
        (
            ErrorKind::Configuration,
            codes::error::BAD_CONFIGURATION,
            transport,
            message.as_str()
        ),
        "{error:?}"
    );
}
