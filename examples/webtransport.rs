//! Connects over WebTransport, falling back to websocket, subscribes to
//! channel `news` and prints its publications (Block 48; unstable, with the
//! `unstable-webtransport` feature).
//!
//! Centrifugo serves WebTransport over TLS with HTTP/3 on, on the TLS port
//! over UDP (see the README's "WebTransport (unstable)" section). Run
//! `cargo run --example webtransport --features unstable-webtransport`.
//! `FERRIFUGE_EXAMPLE_WEBTRANSPORT` and `FERRIFUGE_EXAMPLE_ENDPOINT`
//! override the WebTransport and websocket endpoints.

use ferrifuge::machine::TransportKind;
use ferrifuge::{Client, ClientEvent, Config, SubscriptionEvent};
use futures_util::StreamExt;

const CHANNEL: &str = "news";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let webtransport = std::env::var("FERRIFUGE_EXAMPLE_WEBTRANSPORT")
        .unwrap_or_else(|_| "https://localhost:8443/connection/webtransport".to_owned());
    let websocket = std::env::var("FERRIFUGE_EXAMPLE_ENDPOINT")
        .unwrap_or_else(|_| "wss://localhost:8443/connection/websocket".to_owned());
    // WebTransport first, websocket where UDP does not pass.
    let client = Client::builder_with_transports([
        (TransportKind::WebTransport, webtransport),
        (TransportKind::WebSocket, websocket),
    ])
    .config(Config::default())
    .build()?;
    let mut client_events = client.events();
    let subscription = client.new_subscription(CHANNEL)?;
    let mut events = subscription.events();
    client.connect();
    subscription.subscribe();

    loop {
        tokio::select! {
            Some(event) = client_events.next() => {
                if let ClientEvent::Connected { transport, .. } = &event {
                    println!("connected over {transport}");
                }
            }
            Some(event) = events.next() => match event {
                SubscriptionEvent::Subscribed(_) => println!("subscribed to {CHANNEL}"),
                SubscriptionEvent::Publication(publication) => {
                    println!("{}", String::from_utf8_lossy(&publication.data));
                }
                SubscriptionEvent::Unsubscribed { code, reason } => {
                    println!("unsubscribed: {code} {reason}");
                    break;
                }
                SubscriptionEvent::Error(error) => eprintln!("error: {}", error.message),
                _ => {}
            },
            else => break,
        }
    }
    Ok(())
}
