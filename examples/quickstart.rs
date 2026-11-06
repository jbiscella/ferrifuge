//! Connects, subscribes to channel `news` and prints its publications.
//!
//! Run a server with `examples/centrifugo.json` (see the README), then
//! `cargo run --example quickstart`, and publish:
//! `curl -d '{"channel":"news","data":{"text":"hello"}}' localhost:8000/api/publish`.
//! `FERRIFUGE_EXAMPLE_ENDPOINT` overrides the server's websocket endpoint.

use ferrifuge::{Client, Config, SubscriptionEvent};
use futures_util::StreamExt;

const CHANNEL: &str = "news";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("FERRIFUGE_EXAMPLE_ENDPOINT")
        .unwrap_or_else(|_| "ws://localhost:8000/connection/websocket".to_owned());
    let client = Client::new(endpoint, Config::default())?;
    let subscription = client.new_subscription(CHANNEL)?;
    // Taken before connect() and subscribe(), so that no event is missed.
    let mut events = subscription.events();
    client.connect();
    subscription.subscribe();

    while let Some(event) = events.next().await {
        match event {
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
        }
    }
    Ok(())
}
