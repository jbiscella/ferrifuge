//! A subscription to `news` with Fossil delta compression: the server sends
//! each publication as a delta against the previous one, and the client
//! rebuilds it, so the application still gets whole payloads. The server
//! must allow it (`allowed_delta_types` in `examples/centrifugo.json`).

use ferrifuge::{Client, Config, DeltaFormat, SubscriptionEvent};
use futures_util::StreamExt;

const CHANNEL: &str = "news";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("FERRIFUGE_EXAMPLE_ENDPOINT")
        .unwrap_or_else(|_| "ws://localhost:8000/connection/websocket".to_owned());
    let client = Client::new(endpoint, Config::default())?;
    let subscription = client
        .subscription_builder(CHANNEL)
        .delta(DeltaFormat::Fossil)
        .build()?;
    let mut events = subscription.events();
    client.connect();
    subscription.subscribe();

    while let Some(event) = events.next().await {
        match event {
            SubscriptionEvent::Subscribed(subscribed) => {
                println!("subscribed to {CHANNEL}, delta on: {}", subscribed.delta);
            }
            SubscriptionEvent::Publication(publication) => {
                println!("{}", String::from_utf8_lossy(&publication.data));
            }
            SubscriptionEvent::Error(error) => eprintln!("error: {}", error.message),
            _ => {}
        }
    }
    Ok(())
}
