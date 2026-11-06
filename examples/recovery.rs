//! A recoverable subscription to `news` whose stream position survives a
//! restart: each publication's position is saved to a file, and the next
//! run subscribes with `since`, so the server sends what was missed in
//! between. Run it, stop it, publish, run it again.
//!
//! `FERRIFUGE_EXAMPLE_POSITION` names the file (default `position.txt`).

use ferrifuge::{Client, Config, StreamPosition, SubscriptionEvent};
use futures_util::StreamExt;

const CHANNEL: &str = "news";

fn load(path: &str) -> Option<StreamPosition> {
    let saved = std::fs::read_to_string(path).ok()?;
    let (offset, epoch) = saved.trim().split_once(' ')?;
    Some(StreamPosition {
        offset: offset.parse().ok()?,
        epoch: epoch.to_owned(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("FERRIFUGE_EXAMPLE_ENDPOINT")
        .unwrap_or_else(|_| "ws://localhost:8000/connection/websocket".to_owned());
    let path =
        std::env::var("FERRIFUGE_EXAMPLE_POSITION").unwrap_or_else(|_| "position.txt".to_owned());
    let client = Client::new(endpoint, Config::default())?;
    let mut builder = client.subscription_builder(CHANNEL).recoverable(true);
    let mut epoch = String::new();
    if let Some(position) = load(&path) {
        println!("resuming after offset {}", position.offset);
        epoch.clone_from(&position.epoch);
        builder = builder.since(position);
    }
    let subscription = builder.build()?;
    let mut events = subscription.events();
    client.connect();
    subscription.subscribe();

    while let Some(event) = events.next().await {
        match event {
            SubscriptionEvent::Subscribed(subscribed) => {
                println!(
                    "subscribed: recovering {}, recovered {}",
                    subscribed.was_recovering, subscribed.recovered
                );
                // Where the stream stood: the next run resumes from here
                // even if no publication comes in this one.
                if let Some(position) = subscribed.stream_position {
                    std::fs::write(&path, format!("{} {}", position.offset, position.epoch))?;
                    epoch = position.epoch;
                }
            }
            SubscriptionEvent::Publication(publication) => {
                println!("{}", String::from_utf8_lossy(&publication.data));
                if let Some(new) = publication.epoch {
                    epoch = new;
                }
                // Taken from the stream: saved only now, so a crash before
                // this point recovers it on the next run.
                if let Some(offset) = publication.offset {
                    std::fs::write(&path, format!("{offset} {epoch}"))?;
                }
            }
            SubscriptionEvent::Error(error) => eprintln!("error: {}", error.message),
            _ => {}
        }
    }
    Ok(())
}
