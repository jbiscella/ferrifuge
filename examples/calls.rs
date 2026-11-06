//! Calls over the client's connection: publish to `news`, then read its
//! history, presence and presence stats. RPC needs a handler on the server
//! (an RPC proxy), so it is left out here.

use ferrifuge::{Client, Config, HistoryOptions};

const CHANNEL: &str = "news";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("FERRIFUGE_EXAMPLE_ENDPOINT")
        .unwrap_or_else(|_| "ws://localhost:8000/connection/websocket".to_owned());
    let client = Client::new(endpoint, Config::default())?;
    // Calls made before the connection is up wait for it.
    client.connect();

    client
        .publish(CHANNEL, br#"{"text":"hello from calls"}"#.to_vec())
        .await?;
    let history = client
        .history(CHANNEL, HistoryOptions::default().limit(5).reverse(true))
        .await?;
    println!("last {} publications:", history.publications.len());
    for publication in &history.publications {
        println!("  {}", String::from_utf8_lossy(&publication.data));
    }
    let presence = client.presence(CHANNEL).await?;
    println!("{} clients subscribed", presence.clients.len());
    let stats = client.presence_stats(CHANNEL).await?;
    println!("{} clients, {} users", stats.num_clients, stats.num_users);
    Ok(())
}
