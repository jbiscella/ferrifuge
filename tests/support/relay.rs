//! TCP relay in front of the test server that counts the connections it
//! accepts: one per transport the client opens, so steps can assert that no
//! further connection attempt was made.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub(crate) struct CountingRelay {
    port: u16,
    accepted: Arc<AtomicUsize>,
    server: JoinHandle<()>,
}

impl CountingRelay {
    /// Starts relaying a local port to `target` (`host:port`).
    pub(crate) async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind relay");
        let port = listener.local_addr().expect("relay address").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                let target = target.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Self {
            port,
            accepted,
            server,
        }
    }

    /// Websocket endpoint of the server, through the relay.
    pub(crate) fn websocket_url(&self) -> String {
        format!("ws://127.0.0.1:{}/connection/websocket", self.port)
    }

    /// Connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for CountingRelay {
    fn drop(&mut self) {
        self.server.abort();
    }
}
