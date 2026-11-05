//! TCP relay in front of the test server that counts the connections it
//! accepts: one per transport the client opens, so steps can assert that no
//! further connection attempt was made. It can also stand in for a server
//! that is down, or one that accepts connections but never answers.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// How the relay treats the connections it accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Mode {
    /// Relays them to the target.
    Forward,
    /// Holds them open without relaying or answering anything.
    Silent,
    /// Closes them at once, like a server that is down.
    Down,
}

pub(crate) struct CountingRelay {
    port: u16,
    accepted: Arc<AtomicUsize>,
    mode: Arc<AtomicU8>,
    server: JoinHandle<()>,
    /// Connections held open in [`Mode::Silent`].
    held: Arc<Mutex<Vec<TcpStream>>>,
}

impl CountingRelay {
    /// Starts relaying a local port to `target` (`host:port`).
    pub(crate) async fn start(target: String) -> Self {
        Self::start_in(target, Mode::Forward).await
    }

    /// Starts a relay to `target` treating connections as `mode` says.
    pub(crate) async fn start_in(target: String, mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind relay");
        let port = listener.local_addr().expect("relay address").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicU8::new(mode as u8));
        let held = Arc::new(Mutex::new(Vec::new()));
        let (counter, current, holding) =
            (Arc::clone(&accepted), Arc::clone(&mode), Arc::clone(&held));
        let server = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                match current.load(Ordering::SeqCst) {
                    m if m == Mode::Silent as u8 => holding.lock().unwrap().push(inbound),
                    m if m == Mode::Down as u8 => drop(inbound),
                    _ => {
                        let target = target.clone();
                        tokio::spawn(async move {
                            if let Ok(mut outbound) = TcpStream::connect(&target).await {
                                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
                                    .await;
                            }
                        });
                    }
                }
            }
        });
        Self {
            port,
            accepted,
            mode,
            server,
            held,
        }
    }

    /// Changes how later connections are treated.
    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.store(mode as u8, Ordering::SeqCst);
    }

    /// `host:port` of the relay.
    pub(crate) fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Websocket endpoint of the server, through the relay.
    pub(crate) fn websocket_url(&self) -> String {
        format!("ws://{}/connection/websocket", self.address())
    }

    /// `http_stream` endpoint of the server, through the relay.
    pub(crate) fn http_stream_url(&self) -> String {
        format!("http://{}/connection/http_stream", self.address())
    }

    /// sse endpoint of the server, through the relay.
    pub(crate) fn sse_url(&self) -> String {
        format!("http://{}/connection/sse", self.address())
    }

    /// Connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for CountingRelay {
    fn drop(&mut self) {
        self.server.abort();
        self.held.lock().unwrap().clear();
    }
}
