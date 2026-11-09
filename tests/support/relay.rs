//! TCP relay in front of the test server that counts the connections it
//! accepts: one per transport the client opens, so steps can assert that no
//! further connection attempt was made. It can also stand in for a server
//! that is down, or one that accepts connections but never answers, and it
//! can stop reading what clients send. It counts the bytes it relays to
//! clients, e.g. to see that publications came as deltas (Block 19).

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
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
    /// Keeps new ones waiting, then relays them once the mode changes, so
    /// a step can act while a client reconnects, however slow the runner.
    Hold,
}

pub(crate) struct CountingRelay {
    port: u16,
    accepted: Arc<AtomicUsize>,
    /// Bytes relayed from the target to clients so far.
    to_clients: Arc<AtomicUsize>,
    mode: Arc<AtomicU8>,
    server: JoinHandle<()>,
    /// Connections held open in [`Mode::Silent`].
    held: Arc<Mutex<Vec<TcpStream>>>,
    /// Gates of the forwarded connections, to stop them reading.
    gates: Arc<Mutex<Vec<Weak<Gate>>>>,
}

/// Stops the relay reading from clients, so that what they write stays in
/// their own socket and the relay's receive window, whatever the system
/// buffers further on.
#[derive(Default)]
struct Gate {
    closed: AtomicBool,
    opened: Notify,
}

impl Gate {
    async fn wait_open(&self) {
        loop {
            let opened = self.opened.notified();
            if !self.closed.load(Ordering::SeqCst) {
                return;
            }
            opened.await;
        }
    }
}

/// Relays `inbound` to `outbound` both ways until either side ends, reading
/// from `inbound` only while `gate` is open.
async fn forward(
    inbound: TcpStream,
    outbound: TcpStream,
    gate: Arc<Gate>,
    to_clients: Arc<AtomicUsize>,
) {
    let (mut from_client, mut to_client) = inbound.into_split();
    let (mut from_server, mut to_server) = outbound.into_split();
    let up = async {
        let mut buffer = vec![0; 64 << 10];
        loop {
            gate.wait_open().await;
            let read = from_client.read(&mut buffer).await?;
            if read == 0 {
                return to_server.shutdown().await;
            }
            to_server.write_all(&buffer[..read]).await?;
        }
    };
    let down = async {
        let mut buffer = vec![0; 64 << 10];
        loop {
            let read = from_server.read(&mut buffer).await?;
            if read == 0 {
                return to_client.shutdown().await;
            }
            to_client.write_all(&buffer[..read]).await?;
            to_clients.fetch_add(read, Ordering::SeqCst);
        }
    };
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
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
        let to_clients = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicU8::new(mode as u8));
        let held = Arc::new(Mutex::new(Vec::new()));
        let gates = Arc::new(Mutex::new(Vec::new()));
        let relayed = Arc::clone(&to_clients);
        let (counter, current, holding, gating) = (
            Arc::clone(&accepted),
            Arc::clone(&mode),
            Arc::clone(&held),
            Arc::clone(&gates),
        );
        let server = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                match current.load(Ordering::SeqCst) {
                    m if m == Mode::Silent as u8 => holding.lock().unwrap().push(inbound),
                    m if m == Mode::Down as u8 => drop(inbound),
                    _ => {
                        let gate = Arc::new(Gate::default());
                        gating.lock().unwrap().push(Arc::downgrade(&gate));
                        let target = target.clone();
                        let relayed = Arc::clone(&relayed);
                        let waiting = Arc::clone(&current);
                        tokio::spawn(async move {
                            while waiting.load(Ordering::SeqCst) == Mode::Hold as u8 {
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            }
                            if let Ok(outbound) = TcpStream::connect(&target).await {
                                forward(inbound, outbound, gate, relayed).await;
                            }
                        });
                    }
                }
            }
        });
        Self {
            port,
            accepted,
            to_clients,
            mode,
            server,
            held,
            gates,
        }
    }

    /// Bytes relayed from the target to clients so far.
    pub(crate) fn bytes_to_clients(&self) -> usize {
        self.to_clients.load(Ordering::SeqCst)
    }

    /// Stops reading what clients send on the connections open now; later
    /// ones are read as usual.
    pub(crate) fn stop_reading(&self) {
        for gate in self.gates.lock().unwrap().iter().filter_map(Weak::upgrade) {
            gate.closed.store(true, Ordering::SeqCst);
        }
    }

    /// Changes how later connections are treated.
    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.store(mode as u8, Ordering::SeqCst);
    }

    /// Whether later connections are treated as `mode` says.
    pub(crate) fn is(&self, mode: Mode) -> bool {
        self.mode.load(Ordering::SeqCst) == mode as u8
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
