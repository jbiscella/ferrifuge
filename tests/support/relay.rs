//! TCP relay in front of the test server that counts the connections it
//! accepts: one per transport the client opens, so steps can assert that no
//! further connection attempt was made. It can also stand in for a server
//! that is down, or one that accepts connections but never answers, and it
//! can stop reading what clients send. It counts the bytes it relays to
//! clients, e.g. to see that publications came as deltas (Block 19).

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
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
    /// Relays them with `TCP_NODELAY`, one `write` per byte in each
    /// direction, counting the writes (Block 50, D4).
    OneBytePerWrite,
}

/// When a held connection was accepted, and when its client closed it.
type Lifetime = (std::time::Instant, Option<std::time::Instant>);

pub(crate) struct CountingRelay {
    port: u16,
    accepted: Arc<AtomicUsize>,
    /// When each connection was accepted, in order.
    accepted_at: Arc<Mutex<Vec<std::time::Instant>>>,
    /// Bytes relayed from the target to clients so far.
    to_clients: Arc<AtomicUsize>,
    mode: Arc<AtomicU8>,
    server: JoinHandle<()>,
    /// Connections held open in [`Mode::Silent`]: what they send is read
    /// and dropped, so that the relay sees when the client closes them.
    held: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// When each connection held in [`Mode::Silent`] was accepted, and when
    /// its client closed it, if it has.
    silent: Arc<Mutex<Vec<Lifetime>>>,
    /// Gates of the forwarded connections, to stop them reading.
    gates: Arc<Mutex<Vec<Weak<Gate>>>>,
    /// Writes and bytes of the connections relayed in
    /// [`Mode::OneBytePerWrite`], both directions together.
    single: Arc<Singles>,
}

/// What the connections relayed one byte per write have written.
#[derive(Default)]
struct Singles {
    writes: AtomicUsize,
    bytes: AtomicUsize,
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

/// `socket` with send and receive buffers of `bytes`.
fn sized(socket: TcpSocket, bytes: u32) -> TcpSocket {
    socket
        .set_recv_buffer_size(bytes)
        .expect("a receive buffer size");
    socket
        .set_send_buffer_size(bytes)
        .expect("a send buffer size");
    socket
}

/// A connection to `target`, with buffers of `buffers` bytes if given.
async fn connect(target: &str, buffers: Option<u32>) -> std::io::Result<TcpStream> {
    let Some(bytes) = buffers else {
        return TcpStream::connect(target).await;
    };
    // Each resolved address in turn, as `TcpStream::connect` does: `localhost`
    // may resolve to `::1` first while the published port is IPv4 only.
    let mut last = std::io::Error::other("no address");
    for address in tokio::net::lookup_host(target).await? {
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        match sized(socket, bytes).connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last = error,
        }
    }
    Err(last)
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

/// Relays `inbound` to `outbound` both ways until either side ends, one
/// byte per `write`, with Nagle's algorithm off on both sockets, so that
/// every byte leaves in a write of its own (Block 50, D4).
async fn forward_one_byte_per_write(inbound: TcpStream, outbound: TcpStream, single: Arc<Singles>) {
    for stream in [&inbound, &outbound] {
        stream.set_nodelay(true).expect("TCP_NODELAY");
    }
    let (from_client, to_client) = inbound.into_split();
    let (from_server, to_server) = outbound.into_split();
    let relay = |mut from: tokio::net::tcp::OwnedReadHalf,
                 mut to: tokio::net::tcp::OwnedWriteHalf| {
        let single = Arc::clone(&single);
        async move {
            let mut buffer = vec![0; 64 << 10];
            loop {
                let read = from.read(&mut buffer).await?;
                if read == 0 {
                    return to.shutdown().await;
                }
                single.bytes.fetch_add(read, Ordering::SeqCst);
                for byte in &buffer[..read] {
                    to.write_all(std::slice::from_ref(byte)).await?;
                    single.writes.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    };
    let up: std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>> =
        Box::pin(relay(from_client, to_server));
    let down: std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>> =
        Box::pin(relay(from_server, to_client));
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
        Self::start_with(target, mode, None).await
    }

    /// Starts a forwarding relay to `target` whose sockets, both ways, have
    /// send and receive buffers of `bytes` instead of the system's,
    /// which grow to megabytes: what a client does not read then backs up
    /// to the server sooner, whatever the runner's settings.
    pub(crate) async fn start_with_buffers(target: String, bytes: u32) -> Self {
        Self::start_with(target, Mode::Forward, Some(bytes)).await
    }

    async fn start_with(target: String, mode: Mode, buffers: Option<u32>) -> Self {
        let listener = match buffers {
            None => TcpListener::bind("127.0.0.1:0").await.expect("bind relay"),
            // Accepted connections take the listener's buffer sizes.
            Some(bytes) => {
                let socket = sized(TcpSocket::new_v4().expect("a relay socket"), bytes);
                socket.bind(([127, 0, 0, 1], 0).into()).expect("bind relay");
                socket.listen(1024).expect("listen")
            }
        };
        let port = listener.local_addr().expect("relay address").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_at = Arc::new(Mutex::new(Vec::new()));
        let accepting = Arc::clone(&accepted_at);
        let to_clients = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicU8::new(mode as u8));
        let held = Arc::new(Mutex::new(Vec::new()));
        let silent = Arc::new(Mutex::new(Vec::new()));
        let gates = Arc::new(Mutex::new(Vec::new()));
        let single = Arc::new(Singles::default());
        let singles = Arc::clone(&single);
        let lifetimes = Arc::clone(&silent);
        let relayed = Arc::clone(&to_clients);
        let (counter, current, holding, gating) = (
            Arc::clone(&accepted),
            Arc::clone(&mode),
            Arc::clone(&held),
            Arc::clone(&gates),
        );
        let server = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                accepting.lock().unwrap().push(std::time::Instant::now());
                counter.fetch_add(1, Ordering::SeqCst);
                match current.load(Ordering::SeqCst) {
                    m if m == Mode::Silent as u8 => {
                        let index = {
                            let mut lifetimes = lifetimes.lock().unwrap();
                            lifetimes.push((std::time::Instant::now(), None));
                            lifetimes.len() - 1
                        };
                        let lifetimes = Arc::clone(&lifetimes);
                        holding.lock().unwrap().push(tokio::spawn(async move {
                            let mut inbound = inbound;
                            let mut buffer = [0; 4096];
                            while matches!(inbound.read(&mut buffer).await, Ok(n) if n > 0) {}
                            lifetimes.lock().unwrap()[index].1 = Some(std::time::Instant::now());
                        }));
                    }
                    m if m == Mode::Down as u8 => drop(inbound),
                    m if m == Mode::OneBytePerWrite as u8 => {
                        let target = target.clone();
                        let single = Arc::clone(&singles);
                        tokio::spawn(async move {
                            if let Ok(outbound) = TcpStream::connect(&target).await {
                                forward_one_byte_per_write(inbound, outbound, single).await;
                            }
                        });
                    }
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
                            if let Ok(outbound) = connect(&target, buffers).await {
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
            accepted_at,
            to_clients,
            mode,
            server,
            held,
            silent,
            gates,
            single,
        }
    }

    /// The writes and the bytes of the connections relayed in
    /// [`Mode::OneBytePerWrite`], both directions together.
    pub(crate) fn single_byte_writes(&self) -> (usize, usize) {
        (
            self.single.writes.load(Ordering::SeqCst),
            self.single.bytes.load(Ordering::SeqCst),
        )
    }

    /// How long each connection held in [`Mode::Silent`] stayed open until
    /// its client closed it, in order; `None` while it is open.
    pub(crate) fn silent_lifetimes(&self) -> Vec<Option<std::time::Duration>> {
        self.silent
            .lock()
            .unwrap()
            .iter()
            .map(|(accepted, closed)| closed.map(|closed| closed.duration_since(*accepted)))
            .collect()
    }

    /// When the first connection was accepted, if one was.
    pub(crate) fn first_accepted_at(&self) -> Option<std::time::Instant> {
        self.accepted_at.lock().unwrap().first().copied()
    }

    /// When the first connection held in [`Mode::Silent`] was closed by its
    /// client, if it was.
    pub(crate) fn first_silent_closed_at(&self) -> Option<std::time::Instant> {
        self.silent
            .lock()
            .unwrap()
            .first()
            .and_then(|(_, closed)| *closed)
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
        for held in self.held.lock().unwrap().drain(..) {
            held.abort();
        }
    }
}
