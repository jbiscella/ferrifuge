//! Test proxies for Block 23: an HTTP proxy (CONNECT tunnels and
//! absolute-form forwarding, optionally behind TLS, optionally answering
//! 407) and a SOCKS5 server. Each carries every connection to one server,
//! whatever destination the client asked for, so that an endpoint only the
//! proxy can reach (`centrifugo.invalid`) still works, and records what it
//! was asked.

use std::sync::{Arc, Mutex};

use base64::Engine as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// How a proxy carried a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carried {
    /// An HTTP CONNECT tunnel.
    Tunnel,
    /// A plain HTTP request in absolute form.
    Absolute,
    /// A SOCKS5 CONNECT.
    Socks,
}

/// What a proxy was asked, per connection.
#[derive(Debug, Clone)]
pub(crate) struct Carriage {
    pub(crate) carried: Carried,
    /// The destination asked for: `host:port`.
    pub(crate) destination: String,
    /// The credentials received, as `user:password`.
    pub(crate) credentials: Option<String>,
}

/// What a proxy does with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Carries it, when the credentials (if required) match.
    Carry,
    /// Answers every request with 407.
    Refuse,
}

pub(crate) struct TestProxy {
    port: u16,
    log: Arc<Mutex<Vec<Carriage>>>,
    server: JoinHandle<()>,
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestProxy {
    /// `host:port` of the proxy.
    pub(crate) fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// What it carried so far, in order.
    pub(crate) fn log(&self) -> Vec<Carriage> {
        self.log.lock().unwrap().clone()
    }

    /// An HTTP proxy in front of `target`, behind TLS with the test CA's
    /// certificate when `tls`, requiring `credentials` (`user:password`)
    /// when given.
    pub(crate) async fn http(
        target: String,
        tls: bool,
        credentials: Option<String>,
        mode: Mode,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let acceptor = tls.then(|| tokio_rustls::TlsAcceptor::from(super::tls::server_config()));
        let server = tokio::spawn({
            let log = Arc::clone(&log);
            async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        return;
                    };
                    let (target, credentials, log) =
                        (target.clone(), credentials.clone(), Arc::clone(&log));
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        match acceptor {
                            Some(acceptor) => {
                                if let Ok(stream) = acceptor.accept(tcp).await {
                                    http_proxy(stream, &target, credentials, mode, &log).await;
                                }
                            }
                            None => http_proxy(tcp, &target, credentials, mode, &log).await,
                        }
                    });
                }
            }
        });
        Self { port, log, server }
    }

    /// A SOCKS5 server in front of `target`, requiring `credentials`.
    pub(crate) async fn socks5(target: String, credentials: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let server = tokio::spawn({
            let log = Arc::clone(&log);
            async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        return;
                    };
                    let (target, credentials, log) =
                        (target.clone(), credentials.clone(), Arc::clone(&log));
                    tokio::spawn(async move {
                        let _ = socks5(tcp, &target, &credentials, &log).await;
                    });
                }
            }
        });
        Self { port, log, server }
    }
}

/// Reads an HTTP request head, up to its blank line.
async fn head(stream: &mut (impl AsyncRead + Unpin)) -> Option<Vec<u8>> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).await.ok()? == 0 || head.len() > 64 * 1024 {
            return None;
        }
        head.push(byte[0]);
    }
    Some(head)
}

/// The `user:password` of a `Proxy-Authorization: Basic` header.
fn basic_credentials(head: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if !name.trim().eq_ignore_ascii_case("proxy-authorization") {
            return None;
        }
        let encoded = value.trim().strip_prefix("Basic ")?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        String::from_utf8(decoded).ok()
    })
}

async fn http_proxy(
    mut client: impl AsyncRead + AsyncWrite + Unpin,
    target: &str,
    credentials: Option<String>,
    mode: Mode,
    log: &Mutex<Vec<Carriage>>,
) {
    let Some(head) = head(&mut client).await else {
        return;
    };
    let text = String::from_utf8_lossy(&head).into_owned();
    let request_line = text.lines().next().unwrap_or_default().to_owned();
    let mut parts = request_line.split_whitespace();
    let (method, uri) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    let received = basic_credentials(&text);
    let (carried, destination) = if method.eq_ignore_ascii_case("CONNECT") {
        (Carried::Tunnel, uri.to_owned())
    } else {
        let authority = uri
            .split_once("://")
            .map_or("", |(_, rest)| rest.split('/').next().unwrap_or_default());
        (Carried::Absolute, authority.to_owned())
    };
    log.lock().unwrap().push(Carriage {
        carried,
        destination,
        credentials: received.clone(),
    });
    if mode == Mode::Refuse || (credentials.is_some() && received != credentials) {
        let _ = client
            .write_all(
                b"HTTP/1.1 407 Proxy Authentication Required\r\n\
                  Proxy-Authenticate: Basic realm=\"test\"\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
        // A graceful close: one with unread data resets the connection,
        // which can reach the client before the 407 (seen on Windows).
        let _ = client.shutdown().await;
        let mut rest = [0u8; 1024];
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while client.read(&mut rest).await.is_ok_and(|read| read > 0) {}
        })
        .await;
        return;
    }
    let Ok(mut server) = TcpStream::connect(target).await else {
        return;
    };
    if carried == Carried::Tunnel {
        if client
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .await
            .is_err()
        {
            return;
        }
    } else if server.write_all(&head).await.is_err() {
        // The server takes the request in absolute form, as a proxy would
        // pass it on; later requests on the connection follow as they are.
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
}

async fn socks5(
    mut client: TcpStream,
    target: &str,
    (user, password): &(String, String),
    log: &Mutex<Vec<Carriage>>,
) -> std::io::Result<()> {
    // Greeting: version, methods; username and password (RFC 1929) only.
    let mut greeting = [0u8; 2];
    client.read_exact(&mut greeting).await?;
    let mut methods = vec![0u8; usize::from(greeting[1])];
    client.read_exact(&mut methods).await?;
    if !methods.contains(&2) {
        client.write_all(&[5, 0xff]).await?;
        return Ok(());
    }
    client.write_all(&[5, 2]).await?;
    let mut version = [0u8; 2];
    client.read_exact(&mut version).await?;
    let mut name = vec![0u8; usize::from(version[1])];
    client.read_exact(&mut name).await?;
    let mut length = [0u8; 1];
    client.read_exact(&mut length).await?;
    let mut pass = vec![0u8; usize::from(length[0])];
    client.read_exact(&mut pass).await?;
    let received = format!(
        "{}:{}",
        String::from_utf8_lossy(&name),
        String::from_utf8_lossy(&pass)
    );
    let accepted = name == user.as_bytes() && pass == password.as_bytes();
    client.write_all(&[1, u8::from(!accepted)]).await?;
    if !accepted {
        log.lock().unwrap().push(Carriage {
            carried: Carried::Socks,
            destination: String::new(),
            credentials: Some(received),
        });
        return Ok(());
    }
    // Request: version, CONNECT, reserved, address type.
    let mut request = [0u8; 4];
    client.read_exact(&mut request).await?;
    let host = match request[3] {
        1 => {
            let mut ip = [0u8; 4];
            client.read_exact(&mut ip).await?;
            std::net::Ipv4Addr::from(ip).to_string()
        }
        3 => {
            let mut length = [0u8; 1];
            client.read_exact(&mut length).await?;
            let mut domain = vec![0u8; usize::from(length[0])];
            client.read_exact(&mut domain).await?;
            String::from_utf8_lossy(&domain).into_owned()
        }
        _ => {
            let mut ip = [0u8; 16];
            client.read_exact(&mut ip).await?;
            format!("[{}]", std::net::Ipv6Addr::from(ip))
        }
    };
    let mut port = [0u8; 2];
    client.read_exact(&mut port).await?;
    log.lock().unwrap().push(Carriage {
        carried: Carried::Socks,
        destination: format!("{host}:{}", u16::from_be_bytes(port)),
        credentials: Some(received),
    });
    let mut server = TcpStream::connect(target).await?;
    client.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
    tokio::io::copy_bidirectional(&mut client, &mut server).await?;
    Ok(())
}
