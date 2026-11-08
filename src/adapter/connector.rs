//! An application's connector: the stream the websocket runs over, from
//! the application's own networking (Block 37b, D3, D4).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

/// The error a [`Connector`] fails with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The future a [`Connector`] returns.
pub type Connecting = Pin<Box<dyn Future<Output = Result<ConnectedStream, BoxError>> + Send>>;

/// Gives the client the connected byte stream its websocket runs over,
/// for networking it does not have: a VPN tunnel, a custom proxy, an
/// in-process transport for tests. Set with
/// [`ClientBuilder::connector`](crate::ClientBuilder::connector).
///
/// The client calls it once per connection attempt with the websocket
/// endpoint, its port always written (80 for `ws`, 443 for `wss`), then runs
/// TLS, for `wss://` with its own TLS settings and the endpoint's host, and
/// the websocket upgrade over the stream. The handshake timeout
/// ([`Config::handshake_timeout`](crate::Config)) bounds the future, which
/// is dropped at it: it must be cancel-safe. An error, or a panic, fails
/// the attempt as a refused connection does.
///
/// A closure from the endpoint to a future of any stream implements it:
///
/// ```no_run
/// use ferrifuge::Client;
///
/// let client = Client::builder("ws://centrifugo.example/connection/websocket")
///     .connector(|endpoint: ferrifuge::http::Uri| async move {
///         let host = endpoint.host().unwrap_or_default().to_owned();
///         let port = endpoint.port_u16().unwrap_or(80);
///         tokio::net::TcpStream::connect((host, port)).await
///     })
///     .build();
/// ```
pub trait Connector: Send + Sync {
    /// Opens the stream to `endpoint`.
    fn connect(&self, endpoint: http::Uri) -> Connecting;
}

impl<F, Fut, S, E> Connector for F
where
    F: Fn(http::Uri) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<S, E>> + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    E: Into<BoxError>,
{
    fn connect(&self, endpoint: http::Uri) -> Connecting {
        let connecting = self(endpoint);
        Box::pin(async move {
            connecting
                .await
                .map(ConnectedStream::new)
                .map_err(Into::into)
        })
    }
}

/// One connector for several clients.
impl<C: Connector + ?Sized> Connector for Arc<C> {
    fn connect(&self, endpoint: http::Uri) -> Connecting {
        (**self).connect(endpoint)
    }
}

/// A connector shared between clients.
#[derive(Clone)]
pub(super) struct Shared(pub(super) Arc<dyn Connector>);

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Connector")
    }
}

/// A byte stream a [`Connector`] opened.
pub struct ConnectedStream(pub(super) Box<dyn Io>);

/// What the websocket needs of a stream.
pub(super) trait Io: AsyncRead + AsyncWrite + Send + Unpin {}

impl<S: AsyncRead + AsyncWrite + Send + Unpin> Io for S {}

impl ConnectedStream {
    /// Wraps `stream`.
    pub fn new<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(stream: S) -> Self {
        Self(Box::new(stream))
    }
}

impl fmt::Debug for ConnectedStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectedStream")
    }
}

/// The endpoint given to a connector: `url` with its port always written,
/// path and query kept (Block 37, D3; as Block 23 D1 gives
/// `Proxy::custom`).
pub(super) fn endpoint(url: &reqwest::Url) -> Result<http::Uri, String> {
    let host = url.host_str().ok_or("an endpoint without a host")?;
    let port = url
        .port_or_known_default()
        .ok_or("an endpoint without a port")?;
    let query = url
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    format!("{}://{host}:{port}{}{query}", url.scheme(), url.path())
        .parse()
        .map_err(|_| "an endpoint that does not parse".to_owned())
}

/// Opens the stream through `connector`, failing as a refused connection
/// does on its error or its panic (Block 37, D4).
pub(super) async fn connect(
    connector: &Shared,
    url: &reqwest::Url,
) -> Result<ConnectedStream, String> {
    use futures_util::FutureExt as _;
    let endpoint = endpoint(url)?;
    let connector = Arc::clone(&connector.0);
    let Ok(connecting) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| connector.connect(endpoint)))
    else {
        return Err("connector panicked".to_owned());
    };
    match std::panic::AssertUnwindSafe(connecting)
        .catch_unwind()
        .await
    {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(format!("connector: {error}")),
        Err(_) => Err("connector panicked".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_endpoint_carries_its_port() {
        for (url, expected) in [
            (
                "ws://h/connection/websocket",
                "ws://h:80/connection/websocket",
            ),
            (
                "wss://h/connection/websocket",
                "wss://h:443/connection/websocket",
            ),
            ("ws://h:8000/c?x=1", "ws://h:8000/c?x=1"),
            ("wss://[::1]/c", "wss://[::1]:443/c"),
        ] {
            let url = reqwest::Url::parse(url).unwrap();
            assert_eq!(endpoint(&url).unwrap().to_string(), expected);
        }
    }
}
