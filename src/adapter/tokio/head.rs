//! The upgrade response's HTTP head, read whole before tungstenite parses
//! it (Block 50b's finding).
//!
//! tungstenite's handshake counts its reads of the head and fails one taking
//! more than 64 reads that average under 128 bytes ("Attack attempt
//! detected", a guard meant for servers): a 101 response that reaches the
//! client in many small reads, from a network that fragments, or a server
//! writing tiny TLS records, would fail every handshake. [`HeadReader`]
//! reads the head itself, bounded, and hands it to tungstenite in one read.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The most a head may take before it is handed over as it is: tungstenite's
/// own bound, past which it fails the handshake itself.
pub(super) const HEAD_LIMIT: usize = 64 * 1024;

/// What a [`HeadReader`] does with a read.
enum Phase {
    /// Gathers bytes until the head ends, [`HEAD_LIMIT`] is reached or the
    /// stream ends.
    Gathering,
    /// Hands over what it gathered, from this position.
    Handing(usize),
    /// Reads through.
    Through,
}

/// A stream whose first reads gather the HTTP head of the server's answer
/// to the upgrade, then hand it over with whatever came after it in the
/// same reads, so that tungstenite reads the head in one read and passes
/// the frames after it to the connection (`from_partially_read`). Writes,
/// and reads once the head is handed over, go straight through.
pub(super) struct HeadReader<S> {
    inner: S,
    phase: Phase,
    gathered: Vec<u8>,
    /// Where the search for the end of the head resumes.
    scanned: usize,
}

impl<S> HeadReader<S> {
    pub(super) fn new(inner: S) -> Self {
        Self {
            inner,
            phase: Phase::Gathering,
            gathered: Vec::new(),
            scanned: 0,
        }
    }

    /// The stream underneath.
    pub(super) fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Whether the head has ended, searching only what was not searched
    /// before, from the 3 bytes before it, which may begin `\r\n\r\n`.
    fn head_ended(&mut self) -> bool {
        let from = self.scanned.saturating_sub(3);
        self.scanned = self.gathered.len();
        self.gathered[from..]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for HeadReader<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.phase {
                Phase::Through => return Pin::new(&mut this.inner).poll_read(cx, buf),
                Phase::Handing(from) => {
                    let rest = &this.gathered[from..];
                    let taken = rest.len().min(buf.remaining());
                    buf.put_slice(&rest[..taken]);
                    if from + taken == this.gathered.len() {
                        this.phase = Phase::Through;
                        this.gathered = Vec::new();
                    } else {
                        this.phase = Phase::Handing(from + taken);
                    }
                    return Poll::Ready(Ok(()));
                }
                Phase::Gathering => {
                    let mut chunk = [0; 4096];
                    let mut read = ReadBuf::new(&mut chunk);
                    ready!(Pin::new(&mut this.inner).poll_read(cx, &mut read))?;
                    let ended = read.filled().is_empty();
                    this.gathered.extend_from_slice(read.filled());
                    if ended || this.gathered.len() >= HEAD_LIMIT || this.head_ended() {
                        // At the end of the stream with nothing gathered,
                        // the end goes through.
                        this.phase = if this.gathered.is_empty() {
                            Phase::Through
                        } else {
                            Phase::Handing(0)
                        };
                    }
                }
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for HeadReader<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
