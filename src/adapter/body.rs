//! What the HTTP-streaming transports of both adapters share: the decoders
//! of a response body, the captive-portal check of an open, the emulation
//! policy, and the transport failures.
// Block 44, D8; Block 27; Block 25, D8.

use std::collections::VecDeque;
#[cfg(adapter_tokio)]
use std::num::NonZeroUsize;

use bytes::BytesMut;
use mediatype::{MediaType, Name, names};
use tokio_util::codec::{AnyDelimiterCodec, Decoder as _};

use super::backend::Inbound;
use crate::sans_io::FrameId;

/// What a decoder keeps between messages once a large reply, or a large
/// chunk, grew its buffer. It releases the rest only when it holds no more
/// than this, so a stream of messages below it never pays for a release.
// Block 49d, row 71.
const KEPT: usize = 64 * 1024;

/// Why a transport failed to open, read or write: of what kind, and today's
/// message, which `Display` writes. Code branches on `kind`, never on
/// `message`.
// Block 61, D5.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(in crate::adapter) struct TransportFailure {
    pub(in crate::adapter) kind: FailureKind,
    pub(in crate::adapter) message: String,
}

/// The kinds of [`TransportFailure`].
// Block 61, D5.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(
    adapter_browser,
    allow(
        dead_code,
        reason = "proxies, I/O and TLS fail the native transports only"
    )
)]
pub(in crate::adapter) enum FailureKind {
    /// A message from the server over the size limit, reported as
    /// `Inbound::TooLarge`.
    TooLarge,
    /// An HTTP status the transport cannot go on with.
    Http(u16),
    /// An HTTP status that a captive portal's page may have answered.
    CaptivePortal(u16),
    /// A proxy refused the credentials, or asked for some.
    ProxyAuthentication,
    Timeout,
    Io(std::io::ErrorKind),
    Tls,
    Other,
}

impl TransportFailure {
    pub(in crate::adapter) fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// A failure of no kind the transport tells apart.
    pub(in crate::adapter) fn other(message: impl Into<String>) -> Self {
        Self::new(FailureKind::Other, message)
    }

    /// An I/O failure, of its kind, with its message.
    #[cfg(adapter_tokio)]
    pub(in crate::adapter) fn io(error: &std::io::Error) -> Self {
        Self::new(FailureKind::Io(error.kind()), error.to_string())
    }

    /// A message from the server over the size limit.
    pub(in crate::adapter) fn too_large() -> Self {
        Self::new(FailureKind::TooLarge, "message larger than the size limit")
    }

    /// An open answered with HTTP `status`, its message `message`, saying
    /// that a captive portal may be answering when `portal`.
    pub(in crate::adapter) fn status(status: u16, message: String, portal: bool) -> Self {
        let kind = if portal {
            FailureKind::CaptivePortal(status)
        } else {
            FailureKind::Http(status)
        };
        Self::new(kind, with_portal(message, portal))
    }
}

impl std::fmt::Display for TransportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// What an error message about an open ends with when a captive portal may
/// have answered it.
// Block 25, D8.
pub(in crate::adapter) const CAPTIVE_PORTAL: &str = "; a captive portal may be answering";

/// Whether an open answered with `status` and `content_type`, instead of
/// the transport's, looks like a captive portal's web page: a redirect, 511
/// "Network Authentication Required", or a page served as a success.
// Block 25, D8.
pub(in crate::adapter) fn captive_portal(status: u16, content_type: &str) -> bool {
    (300..400).contains(&status)
        || status == 511
        || ((200..300).contains(&status) && is_media_type(content_type, names::TEXT, names::HTML))
}

/// `message`, saying that a captive portal may be answering when `portal`.
pub(in crate::adapter) fn with_portal(message: String, portal: bool) -> String {
    if portal {
        message + CAPTIVE_PORTAL
    } else {
        message
    }
}

/// Path of the emulation endpoint (centrifuge-js `emulationEndpoint`).
pub(in crate::adapter) const EMULATION_PATH: &str = "/emulation";

/// How the server took an emulation request it answered.
pub(in crate::adapter) enum Sent {
    Accepted,
    /// Rejected with HTTP 413, with the frame it carried.
    Rejected(Vec<u8>),
}

/// What an answer to an emulation request means for the transport.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::adapter) enum Emulated {
    /// The server took the request's commands.
    Accepted,
    /// The server refused this request only: its commands fail.
    Rejected,
    /// The transport cannot go on.
    Failed,
}

/// What an answer of HTTP `status`, `redirected` or not, to an emulation
/// request means. A server without the session's node (404) or failing
/// (5xx) cannot answer the command; other statuses reject only it
/// (centrifuge-js `send`). A redirect is not followed, so its command
/// never arrived either. A proxy's 407 is the native transport's, before
/// this.
// Block 8; Block 61, D7.
pub(in crate::adapter) fn classify_emulation(status: u16, redirected: bool) -> Emulated {
    if redirected || status == 404 || (500..600).contains(&status) || (300..400).contains(&status) {
        Emulated::Failed
    } else if status == 413 {
        // Its commands fail at once (owner decision, 2026-11-04).
        Emulated::Rejected
    } else {
        Emulated::Accepted
    }
}

/// What the transport reports of request `id`: a failure ends it; a 413
/// fails its commands, then counts as written, in `written`.
// Block 8.
pub(in crate::adapter) fn report(
    id: FrameId,
    answered: Result<Sent, TransportFailure>,
    written: &mut VecDeque<FrameId>,
) -> Inbound {
    match answered {
        Err(failure) => Inbound::Closed {
            code: None,
            reason: failure.message,
        },
        Ok(Sent::Rejected(frame)) => {
            written.push_back(id);
            Inbound::Rejected(frame)
        }
        Ok(Sent::Accepted) => Inbound::Written(id),
    }
}

/// The URL operations [`emulation_url`] needs, of each target's parser.
// Block 61, D7.
pub(in crate::adapter) trait EmulationUrl: Sized {
    /// `url` parsed, or the failure the parser's message says.
    fn parse(url: &str) -> Result<Self, TransportFailure>;
    fn set_path(&mut self, path: &str);
    fn clear_query_and_fragment(&mut self);
}

#[cfg(adapter_tokio)]
impl EmulationUrl for reqwest::Url {
    fn parse(url: &str) -> Result<Self, TransportFailure> {
        Self::parse(url).map_err(|e| TransportFailure::other(e.to_string()))
    }

    fn set_path(&mut self, path: &str) {
        Self::set_path(self, path);
    }

    fn clear_query_and_fragment(&mut self) {
        self.set_query(None);
        self.set_fragment(None);
    }
}

#[cfg(adapter_browser)]
impl EmulationUrl for web_sys::Url {
    fn parse(url: &str) -> Result<Self, TransportFailure> {
        Self::new(url)
            .map_err(|error| TransportFailure::other(super::backend::websocket::js_message(&error)))
    }

    fn set_path(&mut self, path: &str) {
        self.set_pathname(path);
    }

    fn clear_query_and_fragment(&mut self) {
        self.set_search("");
        self.set_hash("");
    }
}

/// Where emulation requests go: `configured`, absolute, or `/emulation` on
/// the server of `stream` (centrifuge-js `emulationEndpoint`).
pub(in crate::adapter) fn emulation_url<U: EmulationUrl>(
    stream: &str,
    configured: Option<&str>,
) -> Result<U, TransportFailure> {
    if let Some(endpoint) = configured {
        return U::parse(endpoint);
    }
    let mut url = U::parse(stream)?;
    url.set_path(EMULATION_PATH);
    url.clear_query_and_fragment();
    Ok(url)
}

/// Whether `content_type` parses as a media type of type `ty` and subtype
/// `subty`, whatever their case and parameters (mediatype, RFC 9110 section
/// 8.3); a value that does not parse is neither.
// Block 27, D4.
pub(in crate::adapter) fn is_media_type(content_type: &str, ty: Name<'_>, subty: Name<'_>) -> bool {
    MediaType::parse(content_type.trim()).is_ok_and(|media_type| {
        // `text/event-stream+json` is another media type (Codex, #127).
        media_type.ty == ty && media_type.subty == subty && media_type.suffix.is_none()
    })
}

/// Splits a response body into the messages it carries: one JSON reply per
/// line for `http_stream`, the data of each event for sse (natively only:
/// in the browser, `EventSource` splits its own). Network chunks do not
/// follow message boundaries, so a partial line waits for the next. Both
/// are maintained components, bounded by the largest message; their first
/// error ends the transport, as they are not asked to resynchronise.
// Block 44 D9; Block 27, D1, D2.
pub(in crate::adapter) enum Decoder {
    /// sse-core: events with data, of the default type, reach the client
    /// (centrifuge-js `onmessage`, WHATWG "dispatch the event").
    #[cfg(adapter_tokio)]
    Sse(Sse),
    /// tokio-util's lines, bounded, and the bytes not yet split.
    Lines(AnyDelimiterCodec, BytesMut),
    /// Protobuf replies, each after its uvarint length: the bytes not yet
    /// split, and the largest reply. In-house, as no maintained component
    /// splits uvarint-delimited streams.
    // Block 27, D3; Block 0.
    Varint(Vec<u8>, usize),
}

/// sse-core's decoder, the limit it was made with and, for tests, the
/// capacity it keeps for the next event's data: that of the last event's,
/// as its `dispatch_message` hands the data over and allocates as much
/// again.
#[cfg(adapter_tokio)]
pub(in crate::adapter) struct Sse {
    decoder: sse_core::SseDecoder,
    limit: NonZeroUsize,
    #[cfg(any(test, feature = "__test-hooks"))]
    kept: usize,
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            #[cfg(adapter_tokio)]
            Self::Sse(_) => "Decoder::Sse",
            Self::Lines(..) => "Decoder::Lines",
            Self::Varint(..) => "Decoder::Varint",
        })
    }
}

impl Decoder {
    #[cfg(adapter_tokio)]
    pub(in crate::adapter) fn new(sse: bool, limit: usize) -> Self {
        if sse {
            let limit = NonZeroUsize::new(limit).unwrap_or(NonZeroUsize::MIN);
            Self::Sse(Sse {
                decoder: sse_core::SseDecoder::with_limit(limit),
                limit,
                #[cfg(any(test, feature = "__test-hooks"))]
                kept: 0,
            })
        } else {
            Self::lines(limit)
        }
    }

    /// The splitter of a JSON `http_stream`, whose lines are at most
    /// `limit` bytes each.
    pub(in crate::adapter) fn lines(limit: usize) -> Self {
        Self::Lines(
            AnyDelimiterCodec::new_with_max_length(b"\n".to_vec(), b"\n".to_vec(), limit),
            BytesMut::new(),
        )
    }

    /// The splitter of a Protobuf `http_stream`, whose replies are at most
    /// `limit` bytes each.
    pub(in crate::adapter) fn protobuf(limit: usize) -> Self {
        Self::Varint(Vec::new(), limit)
    }

    /// Adds `chunk`, appending each message it completes to `messages`.
    ///
    /// # Errors
    ///
    /// When a message grows past the limit; the messages completed before
    /// it are appended still.
    pub(in crate::adapter) fn push(
        &mut self,
        chunk: &[u8],
        messages: &mut VecDeque<Vec<u8>>,
    ) -> Result<(), TransportFailure> {
        match self {
            #[cfg(adapter_tokio)]
            Self::Sse(sse) => {
                let mut rest = chunk;
                while let Some(event) = sse.decoder.next(&mut rest) {
                    match event {
                        Ok(sse_core::SseEvent::Message(message)) => {
                            #[cfg(any(test, feature = "__test-hooks"))]
                            {
                                sse.kept = message.data.capacity();
                            }
                            // sse-core allocates as much again for the next
                            // event's data (`Vec::with_capacity`): a decoder
                            // of its own, at this event boundary, starts
                            // empty (row 71).
                            if message.data.capacity() > KEPT {
                                released();
                                sse.decoder = fresh_sse(sse.limit);
                                #[cfg(any(test, feature = "__test-hooks"))]
                                {
                                    sse.kept = 0;
                                }
                            }
                            let default = message.event.is_empty() || message.event == "message";
                            if default && !message.data.is_empty() {
                                let mut data = message.data.into_bytes();
                                // At its size, as lines and Protobuf replies
                                // are: the buffer it came in may be as large
                                // as an earlier event.
                                if data.capacity() > 2 * data.len() {
                                    data.shrink_to_fit();
                                }
                                messages.push_back(data);
                            }
                        }
                        // The client keeps its own reconnect delays.
                        Ok(sse_core::SseEvent::Retry(_)) => {}
                        Err(_) => return Err(TransportFailure::too_large()),
                    }
                }
            }
            Self::Lines(codec, buffer) => {
                buffer.extend_from_slice(chunk);
                let held = buffer.len();
                loop {
                    match codec.decode(buffer) {
                        Ok(Some(line)) => {
                            let line = line.strip_suffix(b"\r").unwrap_or(&line);
                            // Blank lines carry nothing, e.g. keep-alives of
                            // an intermediary (centrifuge-js `decodeReplies`).
                            if !line.iter().all(u8::is_ascii_whitespace) {
                                messages.push_back(line.to_vec());
                            }
                        }
                        Ok(None) => break,
                        Err(_) => return Err(TransportFailure::too_large()),
                    }
                }
                // The lines split off share the allocation, which `reserve`
                // later reclaims whole: the bytes not yet split move to a
                // buffer of their own (row 71).
                if buffer.len() <= KEPT && (held > KEPT || buffer.capacity() > KEPT) {
                    released();
                    let mut kept = BytesMut::with_capacity(KEPT);
                    kept.extend_from_slice(buffer);
                    *buffer = kept;
                }
            }
            Self::Varint(buffer, limit) => {
                buffer.extend_from_slice(chunk);
                let mut start = 0;
                while let Some(end) = next_reply(&buffer[start..], *limit)? {
                    // With its prefix: a one-reply frame for the codec.
                    messages.push_back(buffer[start..start + end].to_vec());
                    start += end;
                }
                buffer.drain(..start);
                if buffer.len() <= KEPT && buffer.capacity() > KEPT {
                    released();
                    buffer.shrink_to(KEPT);
                }
            }
        }
        Ok(())
    }

    /// The capacity the buffer of the decoder keeps between messages.
    #[cfg(feature = "__test-hooks")]
    fn capacity(&self) -> usize {
        match self {
            #[cfg(adapter_tokio)]
            Self::Sse(sse) => sse.kept,
            Self::Lines(_, buffer) => buffer.capacity(),
            Self::Varint(buffer, _) => buffer.capacity(),
        }
    }
}

/// The formats of a stream body, for tests only.
// Block 50, D3.
#[cfg(feature = "__test-hooks")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamFormat {
    /// `http_stream` with the JSON codec: one reply per line.
    HttpStreamJson,
    /// `sse` with the JSON codec: the data of each event. Native only.
    #[cfg(adapter_tokio)]
    SseJson,
    /// `http_stream` with the Protobuf codec: each reply after its uvarint
    /// length.
    HttpStreamProtobuf,
}

/// Feeds `chunks` in turn to a new decoder of `format` at the default size
/// limit, returning the messages it yielded, as the transport takes them: a
/// Protobuf reply with its length prefix.
///
/// # Errors
///
/// The decoder's first error, as the transport would end with it.
// Block 50, D3.
#[cfg(feature = "__test-hooks")]
#[doc(hidden)]
pub fn decode_stream<C: AsRef<[u8]>>(
    format: StreamFormat,
    chunks: impl IntoIterator<Item = C>,
) -> Result<Vec<Vec<u8>>, String> {
    let limit = crate::Config::default().max_message_bytes;
    let mut decoder = match format {
        StreamFormat::HttpStreamJson => Decoder::lines(limit),
        #[cfg(adapter_tokio)]
        StreamFormat::SseJson => Decoder::new(true, limit),
        StreamFormat::HttpStreamProtobuf => Decoder::protobuf(limit),
    };
    let mut messages = VecDeque::new();
    for chunk in chunks {
        decoder
            .push(chunk.as_ref(), &mut messages)
            .map_err(|failure| failure.message)?;
    }
    Ok(messages.into())
}

/// A stream decoder of the HTTP-streaming and WebTransport transports, for
/// tests only: the decoders of both adapters are these.
// Block 49d, row 71.
#[cfg(feature = "__test-hooks")]
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamDecoder {
    name: &'static str,
    decoder: Decoder,
    messages: VecDeque<Vec<u8>>,
}

#[cfg(feature = "__test-hooks")]
impl StreamDecoder {
    /// Every kind of stream decoder, each splitting messages of at most
    /// `limit` bytes: JSON lines, Protobuf replies after their uvarint
    /// length and, natively, sse events.
    #[must_use]
    pub fn every_kind(limit: usize) -> Vec<Self> {
        let decoders = [
            ("JSON lines", Decoder::lines(limit)),
            ("Protobuf", Decoder::protobuf(limit)),
            #[cfg(adapter_tokio)]
            ("sse", Decoder::new(true, limit)),
        ];
        decoders
            .into_iter()
            .map(|(name, decoder)| Self {
                name,
                decoder,
                messages: VecDeque::new(),
            })
            .collect()
    }

    /// Which kind of stream decoder this is.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// `message` as this kind of stream frames it: a line, after its
    /// uvarint length, or the data of an event.
    #[must_use]
    pub fn frame(&self, message: &[u8]) -> Vec<u8> {
        match &self.decoder {
            #[cfg(adapter_tokio)]
            Decoder::Sse(_) => [b"data: ", message, b"\n\n"].concat(),
            Decoder::Lines(..) => [message, b"\n"].concat(),
            Decoder::Varint(..) => {
                let mut framed = Vec::new();
                prost::encoding::encode_varint(message.len() as u64, &mut framed);
                framed.extend_from_slice(message);
                framed
            }
        }
    }

    /// Adds `chunk`, returning the messages it completed.
    ///
    /// # Errors
    ///
    /// As the transports' decoder: a message over the limit.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.decoder
            .push(chunk, &mut self.messages)
            .map_err(|failure| failure.message)?;
        Ok(self.messages.drain(..).collect())
    }

    /// The capacity its buffer keeps between messages: the bytes not yet
    /// split, or for sse, sse-core's buffer of the next event's data.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.decoder.capacity()
    }
}

/// A new sse-core decoder for a stream already past its start: fed one
/// blank line, which dispatches nothing, it leaves its byte-order-mark
/// state, so that a U+FEFF after the boundary stays part of a field name
/// as mid-stream.
// Row 71.
#[cfg(adapter_tokio)]
fn fresh_sse(limit: NonZeroUsize) -> sse_core::SseDecoder {
    let mut decoder = sse_core::SseDecoder::with_limit(limit);
    let mut blank: &[u8] = b"\n";
    let dispatched = decoder.next(&mut blank);
    debug_assert!(dispatched.is_none() && blank.is_empty());
    decoder
}

#[cfg(test)]
thread_local! {
    /// The buffers released on this thread, for the unit tests.
    // Row 71.
    static RELEASED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Counts a release of a decoder's buffer, for the unit tests.
fn released() {
    #[cfg(test)]
    RELEASED.with(|released| released.set(released.get() + 1));
}

/// Where the first reply of `bytes`, a Protobuf stream, ends, its uvarint
/// length prefix included; `None` while more bytes are needed. A zero
/// length is an empty reply, the server's ping.
///
/// # Errors
///
/// When the prefix is not a uvarint, or announces more than `limit` bytes.
// Block 27, D3.
fn next_reply(bytes: &[u8], limit: usize) -> Result<Option<usize>, TransportFailure> {
    // At most 10 bytes; all but the last have the continuation bit.
    let Some(last) = bytes.iter().take(10).position(|byte| byte & 0x80 == 0) else {
        return if bytes.len() < 10 {
            Ok(None)
        } else {
            Err(TransportFailure::other(
                "a reply length that is not a varint",
            ))
        };
    };
    let prefix = &bytes[..=last];
    let length = prost::decode_length_delimiter(prefix)
        .map_err(|_| TransportFailure::other("a reply length that is not a varint"))?;
    if length > limit {
        return Err(TransportFailure::too_large());
    }
    let end = prefix.len() + length;
    Ok((bytes.len() >= end).then_some(end))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Block 61, D7: one emulation policy for both targets.
    #[test]
    fn classify_emulation_fails_on_404_5xx_3xx_and_redirects_and_rejects_on_413() {
        let cases = [
            (200, false, Emulated::Accepted),
            (204, false, Emulated::Accepted),
            (302, false, Emulated::Failed),
            (200, true, Emulated::Failed),
            (0, true, Emulated::Failed),
            (404, false, Emulated::Failed),
            (413, false, Emulated::Rejected),
            (429, false, Emulated::Accepted),
            (503, false, Emulated::Failed),
        ];

        for (status, redirected, expected) in cases {
            assert_eq!(
                classify_emulation(status, redirected),
                expected,
                "{status}, redirected: {redirected}"
            );
        }
    }

    /// The transport path's code in `src/adapter`: `body.rs` and the
    /// files of `tokio/` and `browser/`, each before its test module, by
    /// path.
    #[cfg(adapter_tokio)]
    fn transport_path() -> Vec<(String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/adapter");
        let mut files = vec![root.join("body.rs")];
        for dir in ["tokio", "browser"] {
            for entry in std::fs::read_dir(root.join(dir)).expect("a source directory") {
                files.push(entry.expect("an entry").path());
            }
        }
        files.sort();
        files
            .into_iter()
            .map(|path| {
                // Line endings normalised: a Windows checkout has CRLF.
                let text = std::fs::read_to_string(&path)
                    .expect("a source file")
                    .replace("\r\n", "\n");
                let code = [
                    "#[cfg(test)]\nmod tests",
                    "#[cfg(test)]\npub(in crate::adapter) mod tests",
                ]
                .iter()
                .filter_map(|marker| text.find(marker))
                .min()
                .map_or(text.as_str(), |end| &text[..end])
                .to_owned();
                let relative = path
                    .strip_prefix(&root)
                    .expect("under src/adapter")
                    .to_string_lossy()
                    .replace('\\', "/");
                (relative, code)
            })
            .collect()
    }

    /// The error type of each `Result<_, E>` in `code`, with the head of
    /// the function it is in.
    #[cfg(adapter_tokio)]
    fn result_errors(code: &str) -> Vec<(String, String)> {
        let mut found = Vec::new();
        for (start, _) in code.match_indices("Result<") {
            let mut depth = 0;
            let mut last_comma = start + "Result<".len();
            let mut end = last_comma;
            let bytes = code.as_bytes();
            let mut at = start + "Result".len();
            while at < bytes.len() {
                match bytes[at] {
                    b'<' => depth += 1,
                    b'>' if bytes[at - 1] != b'-' => {
                        depth -= 1;
                        if depth == 0 {
                            end = at;
                            break;
                        }
                    }
                    b',' if depth == 1 => last_comma = at + 1,
                    _ => {}
                }
                at += 1;
            }
            let error = code[last_comma..end].trim().to_owned();
            // The function's head, its visibility included: `pub fn push`.
            let function = code[..start]
                .rmatch_indices("fn ")
                .next()
                .map(|(at, _)| {
                    let line = code[..at].rfind('\n').map_or(0, |line| line + 1);
                    let name = code[at + 3..]
                        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                        .next()
                        .unwrap_or_default();
                    format!("{}fn {name}", code[line..at].trim_start())
                })
                .unwrap_or_default();
            found.push((function, error));
        }
        found
    }

    // Block 61, D5: the transport path fails with typed failures; only the
    // checks named here keep a `String`, each for its reason.
    #[cfg(adapter_tokio)]
    #[test]
    fn the_transport_path_fails_with_transport_failures() {
        const KEEP_STRING: [(&str, &str, &str); 3] = [
            (
                "tokio/webtransport.rs",
                "pub(in crate::adapter) fn check_quic",
                "a check at build too, which `ClientBuilder::build` ends in a `ClientError`",
            ),
            (
                "body.rs",
                "pub fn decode_stream",
                "public, for tests (`__test-hooks`): nothing public changes",
            ),
            (
                "body.rs",
                "pub fn push",
                "`StreamDecoder::push`, public, for tests (`__test-hooks`): nothing public changes",
            ),
        ];
        let mut found = Vec::new();
        let mut kept = Vec::new();
        for (path, code) in transport_path() {
            if code.contains("TOO_LARGE") {
                found.push(format!("{path}: TOO_LARGE"));
            }
            for (function, error) in result_errors(&code) {
                if error != "String" {
                    continue;
                }
                let keep = KEEP_STRING
                    .iter()
                    .find(|(file, name, _)| *file == path && *name == function);
                match keep {
                    Some((file, name, reason)) => {
                        assert!(!reason.is_empty(), "{file}: {name}");
                        kept.push((path.clone(), function));
                    }
                    None => found.push(format!("{path}: {function} returns Result<_, String>")),
                }
            }
        }

        assert!(found.is_empty(), "{found:#?}");
        for (file, name, _) in KEEP_STRING {
            assert!(
                kept.iter()
                    .any(|(path, function)| path == file && function == name),
                "{file}: {name} no longer returns Result<_, String>"
            );
        }
    }

    /// One decoder of each kind, with a limit above the largest reply.
    fn decoders() -> Vec<StreamDecoderUnderTest> {
        let limit = 2 << 20;
        let mut decoders = vec![Decoder::lines(limit), Decoder::protobuf(limit)];
        #[cfg(adapter_tokio)]
        decoders.push(Decoder::new(true, limit));
        decoders.into_iter().map(StreamDecoderUnderTest).collect()
    }

    struct StreamDecoderUnderTest(Decoder);

    impl StreamDecoderUnderTest {
        /// Pushes `size` bytes as one reply, in chunks of 16 KiB.
        fn reply(&mut self, size: usize) {
            let message = vec![b'a'; size];
            let framed = match &self.0 {
                #[cfg(adapter_tokio)]
                Decoder::Sse(_) => [b"data: ", &message[..], b"\n\n"].concat(),
                Decoder::Lines(..) => [&message[..], b"\n"].concat(),
                Decoder::Varint(..) => {
                    let mut framed = Vec::new();
                    prost::encoding::encode_varint(size as u64, &mut framed);
                    [framed, message].concat()
                }
            };
            let mut messages = VecDeque::new();
            for chunk in framed.chunks(16 << 10) {
                self.0.push(chunk, &mut messages).unwrap();
            }
            assert_eq!(messages.len(), 1, "{:?}", self.0);
        }
    }

    fn released_while(run: impl FnOnce()) -> usize {
        let before = RELEASED.with(std::cell::Cell::get);
        run();
        RELEASED.with(std::cell::Cell::get) - before
    }

    // Row 71: replies below what a decoder keeps never pay for a release.
    #[test]
    fn replies_below_what_a_decoder_keeps_release_nothing() {
        for mut decoder in decoders() {
            let released = released_while(|| {
                for _ in 0..100 {
                    decoder.reply(40 << 10);
                    decoder.reply(64);
                }
            });
            assert_eq!(released, 0, "{:?}", decoder.0);
        }
    }

    /// The sse decoder, its data's capacity kept for the next event and the
    /// messages it completed from `chunks`.
    #[cfg(adapter_tokio)]
    fn sse_after(chunks: &[&[u8]]) -> (usize, Vec<Vec<u8>>) {
        let mut decoder = Decoder::new(true, 2 << 20);
        let mut messages = VecDeque::new();
        for chunk in chunks {
            decoder.push(chunk, &mut messages).unwrap();
        }
        let Decoder::Sse(sse) = decoder else {
            unreachable!("an sse decoder")
        };
        (sse.kept, messages.into())
    }

    // Row 71 (reviewer, #249): an event of 60 KiB in chunks of 50 and 10 KiB
    // grows sse-core's buffer past 64 KiB; neither the decoder nor a later
    // message keeps that.
    #[cfg(adapter_tokio)]
    #[test]
    fn an_sse_message_after_a_60_kib_event_is_handed_over_at_its_size() {
        let event = [&b"data: "[..], &vec![b'a'; 60 << 10], b"\n\n"].concat();
        let (first, second) = event.split_at(6 + (50 << 10));
        let (kept, messages) = sse_after(&[first, second, b"data: ok\n\n"]);
        assert!(kept <= KEPT, "the decoder keeps {kept} bytes");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1], b"ok");
        assert!(
            messages[1].capacity() <= KEPT,
            "a message of 2 bytes holds {} bytes",
            messages[1].capacity()
        );
    }

    // Row 71 (reviewer, #249): the decoder that replaces a released one is
    // past its start, so a U+FEFF after the boundary begins an unknown field
    // there as on any other line, and its data is not delivered.
    #[cfg(adapter_tokio)]
    #[test]
    fn a_byte_order_mark_after_a_released_event_is_mid_stream() {
        let large = [&b"data: "[..], &vec![b'a'; 100 << 10], b"\n\n"].concat();
        let after = "\u{feff}data: hidden\n\ndata: shown\n\n".as_bytes();
        let (_, released) = sse_after(&[&large, after]);
        let (_, kept) = sse_after(&[b"data: small\n\n", after]);
        for messages in [released, kept] {
            assert_eq!(messages.last().map(Vec::as_slice), Some(&b"shown"[..]));
            assert_eq!(messages.len(), 2, "{messages:?}");
        }
    }

    // Row 71: a large reply is released once, after it is split.
    #[test]
    fn a_large_reply_is_released_once() {
        for mut decoder in decoders() {
            let released = released_while(|| {
                decoder.reply(1 << 20);
                for _ in 0..10 {
                    decoder.reply(64);
                }
            });
            assert_eq!(released, 1, "{:?}", decoder.0);
        }
    }
}
