//! What the HTTP-streaming transports of both adapters share (Block 44,
//! D8): the decoders of a response body (Block 27) and the captive-portal
//! check of an open (Block 25, D8).

use std::collections::VecDeque;
#[cfg(adapter_tokio)]
use std::num::NonZeroUsize;

use bytes::BytesMut;
use mediatype::{MediaType, Name, names};
use tokio_util::codec::{AnyDelimiterCodec, Decoder as _};

/// The error of an HTTP transport's decoder when the server sent a message
/// larger than the size limit, reported as `Inbound::TooLarge`.
pub(in crate::adapter) const TOO_LARGE: &str = "message larger than the size limit";

/// What an error message about an open ends with when a captive portal may
/// have answered it (Block 25, D8).
pub(in crate::adapter) const CAPTIVE_PORTAL: &str = "; a captive portal may be answering";

/// Whether an open answered with `status` and `content_type`, instead of
/// the transport's, looks like a captive portal's web page (Block 25, D8):
/// a redirect, 511 "Network Authentication Required", or a page served as
/// a success.
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

/// Whether `content_type` parses as a media type of type `ty` and subtype
/// `subty`, whatever their case and parameters (Block 27, D4: mediatype,
/// RFC 9110 section 8.3); a value that does not parse is neither.
pub(in crate::adapter) fn is_media_type(content_type: &str, ty: Name<'_>, subty: Name<'_>) -> bool {
    MediaType::parse(content_type.trim()).is_ok_and(|media_type| {
        // `text/event-stream+json` is another media type (Codex, #127).
        media_type.ty == ty && media_type.subty == subty && media_type.suffix.is_none()
    })
}

/// Splits a response body into the messages it carries: one JSON reply per
/// line for `http_stream`, the data of each event for sse (natively only:
/// in the browser, `EventSource` splits its own, Block 44 D9). Network chunks
/// do not follow message boundaries, so a partial line waits for the next.
/// Both are maintained components, bounded by the largest message (Block
/// 27, D1, D2); their first error ends the transport, as they are not asked
/// to resynchronise.
pub(in crate::adapter) enum Decoder {
    /// sse-core: events with data, of the default type, reach the client
    /// (centrifuge-js `onmessage`, WHATWG "dispatch the event").
    #[cfg(adapter_tokio)]
    Sse(sse_core::SseDecoder),
    /// tokio-util's lines, bounded, and the bytes not yet split.
    Lines(AnyDelimiterCodec, BytesMut),
    /// Protobuf replies, each after its uvarint length (Block 27, D3): the
    /// bytes not yet split, and the largest reply. In-house, as no
    /// maintained component splits uvarint-delimited streams (Block 0).
    Varint(Vec<u8>, usize),
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
            Self::Sse(sse_core::SseDecoder::with_limit(
                NonZeroUsize::new(limit).unwrap_or(NonZeroUsize::MIN),
            ))
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
    ) -> Result<(), String> {
        match self {
            #[cfg(adapter_tokio)]
            Self::Sse(decoder) => {
                let mut rest = chunk;
                while let Some(event) = decoder.next(&mut rest) {
                    match event {
                        Ok(sse_core::SseEvent::Message(message)) => {
                            let default = message.event.is_empty() || message.event == "message";
                            if default && !message.data.is_empty() {
                                messages.push_back(message.data.into_bytes());
                            }
                        }
                        // The client keeps its own reconnect delays.
                        Ok(sse_core::SseEvent::Retry(_)) => {}
                        Err(_) => return Err(TOO_LARGE.to_owned()),
                    }
                }
            }
            Self::Lines(codec, buffer) => {
                buffer.extend_from_slice(chunk);
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
                        Err(_) => return Err(TOO_LARGE.to_owned()),
                    }
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
            }
        }
        Ok(())
    }
}

/// Where the first reply of `bytes`, a Protobuf stream, ends, its uvarint
/// length prefix included; `None` while more bytes are needed (Block 27,
/// D3). A zero length is an empty reply, the server's ping.
///
/// # Errors
///
/// When the prefix is not a uvarint, or announces more than `limit` bytes.
fn next_reply(bytes: &[u8], limit: usize) -> Result<Option<usize>, String> {
    // At most 10 bytes; all but the last have the continuation bit.
    let Some(last) = bytes.iter().take(10).position(|byte| byte & 0x80 == 0) else {
        return if bytes.len() < 10 {
            Ok(None)
        } else {
            Err("a reply length that is not a varint".to_owned())
        };
    };
    let prefix = &bytes[..=last];
    let length = prost::decode_length_delimiter(prefix)
        .map_err(|_| "a reply length that is not a varint".to_owned())?;
    if length > limit {
        return Err(TOO_LARGE.to_owned());
    }
    let end = prefix.len() + length;
    Ok((bytes.len() >= end).then_some(end))
}
