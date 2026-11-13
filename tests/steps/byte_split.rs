//! Steps of Block 50: a stream body split at every byte offset, or
//! delivered one byte per chunk, decodes to the same three messages, given
//! as literals (D1, D2), through `decode_stream` (D3); a websocket relayed
//! one byte per write works (D4).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{Protocol, StreamFormat, decode_stream};
use prost::Message as _;
use serde_json::json;

use super::ping::QUIET_PING_INTERVAL;
use super::subscriptions::{CHANNEL, a_client_subscribed, next_publication};
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::fake_server::Announce;
use crate::support::protobuf;
use crate::support::relay::Mode;

/// The JSON connect reply of every body.
const CONNECT_JSON: &str =
    r#"{"id":1,"connect":{"client":"c1","version":"0.0.0","ping":25,"pong":true}}"#;
/// The JSON publication, whose data holds a 3-byte and a 4-byte character.
const PUBLICATION_JSON: &str = r#"{"push":{"channel":"news","pub":{"data":{"text":"€ and 𝄞"}}}}"#;
/// The JSON ping.
const PING_JSON: &str = "{}";

/// The Protobuf connect reply, with its 1-byte length: `Reply { id: 1,
/// connect: { client: "c1", version: "0.0.0", ping: 25, pong: true } }`.
const CONNECT_PROTOBUF: &[u8] = b"\x13\x08\x01\x2a\x0f\x0a\x02c1\x12\x050.0.0\x38\x19\x40\x01";
/// The Protobuf publication's 2-byte length (300) and its fields up to its
/// data: `Reply { push: { channel: "news", pub: { data } } }`, the data of
/// 285 bytes being [`publication_data`].
const PUBLICATION_PROTOBUF_HEAD: &[u8] =
    b"\xac\x02\x22\xa9\x02\x12\x04news\x22\xa0\x02\x22\x9d\x02";
/// The Protobuf ping: an empty reply, length 0.
const PING_PROTOBUF: &[u8] = b"\x00";

/// The Protobuf publication's data: JSON holding `€` and `𝄞`, padded so
/// that the reply is 300 bytes and its length takes 2 bytes (D1).
fn publication_data() -> String {
    format!(r#"{{"text":"€ and 𝄞, {}"}}"#, "x".repeat(260))
}

/// The messages a decoder yielded, or its error.
type Outcome = Result<Vec<Vec<u8>>, String>;

/// What a Block 50 @E scenario holds.
#[derive(Debug, Default)]
pub(crate) struct ByteSplit {
    format: Option<StreamFormat>,
    /// The three messages as the decoder yields them, in order.
    expected: Vec<Vec<u8>>,
    /// The body, and where each message's last byte ends (exclusive).
    body: Vec<u8>,
    ends: Vec<usize>,
    /// The outcome for each split offset, or for one byte per chunk.
    outcomes: Vec<(usize, Outcome)>,
    /// The pongs the fake server had received before the ping (@D).
    pongs_before: Option<usize>,
}

impl ByteSplit {
    fn format(&self) -> StreamFormat {
        self.format.expect("a body was given")
    }

    /// The offsets whose outcome is not exactly the three messages.
    fn wrong(&self) -> Vec<String> {
        self.outcomes
            .iter()
            .filter(|(_, outcome)| outcome.as_ref().ok() != Some(&self.expected))
            .map(|(offset, outcome)| match outcome {
                Ok(messages) => format!(
                    "offset {offset}: {:?}",
                    messages
                        .iter()
                        .map(|m| String::from_utf8_lossy(m))
                        .collect::<Vec<_>>()
                ),
                Err(error) => format!("offset {offset}: {error}"),
            })
            .collect()
    }
}

#[given(
    regex = r"^a (http_stream JSON|sse JSON|http_stream Protobuf) body holding a connect reply, a publication with multi-byte data and a ping$"
)]
fn a_body(world: &mut ClientWorld, format: String) {
    let split = &mut world.byte_split;
    let (format, expected, framed): (_, Vec<Vec<u8>>, Vec<Vec<u8>>) = match format.as_str() {
        "http_stream JSON" => {
            let messages = [CONNECT_JSON, PUBLICATION_JSON, PING_JSON].map(str::as_bytes);
            (
                StreamFormat::HttpStreamJson,
                messages.iter().map(|m| m.to_vec()).collect(),
                messages.iter().map(|m| [m, &b"\n"[..]].concat()).collect(),
            )
        }
        "sse JSON" => {
            let messages = [CONNECT_JSON, PUBLICATION_JSON, PING_JSON].map(str::as_bytes);
            (
                StreamFormat::SseJson,
                messages.iter().map(|m| m.to_vec()).collect(),
                messages
                    .iter()
                    .map(|m| [&b"data: "[..], m, b"\n\n"].concat())
                    .collect(),
            )
        }
        _ => {
            let publication = [PUBLICATION_PROTOBUF_HEAD, publication_data().as_bytes()].concat();
            // A Protobuf reply comes with its length, as the codec takes it.
            let messages = vec![
                CONNECT_PROTOBUF.to_vec(),
                publication,
                PING_PROTOBUF.to_vec(),
            ];
            (StreamFormat::HttpStreamProtobuf, messages.clone(), messages)
        }
    };
    if format == StreamFormat::HttpStreamProtobuf {
        // D1: the publication's length is 300, in 2 bytes.
        let prefix = &expected[1][..2];
        assert!(
            prefix[0] & 0x80 != 0 && prefix[1] & 0x80 == 0,
            "the publication's length takes 2 bytes: {prefix:02x?}"
        );
        assert_eq!(
            usize::from(prefix[0] & 0x7f) | usize::from(prefix[1]) << 7,
            expected[1].len() - 2
        );
        assert_eq!(expected[1].len() - 2, 300);
    }
    real_replies(format, &expected);
    split.ends = framed
        .iter()
        .scan(0, |end, frame| {
            *end += frame.len();
            Some(*end)
        })
        .collect();
    split.body = framed.concat();
    split.format = Some(format);
    split.expected = expected;
}

/// Checks that the literals are the replies they stand for, decoded as the
/// server's (reviewer, #250): Protobuf with the `client.proto` types the
/// fake server uses, each message exactly one length-delimited reply; JSON
/// with serde. A malformed literal fails here rather than passing unseen,
/// as the decoder splits messages without reading them. In the Given step,
/// as the bdd binary runs no `#[test]`.
fn real_replies(format: StreamFormat, messages: &[Vec<u8>]) {
    if format == StreamFormat::HttpStreamProtobuf {
        let replies: Vec<protobuf::Reply> = messages
            .iter()
            .map(|framed| {
                let mut bytes = framed.as_slice();
                let reply = protobuf::Reply::decode_length_delimited(&mut bytes)
                    .unwrap_or_else(|error| panic!("{framed:02x?}: {error}"));
                assert!(bytes.is_empty(), "{} bytes after the reply", bytes.len());
                reply
            })
            .collect();
        let connect = replies[0].connect.as_ref().expect("a connect reply");
        assert_eq!(replies[0].id, 1);
        assert_eq!(
            (connect.client.as_str(), connect.version.as_str()),
            ("c1", "0.0.0")
        );
        assert_eq!((connect.ping, connect.pong), (25, true));
        let push = replies[1].push.as_ref().expect("a push");
        assert_eq!(push.channel, "news");
        let publication = push.r#pub.as_ref().expect("a publication");
        assert_eq!(publication.data, publication_data().as_bytes());
        assert_eq!(replies[2], protobuf::Reply::default());
    } else {
        let replies: Vec<serde_json::Value> = messages
            .iter()
            .map(|message| serde_json::from_slice(message).expect("a JSON reply"))
            .collect();
        assert_eq!(replies[0]["id"], 1);
        assert_eq!(replies[0]["connect"]["client"], "c1");
        assert_eq!(replies[1]["push"]["channel"], "news");
        assert_eq!(replies[1]["push"]["pub"]["data"]["text"], "€ and 𝄞");
        assert_eq!(replies[2], json!({}));
    }
}

#[when("it is delivered in two chunks, split at each byte offset in turn")]
fn split_at_each_offset(world: &mut ClientWorld) {
    let split = &mut world.byte_split;
    let format = split.format();
    // D's structural acceptance: every offset of 0..body.len().
    split.outcomes = (0..split.body.len())
        .map(|offset| {
            let (first, second) = split.body.split_at(offset);
            (offset, decode_stream(format, &[first, second]))
        })
        .collect();
}

#[when("it is delivered one byte per chunk")]
fn one_byte_per_chunk(world: &mut ClientWorld) {
    let split = &mut world.byte_split;
    let chunks: Vec<&[u8]> = split.body.chunks(1).collect();
    split.outcomes = vec![(1, decode_stream(split.format(), &chunks))];
}

#[then("the decoder yields exactly those three messages, in order, for every offset")]
fn the_messages_at_every_offset(world: &mut ClientWorld) {
    let split = &world.byte_split;
    assert_eq!(split.outcomes.len(), split.body.len());
    let wrong = split.wrong();
    assert!(wrong.is_empty(), "{} offsets: {wrong:?}", wrong.len());
}

#[then("no offset gives an error")]
fn no_offset_errs(world: &mut ClientWorld) {
    let errors: Vec<_> = world
        .byte_split
        .outcomes
        .iter()
        .filter_map(|(offset, outcome)| outcome.as_ref().err().map(|e| format!("{offset}: {e}")))
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
}

#[then("the decoder yields exactly those three messages, in order")]
fn the_messages(world: &mut ClientWorld) {
    let wrong = world.byte_split.wrong();
    assert!(wrong.is_empty(), "{wrong:?}");
}

#[then("the decoder emitted no message before the byte that ends it")]
fn no_message_early(world: &mut ClientWorld) {
    let split = &world.byte_split;
    let chunks: Vec<&[u8]> = split.body.chunks(1).collect();
    // After the first `bytes` bytes, exactly the messages ending by then.
    let wrong: Vec<_> = (0..=chunks.len())
        .filter_map(|bytes| {
            let yielded = decode_stream(split.format(), &chunks[..bytes]).map(|m| m.len());
            let ended = split.ends.iter().filter(|end| **end <= bytes).count();
            (yielded != Ok(ended)).then(|| format!("after {bytes} bytes: {yielded:?}, not {ended}"))
        })
        .collect();
    assert!(wrong.is_empty(), "{wrong:?}");
}

// --- A websocket relayed one byte per write works (@D, D4) ---------------

/// The publication's data: a 3-byte and a 4-byte character, which the
/// relay's single bytes split.
const RELAYED_DATA: &str = r#"{"text":"€ and 𝄞"}"#;

#[given(
    regex = r#"^a client configured with the (JSON|Protobuf) protocol, subscribed to channel "news" on the fake server through a relay writing one byte per write in both directions$"#
)]
async fn a_client_behind_a_one_byte_relay(world: &mut ClientWorld, protocol: String) {
    world
        .restart_fake(Announce {
            ping: QUIET_PING_INTERVAL,
            pong: true,
            ..Announce::default()
        })
        .await;
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::OneBytePerWrite);
    if protocol == "Protobuf" {
        world.protocol = Protocol::Protobuf;
    }
    a_client_subscribed(world).await;
}

#[when(r#"the fake server sends a publication on "news" and a ping in one frame"#)]
fn a_publication_and_a_ping_in_one_frame(world: &mut ClientWorld) {
    world.byte_split.pongs_before = Some(pongs(world));
    if world.protocol == Protocol::Protobuf {
        let publication = protobuf::Reply {
            push: Some(protobuf::Push {
                channel: CHANNEL.to_owned(),
                r#pub: Some(protobuf::Publication {
                    data: RELAYED_DATA.as_bytes().to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        world
            .fake()
            .send_protobuf(&[publication, protobuf::Reply::default()]);
    } else {
        let publication = json!({"push": {"channel": CHANNEL, "pub": {"data": "DATA"}}})
            .to_string()
            .replace(r#""DATA""#, RELAYED_DATA);
        // Several JSON replies in one frame are separated by newlines.
        world.fake().send(&format!("{publication}\n{{}}"));
    }
}

#[then("the subscription receives that publication with its exact data")]
async fn that_publication_with_its_exact_data(world: &mut ClientWorld) {
    let publication = next_publication(world, false).await;
    assert_eq!(
        String::from_utf8_lossy(&publication.data),
        RELAYED_DATA,
        "{publication:?}"
    );
}

#[then("the fake server received a pong after the ping")]
async fn a_pong_after_the_ping(world: &mut ClientWorld) {
    let before = world.byte_split.pongs_before.expect("a ping sent");
    let ponged = tokio::time::timeout(EVENT_TIMEOUT, async {
        while pongs(world) <= before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(ponged.is_ok(), "no pong after the ping: {before} before");
}

#[then("the relay made at least one write per byte relayed")]
fn one_write_per_byte(world: &mut ClientWorld) {
    let (writes, bytes) = world.relay.as_ref().expect("a relay").single_byte_writes();
    assert!(bytes > 0, "nothing relayed one byte per write");
    assert!(writes >= bytes, "{writes} writes for {bytes} bytes");
}

/// The pongs the fake server has received: empty commands, in either
/// encoding.
fn pongs(world: &ClientWorld) -> usize {
    let fake = world.fake();
    let json = fake.received().iter().filter(|m| **m == json!({})).count();
    let protobuf = fake
        .received_protobuf()
        .iter()
        .filter(|command| **command == protobuf::Command::default())
        .count();
    json + protobuf
}
