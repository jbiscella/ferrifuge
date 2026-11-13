//! Instruction counts of decoding one large reply delivered one byte per
//! chunk (Block 50, D5, `@P`), with gungraun on Valgrind, at 256 KiB and
//! 1 MiB for each stream format. `tests/perf_gates.rs` gates on their
//! ratio: a decoder linear in the bytes costs 4 times as much for 4 times
//! the bytes, one that rescans its buffer 16 times.

// @P scenario: A large reply delivered one byte per chunk costs what its size predicts

// The gungraun macros generate undocumented items.
#![allow(missing_docs)]

use ferrifuge::StreamFormat;

/// The bytes of a reply's JSON before and after its padding.
const JSON_HEAD: &str = r#"{"push":{"channel":"news","pub":{"data":""#;
const JSON_TAIL: &str = r#""}}}"#;

/// One reply of exactly `size` bytes in `format`, framed as its stream
/// carries it, and the length of the message the decoder yields for it.
fn body((format, size): (StreamFormat, usize)) -> (StreamFormat, Vec<u8>, usize) {
    let json = || {
        let padding = "x".repeat(size - JSON_HEAD.len() - JSON_TAIL.len());
        format!("{JSON_HEAD}{padding}{JSON_TAIL}").into_bytes()
    };
    let (framed, message) = match format {
        StreamFormat::HttpStreamJson => ([json(), b"\n".to_vec()].concat(), size),
        StreamFormat::SseJson => ([&b"data: "[..], &json(), b"\n\n"].concat(), size),
        StreamFormat::HttpStreamProtobuf => {
            let reply = protobuf_reply(size);
            let framed = [varint(reply.len()), reply].concat();
            let length = framed.len();
            // The decoder yields a Protobuf reply with its length prefix.
            (framed, length)
        }
    };
    (format, framed, message)
}

/// `value` as a uvarint.
fn varint(mut value: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    while value >= 0x80 {
        bytes.push(u8::try_from(value & 0x7f).expect("7 bits") | 0x80);
        value >>= 7;
    }
    bytes.push(u8::try_from(value).expect("below 0x80"));
    bytes
}

/// A length-delimited field `tag` holding `payload`.
fn field(tag: u8, payload: &[u8]) -> Vec<u8> {
    [&[tag][..], &varint(payload.len()), payload].concat()
}

/// `Reply { push: { channel: "news", pub: { data } } }` of exactly `size`
/// bytes, its data padded to fit (`client.proto` field numbers: push 4,
/// channel 2, pub 4, data 4).
fn protobuf_reply(size: usize) -> Vec<u8> {
    let reply = |data: usize| {
        let publication = field(0x22, &vec![b'x'; data]);
        let push = [field(0x12, b"news"), field(0x22, &publication)].concat();
        field(0x22, &push)
    };
    // The fields around the data take at most a few dozen bytes.
    (size.saturating_sub(64)..=size)
        .rev()
        .map(reply)
        .find(|reply| reply.len() <= size)
        .filter(|reply| reply.len() == size)
        .unwrap_or_else(|| panic!("no Protobuf reply of exactly {size} bytes"))
}

/// Checks the one message decoded, outside the measurement.
fn teardown((outcome, length): (Result<Vec<Vec<u8>>, String>, usize)) {
    let messages = outcome.expect("the reply decodes");
    assert_eq!(messages.len(), 1, "one reply");
    assert_eq!(messages[0].len(), length, "the whole reply");
}

/// The gungraun harness, run by [`main`] under `cargo bench`. The
/// benchmark, its group and the macro's `main` share this module: gungraun
/// finds the benchmark by its module path.
mod harness {
    use std::hint::black_box;

    use ferrifuge::{StreamFormat, decode_stream};
    use gungraun::{library_benchmark, library_benchmark_group};

    use super::{body, teardown};

    const SMALL: usize = 256 << 10;
    const LARGE: usize = 1 << 20;

    #[library_benchmark]
    #[bench::json_256_kib(setup = body, args = ((StreamFormat::HttpStreamJson, SMALL)), teardown = teardown)]
    #[bench::json_1_mib(setup = body, args = ((StreamFormat::HttpStreamJson, LARGE)), teardown = teardown)]
    #[bench::sse_256_kib(setup = body, args = ((StreamFormat::SseJson, SMALL)), teardown = teardown)]
    #[bench::sse_1_mib(setup = body, args = ((StreamFormat::SseJson, LARGE)), teardown = teardown)]
    #[bench::protobuf_256_kib(setup = body, args = ((StreamFormat::HttpStreamProtobuf, SMALL)), teardown = teardown)]
    #[bench::protobuf_1_mib(setup = body, args = ((StreamFormat::HttpStreamProtobuf, LARGE)), teardown = teardown)]
    fn one_byte_per_chunk(
        (format, body, length): (StreamFormat, Vec<u8>, usize),
    ) -> (Result<Vec<Vec<u8>>, String>, usize) {
        let outcome = decode_stream(black_box(format), black_box(&body).chunks(1));
        (black_box(outcome), length)
    }

    library_benchmark_group!(name = decoding, benchmarks = [one_byte_per_chunk]);

    gungraun::main!(library_benchmark_groups = decoding);

    /// The macro's `main`, which it keeps private.
    pub(super) fn run() {
        main();
    }
}

/// Runs the harness under `cargo bench`, which passes `--bench`, and when
/// gungraun-runner runs this binary under Valgrind (`--gungraun-run`).
/// Under `cargo test --all-targets` it does nothing: the runner and
/// Valgrind are installed only in the perf job.
fn main() {
    if std::env::args().any(|arg| arg == "--bench" || arg.starts_with("--gungraun")) {
        harness::run();
    } else {
        println!("decode benchmark skipped: run it with cargo bench");
    }
}
