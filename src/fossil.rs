//! Block 19: applying a Fossil delta to the data it was made against.
//!
//! Written from the format's description ("Fossil Delta Format",
//! fossil-scm.org) and cross-checked against centrifuge-js 63ed73e
//! `src/fossil.ts` (Block 19, D1): a header (the target's size, then `\n`),
//! segments (`count@offset,` copies bytes of the base, `count:` inserts the
//! next `count` bytes of the delta), and a trailer (a checksum, then `;`)
//! ending the delta.
//!
//! Every check but the checksum is made before the target is allocated, so
//! that a short delta cannot make the client allocate the size it declares
//! (D6).

use std::ops::Range;

/// Why a delta cannot be applied (Block 19, D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeltaError {
    /// The header is not a number followed by `\n`.
    Header,
    /// The target it declares is larger than the limit.
    TooLarge,
    /// A number has no digit, or a value above 2^32 − 1.
    Number,
    /// A segment is neither a copy nor an insert, or a copy is not followed
    /// by `,`.
    Segment,
    /// A copy reads past the end of the base.
    CopyPastBase,
    /// An insert runs past the end of the delta.
    InsertPastDelta,
    /// The segments build more or fewer bytes than the header declares.
    Size,
    /// The trailer is missing, or bytes follow it.
    Trailer,
    /// The target's checksum is not the trailer's.
    Checksum,
    /// A delta came before any whole publication of the subscribe (D5).
    NoBase,
    /// A delta came on a subscription without delta on (D5).
    NotOn,
    /// With JSON, the data of a publication with delta on is not a JSON
    /// string (D7).
    NotAString,
    /// The publications of one message together build more than the
    /// inbound message limit (D6).
    MessageTooLarge,
}

impl std::fmt::Display for DeltaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Header => "delta header is not a size followed by a newline",
            Self::TooLarge => "delta declares a target larger than the inbound message limit",
            Self::Number => "delta number has no digit or exceeds 32 bits",
            Self::Segment => "delta segment is neither a copy nor an insert",
            Self::CopyPastBase => "delta copies past the end of the previous data",
            Self::InsertPastDelta => "delta inserts past its own end",
            Self::Size => "delta builds a different size than it declares",
            Self::Trailer => "delta trailer is missing or followed by bytes",
            Self::Checksum => "delta checksum does not match",
            Self::NoBase => "delta publication before any whole publication",
            Self::NotOn => "delta publication on a subscription without delta",
            Self::NotAString => "publication data of a delta subscription is not a JSON string",
            Self::MessageTooLarge => {
                "delta publications of one message exceed the inbound message limit"
            }
        })
    }
}

/// Rebuilds the target `delta` describes against `base`, at most `limit`
/// bytes long.
pub(crate) fn apply(base: &[u8], delta: &[u8], limit: usize) -> Result<Vec<u8>, DeltaError> {
    let mut header = Reader { delta, at: 0 };
    let size = header.number().map_err(|_| DeltaError::Header)?;
    if header.next() != Some(b'\n') {
        return Err(DeltaError::Header);
    }
    let size = usize::try_from(size).map_err(|_| DeltaError::TooLarge)?;
    if size > limit {
        return Err(DeltaError::TooLarge);
    }
    let segments = Segments {
        base,
        delta,
        start: header.at,
        size,
    };
    // Every check but the checksum, with nothing allocated.
    let checksum = segments.walk(|_| {})?;
    let mut target = Vec::with_capacity(size);
    segments.walk(|bytes| target.extend_from_slice(bytes))?;
    if self::checksum(&target) != checksum {
        return Err(DeltaError::Checksum);
    }
    Ok(target)
}

/// The segment list of a delta whose header declared `size`.
struct Segments<'a> {
    base: &'a [u8],
    delta: &'a [u8],
    /// Where the segments start, after the header.
    start: usize,
    size: usize,
}

impl Segments<'_> {
    /// Gives `out` the bytes each segment adds, in order, and returns the
    /// trailer's checksum once the segments built exactly `size` bytes.
    fn walk(&self, mut out: impl FnMut(&[u8])) -> Result<u32, DeltaError> {
        let mut reader = Reader {
            delta: self.delta,
            at: self.start,
        };
        let mut built = 0;
        loop {
            if reader.at == self.delta.len() {
                return Err(DeltaError::Trailer);
            }
            let count = reader.number()?;
            let added = match reader.next() {
                Some(b'@') => {
                    let offset = reader.number()?;
                    if reader.next() != Some(b',') {
                        return Err(DeltaError::Segment);
                    }
                    let range = span(widen(offset), count, self.base.len())
                        .ok_or(DeltaError::CopyPastBase)?;
                    &self.base[range]
                }
                Some(b':') => {
                    let range = span(reader.at, count, self.delta.len())
                        .ok_or(DeltaError::InsertPastDelta)?;
                    reader.at = range.end;
                    &self.delta[range]
                }
                Some(b';') => {
                    if built != self.size {
                        return Err(DeltaError::Size);
                    }
                    if reader.at != self.delta.len() {
                        return Err(DeltaError::Trailer);
                    }
                    return Ok(count);
                }
                _ => return Err(DeltaError::Segment),
            };
            built += added.len();
            if built > self.size {
                return Err(DeltaError::Size);
            }
            out(added);
        }
    }
}

/// `count` bytes from `start`, when they end within `len`.
fn span(start: usize, count: u32, len: usize) -> Option<Range<usize>> {
    let end = start.checked_add(widen(count))?;
    (end <= len).then_some(start..end)
}

fn widen(value: u32) -> usize {
    // A u32 fits a usize on every target Ferrifuge builds for (32 and 64
    // bits); saturating keeps an out-of-range copy failing elsewhere.
    usize::try_from(value).unwrap_or(usize::MAX)
}

/// Reads the delta byte by byte.
struct Reader<'a> {
    delta: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn next(&mut self) -> Option<u8> {
        let byte = *self.delta.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    /// A number: base-64 digits, most significant first (D2), at least
    /// one, the value within 32 bits.
    fn number(&mut self) -> Result<u32, DeltaError> {
        let mut value: u64 = 0;
        let mut digits = 0;
        while let Some(digit) = self.delta.get(self.at).copied().and_then(digit) {
            value = (value << 6) | u64::from(digit);
            if value > u64::from(u32::MAX) {
                return Err(DeltaError::Number);
            }
            digits += 1;
            self.at += 1;
        }
        if digits == 0 {
            return Err(DeltaError::Number);
        }
        u32::try_from(value).map_err(|_| DeltaError::Number)
    }
}

/// The value of a digit of the alphabet `0-9A-Z_a-z~`.
fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'Z' => Some(byte - b'A' + 10),
        b'_' => Some(36),
        b'a'..=b'z' => Some(byte - b'a' + 37),
        b'~' => Some(63),
        _ => None,
    }
}

/// The sum, modulo 2^32, of `target` read as big-endian 32-bit words, the
/// last zero-padded (D2).
fn checksum(target: &[u8]) -> u32 {
    target.chunks(4).fold(0u32, |sum, word| {
        let mut padded = [0; 4];
        padded[..word.len()].copy_from_slice(word);
        sum.wrapping_add(u32::from_be_bytes(padded))
    })
}

/// A delta turning `base` into `target`: a copy of their common prefix, an
/// insert of what differs, a copy of their common suffix. For the tests'
/// servers (Block 19, D9); the client never encodes deltas.
#[cfg(any(test, feature = "test-util"))]
#[must_use]
pub fn write(base: &[u8], target: &[u8]) -> Vec<u8> {
    let prefix = base.iter().zip(target).take_while(|(a, b)| a == b).count();
    let suffix = base[prefix..]
        .iter()
        .rev()
        .zip(target[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let middle = &target[prefix..target.len() - suffix];
    let mut delta = number(len(target.len()));
    delta.push(b'\n');
    if prefix > 0 {
        delta.extend(number(len(prefix)));
        delta.extend(b"@0,");
    }
    if !middle.is_empty() {
        delta.extend(number(len(middle.len())));
        delta.push(b':');
        delta.extend_from_slice(middle);
    }
    if suffix > 0 {
        delta.extend(number(len(suffix)));
        delta.push(b'@');
        delta.extend(number(len(base.len() - suffix)));
        delta.push(b',');
    }
    delta.extend(number(checksum(target)));
    delta.push(b';');
    delta
}

#[cfg(any(test, feature = "test-util"))]
fn len(value: usize) -> u64 {
    u64::try_from(value).expect("a length fits 64 bits")
}

/// `value` in the delta's base-64 digits, most significant first.
#[cfg(any(test, feature = "test-util"))]
fn number(value: impl Into<u64>) -> Vec<u8> {
    const ALPHABET: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
    let mut value = value.into();
    let mut digits = Vec::new();
    loop {
        digits.push(ALPHABET[(value % 64) as usize]);
        value /= 64;
        if value == 0 {
            break;
        }
    }
    digits.reverse();
    digits
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 64 << 20;

    /// The five deltas Fossil itself produced, with their origin and target
    /// (centrifugal/fdelta `testdata/fossil-vectors`, BSD-2-Clause, see
    /// `tests/fixtures/fossil-vectors/LICENSE`).
    const VECTORS: [(&[u8], &[u8], &[u8]); 5] = [
        (
            include_bytes!("../tests/fixtures/fossil-vectors/1/origin"),
            include_bytes!("../tests/fixtures/fossil-vectors/1/target"),
            include_bytes!("../tests/fixtures/fossil-vectors/1/delta"),
        ),
        (
            include_bytes!("../tests/fixtures/fossil-vectors/2/origin"),
            include_bytes!("../tests/fixtures/fossil-vectors/2/target"),
            include_bytes!("../tests/fixtures/fossil-vectors/2/delta"),
        ),
        (
            include_bytes!("../tests/fixtures/fossil-vectors/3/origin"),
            include_bytes!("../tests/fixtures/fossil-vectors/3/target"),
            include_bytes!("../tests/fixtures/fossil-vectors/3/delta"),
        ),
        (
            include_bytes!("../tests/fixtures/fossil-vectors/4/origin"),
            include_bytes!("../tests/fixtures/fossil-vectors/4/target"),
            include_bytes!("../tests/fixtures/fossil-vectors/4/delta"),
        ),
        (
            include_bytes!("../tests/fixtures/fossil-vectors/5/origin"),
            include_bytes!("../tests/fixtures/fossil-vectors/5/target"),
            include_bytes!("../tests/fixtures/fossil-vectors/5/delta"),
        ),
    ];

    #[test]
    fn fossils_own_deltas_rebuild_their_targets() {
        for (i, (origin, target, delta)) in VECTORS.iter().enumerate() {
            assert_eq!(
                apply(origin, delta, LIMIT).as_deref(),
                Ok(*target),
                "vector {}",
                i + 1
            );
        }
    }

    #[test]
    fn the_documents_example_number_decodes_most_significant_digit_first() {
        let mut reader = Reader {
            delta: b"1Xb",
            at: 0,
        };

        assert_eq!(reader.number(), Ok(6246));
    }

    #[test]
    fn the_checksum_wraps_at_2_pow_32() {
        // Two words whose sum is 2^32: 0 modulo 2^32, 1 modulo 2^32 - 1.
        let target = [0x80, 0, 0, 0, 0x80, 0, 0, 0];

        assert_eq!(checksum(&target), 0);
    }

    #[test]
    fn a_copy_of_count_0_copies_nothing() {
        let delta = [b"3\n0@0,3:abc".as_slice(), &number(checksum(b"abc")), b";"].concat();

        assert_eq!(apply(b"xyz", &delta, LIMIT).as_deref(), Ok(&b"abc"[..]));
    }

    /// `body` between a header declaring `size` and a trailer carrying the
    /// checksum of `target`.
    fn delta(size: usize, body: &[u8], target: &[u8]) -> Vec<u8> {
        [
            &number(len(size))[..],
            b"\n",
            body,
            &number(checksum(target)),
            b";",
        ]
        .concat()
    }

    #[test]
    fn each_malformed_delta_fails_with_its_reason() {
        let base = b"0123456789";
        let cases: [(Vec<u8>, DeltaError); 13] = [
            (b"3".to_vec(), DeltaError::Header),
            (b"\n0;".to_vec(), DeltaError::Header),
            (b"3;".to_vec(), DeltaError::Header),
            (delta(3, b"3?abc", b"abc"), DeltaError::Segment),
            (delta(3, b"3@0;", b"012"), DeltaError::Segment),
            (delta(3, b"3@8,", b"89?"), DeltaError::CopyPastBase),
            (b"3\n3:ab".to_vec(), DeltaError::InsertPastDelta),
            (delta(4, b"3:abc", b"abc"), DeltaError::Size),
            (delta(2, b"3:abc", b"abc"), DeltaError::Size),
            (delta(3, b"3:abc", b"abd"), DeltaError::Checksum),
            (b"3\n3:abc".to_vec(), DeltaError::Trailer),
            (
                [delta(3, b"3:abc", b"abc"), b"x".to_vec()].concat(),
                DeltaError::Trailer,
            ),
            (delta(3, b":abc", b"abc"), DeltaError::Number),
        ];

        for (delta, reason) in cases {
            assert_eq!(
                apply(base, &delta, LIMIT),
                Err(reason),
                "{}",
                String::from_utf8_lossy(&delta)
            );
        }
    }

    #[test]
    fn a_number_above_32_bits_fails() {
        // 2^32: "4" followed by five zero digits.
        let delta = b"400000\n";

        assert_eq!(apply(b"", delta, usize::MAX), Err(DeltaError::Header));
        let mut reader = Reader { delta, at: 0 };
        assert_eq!(reader.number(), Err(DeltaError::Number));
    }

    #[test]
    fn a_byte_with_the_high_bit_set_is_no_digit() {
        // 0xb0 is '0' with its high bit set, which centrifuge-js reads as
        // '0' (D3).
        let delta = b"\xb0\n0;";

        assert_eq!(apply(b"", delta, LIMIT), Err(DeltaError::Header));
    }

    #[test]
    fn a_target_larger_than_the_limit_fails_before_anything_is_allocated() {
        // 18 bytes declaring 2^32 - 1 bytes: refused from the header alone.
        let delta = b"3~~~~~\n3~~~~~@0,0;";

        assert_eq!(apply(b"", delta, 1024), Err(DeltaError::TooLarge));
    }

    #[test]
    fn written_deltas_copy_what_base_and_target_share() {
        let base = b"the quick brown fox jumps";
        let target = b"the quick red fox jumps";

        let delta = write(base, target);

        assert!(
            delta.len() < target.len(),
            "{}",
            String::from_utf8_lossy(&delta)
        );
        assert_eq!(apply(base, &delta, LIMIT).as_deref(), Ok(&target[..]));
    }

    /// Fuzz target (Block 19, D1): any delta against any base either fails
    /// or builds a target within the limit, and never panics.
    /// Coverage-guided with `cargo bolero test fossil::tests::fuzz_apply
    /// --engine libfuzzer`.
    #[test]
    fn fuzz_apply() {
        bolero::check!()
            .with_type::<(Vec<u8>, Vec<u8>)>()
            .for_each(|(base, delta)| {
                if let Ok(target) = apply(base, delta, 4096) {
                    assert!(target.len() <= 4096);
                }
            });
    }

    /// Fuzz target (Block 19, D1): a delta the tests' writer makes from any
    /// base to any target rebuilds that target.
    #[test]
    fn fuzz_written_delta_round_trip() {
        bolero::check!()
            .with_type::<(Vec<u8>, Vec<u8>)>()
            .for_each(|(base, target)| {
                let delta = write(base, target);
                assert_eq!(apply(base, &delta, LIMIT).as_deref(), Ok(&target[..]));
            });
    }
}
