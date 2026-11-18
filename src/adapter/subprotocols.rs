//! The websocket subprotocols a client offers (Block 67).

use std::sync::Arc;

use crate::{ClientError, ErrorKind, Protocol, codes};

/// The message of an attempt whose server did not select the encoding's
/// subprotocol; it names no value (Block 67, D4).
pub(in crate::adapter) const NOT_ACCEPTED: &str = "websocket subprotocol not accepted";

/// The values a websocket upgrade offers: the encoding's own first, then
/// the application's (D3).
#[derive(Clone)]
pub(in crate::adapter) struct Offer {
    values: Arc<[String]>,
    /// The application set values: the server must select the first.
    custom: bool,
    /// The encoding is Protobuf (D7: read apart from the offer).
    pub(in crate::adapter) protobuf: bool,
}

/// The count only: the values may carry identifiers (D5).
impl std::fmt::Debug for Offer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Offer")
            .field("values", &self.values.len())
            .field("custom", &self.custom)
            .field("protobuf", &self.protobuf)
            .finish()
    }
}

impl Offer {
    /// The offer of `protocol` with the application's `custom` values,
    /// already checked by [`check`]: with none, the encoding's own value
    /// alone, if it has one; with some, `centrifuge-json` for JSON.
    pub(in crate::adapter) fn new(protocol: Protocol, custom: &[String]) -> Self {
        let own = match protocol {
            Protocol::Protobuf => Some("centrifuge-protobuf"),
            Protocol::Json => (!custom.is_empty()).then_some("centrifuge-json"),
        };
        Self {
            values: own
                .map(str::to_owned)
                .into_iter()
                .chain(custom.iter().cloned())
                .collect(),
            custom: !custom.is_empty(),
            protobuf: protocol == Protocol::Protobuf,
        }
    }

    /// The values in the order offered.
    #[cfg_attr(not(adapter_browser), allow(dead_code, reason = "the browser's"))]
    pub(in crate::adapter) fn values(&self) -> &[String] {
        &self.values
    }

    /// The one `Sec-WebSocket-Protocol` header's value, if any (D3).
    #[cfg_attr(not(adapter_tokio), allow(dead_code, reason = "the tokio adapter's"))]
    pub(in crate::adapter) fn header(&self) -> Option<String> {
        (!self.values.is_empty()).then(|| self.values.join(", "))
    }

    /// Whether the application set values of its own.
    pub(in crate::adapter) fn custom(&self) -> bool {
        self.custom
    }

    /// Whether the server's `selected` value, if any, lets the attempt go
    /// on: with the application's values, only the encoding's own (D4).
    pub(in crate::adapter) fn accepts(&self, selected: Option<&str>) -> bool {
        !self.custom
            || selected
                .is_some_and(|selected| Some(selected) == self.values.first().map(String::as_str))
    }
}

/// Whether `value` is an RFC 6455 token: non-empty, U+0021 to U+007E,
/// none of RFC 2616's separators.
fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && !b"()<>@,;:\\\"/[]?={}".contains(&byte))
}

/// Checks the application's values (D2): each a token, none repeated,
/// none of the encodings'; the error names the value's index, never it.
pub(in crate::adapter) fn check(values: &[String]) -> Result<(), ClientError> {
    for (index, value) in values.iter().enumerate() {
        let problem = if !is_token(value) {
            Some("is not a valid token")
        } else if values[..index].contains(value) {
            Some("repeats an earlier one")
        } else if value == "centrifuge-json" || value == "centrifuge-protobuf" {
            Some("is reserved for the encoding")
        } else {
            None
        };
        if let Some(problem) = problem {
            return Err(ClientError {
                kind: ErrorKind::Configuration,
                code: codes::error::BAD_CONFIGURATION,
                message: format!("websocket subprotocol {index} {problem}"),
                transport: None,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn without_values_the_offer_is_the_encodings_alone() {
        let json = Offer::new(Protocol::Json, &[]);
        let protobuf = Offer::new(Protocol::Protobuf, &[]);

        assert_eq!((json.header(), json.custom()), (None, false));
        assert_eq!(
            (protobuf.header().as_deref(), protobuf.custom()),
            (Some("centrifuge-protobuf"), false)
        );
        assert!(json.accepts(None) && protobuf.accepts(Some("centrifuge-protobuf")));
    }

    #[test]
    fn values_follow_the_encodings_own_and_only_it_is_accepted() {
        let values = strings(&["tenant-a", "region-1"]);
        let json = Offer::new(Protocol::Json, &values);
        let protobuf = Offer::new(Protocol::Protobuf, &values);

        assert_eq!(
            json.header().as_deref(),
            Some("centrifuge-json, tenant-a, region-1")
        );
        assert_eq!(
            protobuf.header().as_deref(),
            Some("centrifuge-protobuf, tenant-a, region-1")
        );
        assert!(json.accepts(Some("centrifuge-json")));
        assert!(!json.accepts(Some("tenant-a")));
        assert!(!json.accepts(None));
        assert!(!protobuf.accepts(Some("centrifuge-json")));
        assert!(!json.protobuf && protobuf.protobuf);
    }

    #[test]
    fn the_debug_output_counts_the_values() {
        let offer = Offer::new(Protocol::Json, &strings(&["tenant-secret-42"]));

        let shown = format!("{offer:?}");

        assert!(shown.contains("values: 2"), "{shown}");
        assert!(!shown.contains("tenant-secret-42"), "{shown}");
    }

    #[test]
    fn a_value_not_a_token_repeated_or_reserved_is_refused_by_index() {
        for (values, index, problem) in [
            (&["tenant a"][..], 0, "is not a valid token"),
            (&["a", "b,c"], 1, "is not a valid token"),
            (&["a", ""], 1, "is not a valid token"),
            (&["tenänt"], 0, "is not a valid token"),
            (&["a", "b", "a"], 2, "repeats an earlier one"),
            (&["centrifuge-json"], 0, "is reserved for the encoding"),
            (
                &["a", "centrifuge-protobuf"],
                1,
                "is reserved for the encoding",
            ),
        ] {
            let error = check(&strings(values)).expect_err("refused");

            assert_eq!(
                error.message,
                format!("websocket subprotocol {index} {problem}"),
                "{values:?}"
            );
            assert_eq!(error.kind, ErrorKind::Configuration);
        }
        assert!(check(&strings(&["tenant-a", "x!#$%&'*+-.^_`|~9"])).is_ok());
    }
}
