//! Server-side filters of publications by their tags (Block 35a).

/// A filter of a subscription's publications by their tags, applied by the
/// server, which sends only the publications it matches (`client.proto`
/// `FilterNode`; Centrifugo "Channel publication filtering", channel option
/// `allow_tags_filter`). It saves bandwidth; it is not access control.
///
/// Built from comparisons on one tag, combined with [`TagFilter::and`],
/// [`TagFilter::or`] and `!`: an empty combination cannot be written.
///
/// ```
/// use ferrifuge::TagFilter;
///
/// let filter = TagFilter::is_in("ticker", ["BTC", "ETH"]).and(!TagFilter::eq("venue", "test"));
/// let any = [TagFilter::eq("a", "1"), TagFilter::eq("b", "2")]
///     .into_iter()
///     .reduce(TagFilter::or);
/// assert!(any.is_some());
/// # let _ = filter;
/// ```
///
/// A filter cannot go with delta compression (`client.proto`: a filter
/// "disables delta compression"). Its `Debug` output shows the tag keys and
/// the operators, never the values.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TagFilter(pub(crate) Box<Node>);

/// The `FilterNode` a filter is sent as: `op` empty for a comparison on
/// `key`, else `"and"`, `"or"` or `"not"` over `nodes`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Node {
    pub(crate) op: &'static str,
    pub(crate) key: String,
    pub(crate) cmp: &'static str,
    pub(crate) val: String,
    pub(crate) vals: Vec<String>,
    pub(crate) nodes: Vec<Node>,
}

impl TagFilter {
    fn leaf(key: impl Into<String>, cmp: &'static str, val: impl Into<String>) -> Self {
        Self(Box::new(Node {
            op: "",
            key: key.into(),
            cmp,
            val: val.into(),
            vals: Vec::new(),
            nodes: Vec::new(),
        }))
    }

    fn set(
        key: impl Into<String>,
        cmp: &'static str,
        vals: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self(Box::new(Node {
            op: "",
            key: key.into(),
            cmp,
            val: String::new(),
            vals: vals.into_iter().map(Into::into).collect(),
            nodes: Vec::new(),
        }))
    }

    /// The tag `key` equals `value`.
    #[must_use]
    pub fn eq(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "eq", value)
    }

    /// The tag `key` differs from `value`; a missing tag matches.
    #[must_use]
    pub fn neq(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "neq", value)
    }

    /// The tag `key` is one of `values`; a missing tag is compared as an
    /// empty string. At least one value, or attaching it fails.
    #[doc(alias = "in")]
    #[must_use]
    pub fn is_in(
        key: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::set(key, "in", values)
    }

    /// The tag `key` is none of `values`. At least one value, or attaching
    /// it fails.
    #[doc(alias = "nin")]
    #[must_use]
    pub fn not_in(
        key: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::set(key, "nin", values)
    }

    /// The tag `key` is present.
    #[doc(alias = "ex")]
    #[must_use]
    pub fn exists(key: impl Into<String>) -> Self {
        Self::leaf(key, "ex", "")
    }

    /// The tag `key` is absent.
    #[doc(alias = "nex")]
    #[must_use]
    pub fn not_exists(key: impl Into<String>) -> Self {
        Self::leaf(key, "nex", "")
    }

    /// The tag `key` starts with `prefix`.
    #[doc(alias = "sw")]
    #[must_use]
    pub fn starts_with(key: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::leaf(key, "sw", prefix)
    }

    /// The tag `key` ends with `suffix`.
    #[doc(alias = "ew")]
    #[must_use]
    pub fn ends_with(key: impl Into<String>, suffix: impl Into<String>) -> Self {
        Self::leaf(key, "ew", suffix)
    }

    /// The tag `key` contains `part`.
    #[doc(alias = "ct")]
    #[must_use]
    pub fn contains(key: impl Into<String>, part: impl Into<String>) -> Self {
        Self::leaf(key, "ct", part)
    }

    /// The tag `key`, a decimal number, is less than `value` (decimal
    /// text); a tag that is not a number never matches.
    #[must_use]
    pub fn lt(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "lt", value)
    }

    /// As [`TagFilter::lt`], less than or equal.
    #[must_use]
    pub fn lte(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "lte", value)
    }

    /// As [`TagFilter::lt`], greater than.
    #[must_use]
    pub fn gt(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "gt", value)
    }

    /// As [`TagFilter::lt`], greater than or equal.
    #[must_use]
    pub fn gte(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::leaf(key, "gte", value)
    }

    /// Both `self` and `other` match; chained `and`s make one node.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::combine("and", self, other)
    }

    /// `self` or `other` matches; chained `or`s make one node.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::combine("or", self, other)
    }

    fn combine(op: &'static str, left: Self, right: Self) -> Self {
        let mut nodes = Vec::new();
        for Self(node) in [left, right] {
            if node.op == op {
                nodes.extend(node.nodes);
            } else {
                nodes.push(*node);
            }
        }
        Self(Box::new(Node {
            op,
            key: String::new(),
            cmp: "",
            val: String::new(),
            vals: Vec::new(),
            nodes,
        }))
    }

    /// What no signature prevents and the server refuses (Block 35, D1): a
    /// comparison but `exists` and `not_exists` with an empty key, a set
    /// comparison with no value.
    pub(crate) fn check(&self) -> Result<(), String> {
        fn walk(node: &Node) -> Result<(), String> {
            if node.op.is_empty() {
                if node.key.is_empty() && !matches!(node.cmp, "ex" | "nex") {
                    return Err(format!("a {:?} comparison with an empty key", node.cmp));
                }
                if matches!(node.cmp, "in" | "nin") && node.vals.is_empty() {
                    return Err(format!(
                        "a {:?} comparison on {:?} with no value",
                        node.cmp, node.key
                    ));
                }
                return Ok(());
            }
            node.nodes.iter().try_for_each(walk)
        }
        walk(&self.0)
    }
}

impl std::ops::Not for TagFilter {
    type Output = Self;

    /// `self` does not match.
    fn not(self) -> Self {
        Self(Box::new(Node {
            op: "not",
            key: String::new(),
            cmp: "",
            val: String::new(),
            vals: Vec::new(),
            nodes: vec![*self.0],
        }))
    }
}

/// Keys and operators only: tag values may be personal data (Block 21, D5).
impl std::fmt::Debug for TagFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write(node: &Node, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if node.op.is_empty() {
                return write!(f, "{} {}", node.key, node.cmp);
            }
            write!(f, "{}(", node.op)?;
            for (index, child) in node.nodes.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write(child, f)?;
            }
            f.write_str(")")
        }
        f.write_str("TagFilter(")?;
        write(&self.0, f)?;
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chained_ands_and_ors_make_one_node() {
        let filter = TagFilter::eq("a", "1")
            .and(TagFilter::eq("b", "2"))
            .and(TagFilter::eq("c", "3"));
        let either =
            TagFilter::eq("a", "1").or(TagFilter::eq("b", "2").or(TagFilter::eq("c", "3")));

        assert_eq!((filter.0.op, filter.0.nodes.len()), ("and", 3));
        assert_eq!((either.0.op, either.0.nodes.len()), ("or", 3));
    }

    #[test]
    fn not_wraps_one_node_and_mixed_operations_nest() {
        let filter = !TagFilter::eq("a", "1").or(TagFilter::eq("b", "2"));
        let mixed =
            TagFilter::eq("a", "1").and(TagFilter::eq("b", "2").or(TagFilter::eq("c", "3")));

        assert_eq!((filter.0.op, filter.0.nodes.len()), ("not", 1));
        assert_eq!(mixed.0.nodes[1].op, "or");
    }

    #[test]
    fn the_check_refuses_what_the_server_would() {
        assert!(TagFilter::eq("", "1").check().is_err());
        assert!(
            TagFilter::is_in("ticker", Vec::<String>::new())
                .check()
                .is_err()
        );
        assert!(TagFilter::exists("").check().is_ok());
        assert!(
            TagFilter::eq("a", "1")
                .and(TagFilter::not_in("b", Vec::<String>::new()))
                .check()
                .is_err()
        );
        assert!(TagFilter::is_in("ticker", ["BTC"]).check().is_ok());
    }

    #[test]
    fn debug_shows_keys_and_operators_never_values() {
        let filter =
            TagFilter::eq("ticker", "secret-value").and(!TagFilter::is_in("venue", ["v-secret"]));

        let debug = format!("{filter:?}");

        assert_eq!(debug, "TagFilter(and(ticker eq, not(venue in)))");
        assert!(!debug.contains("secret"));
    }
}
