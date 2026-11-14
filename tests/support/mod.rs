//! Test infrastructure shared by the integration test targets.

#![allow(dead_code)] // Each test target uses a different subset.

pub(crate) mod bug_catalogue;
pub(crate) mod centrifugo;
pub(crate) mod fake_server;
#[cfg(feature = "unstable-webtransport")]
pub(crate) mod fake_webtransport;
pub(crate) mod forward_proxy;
pub(crate) mod http_proxy;
pub(crate) mod invariants;
pub(crate) mod logs;
pub(crate) mod perf_server;
pub(crate) mod protobuf;
pub(crate) mod proxy;
pub(crate) mod real_server;
pub(crate) mod relay;
pub(crate) mod tls;
pub(crate) mod token;
#[cfg(feature = "unstable-webtransport")]
pub(crate) mod webtransport_relay;
