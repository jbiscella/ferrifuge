//! Test infrastructure shared by the integration test targets.

#![allow(dead_code)] // Each test target uses a different subset.

pub(crate) mod centrifugo;
pub(crate) mod fake_server;
pub(crate) mod proxy;
pub(crate) mod relay;
pub(crate) mod token;
