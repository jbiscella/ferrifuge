//! A module using ferrifuge, whose imports `check.mjs` reads.

use ferrifuge::Config;
use ferrifuge::machine::{Input, Machine};

/// The core alone: a machine starting to connect.
#[unsafe(no_mangle)]
pub extern "C" fn connect_actions() -> usize {
    let mut machine = Machine::new(Config::default());
    machine.handle(Input::Connect).len()
}

/// The browser adapter: a client connecting over websocket.
#[cfg(feature = "browser")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn connect_client(endpoint: &str) -> bool {
    ferrifuge::Client::new(endpoint, Config::default())
        .map(|client| client.connect())
        .is_ok()
}
