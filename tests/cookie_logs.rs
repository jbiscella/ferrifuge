//! A cookie `MemoryJar` stores reaches no `log` record, at any level
//! (Block 43, D6): `cookie_store`'s `store_response_cookies` would log a
//! non-Secure cookie with its value at DEBUG. In its own binary, which sets
//! the process's `log` logger.
#![cfg(feature = "cookies")]

use std::sync::Mutex;

use ferrifuge::http::HeaderValue;
use ferrifuge::{CookieJar, MemoryJar};
use log::{LevelFilter, Log, Metadata, Record};

/// Every record the logger got, as text.
static RECORDS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        RECORDS
            .lock()
            .unwrap()
            .push(format!("{} {}", record.target(), record.args()));
    }

    fn flush(&self) {}
}

#[test]
fn a_stored_cookie_reaches_no_log_record() {
    static CAPTURE: Capture = Capture;
    log::set_logger(&CAPTURE).expect("the only logger");
    log::set_max_level(LevelFilter::Trace);
    let jar = MemoryJar::new();
    let url = ferrifuge::url::Url::parse("http://edge.example.com/").unwrap();
    let cookie = HeaderValue::from_static("plain=v-1c0f-log; Path=/");

    jar.set_cookies(&mut std::iter::once(&cookie), &url);
    let sent = jar.cookies(&url);

    assert_eq!(sent, Some(HeaderValue::from_static("plain=v-1c0f-log")));
    let records = RECORDS.lock().unwrap();
    assert!(
        records.iter().all(|record| !record.contains("v-1c0f-log")),
        "{records:#?}"
    );
}
