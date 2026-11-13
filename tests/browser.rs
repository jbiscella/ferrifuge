//! Runs the @W tests of `tests/browser_wasm.rs`, and the browser adapter's
//! wasm unit tests, in browsers (Block 44, D18): starts Centrifugo and the
//! relay in front of it, then runs `cargo test` for wasm32-unknown-unknown
//! under `wasm-bindgen-test-runner` once per browser, with the server's and
//! the relay's addresses given at compile time.
//!
//! `FERRIFUGE_BROWSERS` names the browsers, comma-separated, among
//! `chrome`, `firefox` and `safari`; unset, the run is skipped, as on a
//! machine without a browser driver. Each browser's driver is found as
//! `wasm-bindgen-test-runner` takes it (`CHROMEDRIVER`, `GECKODRIVER`,
//! `SAFARIDRIVER`), else in the directory `ubuntu-latest` names
//! (`CHROMEWEBDRIVER`, `GECKOWEBDRIVER`), else on the `PATH`.
//! `FERRIFUGE_CENTRIFUGO` names a Centrifugo binary to run instead of the
//! container, for runners without Docker (macOS).
//!
//! Built with `unstable-webtransport`, it also starts a Centrifugo serving
//! TLS with HTTP/3 and WebTransport, in a container, and the WebTransport
//! relay in front of it, which offers `h3` as browsers require (Block 48,
//! D19). Its endpoint goes in `FERRIFUGE_W_WEBTRANSPORT`, its commands on
//! the HTTP relay's control path; each browser gets the capabilities that
//! make it trust the relay's certificate: Chrome the SPKI flag with QUIC
//! forced on the relay's origin, Firefox `acceptInsecureCerts` with HTTP/3
//! allowed under a root it does not ship
//! (`network.http.http3.disable_when_third_party_roots_found`). Safari's
//! driver takes no capability from the runner.

mod support;

use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use support::centrifugo::{API_KEY, Centrifugo};
use support::http_proxy::HttpProxy;
use support::token::SHARED_POLL_SECRET;

/// The server's configuration (Block 44, D18): the emulated transports,
/// any origin, the `history` namespace with history and recovery, and
/// client publish, for anonymous clients; the `map` namespace (Block 45,
/// D16); the `poll_v` namespace of shared poll channels in versioned mode,
/// written by the server API's `shared_poll_publish` through the relay,
/// its refresh endpoint one nothing listens on (Block 47, D17, D18); the
/// API key, for a Centrifugo binary run here.
const SERVER: &[(&str, &str)] = &[
    (
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    ),
    ("CENTRIFUGO_CLIENT_ALLOWED_ORIGINS", "*"),
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_NAMESPACES",
        r#"[{"name":"history","history_size":300,"history_ttl":"300s","force_recovery":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true,"allow_publish_for_client":true,"allow_publish_for_anonymous":true},{"name":"map","subscription_type":"map","map":{"mode":"persistent","stream_size":100,"stream_ttl":"300s","min_page_size":1,"default_page_size":2,"allow_publish_for_client":true,"allow_publish_for_anonymous":true,"allow_remove_for_client":true,"allow_remove_for_anonymous":true},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"poll_v","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s","publish_enabled":true},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true}]"#,
    ),
    ("CENTRIFUGO_SHARED_POLL_HMAC_SECRET_KEY", SHARED_POLL_SECRET),
    (
        "CENTRIFUGO_CHANNEL_PROXY_SHARED_POLL_REFRESH_ENDPOINT",
        "http://127.0.0.1:9/",
    ),
    ("CENTRIFUGO_HTTP_API_KEY", API_KEY),
];

/// The longest one browser's run may take, its build included.
const RUN_LIMIT: Duration = Duration::from_secs(30 * 60);

/// A browser, and the variable naming its driver.
struct Browser {
    name: &'static str,
    variable: &'static str,
    /// The variable of `ubuntu-latest` naming the driver's directory.
    directory: Option<&'static str>,
    executable: &'static str,
}

const BROWSERS: &[Browser] = &[
    Browser {
        name: "chrome",
        variable: "CHROMEDRIVER",
        directory: Some("CHROMEWEBDRIVER"),
        executable: "chromedriver",
    },
    Browser {
        name: "firefox",
        variable: "GECKODRIVER",
        directory: Some("GECKOWEBDRIVER"),
        executable: "geckodriver",
    },
    Browser {
        name: "safari",
        variable: "SAFARIDRIVER",
        directory: None,
        executable: "safaridriver",
    },
];

impl Browser {
    fn driver(&self) -> PathBuf {
        if let Some(path) = env::var_os(self.variable) {
            return path.into();
        }
        if let Some(directory) = self.directory.and_then(env::var_os) {
            return PathBuf::from(directory).join(self.executable);
        }
        self.executable.into()
    }
}

/// The server: a container, or a Centrifugo binary run here.
enum Server {
    Container(Box<Centrifugo>),
    Process(Child, String),
}

impl Server {
    async fn start() -> Self {
        let Some(binary) = env::var_os("FERRIFUGE_CENTRIFUGO") else {
            return Self::Container(Box::new(Centrifugo::start(SERVER).await));
        };
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let child = Command::new(binary)
            .envs(SERVER.iter().copied())
            .env("CENTRIFUGO_HTTP_SERVER_ADDRESS", "127.0.0.1")
            .env("CENTRIFUGO_HTTP_SERVER_PORT", port.to_string())
            .env("CENTRIFUGO_HEALTH_ENABLED", "true")
            .stdout(Stdio::null())
            .spawn()
            .expect("start Centrifugo");
        let address = format!("127.0.0.1:{port}");
        let health = format!("http://{address}/health");
        let started = Instant::now();
        while !reqwest::get(&health)
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "Centrifugo did not start"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Self::Process(child, address)
    }

    fn address(&self) -> &str {
        match self {
            Self::Container(server) => server.address(),
            Self::Process(_, address) => address,
        }
    }

    async fn stop(self) {
        match self {
            Self::Container(server) => server.stop().await,
            Self::Process(mut child, _) => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// The WebTransport server and the relay in front of it (Block 48, D19).
#[cfg(feature = "unstable-webtransport")]
struct WebTransport {
    server: Centrifugo,
    relay: support::webtransport_relay::WebTransportRelay,
}

#[cfg(feature = "unstable-webtransport")]
impl WebTransport {
    /// Started with the container only: hosted macOS has no Docker, and a
    /// Centrifugo binary run here serves no TLS.
    async fn start(http_relay: &HttpProxy) -> Option<Self> {
        if env::var_os("FERRIFUGE_CENTRIFUGO").is_some() {
            return None;
        }
        support::tls::create();
        let (cert, key) = support::tls::server_pem();
        let server = Centrifugo::start_http3(SERVER, &cert, &key, true).await;
        let relay = support::webtransport_relay::WebTransportRelay::start(
            server.webtransport_address().to_owned(),
        );
        let state = relay.state();
        // A handle of the API only: the harness keeps the server, to stop
        // it before the runtime ends.
        let api = server.api().clone();
        http_relay.set_extra_control(std::sync::Arc::new(move |command: String| {
            let state = std::sync::Arc::clone(&state);
            let api = api.clone();
            Box::pin(async move {
                if let Some(answer) = state.control(&command) {
                    return Some(answer);
                }
                // The server API, out of a page's reach (no CORS).
                let channel = command.strip_prefix("webtransport-publish/")?;
                let published = api
                    .call(
                        "publish",
                        serde_json::json!({"channel": channel, "data": {"from": "api"}}),
                    )
                    .await;
                Some(match published {
                    Ok(_) => "ok".to_owned(),
                    Err(error) => format!("{error:?}"),
                })
            })
        }));
        Some(Self { server, relay })
    }

    /// The capabilities that make `browser` trust the relay, in a file the
    /// runner reads (`WASM_BINDGEN_TEST_WEBDRIVER_JSON`).
    fn capabilities(&self, browser: &Browser) -> Option<PathBuf> {
        let capabilities = match browser.name {
            "chrome" => serde_json::json!({"goog:chromeOptions": {"args": [
                format!("--ignore-certificate-errors-spki-list={}", self.relay.spki_sha256()),
                format!("--origin-to-force-quic-on={}", self.relay.origin()),
            ]}}),
            "firefox" => serde_json::json!({
                "acceptInsecureCerts": true,
                "moz:firefoxOptions": {"prefs": {
                    "network.http.http3.disable_when_third_party_roots_found": false,
                }},
            }),
            _ => return None,
        };
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("webdriver-{}.json", browser.name));
        std::fs::write(&path, capabilities.to_string()).expect("write the capabilities");
        Some(path)
    }

    async fn stop(self) {
        drop(self.relay);
        self.server.stop().await;
    }
}

/// What a run in a browser gets about WebTransport: the relay's endpoint
/// and the browser's capabilities file.
#[derive(Default)]
struct WebTransportRun {
    endpoint: Option<String>,
    capabilities: Option<PathBuf>,
}

/// Runs the wasm tests in `browser`; whether they passed.
fn run_in(browser: &Browser, server: &str, relay: &str, webtransport: &WebTransportRun) -> bool {
    let root = env!("CARGO_MANIFEST_DIR");
    let mut command = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(root)
        .args([
            "test",
            "--target",
            "wasm32-unknown-unknown",
            "--no-default-features",
            "--features",
            "browser,unstable",
            "--lib",
            "--test",
            "browser_wasm",
        ])
        // Its own: the wasm build would otherwise wait for this one's lock.
        .env("CARGO_TARGET_DIR", format!("{root}/target/browser"))
        .env(
            "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER",
            "wasm-bindgen-test-runner",
        )
        .env("FERRIFUGE_W_SERVER", server)
        .env("FERRIFUGE_W_RELAY", relay);
    if let Some(endpoint) = &webtransport.endpoint {
        command.env("FERRIFUGE_W_WEBTRANSPORT", endpoint);
    }
    if let Some(capabilities) = &webtransport.capabilities {
        command.env("WASM_BINDGEN_TEST_WEBDRIVER_JSON", capabilities);
    }
    // The runner gives the whole run in a browser 20 s by default, not each
    // test; the run takes longer than that since 44e's sse tests.
    if env::var_os("WASM_BINDGEN_TEST_TIMEOUT").is_none() {
        command.env("WASM_BINDGEN_TEST_TIMEOUT", "300");
    }
    for other in BROWSERS {
        command.env_remove(other.variable);
    }
    command.env(browser.variable, browser.driver());
    let mut child = command.spawn().expect("run cargo test");
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("the run's status") {
            return status.success();
        }
        if started.elapsed() > RUN_LIMIT {
            eprintln!("{}: no result within {RUN_LIMIT:?}", browser.name);
            let _ = child.kill();
            let _ = child.wait();
            return false;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn main() -> ExitCode {
    let Ok(names) = env::var("FERRIFUGE_BROWSERS") else {
        eprintln!("browser tests skipped: FERRIFUGE_BROWSERS names no browser");
        return ExitCode::SUCCESS;
    };
    let browsers: Vec<&Browser> = names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| {
            BROWSERS
                .iter()
                .find(|browser| browser.name == name)
                .unwrap_or_else(|| panic!("unknown browser {name:?}"))
        })
        .collect();
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let (server, relay) = runtime.block_on(async {
        let server = Server::start().await;
        let relay = HttpProxy::start(server.address().to_owned()).await;
        (server, relay)
    });
    #[cfg(feature = "unstable-webtransport")]
    let webtransport = runtime.block_on(WebTransport::start(&relay));
    let mut failed = Vec::new();
    for browser in browsers {
        eprintln!("== {}", browser.name);
        #[cfg(feature = "unstable-webtransport")]
        let run = webtransport
            .as_ref()
            .map(|webtransport| WebTransportRun {
                endpoint: Some(webtransport.relay.url()),
                capabilities: webtransport.capabilities(browser),
            })
            .unwrap_or_default();
        #[cfg(not(feature = "unstable-webtransport"))]
        let run = WebTransportRun::default();
        if !run_in(browser, server.address(), &relay.address(), &run) {
            failed.push(browser.name);
        }
    }
    drop(relay);
    #[cfg(feature = "unstable-webtransport")]
    if let Some(webtransport) = webtransport {
        runtime.block_on(webtransport.stop());
    }
    runtime.block_on(server.stop());
    if failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        eprintln!("browser tests failed in {failed:?}");
        ExitCode::FAILURE
    }
}
