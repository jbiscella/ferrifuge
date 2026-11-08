//! Proxies from the environment (Block 23, structural acceptance), in
//! their own binary: environment variables are process-wide, and setting
//! one in-process is `unsafe`. The parent runs the fake server and a test
//! proxy, then runs itself once per case with the variables set; the child
//! connects a client, and the parent checks what the proxy carried.

mod support;

use std::process::{Command, ExitCode};
use std::time::Duration;

use ferrifuge::machine::TransportKind;
use ferrifuge::{Client, ClientEvent, Proxy};
use futures_util::StreamExt;
use support::fake_server::{Announce, FakeServer};
use support::forward_proxy::{Carried, Mode, TestProxy};

/// The variable that tells the child which case to run.
const CASE: &str = "FERRIFUGE_PROXY_ENV_CASE";
const VARIABLES: [&str; 8] = [
    "ALL_PROXY",
    "all_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "NO_PROXY",
    "no_proxy",
];

fn main() -> ExitCode {
    // `cargo test <filter>` passes a filter to every test target.
    if std::env::args()
        .nth(1)
        .is_some_and(|arg| !arg.starts_with('-'))
    {
        return ExitCode::SUCCESS;
    }
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    match std::env::var(CASE) {
        Ok(case) => runtime.block_on(child(&case)),
        Err(_) => runtime.block_on(parent()),
    }
}

/// What a child reported, by its exit status.
async fn run(case: &str, env: &[(&str, String)], fake: &str) -> bool {
    let mut command = Command::new(std::env::current_exe().expect("this test"));
    for variable in VARIABLES {
        command.env_remove(variable);
    }
    command
        .env(CASE, case)
        .env("FERRIFUGE_PROXY_ENV_FAKE", fake);
    for (name, value) in env {
        command.env(name, value);
    }
    let status = tokio::task::spawn_blocking(move || command.status())
        .await
        .expect("the child ran")
        .expect("the child started");
    status.success()
}

async fn parent() -> ExitCode {
    let fake = FakeServer::start(Announce::default()).await;
    let mut failures = Vec::new();
    let mut check = |name: &str, ok: bool| {
        println!("{} {name}", if ok { "ok  " } else { "FAIL" });
        if !ok {
            failures.push(name.to_owned());
        }
    };

    // ALL_PROXY makes websocket go through the proxy (F2), and a plain
    // http_stream request reach it in absolute form.
    let proxy = TestProxy::http(fake.address(), false, None, Mode::Carry).await;
    let all = [("ALL_PROXY", format!("http://{}", proxy.address()))];
    let connected = run("websocket", &all, &fake.address()).await;
    let tunnelled = proxy.log().iter().any(|carriage| {
        carriage.carried == Carried::Tunnel && carriage.destination == fake.address()
    });
    check(
        "ALL_PROXY: websocket through the proxy",
        connected && tunnelled,
    );
    run("http_stream", &all, &fake.address()).await;
    let forwarded = proxy.log().iter().any(|carriage| {
        carriage.carried == Carried::Absolute && carriage.destination == "centrifugo.invalid:8000"
    });
    check("ALL_PROXY: http_stream in absolute form", forwarded);

    // NO_PROXY reaches the host directly.
    let proxy = TestProxy::http(fake.address(), false, None, Mode::Carry).await;
    let excluded = [
        ("ALL_PROXY", format!("http://{}", proxy.address())),
        ("NO_PROXY", "127.0.0.1".to_owned()),
    ];
    let connected = run("websocket", &excluded, &fake.address()).await;
    check("NO_PROXY: direct", connected && proxy.log().is_empty());

    // Proxy::none() ignores the environment.
    let proxy = TestProxy::http(fake.address(), false, None, Mode::Carry).await;
    let all = [("ALL_PROXY", format!("http://{}", proxy.address()))];
    let connected = run("websocket-none", &all, &fake.address()).await;
    check("Proxy::none(): direct", connected && proxy.log().is_empty());

    // A connector reads no proxy setting, the environment's included
    // (Block 37, D4).
    let proxy = TestProxy::http(fake.address(), false, None, Mode::Carry).await;
    let all = [("ALL_PROXY", format!("http://{}", proxy.address()))];
    let connected = run("websocket-connector", &all, &fake.address()).await;
    check(
        "a connector: its own stream, no proxy",
        connected && proxy.log().is_empty(),
    );

    // A scheme ProxyTarget refuses fails every transport, never direct.
    let socks4 = [("ALL_PROXY", "socks4://127.0.0.1:1".to_owned())];
    check(
        "socks4: websocket fails naming the scheme",
        run("websocket-refused", &socks4, &fake.address()).await,
    );
    check(
        "socks4: http_stream fails naming the scheme",
        run("http_stream-refused", &socks4, &fake.address()).await,
    );

    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        eprintln!("failed: {failures:?}");
        ExitCode::FAILURE
    }
}

/// Runs case `case` in the child: success when the client did what the
/// case expects.
async fn child(case: &str) -> ExitCode {
    let fake = std::env::var("FERRIFUGE_PROXY_ENV_FAKE").expect("the fake server");
    let websocket = format!("ws://{fake}/connection/websocket");
    let http_stream = "http://centrifugo.invalid:8000/connection/http_stream".to_owned();
    let builder = match case {
        "websocket" | "websocket-refused" => Client::builder(websocket),
        "websocket-none" => Client::builder(websocket).proxy(Proxy::none()),
        "websocket-connector" => {
            let target = fake.clone();
            Client::builder("ws://centrifugo.invalid/connection/websocket").connector(
                move |_: ferrifuge::http::Uri| tokio::net::TcpStream::connect(target.clone()),
            )
        }
        _ => Client::builder_with_transports([(TransportKind::HttpStream, http_stream)]),
    };
    let client = builder.build().expect("a client");
    let mut events = client.events();
    client.connect();
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.next().await {
            match event {
                ClientEvent::Connected { .. } => return Ok(()),
                ClientEvent::Error(error) => return Err(error.message),
                _ => {}
            }
        }
        Err("the stream ended".to_owned())
    })
    .await
    .unwrap_or_else(|_| Err("no outcome in time".to_owned()));
    let ok = match case {
        "websocket" | "websocket-none" | "websocket-connector" => outcome.is_ok(),
        // Reaching the proxy is what the parent checks.
        "http_stream" => true,
        _ => matches!(&outcome, Err(message) if message.contains("socks4")),
    };
    if !ok {
        eprintln!("{case}: {outcome:?}");
    }
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
