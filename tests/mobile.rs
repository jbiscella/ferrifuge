//! Runs the device program of `tests/mobile_device.rs` on an Android
//! emulator or an iOS simulator against a real Centrifugo (Block 58, D1),
//! as `tests/browser.rs` runs the @W tests in browsers.
//!
//! `FERRIFUGE_MOBILE` is `android` or `ios`; unset, the run is skipped.
//! `FERRIFUGE_MOBILE_BINARY` is the device program: its path on the
//! emulator for Android, run through `adb shell`, and on the host for iOS,
//! run through `xcrun simctl spawn` on the simulator `FERRIFUGE_MOBILE_DEVICE`
//! names (a UDID). The server is a container on Linux and the binary
//! `FERRIFUGE_CENTRIFUGO` names on macOS. `FERRIFUGE_MOBILE_ARTIFACTS`, when
//! set, is where the program's lines and the servers' output are written.
//!
//! On `ready` the harness publishes, through the server API, a publication
//! carrying a nonce new to the run. A run passes when every step reported
//! `ok`, `done` arrived, the program exited 0, and the server then lists no
//! client on the channel within 10 s. On iOS a second server serves TLS
//! with a chain made for the run: the program first runs in `untrusted`
//! mode, then the CA is added with `simctl keychain add-root-cert`, then
//! the program runs all its steps over `wss://`.

mod support;

use std::env;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use support::centrifugo::{API_KEY, Centrifugo, ServerApi};

/// Anonymous connect and subscribe on channels without a namespace.
const SERVER: &[(&str, &str)] = &[
    (
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_ANONYMOUS",
        "true",
    ),
];

/// The bound of one run of the program.
const RUN_LIMIT: Duration = Duration::from_secs(180);

/// How long the server may still list a client on the channel after the
/// program is done.
const GONE_WITHIN: Duration = Duration::from_secs(10);

/// The device program's directory on the emulator (`ci/mobile-android.sh`).
const ANDROID_DIR: &str = "/data/local/tmp";

/// 10.0.2.2 is the emulator's alias for the host's loopback, where Docker
/// publishes the container's port.
const ANDROID_HOST: &str = "10.0.2.2";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Platform {
    Android,
    Ios,
}

fn artifacts() -> PathBuf {
    let dir = env::var_os("FERRIFUGE_MOBILE_ARTIFACTS").map_or_else(
        || PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("mobile"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&dir).expect("the artifacts directory");
    dir
}

/// A string no earlier run made.
fn nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!("{nanos:x}{:x}", std::process::id())
}

/// A port nothing listens on, for a Centrifugo binary.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

/// The server: a container, or a Centrifugo binary run here.
enum Server {
    Container(Box<Centrifugo>),
    Process {
        child: Child,
        port: u16,
        api: ServerApi,
    },
}

impl Server {
    /// A container with plain websocket.
    async fn container() -> Self {
        Self::Container(Box::new(Centrifugo::start(SERVER).await))
    }

    /// The binary `FERRIFUGE_CENTRIFUGO` names, on 127.0.0.1, with TLS
    /// when `tls` holds a certificate and its key; its output goes to
    /// `<name>.log` among the artifacts.
    async fn binary(name: &str, tls: Option<(&str, &str)>) -> Self {
        let binary = env::var_os("FERRIFUGE_CENTRIFUGO")
            .expect("FERRIFUGE_CENTRIFUGO names the Centrifugo binary");
        let port = free_port();
        let internal = if tls.is_some() { free_port() } else { port };
        let log = std::fs::File::create(artifacts().join(format!("{name}.log")))
            .expect("the server's log file");
        let mut command = Command::new(binary);
        command
            .envs(SERVER.iter().copied())
            .env("CENTRIFUGO_HTTP_API_KEY", API_KEY)
            .env("CENTRIFUGO_HTTP_SERVER_ADDRESS", "127.0.0.1")
            .env("CENTRIFUGO_HTTP_SERVER_PORT", port.to_string())
            .env("CENTRIFUGO_HEALTH_ENABLED", "true")
            .stdout(log.try_clone().expect("the server's log file"))
            .stderr(log);
        if let Some((cert, key)) = tls {
            command
                .env("CENTRIFUGO_HTTP_SERVER_INTERNAL_PORT", internal.to_string())
                .env("CENTRIFUGO_HTTP_SERVER_TLS_ENABLED", "true")
                .env("CENTRIFUGO_HTTP_SERVER_TLS_CERT_PEM", cert)
                .env("CENTRIFUGO_HTTP_SERVER_TLS_KEY_PEM", key)
                .env("CENTRIFUGO_HTTP_SERVER_TLS_EXTERNAL", "true");
        }
        let child = command.spawn().expect("start Centrifugo");
        let base = format!("http://127.0.0.1:{internal}");
        let started = Instant::now();
        while !reqwest::get(format!("{base}/health"))
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "Centrifugo did not start"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Self::Process {
            child,
            port,
            api: ServerApi::new(&base),
        }
    }

    /// The port of the client endpoints, on the host.
    fn port(&self) -> u16 {
        match self {
            Self::Container(server) => server
                .address()
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse().ok())
                .expect("the container's port"),
            Self::Process { port, .. } => *port,
        }
    }

    fn api(&self) -> &ServerApi {
        match self {
            Self::Container(server) => server.api(),
            Self::Process { api, .. } => api,
        }
    }

    async fn stop(self, name: &str) {
        match self {
            Self::Container(server) => {
                let logs = server.last_logs().join("\n");
                let _ = std::fs::write(artifacts().join(format!("{name}.log")), logs);
                server.stop().await;
            }
            Self::Process { mut child, .. } => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// One run of the device program.
struct Run<'a> {
    /// Names its log among the artifacts.
    name: &'a str,
    url: String,
    /// `untrusted`: only `connect`, which must be refused.
    untrusted: bool,
}

/// The device program's command, with its variables.
fn command(platform: Platform, variables: &[(&str, String)]) -> Command {
    let binary = env::var("FERRIFUGE_MOBILE_BINARY").expect("FERRIFUGE_MOBILE_BINARY is set");
    match platform {
        Platform::Android => {
            let assignments: Vec<String> = variables
                .iter()
                .map(|(name, value)| format!("{name}='{value}'"))
                .collect();
            let mut command = Command::new("adb");
            command.args([
                "shell",
                &format!("cd {ANDROID_DIR} && {} {binary}", assignments.join(" ")),
            ]);
            command
        }
        Platform::Ios => {
            let device =
                env::var("FERRIFUGE_MOBILE_DEVICE").expect("FERRIFUGE_MOBILE_DEVICE is set");
            let mut command = Command::new("xcrun");
            command.args(["simctl", "spawn", &device, &binary]);
            for (name, value) in variables {
                command.env(format!("SIMCTL_CHILD_{name}"), value);
            }
            command
        }
    }
}

/// Stops a device process the host process left behind.
fn kill_on_device(platform: Platform) {
    let binary = env::var("FERRIFUGE_MOBILE_BINARY").unwrap_or_default();
    let name = binary.rsplit('/').next().unwrap_or_default().to_owned();
    let _ = match platform {
        Platform::Android => Command::new("adb")
            .args(["shell", "pkill", "-x", &name])
            .status(),
        // A simulator's processes are the host's.
        Platform::Ios => Command::new("pkill").args(["-f", &binary]).status(),
    };
}

/// What one run of the program reported.
#[derive(Default)]
struct Reported {
    lines: Vec<String>,
    /// The name of the last step reported.
    last_step: String,
    /// The first failure seen.
    failed: Option<String>,
    done: bool,
}

/// Reads the program's lines until it ends or [`RUN_LIMIT`] passes,
/// publishing the nonce through `server`'s API on `ready`.
fn watch(
    runtime: &tokio::runtime::Runtime,
    platform: Platform,
    server: &Server,
    child: &mut Child,
    (name, channel, nonce): (&str, &str, &str),
) -> Reported {
    let stdout = child.stdout.take().expect("the program's output");
    let (lines, received) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let started = Instant::now();
    let mut reported = Reported {
        last_step: "none".to_owned(),
        ..Reported::default()
    };
    loop {
        let left = RUN_LIMIT.saturating_sub(started.elapsed());
        match received.recv_timeout(left) {
            Ok(line) => {
                eprintln!("{name}: {line}");
                if let Some(step) = line.strip_prefix("step ") {
                    step.split(' ')
                        .next()
                        .unwrap_or_default()
                        .clone_into(&mut reported.last_step);
                    if !step.ends_with(" ok") {
                        reported.failed.get_or_insert_with(|| line.clone());
                    }
                } else if line == "ready" {
                    let published = runtime.block_on(server.api().call(
                        "publish",
                        serde_json::json!({"channel": channel, "data": {"nonce": nonce}}),
                    ));
                    if let Err(error) = published {
                        reported.failed.get_or_insert(format!("publish: {error:?}"));
                    }
                } else if line == "done" {
                    reported.done = true;
                }
                reported.lines.push(line);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = child.kill();
                kill_on_device(platform);
                reported.failed.get_or_insert(format!(
                    "no end within {RUN_LIMIT:?}; last step reported: {}",
                    reported.last_step
                ));
                break;
            }
        }
    }
    let _ = reader.join();
    reported
}

/// Runs the program once against `server`; whether the run passed.
fn run(runtime: &tokio::runtime::Runtime, platform: Platform, server: &Server, run: &Run) -> bool {
    let nonce = nonce();
    let channel = format!("mobile-{nonce}");
    let mut variables = vec![
        ("FERRIFUGE_MOBILE_URL", run.url.clone()),
        ("FERRIFUGE_MOBILE_CHANNEL", channel.clone()),
        ("FERRIFUGE_MOBILE_NONCE", nonce.clone()),
    ];
    if run.untrusted {
        variables.push(("FERRIFUGE_MOBILE_EXPECT", "untrusted".to_owned()));
    }
    eprintln!("== {}: {}", run.name, run.url);
    let mut child = command(platform, &variables)
        .stdout(Stdio::piped())
        .spawn()
        .expect("run the device program");
    let reported = watch(
        runtime,
        platform,
        server,
        &mut child,
        (run.name, &channel, &nonce),
    );
    let status = child.wait().expect("the program's status");
    eprintln!("{}: {status}", run.name);
    let _ = std::fs::write(
        artifacts().join(format!("{}.lines", run.name)),
        reported.lines.join("\n"),
    );
    let last_step = &reported.last_step;
    let failed = reported
        .failed
        .or_else(|| (!status.success()).then(|| format!("the program exited with {status}")))
        // An untrusted run ends after `connect`, with no `done`.
        .or_else(|| {
            (run.untrusted && last_step != "connect")
                .then(|| format!("no `connect` step; last step reported: {last_step}"))
        })
        .or_else(|| {
            (!run.untrusted && !reported.done)
                .then(|| format!("no `done`; last step reported: {last_step}"))
        })
        .or_else(|| {
            if run.untrusted {
                return None;
            }
            runtime.block_on(gone(server.api(), &channel)).err().map(|clients| {
                format!(
                    "the server still lists {clients} client(s) on {channel} {GONE_WITHIN:?} after the run"
                )
            })
        });
    match failed {
        None => {
            eprintln!("{}: passed", run.name);
            true
        }
        Some(reason) => {
            eprintln!("{}: FAILED: {reason}", run.name);
            false
        }
    }
}

/// Waits until the server lists no client on `channel`; else the number it
/// still lists after [`GONE_WITHIN`].
async fn gone(api: &ServerApi, channel: &str) -> Result<(), u64> {
    let started = Instant::now();
    loop {
        let listed = api
            .call("channels", serde_json::json!({"pattern": channel}))
            .await
            .expect("the channels API");
        let clients = listed["channels"][channel]["num_clients"]
            .as_u64()
            .unwrap_or_default();
        if clients == 0 {
            return Ok(());
        }
        if started.elapsed() > GONE_WITHIN {
            return Err(clients);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Trusts `ca` on the simulator `FERRIFUGE_MOBILE_DEVICE` names.
fn add_root_cert(ca: &std::path::Path) -> bool {
    let device = env::var("FERRIFUGE_MOBILE_DEVICE").expect("FERRIFUGE_MOBILE_DEVICE is set");
    let status = Command::new("xcrun")
        .args(["simctl", "keychain", &device, "add-root-cert"])
        .arg(ca)
        .status()
        .expect("run simctl keychain");
    eprintln!("simctl keychain add-root-cert: {status}");
    status.success()
}

fn android(runtime: &tokio::runtime::Runtime) -> bool {
    let server = runtime.block_on(Server::container());
    let url = format!("ws://{ANDROID_HOST}:{}/connection/websocket", server.port());
    let passed = run(
        runtime,
        Platform::Android,
        &server,
        &Run {
            name: "android-ws",
            url,
            untrusted: false,
        },
    );
    runtime.block_on(server.stop("centrifugo"));
    passed
}

fn ios(runtime: &tokio::runtime::Runtime) -> bool {
    let plain = runtime.block_on(Server::binary("centrifugo", None));
    let url = format!("ws://127.0.0.1:{}/connection/websocket", plain.port());
    let mut passed = run(
        runtime,
        Platform::Ios,
        &plain,
        &Run {
            name: "ios-ws",
            url,
            untrusted: false,
        },
    );
    runtime.block_on(plain.stop("centrifugo"));

    let chain = support::tls::run_chain();
    let tls = runtime.block_on(Server::binary(
        "centrifugo-tls",
        Some((&chain.server_pem, &chain.key_pem)),
    ));
    let url = format!("wss://127.0.0.1:{}/connection/websocket", tls.port());
    // Refused before the CA is trusted, then trusted by the device alone.
    passed &= run(
        runtime,
        Platform::Ios,
        &tls,
        &Run {
            name: "ios-wss-untrusted",
            url: url.clone(),
            untrusted: true,
        },
    );
    passed &= add_root_cert(&chain.ca_path);
    passed &= run(
        runtime,
        Platform::Ios,
        &tls,
        &Run {
            name: "ios-wss",
            url,
            untrusted: false,
        },
    );
    runtime.block_on(tls.stop("centrifugo-tls"));
    passed
}

fn main() -> ExitCode {
    let platform = match env::var("FERRIFUGE_MOBILE").as_deref() {
        Ok("android") => Platform::Android,
        Ok("ios") => Platform::Ios,
        Ok(other) => panic!("unknown FERRIFUGE_MOBILE {other:?}: android or ios"),
        Err(_) => {
            eprintln!("mobile runs skipped: FERRIFUGE_MOBILE names no platform");
            return ExitCode::SUCCESS;
        }
    };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let passed = match platform {
        Platform::Android => android(&runtime),
        Platform::Ios => ios(&runtime),
    };
    if passed {
        ExitCode::SUCCESS
    } else {
        eprintln!("mobile runs failed");
        ExitCode::FAILURE
    }
}
