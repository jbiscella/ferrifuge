//! The fake server in a child process, for the `@P` harnesses (Block 14):
//! a copy of the running test binary, so that dhat counts only the client's
//! allocations. The child takes one command per line on stdin:
//! `publish <channel> <count>` sends that many JSON publications of 1 KiB,
//! `paced <channel> <count> <per second>` sends them at that rate, each
//! with its send time in microseconds since the Unix epoch (`sent`),
//! `close <code>` closes the connection with that code, `debounce <ms>`
//! advises that interval in subscribe replies. It exits when stdin closes.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, io, thread};

use super::fake_server::{Announce, FakeServer};

/// Makes a test binary the fake server.
const ROLE: &str = "FERRIFUGE_PERF_FAKE_SERVER";
/// The fake server's ping interval in seconds; no pings when unset.
const PING: &str = "FERRIFUGE_PERF_FAKE_SERVER_PING";
/// Set: the child also serves WebTransport, bridged to its fake server
/// (Block 48, D19), and its first line is that endpoint.
#[cfg(feature = "unstable-webtransport")]
const WEBTRANSPORT: &str = "FERRIFUGE_PERF_FAKE_SERVER_WEBTRANSPORT";
/// Payload of each publication, so that its JSON data is 1 KiB.
const TEXT_LEN: usize = 1024 - r#"{"t":""}"#.len();

/// Serves as the fake server if this process is the child, and then says
/// so: the caller's `main` returns at once.
pub(crate) fn serve_if_child() -> bool {
    if env::var_os(ROLE).is_none() {
        return false;
    }
    let (lines, mut commands) = tokio::sync::mpsc::unbounded_channel();
    thread::spawn(move || {
        for line in io::stdin().lines() {
            let Ok(line) = line else { break };
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let ping = env::var(PING).map_or(0, |ping| ping.parse().expect("a ping interval"));
        let server = FakeServer::start(Announce {
            ping,
            pong: ping > 0,
            ..Announce::default()
        })
        .await;
        if ping > 0 {
            server.ping_every(Duration::from_secs(ping.into()));
        }
        #[cfg(feature = "unstable-webtransport")]
        let _webtransport = if env::var_os(WEBTRANSPORT).is_some() {
            let webtransport =
                super::fake_webtransport::FakeWebTransport::start(&server, false).await;
            println!("{}", webtransport.url());
            Some(webtransport)
        } else {
            println!("{}", server.address());
            None
        };
        #[cfg(not(feature = "unstable-webtransport"))]
        println!("{}", server.address());
        io::stdout().flush().expect("stdout");
        let text = "x".repeat(TEXT_LEN);
        while let Some(line) = commands.recv().await {
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["publish", channel, count] => {
                    let publication = format!(
                        r#"{{"push":{{"channel":"{channel}","pub":{{"data":{{"t":"{text}"}}}}}}}}"#
                    );
                    for _ in 0..count.parse::<usize>().expect("a count") {
                        server.send(&publication);
                    }
                }
                ["paced", channel, count, per_second] => {
                    let period =
                        Duration::from_secs(1) / per_second.parse::<u32>().expect("a rate");
                    let mut ticks = tokio::time::interval(period);
                    for _ in 0..count.parse::<usize>().expect("a count") {
                        ticks.tick().await;
                        server.send(&stamped_publication(channel));
                    }
                }
                ["close", code] => server.close(code.parse().expect("a close code"), "perf"),
                ["debounce", interval] => {
                    server.advise_publish_debounce(Some(interval.parse().expect("an interval")));
                }
                _ => panic!("unknown command {line:?}"),
            }
        }
    });
    true
}

/// A publication of 1 KiB to `channel` carrying its send time.
fn stamped_publication(channel: &str) -> String {
    let sent = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros();
    // A plain JSON number: leading zeros would make the frame invalid.
    let data = format!(r#"{{"sent":{sent},"t":""}}"#);
    let text = "x".repeat(1024 - data.len());
    format!(
        r#"{{"push":{{"channel":"{channel}","pub":{{"data":{{"sent":{sent},"t":"{text}"}}}}}}}}"#
    )
}

/// The child process serving as the fake server; killed on drop.
pub(crate) struct ChildServer {
    child: Child,
    stdin: ChildStdin,
    address: String,
}

impl ChildServer {
    pub(crate) fn start() -> Self {
        Self::start_pinging(0)
    }

    /// A server that pings every `ping` seconds; none with 0.
    pub(crate) fn start_pinging(ping: u32) -> Self {
        Self::spawn(
            Command::new(env::current_exe().expect("this binary")).env(PING, ping.to_string()),
        )
    }

    /// A server that also serves WebTransport with the test CA's
    /// certificate (Block 48, D19): [`ChildServer::webtransport_url`] is
    /// its endpoint. The caller creates the test CA first.
    #[cfg(feature = "unstable-webtransport")]
    pub(crate) fn start_webtransport() -> Self {
        Self::spawn(Command::new(env::current_exe().expect("this binary")).env(WEBTRANSPORT, "1"))
    }

    #[cfg(feature = "unstable-webtransport")]
    pub(crate) fn webtransport_url(&self) -> String {
        self.address.clone()
    }

    fn spawn(command: &mut Command) -> Self {
        let mut child = command
            .env(ROLE, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("start the fake server");
        let stdin = child.stdin.take().expect("piped stdin");
        let mut address = String::new();
        BufReader::new(child.stdout.take().expect("piped stdout"))
            .read_line(&mut address)
            .expect("the fake server's address");
        Self {
            child,
            stdin,
            address: address.trim().to_owned(),
        }
    }

    pub(crate) fn websocket_url(&self) -> String {
        format!("ws://{}/connection/websocket", self.address)
    }

    /// Sends `count` publications of 1 KiB to `channel`.
    pub(crate) fn publish(&mut self, channel: &str, count: usize) {
        self.command(&format!("publish {channel} {count}"));
    }

    /// Sends `count` publications of 1 KiB to `channel`, `per_second` of
    /// them a second, each with its send time (`sent`, microseconds since
    /// the Unix epoch).
    pub(crate) fn publish_paced(&mut self, channel: &str, count: usize, per_second: u32) {
        self.command(&format!("paced {channel} {count} {per_second}"));
    }

    /// Advises `interval` milliseconds of publish debouncing in the
    /// subscribe replies from now on (Block 46).
    pub(crate) fn advise_publish_debounce(&mut self, interval: u32) {
        self.command(&format!("debounce {interval}"));
    }

    /// Closes the current connection with `code`.
    pub(crate) fn close(&mut self, code: u16) {
        self.command(&format!("close {code}"));
    }

    fn command(&mut self, line: &str) {
        writeln!(self.stdin, "{line}").expect("a command to the fake server");
    }
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
