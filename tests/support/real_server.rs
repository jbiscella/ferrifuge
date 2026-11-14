//! The environment of Block 51's real-server chaos (D2): per run, a
//! user-defined Docker network holding a Centrifugo container and a
//! Toxiproxy container, and for the Redis row a Redis container. The client
//! connects only through Toxiproxy, which reaches Centrifugo by its name on
//! the network, so a restart of Centrifugo does not change the upstream;
//! the publisher and the server API go to Centrifugo directly, so a toxic
//! never breaks the oracle. Both published ports are fixed host ports
//! picked here, so that a restart cannot move them. Containers are driven
//! with the `docker` CLI, no new dependency.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::process::Command;

use super::centrifugo::{API_KEY, IMAGE, RUN_LABEL, ServerApi, TAG};
use super::token::HMAC_SECRET;

/// Toxiproxy, pinned by tag and digest (Block 51, structural acceptance;
/// the digest read from GHCR on 2026-11-14).
pub(crate) const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy:2.13.0@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa";
/// Redis for the Redis row, never restarted (D2), pinned by tag and digest
/// (read from Docker Hub on 2026-11-14).
pub(crate) const REDIS_IMAGE: &str =
    "redis:8.4-alpine@sha256:44b295aedd412d52a9846f51573e906e0a9c7d83ba431f4134b1b338d45ba732";

/// Centrifugo's port in its container.
const CENTRIFUGO_PORT: u16 = 8000;
/// Toxiproxy's API port in its container.
const TOXIPROXY_API_PORT: u16 = 8474;
/// The port of the proxy to Centrifugo in Toxiproxy's container.
const PROXY_PORT: u16 = 18000;
/// The proxy's name in Toxiproxy.
pub(crate) const PROXY: &str = "centrifugo";
/// How long a started container may take to answer.
const READY_WITHIN: Duration = Duration::from_secs(60);

/// The engine of a row (D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Engine {
    Memory,
    Redis,
}

/// Runs `docker` with `args`, returning its standard output, trimmed.
///
/// # Errors
///
/// Docker's standard error when it fails.
pub(crate) async fn docker(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|error| format!("docker {}: {error}", args.join(" ")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// A free TCP port on the loopback interface, to publish a container on.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("a free port")
}

/// The server configuration of D5, with the engine of the row.
pub(crate) fn configuration(engine: Engine) -> Value {
    let mut config = json!({
        "client": {
            "token": {"hmac_secret_key": HMAC_SECRET},
            "allowed_origins": ["*"],
            "ping_interval": "2s",
            "pong_timeout": "1s",
            "expired_close_delay": "3s",
            "expired_sub_close_delay": "3s",
            "recovery_max_publication_limit": 2000
        },
        "http_api": {"key": API_KEY},
        // For the readiness probe.
        "health": {"enabled": true},
        "http_stream": {"enabled": true},
        "sse": {"enabled": true},
        "channel": {
            "namespaces": [{
                "name": "recoverable",
                "history_size": 3000,
                "history_ttl": "600s",
                "force_recovery": true,
                "force_positioning": true,
                "presence": true,
                "join_leave": true,
                "allow_subscribe_for_client": true
            }]
        }
    });
    if engine == Engine::Redis {
        config["engine"] = json!({
            "type": "redis",
            "redis": {"address": "redis://redis:6379"}
        });
    }
    config
}

/// The containers and network of one run, removed when dropped.
pub(crate) struct Environment {
    run: String,
    network: String,
    centrifugo: String,
    toxiproxy: String,
    redis: Option<String>,
    /// Centrifugo's published port, for the server API and the publisher.
    centrifugo_port: u16,
    /// The proxy's published port, for the client.
    proxy_port: u16,
    toxiproxy_api_port: u16,
    config_dir: PathBuf,
    api: ServerApi,
    toxics: Toxiproxy,
}

impl Environment {
    /// Starts the network and the containers of run `run` (a name unique
    /// to this process) with `config`, Centrifugo's configuration, and
    /// waits until Centrifugo and Toxiproxy answer.
    pub(crate) async fn start(run: &str, engine: Engine, config: &Value) -> Result<Self, String> {
        let name = |role: &str| format!("ferrifuge-chaos-{run}-{role}");
        let network = name("net");
        let config_dir = std::env::temp_dir().join(name("config"));
        std::fs::create_dir_all(&config_dir).map_err(|e| e.to_string())?;
        std::fs::write(
            config_dir.join("config.json"),
            serde_json::to_vec_pretty(config).expect("a JSON configuration"),
        )
        .map_err(|e| e.to_string())?;
        let mut environment = Self {
            run: run.to_owned(),
            network: network.clone(),
            centrifugo: name("centrifugo"),
            toxiproxy: name("toxiproxy"),
            redis: (engine == Engine::Redis).then(|| name("redis")),
            centrifugo_port: free_port(),
            proxy_port: free_port(),
            toxiproxy_api_port: free_port(),
            config_dir,
            api: ServerApi::new("http://127.0.0.1:0"),
            toxics: Toxiproxy::new(0),
        };
        environment.api = ServerApi::new(&environment.api_url());
        environment.toxics = Toxiproxy::new(environment.toxiproxy_api_port);
        environment.run_containers(&network).await?;
        environment.wait_ready().await?;
        environment
            .toxics
            .create(
                PROXY,
                &format!("0.0.0.0:{PROXY_PORT}"),
                &format!("centrifugo:{CENTRIFUGO_PORT}"),
            )
            .await?;
        Ok(environment)
    }

    /// Creates the network and runs the containers on it.
    async fn run_containers(&self, network: &str) -> Result<(), String> {
        let label = format!("{RUN_LABEL}={}", self.run);
        docker(&["network", "create", "--label", &label, network]).await?;
        if let Some(redis) = &self.redis {
            docker(&[
                "run",
                "-d",
                "--name",
                redis,
                "--network",
                network,
                "--network-alias",
                "redis",
                "--label",
                &label,
                REDIS_IMAGE,
            ])
            .await?;
        }
        let image = format!("{IMAGE}:{TAG}");
        let centrifugo_publish = format!("127.0.0.1:{}:{CENTRIFUGO_PORT}", self.centrifugo_port);
        let mount = format!("{}:/centrifugo", self.config_dir.display());
        docker(&[
            "run",
            "-d",
            "--name",
            &self.centrifugo,
            "--network",
            network,
            "--network-alias",
            "centrifugo",
            "--label",
            &label,
            "-p",
            &centrifugo_publish,
            "-v",
            &mount,
            &image,
            "centrifugo",
            "--config",
            "/centrifugo/config.json",
        ])
        .await?;
        let proxy_publish = format!("127.0.0.1:{}:{PROXY_PORT}", self.proxy_port);
        let api_publish = format!("127.0.0.1:{}:{TOXIPROXY_API_PORT}", self.toxiproxy_api_port);
        docker(&[
            "run",
            "-d",
            "--name",
            &self.toxiproxy,
            "--network",
            network,
            "--label",
            &label,
            "-p",
            &proxy_publish,
            "-p",
            &api_publish,
            TOXIPROXY_IMAGE,
        ])
        .await?;
        Ok(())
    }

    /// Waits until Centrifugo's health endpoint and Toxiproxy's API answer.
    async fn wait_ready(&self) -> Result<(), String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|e| e.to_string())?;
        let health = format!("{}/health", self.api_url());
        let version = format!("{}/version", self.toxics.base);
        let deadline = tokio::time::Instant::now() + READY_WITHIN;
        for url in [health, version] {
            loop {
                match http.get(&url).send().await {
                    Ok(response) if response.status().is_success() => break,
                    _ if tokio::time::Instant::now() >= deadline => {
                        return Err(format!("{url} did not answer within {READY_WITHIN:?}"));
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
        Ok(())
    }

    /// The server API, straight to Centrifugo.
    pub(crate) fn api(&self) -> &ServerApi {
        &self.api
    }

    /// Toxiproxy's API.
    pub(crate) fn toxics(&self) -> &Toxiproxy {
        &self.toxics
    }

    /// The base URL of Centrifugo's API, straight to it.
    pub(crate) fn api_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.centrifugo_port)
    }

    /// The client's websocket endpoint, through Toxiproxy.
    pub(crate) fn websocket_url(&self) -> String {
        format!("ws://127.0.0.1:{}/connection/websocket", self.proxy_port)
    }

    /// The client's `http_stream` endpoint, through Toxiproxy.
    pub(crate) fn http_stream_url(&self) -> String {
        format!(
            "http://127.0.0.1:{}/connection/http_stream",
            self.proxy_port
        )
    }

    /// The client's `sse` endpoint, through Toxiproxy.
    pub(crate) fn sse_url(&self) -> String {
        format!("http://127.0.0.1:{}/connection/sse", self.proxy_port)
    }

    /// The logs of every container of the run, for a failing seed's
    /// artifact.
    pub(crate) async fn logs(&self) -> String {
        let mut logs = String::new();
        for container in [
            Some(&self.centrifugo),
            Some(&self.toxiproxy),
            self.redis.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let output = Command::new("docker")
                .args(["logs", "--timestamps", container])
                .output()
                .await;
            let _ = writeln!(logs, "==== {container}");
            if let Ok(output) = output {
                logs.push_str(&String::from_utf8_lossy(&output.stdout));
                logs.push_str(&String::from_utf8_lossy(&output.stderr));
            }
        }
        logs
    }

    /// The run's name.
    pub(crate) fn run(&self) -> &str {
        &self.run
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        // Synchronous, as a drop cannot await: removing the containers
        // and the network takes about a second.
        let containers: Vec<&str> = [
            Some(&self.centrifugo),
            Some(&self.toxiproxy),
            self.redis.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect();
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f"])
            .args(&containers)
            .output();
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.network])
            .output();
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

/// The direction a toxic acts on (Toxiproxy's `stream`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stream {
    /// From the server to the client.
    Downstream,
    /// From the client to the server.
    Upstream,
}

impl Stream {
    fn name(self) -> &'static str {
        match self {
            Self::Downstream => "downstream",
            Self::Upstream => "upstream",
        }
    }
}

/// Client of Toxiproxy's HTTP API (v2.13.0, README "HTTP API").
#[derive(Clone)]
pub(crate) struct Toxiproxy {
    http: reqwest::Client,
    base: String,
}

impl Toxiproxy {
    fn new(port: u16) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("http://127.0.0.1:{port}"),
        }
    }

    /// Sends `request`, failing on a status that is not a success, with
    /// Toxiproxy's message.
    async fn send(&self, request: reqwest::RequestBuilder, what: &str) -> Result<Value, String> {
        let response = request.send().await.map_err(|e| format!("{what}: {e}"))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{what}: {status} {text}"));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Creates proxy `name` from `listen` to `upstream`, enabled.
    pub(crate) async fn create(
        &self,
        name: &str,
        listen: &str,
        upstream: &str,
    ) -> Result<(), String> {
        let body = json!({"name": name, "listen": listen, "upstream": upstream, "enabled": true});
        self.send(
            self.http.post(format!("{}/proxies", self.base)).json(&body),
            "create proxy",
        )
        .await
        .map(drop)
    }

    /// Adds toxic `name` of `kind` on `stream` of `proxy`, with toxicity 1
    /// and `attributes` (D3).
    pub(crate) async fn add(
        &self,
        proxy: &str,
        name: &str,
        kind: &str,
        stream: Stream,
        attributes: Value,
    ) -> Result<(), String> {
        let body = json!({
            "name": name,
            "type": kind,
            "stream": stream.name(),
            "toxicity": 1.0,
            "attributes": attributes,
        });
        self.send(
            self.http
                .post(format!("{}/proxies/{proxy}/toxics", self.base))
                .json(&body),
            "add toxic",
        )
        .await
        .map(drop)
    }

    /// Removes toxic `name` of `proxy`.
    pub(crate) async fn remove(&self, proxy: &str, name: &str) -> Result<(), String> {
        self.send(
            self.http
                .delete(format!("{}/proxies/{proxy}/toxics/{name}", self.base)),
            "remove toxic",
        )
        .await
        .map(drop)
    }

    /// The names of the toxics `proxy` has.
    pub(crate) async fn toxics(&self, proxy: &str) -> Result<Vec<String>, String> {
        let toxics = self
            .send(
                self.http
                    .get(format!("{}/proxies/{proxy}/toxics", self.base)),
                "list toxics",
            )
            .await?;
        Ok(toxics
            .as_array()
            .map(|toxics| {
                toxics
                    .iter()
                    .filter_map(|toxic| toxic["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Turns `proxy` down (connections refused, D3's "proxy down") or up.
    pub(crate) async fn set_enabled(&self, proxy: &str, enabled: bool) -> Result<(), String> {
        self.send(
            self.http
                .post(format!("{}/proxies/{proxy}", self.base))
                .json(&json!({"enabled": enabled})),
            "set proxy enabled",
        )
        .await
        .map(drop)
    }
}
