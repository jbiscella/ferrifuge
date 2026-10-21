//! Real Centrifugo server for @A and @B scenarios, run with testcontainers.
//!
//! Configuration is passed as Centrifugo environment variables, so each
//! scenario can start its own config variant.

use serde_json::Value;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub(crate) const IMAGE: &str = "centrifugo/centrifugo";
pub(crate) const TAG: &str = "v6.10.1";
pub(crate) const API_KEY: &str = "test-api-key";
/// Name under which the container reaches the host, e.g. a proxy backend.
pub(crate) const HOST_GATEWAY: &str = "host.docker.internal";

const PORT: u16 = 8000;
/// Port of the API and health endpoints when the client endpoints use TLS.
const INTERNAL_PORT: u16 = 9000;

pub(crate) struct Centrifugo {
    // Dropping the handle stops and removes the container.
    _container: ContainerAsync<GenericImage>,
    /// `host:port` of the client endpoints.
    address: String,
    tls: bool,
    api: ServerApi,
}

impl Centrifugo {
    /// Starts a server with the HTTP API and health endpoint enabled, plus
    /// `env`: extra `CENTRIFUGO_*` variables for this config variant.
    pub(crate) async fn start(env: &[(&str, &str)]) -> Self {
        Self::launch(env, None).await
    }

    /// Like [`Self::start`], with the client endpoints served over TLS with
    /// `cert_pem` and `key_pem`. The API and health endpoints stay plain
    /// HTTP on an internal port, so the test drives the server as usual.
    pub(crate) async fn start_tls(env: &[(&str, &str)], cert_pem: &str, key_pem: &str) -> Self {
        Self::launch(env, Some((cert_pem, key_pem))).await
    }

    async fn launch(env: &[(&str, &str)], tls: Option<(&str, &str)>) -> Self {
        let api_port = if tls.is_some() { INTERNAL_PORT } else { PORT };
        let mut image = GenericImage::new(IMAGE, TAG).with_exposed_port(ContainerPort::Tcp(PORT));
        if tls.is_some() {
            image = image.with_exposed_port(ContainerPort::Tcp(INTERNAL_PORT));
        }
        let mut request = image
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/health")
                    .with_port(ContainerPort::Tcp(api_port))
                    .with_expected_status_code(200u16),
            ))
            .with_cmd(["centrifugo"])
            .with_host(HOST_GATEWAY, Host::HostGateway)
            .with_env_var("CENTRIFUGO_HTTP_API_KEY", API_KEY)
            .with_env_var("CENTRIFUGO_HEALTH_ENABLED", "true");
        if let Some((cert_pem, key_pem)) = tls {
            request = request
                .with_env_var("CENTRIFUGO_HTTP_SERVER_TLS_ENABLED", "true")
                .with_env_var("CENTRIFUGO_HTTP_SERVER_TLS_CERT_PEM", cert_pem)
                .with_env_var("CENTRIFUGO_HTTP_SERVER_TLS_KEY_PEM", key_pem)
                .with_env_var("CENTRIFUGO_HTTP_SERVER_TLS_EXTERNAL", "true")
                .with_env_var(
                    "CENTRIFUGO_HTTP_SERVER_INTERNAL_PORT",
                    INTERNAL_PORT.to_string(),
                );
        }
        for (name, value) in env {
            request = request.with_env_var(*name, *value);
        }
        let container = request.start().await.expect("start Centrifugo container");

        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(PORT)
            .await
            .expect("container port");
        let api_port = container
            .get_host_port_ipv4(api_port)
            .await
            .expect("container API port");
        let api = ServerApi::new(&format!("http://{host}:{api_port}"));
        Self {
            _container: container,
            address: format!("{host}:{port}"),
            tls: tls.is_some(),
            api,
        }
    }

    /// `host:port` of the server's client endpoints, and of its API unless
    /// they use TLS.
    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn websocket_url(&self) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        format!("{scheme}://{}/connection/websocket", self.address)
    }

    pub(crate) fn api(&self) -> &ServerApi {
        &self.api
    }
}

/// Client for the server HTTP API (<https://centrifugal.dev/docs/server/server_api>).
pub(crate) struct ServerApi {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) code: u64,
    pub(crate) message: String,
}

impl ServerApi {
    fn new(base_url: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.to_owned(),
        }
    }

    /// Calls `method` and returns its `result`. Centrifugo reports method
    /// errors in the body with HTTP 200; transport failures panic.
    pub(crate) async fn call(&self, method: &str, params: Value) -> Result<Value, ApiError> {
        let response = self
            .http
            .post(format!("{}/api/{method}", self.base_url))
            .header("X-API-Key", API_KEY)
            .json(&params)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .unwrap_or_else(|e| panic!("server API {method}: {e}"));
        let mut body: Value = response.json().await.expect("server API JSON body");
        match body.get_mut("error").map(Value::take) {
            Some(error) => Err(ApiError {
                code: error["code"].as_u64().unwrap_or_default(),
                message: error["message"].as_str().unwrap_or_default().to_owned(),
            }),
            None => Ok(body.get_mut("result").map(Value::take).unwrap_or_default()),
        }
    }
}
