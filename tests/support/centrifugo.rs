//! Real Centrifugo server for @A and @B scenarios, run with testcontainers.
//!
//! Configuration is passed as Centrifugo environment variables, so each
//! scenario can start its own config variant.

use serde_json::Value;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub(crate) const IMAGE: &str = "centrifugo/centrifugo";
pub(crate) const TAG: &str = "v6.10.1";
pub(crate) const API_KEY: &str = "test-api-key";

const PORT: u16 = 8000;

pub(crate) struct Centrifugo {
    // Dropping the handle stops and removes the container.
    _container: ContainerAsync<GenericImage>,
    base_url: String,
    api: ServerApi,
}

impl Centrifugo {
    /// Starts a server with the HTTP API and health endpoint enabled, plus
    /// `env`: extra `CENTRIFUGO_*` variables for this config variant.
    pub(crate) async fn start(env: &[(&str, &str)]) -> Self {
        let mut request = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(ContainerPort::Tcp(PORT))
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/health").with_expected_status_code(200u16),
            ))
            .with_cmd(["centrifugo"])
            .with_env_var("CENTRIFUGO_HTTP_API_KEY", API_KEY)
            .with_env_var("CENTRIFUGO_HEALTH_ENABLED", "true");
        for (name, value) in env {
            request = request.with_env_var(*name, *value);
        }
        let container = request.start().await.expect("start Centrifugo container");

        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(PORT)
            .await
            .expect("container port");
        let base_url = format!("http://{host}:{port}");
        let api = ServerApi::new(&base_url);
        Self {
            _container: container,
            base_url,
            api,
        }
    }

    pub(crate) fn websocket_url(&self) -> String {
        format!(
            "{}/connection/websocket",
            self.base_url.replacen("http", "ws", 1)
        )
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
