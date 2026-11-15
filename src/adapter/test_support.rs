use std::sync::Arc;

use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};

use crate::sans_io::{Action, Input, TransportId};
use crate::{ClientEvent, State};

use super::client::Client;
use super::shared::Core;
use super::shared::lock;

/// A current-thread runtime nothing drives, so that its client's
/// driver never runs until the runtime is dropped.
pub(super) fn idle_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The events queued for `stream`, until it is pending or ends, and
/// whether it ended.
pub(super) fn queued(
    stream: &mut (impl Stream<Item = ClientEvent> + Unpin),
) -> (Vec<ClientEvent>, bool) {
    let mut events = Vec::new();
    loop {
        match stream.next().now_or_never() {
            Some(Some(event)) => events.push(event),
            Some(None) => return (events, true),
            None => return (events, false),
        }
    }
}

/// What a test learns from and tells a [`stalling_http_stream_with`]
/// server: `emulation` is notified when an emulation request has been
/// read, and `release` closes the held emulation requests, unanswered.
#[derive(Clone, Default)]
pub(super) struct Stall {
    pub(super) emulation: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}

/// An `http_stream` server on `listener`: the stream request gets a
/// connect reply and stays open; an emulation request is read and never
/// answered, so closing the transport waits for it.
pub(super) async fn stalling_http_stream(listener: tokio::net::TcpListener) {
    stalling_http_stream_with(listener, Stall::default()).await;
}

/// [`stalling_http_stream`], whose emulation requests `stall` reports
/// and releases.
pub(super) async fn stalling_http_stream_with(listener: tokio::net::TcpListener, stall: Stall) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut held = Vec::new();
    let mut emulations = Vec::new();
    loop {
        let mut socket = tokio::select! {
            accepted = listener.accept() => accepted.unwrap().0,
            () = stall.release.notified() => {
                emulations.clear();
                continue;
            }
        };
        // The whole request: its headers, then a body of Content-Length.
        let mut request = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            request.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&request).to_lowercase();
            let complete = text.split_once("\r\n\r\n").is_some_and(|(head, body)| {
                let length = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .and_then(|length| length.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                body.len() >= length
            });
            if read == 0 || complete {
                break;
            }
        }
        let text = String::from_utf8_lossy(&request).into_owned();
        if text.starts_with("POST /connection/") {
            // The connect command's id, from the request body.
            let id: String = text.split("\"id\":").nth(1).map_or_else(
                || "1".to_owned(),
                |rest| rest.chars().take_while(char::is_ascii_digit).collect(),
            );
            let reply = format!(
                "{{\"id\":{id},\"connect\":{{\"client\":\"c\",\"session\":\"s\",\"node\":\"n\"}}}}\n"
            );
            let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n";
            let chunk = format!("{response}{:x}\r\n{reply}\r\n", reply.len());
            let _ = socket.write_all(chunk.as_bytes()).await;
            held.push(socket);
        } else {
            emulations.push(socket);
            stall.emulation.notify_one();
        }
    }
}

/// Takes `client`'s core, connected by `reply` to its connect command,
/// with the actions so far performed. The caller holds the core, so the
/// driver task does not run until it is dropped.
pub(super) fn connected_core<'a>(
    client: &'a Client,
    reply: &str,
) -> std::sync::MutexGuard<'a, Core> {
    connected_core_over(client, reply).0
}

/// As [`connected_core`], with the transport.
pub(super) fn connected_core_over<'a>(
    client: &'a Client,
    reply: &str,
) -> (std::sync::MutexGuard<'a, Core>, TransportId) {
    let shared = client.shared();
    let mut core = lock(&shared.core);
    shared.handle(&mut core, Input::Connect);
    let transport = core
        .actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt");
    shared.handle(&mut core, Input::TransportOpened(transport));
    // The connect command leaves as the driver would send it.
    shared.handle(&mut core, Input::Flush);
    shared.handle(
        &mut core,
        Input::Frame {
            transport,
            data: reply.as_bytes().to_vec(),
        },
    );
    assert_eq!(core.machine.state(), State::Connected);
    core.actions.clear();
    (core, transport)
}
