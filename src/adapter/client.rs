//! Client driven by tokio.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use futures_channel::mpsc as events;
use futures_core::Stream;
use tokio::sync::mpsc;
use tokio::time::{Instant, Sleep};

use super::transport::{Inbound, WebSocket};
use crate::machine::{Action, Input, Machine, Timer, TransportId};
use crate::{ClientEvent, Config, State};

/// Centrifugo client running on tokio.
///
/// Dropping the client disconnects it.
#[derive(Debug)]
pub struct Client {
    commands: mpsc::UnboundedSender<Input>,
    shared: Arc<Shared>,
}

/// What the client handle and its driver task share.
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    subscribers: Mutex<Vec<events::UnboundedSender<ClientEvent>>>,
}

impl Client {
    /// Creates a disconnected client for the websocket `endpoint`, e.g.
    /// `ws://localhost:8000/connection/websocket`.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime: the client runs as a task on it.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, config: Config) -> Self {
        let (commands, inputs) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            state: Mutex::new(State::Disconnected),
            subscribers: Mutex::new(Vec::new()),
        });
        let driver = Driver {
            endpoint: endpoint.into(),
            machine: Machine::new(config),
            shared: Arc::clone(&shared),
            pending: VecDeque::new(),
            opening: None,
            transport: None,
            reconnect: None,
            connect_timeout: None,
        };
        tokio::spawn(driver.run(inputs));
        Self { commands, shared }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        *lock(&self.shared.state)
    }

    /// Events from now on. Each call returns an independent stream.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + Send + Unpin + 'static {
        let (sender, receiver) = events::unbounded();
        lock(&self.shared.subscribers).push(sender);
        receiver
    }

    /// Starts connecting; reconnects until connected or disconnected.
    pub fn connect(&self) {
        // Fails only once the driver has stopped, which a live handle prevents.
        let _ = self.commands.send(Input::Connect);
    }

    /// Disconnects and stops reconnecting.
    pub fn disconnect(&self) {
        let _ = self.commands.send(Input::Disconnect);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // No code panics while holding these locks; recover rather than cascade.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

type Opening = Pin<Box<dyn Future<Output = Result<WebSocket, String>> + Send>>;

/// Runs the machine: turns API calls, transport activity and timers into
/// inputs, and performs the actions they produce.
struct Driver {
    endpoint: String,
    machine: Machine,
    shared: Arc<Shared>,
    /// Inputs produced while performing actions, e.g. a failed send.
    pending: VecDeque<Input>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, WebSocket)>,
    reconnect: Option<Pin<Box<Sleep>>>,
    connect_timeout: Option<Pin<Box<Sleep>>>,
}

impl Driver {
    async fn run(mut self, mut inputs: mpsc::UnboundedReceiver<Input>) {
        loop {
            let input = tokio::select! {
                input = inputs.recv() => {
                    let Some(input) = input else {
                        // Every client handle is gone.
                        self.handle(Input::Disconnect).await;
                        return;
                    };
                    input
                }
                (id, result) = opened(&mut self.opening) => {
                    self.opening = None;
                    match result {
                        Ok(websocket) => {
                            self.transport = Some((id, websocket));
                            Input::TransportOpened(id)
                        }
                        Err(message) => Input::TransportFailed { transport: id, message },
                    }
                }
                (id, inbound) = received(&mut self.transport) => match inbound {
                    Inbound::Frame(data) => Input::Frame { transport: id, data },
                    Inbound::Closed { code, reason } => {
                        self.transport = None;
                        Input::TransportClosed { transport: id, code, reason }
                    }
                },
                () = fired(&mut self.reconnect) => {
                    self.reconnect = None;
                    Input::TimerFired(Timer::Reconnect)
                }
                () = fired(&mut self.connect_timeout) => {
                    self.connect_timeout = None;
                    Input::TimerFired(Timer::ConnectTimeout)
                }
            };
            self.handle(input).await;
        }
    }

    async fn handle(&mut self, input: Input) {
        self.pending.push_back(input);
        while let Some(input) = self.pending.pop_front() {
            let actions = self.machine.handle(input);
            // Before any event goes out: a subscriber reacting to an event
            // must see the state it reports.
            *lock(&self.shared.state) = self.machine.state();
            for action in actions {
                self.perform(action).await;
            }
        }
    }

    async fn perform(&mut self, action: Action) {
        match action {
            Action::OpenTransport(id) => {
                let endpoint = self.endpoint.clone();
                self.opening = Some((
                    id,
                    Box::pin(async move { WebSocket::open(&endpoint).await }),
                ));
            }
            Action::CloseTransport(id) => {
                if self
                    .opening
                    .as_ref()
                    .is_some_and(|(opening, _)| *opening == id)
                {
                    self.opening = None;
                }
                if let Some((_, websocket)) = self.transport.take_if(|(open, _)| *open == id) {
                    tokio::spawn(websocket.close());
                }
            }
            Action::Send { transport, frame } => {
                if let Some((id, websocket)) = &mut self.transport
                    && *id == transport
                    && websocket.send(frame).await.is_err()
                {
                    self.transport = None;
                    self.pending.push_back(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                }
            }
            Action::SetTimer(timer, delay) => {
                *self.timer(timer) =
                    Some(Box::pin(tokio::time::sleep_until(Instant::now() + delay)));
            }
            Action::CancelTimer(timer) => *self.timer(timer) = None,
            Action::Emit(event) => {
                lock(&self.shared.subscribers)
                    .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
            }
        }
    }

    fn timer(&mut self, timer: Timer) -> &mut Option<Pin<Box<Sleep>>> {
        match timer {
            Timer::Reconnect => &mut self.reconnect,
            Timer::ConnectTimeout => &mut self.connect_timeout,
        }
    }
}

/// Resolves when the transport being opened is ready; never without one.
async fn opened(
    opening: &mut Option<(TransportId, Opening)>,
) -> (TransportId, Result<WebSocket, String>) {
    match opening {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

/// Resolves with the next inbound message; never without a transport.
async fn received(transport: &mut Option<(TransportId, WebSocket)>) -> (TransportId, Inbound) {
    match transport {
        Some((id, websocket)) => (*id, websocket.recv().await),
        None => std::future::pending().await,
    }
}

/// Resolves when the timer fires; never without one.
async fn fired(timer: &mut Option<Pin<Box<Sleep>>>) {
    match timer {
        Some(sleep) => sleep.await,
        None => std::future::pending().await,
    }
}
