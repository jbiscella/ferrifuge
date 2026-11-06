//! Server-side subscriptions (Block 12): the ones the server made for the
//! connection, which the client tracks in a registry of their own and
//! reports through [`crate::ClientEvent::ServerSubscription`], but does not
//! control (spec, "Server-side subscriptions").

use std::collections::BTreeMap;

use super::Machine;
use super::delivery::{Commit, Owner};
use super::subscriptions::UNMATCHABLE_EPOCH;
use crate::wire::{SubscribeRequest, SubscribeResult};
use crate::{
    ClientError, ClientEvent, ClientInfo, ErrorKind, Publication, ServerSubscriptionEvent,
    StreamPosition, Subscribed, codes,
};

/// Reason of [`codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`].
const NOT_GRANTED_REASON: &str = "not subscribed by the server after reconnect";

/// The registry of server-side subscriptions, by channel.
#[derive(Debug, Default)]
pub(super) struct ServerSubscriptions {
    by_channel: BTreeMap<String, ServerSubscription>,
}

/// A server-side subscription in the registry.
#[derive(Debug)]
struct ServerSubscription {
    /// Subscribed on the current connection; losing it makes the
    /// subscription subscribing until the next connection.
    subscribed: bool,
    /// Position in the channel's stream to recover from on the next
    /// connect, when the server made the subscription recoverable: from the
    /// connect reply or the `Subscribe` push, then of every publication
    /// delivered (centrifuge-js `_handleSubscribe`, `_handlePublication`).
    position: Option<StreamPosition>,
}

impl Machine {
    /// The channels of the server-side subscriptions in the registry, in
    /// channel order.
    #[must_use]
    pub fn server_subscriptions(&self) -> Vec<String> {
        self.server_subscriptions
            .by_channel
            .keys()
            .cloned()
            .collect()
    }

    /// The connect reply granted `subs`, after the client's `connected`
    /// event (centrifuge-js `_connectResponse`): each is subscribed, its
    /// initial or recovered publications following its `subscribed` event
    /// whether or not it recovers (Block 12); one of the registry the reply
    /// leaves out ends with [`codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`]
    /// and leaves the registry (centrifuge-js `_processServerSubs`).
    /// Whether a subscription of the reply carried a delta publication,
    /// which the client never asks for on server-side subscriptions
    /// (Block 19, D8): the caller reconnects once the reply is handled.
    #[must_use]
    pub(super) fn server_subscriptions_connected(
        &mut self,
        subs: BTreeMap<String, SubscribeResult>,
    ) -> bool {
        let mut delta = false;
        let gone: Vec<String> = self
            .server_subscriptions
            .by_channel
            .keys()
            .filter(|channel| !subs.contains_key(*channel))
            .cloned()
            .collect();
        for (channel, result) in subs {
            delta |= self.subscribe_server_side(&channel, result);
        }
        for channel in gone {
            self.server_subscriptions.by_channel.remove(&channel);
            self.emit_server_subscription(
                &channel,
                ServerSubscriptionEvent::Unsubscribed {
                    code: codes::unsubscribed::NOT_GRANTED_ON_RECONNECT,
                    reason: NOT_GRANTED_REASON.to_owned(),
                },
            );
        }
        delta
    }

    /// The server subscribed the connection to `channel` during the
    /// connection (`Subscribe` push; spec, "Server-side subscriptions").
    pub(super) fn server_subscription_pushed(&mut self, channel: &str, result: SubscribeResult) {
        if self.subscribe_server_side(channel, result) {
            self.server_delta_failed();
        }
    }

    /// A server-side subscription received a delta publication: an error
    /// event, then the client reconnects with code 5, its reconnect backoff
    /// not reset by this connection, and recovers its server-side
    /// subscriptions from their last positions taken (Block 19, D8).
    pub(super) fn server_delta_failed(&mut self) {
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Delta,
            code: codes::error::DELTA,
            message: "delta publication on a server-side subscription".to_owned(),
            transport: None,
        }));
        self.disconnect(codes::connecting::DELTA_ERROR, "delta error", true);
    }

    /// The server ended the server-side subscription to `channel`
    /// (`Unsubscribe` push, which no subscribed client-side subscription
    /// took): it leaves the registry, with the push's code and reason
    /// (Block 12; centrifuge-js `_handleUnsubscribe` passes only the
    /// channel).
    pub(super) fn server_subscription_unsubscribed(
        &mut self,
        channel: &str,
        code: u32,
        reason: &str,
    ) {
        if self
            .server_subscriptions
            .by_channel
            .remove(channel)
            .is_some()
        {
            self.emit_server_subscription(
                channel,
                ServerSubscriptionEvent::Unsubscribed {
                    code,
                    reason: reason.to_owned(),
                },
            );
        }
    }

    /// Registers `channel` as subscribed and emits its `subscribed` event,
    /// then its initial or recovered publications.
    /// Whether one of its publications was a delta, which is not delivered,
    /// nor any after it (Block 19, D8).
    fn subscribe_server_side(&mut self, channel: &str, result: SubscribeResult) -> bool {
        // Checked before any event is emitted, so that the reconnect
        // withdraws none of the message being handled (Codex on PR #99).
        if result
            .publications
            .iter()
            .any(|publication| publication.delta)
        {
            return true;
        }
        // A recoverable subscription's position: the stream's, or after a
        // recovery the one it recovers from. With publications, it is
        // reached once the last of them is taken (Block 15).
        let replied = result.recoverable.then(|| StreamPosition {
            offset: result.offset,
            epoch: result.epoch.clone(),
        });
        let position = replied
            .as_ref()
            .map(|replied| Machine::reply_baseline(replied, &result.publications));
        self.withdraw(&Owner::Server(channel.to_owned()));
        self.server_subscriptions.by_channel.insert(
            channel.to_owned(),
            ServerSubscription {
                subscribed: true,
                position,
            },
        );
        let stream_position = (result.positioned || result.recoverable).then(|| StreamPosition {
            offset: result.offset,
            epoch: result.epoch.clone(),
        });
        self.emit_server_subscription(
            channel,
            ServerSubscriptionEvent::Subscribed(Subscribed {
                data: result.data,
                positioned: result.positioned,
                recoverable: result.recoverable,
                was_recovering: result.was_recovering,
                recovered: result.recovered,
                stream_position,
                // The client never asks for delta here (Block 19).
                delta: false,
            }),
        );
        // Initial or recovered publications follow it and move the position
        // once taken.
        let last = result.publications.len().saturating_sub(1);
        for (index, publication) in result.publications.into_iter().enumerate() {
            let reply = replied.as_ref().map(|position| (position, index == last));
            self.emit_server_publication(channel, publication, reply);
        }
        false
    }

    /// The server-side subscriptions the next connect asks to recover, from
    /// their positions (`client.proto`, `ConnectRequest.subs`).
    pub(super) fn server_subscriptions_to_recover(&self) -> BTreeMap<String, SubscribeRequest> {
        self.server_subscriptions
            .by_channel
            .iter()
            .filter_map(|(channel, sub)| {
                let position = sub.position.as_ref()?;
                Some((
                    channel.clone(),
                    SubscribeRequest {
                        recover: true,
                        epoch: position.epoch.clone(),
                        offset: position.offset,
                        ..SubscribeRequest::default()
                    },
                ))
            })
            .collect()
    }

    /// "State invalidated" (disconnect code 3014): a server-side
    /// subscription with a position keeps one the server can never match, so
    /// its recovery at the next connect fails and its `subscribed` event says
    /// so (owner decision, 2026-11-04; centrifuge-js `_disconnect`).
    pub(super) fn invalidate_server_subscriptions(&mut self) {
        let mut channels = Vec::new();
        for (channel, sub) in &mut self.server_subscriptions.by_channel {
            if let Some(position) = &mut sub.position {
                position.offset = 0;
                UNMATCHABLE_EPOCH.clone_into(&mut position.epoch);
            }
            channels.push(channel.clone());
        }
        for channel in channels {
            self.withdraw(&Owner::Server(channel));
        }
    }

    /// The connection is lost or `disconnect()` was called: the server-side
    /// subscriptions stay in the registry, subscribing until the next
    /// connection (spec, "Server-side subscriptions").
    pub(super) fn server_subscriptions_disconnected(&mut self) {
        let mut subscribing = Vec::new();
        for (channel, sub) in &mut self.server_subscriptions.by_channel {
            if std::mem::take(&mut sub.subscribed) {
                subscribing.push(channel.clone());
            }
        }
        for channel in subscribing {
            self.emit_server_subscription(&channel, ServerSubscriptionEvent::Subscribing);
        }
    }

    /// A publication pushed for `channel` that no subscribed client-side
    /// subscription takes: the server-side subscription's, if subscribed
    /// (Block 12; centrifuge-js `_getSubForPush`).
    pub(super) fn server_publication(&mut self, channel: &str, publication: Publication) {
        if !self
            .server_subscriptions
            .by_channel
            .get(channel)
            .is_some_and(|sub| sub.subscribed)
        {
            return;
        }
        if publication.delta {
            self.server_delta_failed();
            return;
        }
        self.emit_server_publication(channel, publication, None);
    }

    /// A join or leave of the server-side subscription to `channel`, when it
    /// is subscribed.
    pub(super) fn server_join_or_leave(&mut self, channel: &str, info: ClientInfo, join: bool) {
        if !self
            .server_subscriptions
            .by_channel
            .get(channel)
            .is_some_and(|sub| sub.subscribed)
        {
            return;
        }
        let Some(id) = self.admit_event(None, false) else {
            return;
        };
        self.deliver_unpositioned(id, Owner::Server(channel.to_owned()));
        let event = if join {
            ServerSubscriptionEvent::Join(info)
        } else {
            ServerSubscriptionEvent::Leave(info)
        };
        self.actions.push(super::Action::Emit(
            id,
            ClientEvent::ServerSubscription {
                channel: channel.to_owned(),
                event,
            },
        ));
    }

    /// Emits a publication of the server-side subscription to `channel`,
    /// which moves its position once taken; `reply` is the position of the
    /// reply it came with, and whether it is that reply's last publication.
    fn emit_server_publication(
        &mut self,
        channel: &str,
        publication: Publication,
        reply: Option<(&StreamPosition, bool)>,
    ) {
        let Some(id) = self.admit_event(None, false) else {
            return;
        };
        self.deliver_publication(id, Owner::Server(channel.to_owned()), &publication, reply);
        self.actions.push(super::Action::Emit(
            id,
            ClientEvent::ServerSubscription {
                channel: channel.to_owned(),
                event: ServerSubscriptionEvent::Publication(publication),
            },
        ));
    }

    /// Applies a taken publication's `commit` to the position of the
    /// server-side subscription to `channel`, when it keeps one.
    pub(super) fn commit_server_subscription_position(&mut self, channel: &str, commit: Commit) {
        if let Some(sub) = self.server_subscriptions.by_channel.get_mut(channel)
            && (commit.replaces || sub.position.is_some())
        {
            commit.apply(&mut sub.position);
        }
    }

    fn emit_server_subscription(&mut self, channel: &str, event: ServerSubscriptionEvent) {
        // Leaving subscribed withdraws the publications not yet taken.
        if matches!(
            event,
            ServerSubscriptionEvent::Subscribing | ServerSubscriptionEvent::Unsubscribed { .. }
        ) {
            self.withdraw(&Owner::Server(channel.to_owned()));
        }
        self.emit(ClientEvent::ServerSubscription {
            channel: channel.to_owned(),
            event,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::machine::{Action, Input, Timer, TransportId};

    /// A machine over a websocket that opened; the connect command is 1.
    fn opened() -> (Machine, TransportId) {
        let mut machine = Machine::new(Config::default());
        let transport = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));
        (machine, transport)
    }

    fn reply(transport: TransportId, json: &str) -> Input {
        Input::Frame {
            transport,
            data: json.as_bytes().to_vec(),
        }
    }

    fn events(actions: &[Action]) -> Vec<&ClientEvent> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Emit(_, event) => Some(event),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn server_side_subscriptions_follow_the_connected_event() {
        let (mut machine, transport) = opened();

        let actions = machine.handle(reply(
            transport,
            r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{"publications":[{"data":{},"offset":1}]}}}}"#,
        ));

        let names: Vec<_> = events(&actions).iter().map(|event| event.name()).collect();
        assert_eq!(names, ["connected", "subscribed", "publication"]);
        assert_eq!(machine.server_subscriptions(), ["alerts"]);
    }

    // Codex on PR #64: a grant without recovery forgets the position, also
    // when it comes with publications.
    #[test]
    fn a_grant_without_recovery_drops_the_position_even_with_publications() {
        let (mut machine, transport) = opened();
        machine.handle(reply(
            transport,
            r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{"recoverable":true,"epoch":"e","offset":0}}}}"#,
        ));

        machine.handle(reply(
            transport,
            r#"{"push":{"channel":"alerts","subscribe":{"publications":[{"data":{},"offset":5}]}}}"#,
        ));

        assert_eq!(
            machine.server_subscriptions.by_channel["alerts"].position,
            None
        );
    }

    // Client-side precedence applies to Unsubscribe pushes and publications
    // only: a Subscribe push always makes a server-side subscription
    // (centrifuge-js 63ed73e `_handleSubscribe`, centrifuge.ts:2630-2638).
    #[test]
    fn a_subscribe_push_registers_the_channel_even_when_a_client_side_subscription_is_subscribed() {
        let (mut machine, transport) = opened();
        machine.handle(reply(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
        let id = machine
            .add_subscription(
                "alerts".into(),
                crate::machine::SubscriptionOptions::default(),
            )
            .unwrap();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        assert_eq!(
            machine.subscription_state(id),
            Some(crate::SubscriptionState::Subscribed)
        );

        let actions = machine.handle(reply(
            transport,
            r#"{"push":{"channel":"alerts","subscribe":{"data":{"tier":"gold"}}}}"#,
        ));

        assert!(
            events(&actions).iter().any(|event| matches!(
                event,
                ClientEvent::ServerSubscription {
                    channel,
                    event: ServerSubscriptionEvent::Subscribed(subscribed),
                } if channel == "alerts" && subscribed.data.as_deref() == Some(&br#"{"tier":"gold"}"#[..])
            )),
            "{actions:?}"
        );
        assert_eq!(machine.server_subscriptions(), ["alerts"]);
        assert_eq!(
            machine.subscription_state(id),
            Some(crate::SubscriptionState::Subscribed)
        );
    }

    #[test]
    fn an_unsubscribe_push_ends_a_server_side_subscription_with_its_code_and_reason() {
        let (mut machine, transport) = opened();
        machine.handle(reply(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
        machine.handle(reply(
            transport,
            r#"{"push":{"channel":"alerts","subscribe":{}}}"#,
        ));

        let actions = machine.handle(reply(
            transport,
            r#"{"push":{"channel":"alerts","unsubscribe":{"code":2000,"reason":"alerts closed"}}}"#,
        ));

        assert_eq!(
            events(&actions),
            [&ClientEvent::ServerSubscription {
                channel: "alerts".into(),
                event: ServerSubscriptionEvent::Unsubscribed {
                    code: 2000,
                    reason: "alerts closed".into(),
                },
            }]
        );
        assert_eq!(machine.server_subscriptions(), Vec::<String>::new());
    }

    #[test]
    fn a_subscription_a_reconnect_no_longer_grants_ends_with_code_4() {
        let (mut machine, transport) = opened();
        machine.handle(reply(
            transport,
            r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{}}}}"#,
        ));
        machine.handle(Input::TransportClosed {
            transport,
            code: Some(3000),
            reason: "reconnect".into(),
        });
        let transport = machine
            .handle(Input::TimerFired(Timer::Reconnect))
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let actions = machine.handle(reply(transport, r#"{"id":2,"connect":{"client":"c"}}"#));

        assert!(
            events(&actions).contains(&&ClientEvent::ServerSubscription {
                channel: "alerts".into(),
                event: ServerSubscriptionEvent::Unsubscribed {
                    code: codes::unsubscribed::NOT_GRANTED_ON_RECONNECT,
                    reason: "not subscribed by the server after reconnect".into(),
                },
            }),
            "{actions:?}"
        );
        assert_eq!(machine.server_subscriptions(), Vec::<String>::new());
    }
}
