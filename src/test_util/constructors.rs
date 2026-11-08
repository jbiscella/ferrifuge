//! Constructors of the events and results the client yields, so that an
//! application can unit-test the code that handles them (Block 37, D2).
//!
//! A field a constructor does not name holds what the client yields when
//! the server leaves it out. Each setter replaces its field; an `Option`
//! field's setter sets `Some`.

use std::time::SystemTime;

use crate::machine::TransportKind;
use crate::{
    ClientEvent, ClientInfo, DeltaStatistics, HistoryResult, PresenceResult, PresenceStatsResult,
    Publication, State, Statistics, StreamPosition, Subscribed,
};

/// A setter of a field holding the value it is given.
macro_rules! setter {
    ($(#[$doc:meta])* $field:ident: $type:ty) => {
        $(#[$doc])*
        #[must_use]
        pub fn $field(mut self, $field: $type) -> Self {
            self.$field = $field;
            self
        }
    };
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl ClientEvent {
    /// The `connected` event the client emits once connected over
    /// `transport` (`"websocket"`, `"http_stream"`, `"sse"`), with the
    /// client id the server assigned and the data of its connect reply.
    ///
    /// ```
    /// use ferrifuge::ClientEvent;
    ///
    /// let event = ClientEvent::connected("c1", "websocket", Some(br#"{"a":1}"#.to_vec()));
    /// assert!(matches!(event, ClientEvent::Connected { client_id, .. } if client_id == "c1"));
    /// ```
    #[must_use]
    pub fn connected(
        client_id: impl Into<String>,
        transport: impl Into<String>,
        data: Option<Vec<u8>>,
    ) -> Self {
        Self::Connected {
            client_id: client_id.into(),
            transport: transport.into(),
            data,
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Publication {
    /// A publication carrying `data`, with no offset, tags, info, epoch or
    /// channel.
    ///
    /// ```
    /// use ferrifuge::{ClientInfo, Publication};
    ///
    /// let publication = Publication::new(br#"{"n":1}"#.to_vec())
    ///     .offset(4)
    ///     .tags([("k", "v")])
    ///     .info(ClientInfo::new("c1", "u1"));
    /// assert_eq!(publication.offset, Some(4));
    /// assert_eq!(publication.tags["k"], "v");
    /// ```
    #[must_use]
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            ..Self::default()
        }
    }

    /// Sets the offset in the channel's stream.
    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Replaces the tags.
    #[must_use]
    pub fn tags<K: Into<String>, V: Into<String>>(
        mut self,
        tags: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.tags = tags
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    /// Sets the publisher.
    #[must_use]
    pub fn info(mut self, info: ClientInfo) -> Self {
        self.info = Some(info);
        self
    }

    /// Sets the epoch of the channel's stream.
    #[must_use]
    pub fn epoch(mut self, epoch: impl Into<String>) -> Self {
        self.epoch = Some(epoch.into());
        self
    }

    /// Sets the channel, for a publication of another channel than its
    /// subscription's, as a wildcard subscription receives (Block 35c).
    #[must_use]
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl ClientInfo {
    /// The client `client` of user `user`, with no connection or channel
    /// info.
    ///
    /// ```
    /// use ferrifuge::ClientInfo;
    ///
    /// let info = ClientInfo::new("c1", "u1").conn_info(br#"{"device":"a"}"#.to_vec());
    /// assert_eq!(info.user, "u1");
    /// ```
    #[must_use]
    pub fn new(client: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            client: client.into(),
            user: user.into(),
            ..Self::default()
        }
    }

    /// Sets the connection info.
    #[must_use]
    pub fn conn_info(mut self, conn_info: impl Into<Vec<u8>>) -> Self {
        self.conn_info = Some(conn_info.into());
        self
    }

    /// Sets the channel info.
    #[must_use]
    pub fn chan_info(mut self, chan_info: impl Into<Vec<u8>>) -> Self {
        self.chan_info = Some(chan_info.into());
        self
    }
}

/// The details of a `subscribed` event, from [`Subscribed::default`]:
///
/// ```
/// use ferrifuge::{StreamPosition, Subscribed};
///
/// let subscribed = Subscribed::default()
///     .recoverable(true)
///     .positioned(true)
///     .stream_position(StreamPosition { offset: 3, epoch: "e".into() });
/// assert!(subscribed.recoverable);
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Subscribed {
    /// Sets the data the server attached to the subscribe reply.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.data = Some(data.into());
        self
    }

    setter!(
        /// Sets whether the subscription is positioned.
        positioned: bool
    );
    setter!(
        /// Sets whether missed publications can be recovered.
        recoverable: bool
    );
    setter!(
        /// Sets whether the subscribe asked to recover.
        was_recovering: bool
    );
    setter!(
        /// Sets whether every missed publication was recovered.
        recovered: bool
    );
    setter!(
        /// Sets whether the server sends publications as deltas.
        delta: bool
    );
    setter!(
        /// Sets whether publications follow the event.
        has_recovered_publications: bool
    );

    /// Sets the stream position.
    #[must_use]
    pub fn stream_position(mut self, stream_position: StreamPosition) -> Self {
        self.stream_position = Some(stream_position);
        self
    }
}

/// A history result, from [`HistoryResult::default`]:
///
/// ```
/// use ferrifuge::{HistoryResult, Publication};
///
/// let history = HistoryResult::default()
///     .publications([Publication::new(br#"{"n":1}"#.to_vec()).offset(1)])
///     .epoch("e")
///     .offset(1);
/// assert_eq!(history.publications.len(), 1);
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl HistoryResult {
    /// Replaces the publications.
    #[must_use]
    pub fn publications(mut self, publications: impl IntoIterator<Item = Publication>) -> Self {
        self.publications = publications.into_iter().collect();
        self
    }

    /// Sets the epoch of the stream.
    #[must_use]
    pub fn epoch(mut self, epoch: impl Into<String>) -> Self {
        self.epoch = epoch.into();
        self
    }

    setter!(
        /// Sets the offset of the top of the stream.
        offset: u64
    );
}

/// A presence result, from [`PresenceResult::default`]:
///
/// ```
/// use ferrifuge::{ClientInfo, PresenceResult};
///
/// let presence = PresenceResult::default().clients([ClientInfo::new("c1", "u1")]);
/// assert_eq!(presence.clients["c1"].user, "u1");
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl PresenceResult {
    /// Replaces the clients, keyed by their client id as the server keys
    /// them.
    #[must_use]
    pub fn clients(mut self, clients: impl IntoIterator<Item = ClientInfo>) -> Self {
        self.clients = clients
            .into_iter()
            .map(|info| (info.client.clone(), info))
            .collect();
        self
    }
}

/// A presence stats result, from [`PresenceStatsResult::default`]:
///
/// ```
/// use ferrifuge::PresenceStatsResult;
///
/// let stats = PresenceStatsResult::default().num_clients(2).num_users(1);
/// assert_eq!((stats.num_clients, stats.num_users), (2, 1));
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl PresenceStatsResult {
    setter!(
        /// Sets the number of clients.
        num_clients: u32
    );
    setter!(
        /// Sets the number of distinct users.
        num_users: u32
    );
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Statistics {
    /// The statistics of a client in `state`, with every counter at 0 and
    /// no transport.
    ///
    /// ```
    /// use ferrifuge::{State, Statistics};
    /// use ferrifuge::machine::TransportKind;
    ///
    /// let statistics = Statistics::new(State::Connected)
    ///     .transport(TransportKind::WebSocket)
    ///     .connects(1);
    /// assert_eq!(statistics.connects, 1);
    /// ```
    #[must_use]
    pub fn new(state: State) -> Self {
        Self {
            state,
            transport: None,
            connected_since: None,
            connects: 0,
            failed_attempts: 0,
            bytes_in: 0,
            bytes_out: 0,
            messages_in: 0,
            messages_out: 0,
            publications_delivered: 0,
            subscriptions: 0,
            server_subscriptions: 0,
        }
    }

    setter!(
        /// Sets the connection state.
        state: State
    );

    /// Sets the transport in use.
    #[must_use]
    pub fn transport(mut self, transport: TransportKind) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Sets when the client connected.
    #[must_use]
    pub fn connected_since(mut self, connected_since: SystemTime) -> Self {
        self.connected_since = Some(connected_since);
        self
    }

    setter!(
        /// Sets the number of connections established.
        connects: u64
    );
    setter!(
        /// Sets the number of failed attempts.
        failed_attempts: u64
    );
    setter!(
        /// Sets the bytes read.
        bytes_in: u64
    );
    setter!(
        /// Sets the bytes written.
        bytes_out: u64
    );
    setter!(
        /// Sets the messages read.
        messages_in: u64
    );
    setter!(
        /// Sets the messages written.
        messages_out: u64
    );
    setter!(
        /// Sets the publications delivered.
        publications_delivered: u64
    );
    setter!(
        /// Sets the number of client-side subscriptions.
        subscriptions: usize
    );
    setter!(
        /// Sets the number of server-side subscriptions.
        server_subscriptions: usize
    );
}

/// Delta statistics, from [`DeltaStatistics::default`]:
///
/// ```
/// use ferrifuge::DeltaStatistics;
///
/// let delta = DeltaStatistics::default().publications(2).delta_payloads(1);
/// assert_eq!(delta.delta_payloads, 1);
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl DeltaStatistics {
    setter!(
        /// Sets the publications counted.
        publications: u64
    );
    setter!(
        /// Sets the publications that carried a full payload.
        full_payloads: u64
    );
    setter!(
        /// Sets the publications that carried a delta.
        delta_payloads: u64
    );
    setter!(
        /// Sets the payload bytes received.
        bytes_received: u64
    );
    setter!(
        /// Sets the payload bytes after the deltas were applied.
        bytes_decoded: u64
    );
}

#[cfg(test)]
// Each setter is compared with assigning its field (Block 37, D2).
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    /// A value built by a setter, and the assignment it must equal.
    type Case<T> = (T, fn(&mut T));

    #[test]
    fn each_publication_setter_sets_its_field_only() {
        let info = ClientInfo::new("c1", "u1");
        let mut expected = Publication::default();
        expected.data = b"d".to_vec();
        assert_eq!(Publication::new(b"d".to_vec()), expected);
        expected.offset = Some(4);
        assert_eq!(Publication::new(b"d".to_vec()).offset(4), expected);
        let mut tagged = Publication::new(b"d".to_vec());
        tagged.tags.insert("k".into(), "v".into());
        assert_eq!(Publication::new(b"d".to_vec()).tags([("k", "v")]), tagged);
        let mut with_info = Publication::new(b"d".to_vec());
        with_info.info = Some(info.clone());
        assert_eq!(Publication::new(b"d".to_vec()).info(info), with_info);
        let mut with_epoch = Publication::new(b"d".to_vec());
        with_epoch.epoch = Some("e".into());
        assert_eq!(Publication::new(b"d".to_vec()).epoch("e"), with_epoch);
        let mut with_channel = Publication::new(b"d".to_vec());
        with_channel.channel = Some("news.a".into());
        assert_eq!(
            Publication::new(b"d".to_vec()).channel("news.a"),
            with_channel
        );
        assert!(!Publication::new(b"d".to_vec()).delta);
    }

    #[test]
    fn each_client_info_setter_sets_its_field_only() {
        let mut expected = ClientInfo::default();
        expected.client = "c1".into();
        expected.user = "u1".into();
        assert_eq!(ClientInfo::new("c1", "u1"), expected);
        expected.conn_info = Some(b"c".to_vec());
        assert_eq!(
            ClientInfo::new("c1", "u1").conn_info(b"c".to_vec()),
            expected
        );
        expected.conn_info = None;
        expected.chan_info = Some(b"h".to_vec());
        assert_eq!(
            ClientInfo::new("c1", "u1").chan_info(b"h".to_vec()),
            expected
        );
    }

    #[test]
    fn each_subscribed_setter_sets_its_field_only() {
        let position = StreamPosition {
            offset: 3,
            epoch: "e".into(),
        };
        let cases: [Case<Subscribed>; 8] = [
            (Subscribed::default().data(b"x".to_vec()), |s| {
                s.data = Some(b"x".to_vec());
            }),
            (Subscribed::default().positioned(true), |s| {
                s.positioned = true;
            }),
            (Subscribed::default().recoverable(true), |s| {
                s.recoverable = true;
            }),
            (Subscribed::default().was_recovering(true), |s| {
                s.was_recovering = true;
            }),
            (Subscribed::default().recovered(true), |s| {
                s.recovered = true;
            }),
            (Subscribed::default().delta(true), |s| s.delta = true),
            (
                Subscribed::default().has_recovered_publications(true),
                |s| {
                    s.has_recovered_publications = true;
                },
            ),
            (Subscribed::default().stream_position(position), |s| {
                s.stream_position = Some(StreamPosition {
                    offset: 3,
                    epoch: "e".into(),
                });
            }),
        ];
        for (built, assign) in cases {
            let mut expected = Subscribed::default();
            assign(&mut expected);
            assert_eq!(built, expected);
        }
    }

    #[test]
    fn each_result_setter_sets_its_field_only() {
        let publication = Publication::new(b"d".to_vec()).offset(1);
        let mut history = HistoryResult::default();
        history.publications = vec![publication.clone()];
        assert_eq!(
            HistoryResult::default().publications([publication]),
            history
        );
        let mut history = HistoryResult::default();
        history.epoch = "e".into();
        assert_eq!(HistoryResult::default().epoch("e"), history);
        let mut history = HistoryResult::default();
        history.offset = 2;
        assert_eq!(HistoryResult::default().offset(2), history);

        let mut presence = PresenceResult::default();
        presence
            .clients
            .insert("c1".into(), ClientInfo::new("c1", "u1"));
        assert_eq!(
            PresenceResult::default().clients([ClientInfo::new("c1", "u1")]),
            presence
        );

        let mut stats = PresenceStatsResult::default();
        stats.num_clients = 2;
        assert_eq!(PresenceStatsResult::default().num_clients(2), stats);
        let mut stats = PresenceStatsResult::default();
        stats.num_users = 1;
        assert_eq!(PresenceStatsResult::default().num_users(1), stats);
    }

    #[test]
    fn each_statistics_setter_sets_its_field_only() {
        let at = SystemTime::UNIX_EPOCH;
        let cases: [Case<Statistics>; 12] = [
            (
                Statistics::new(State::Disconnected).state(State::Connected),
                |s| {
                    s.state = State::Connected;
                },
            ),
            (
                Statistics::new(State::Disconnected).transport(TransportKind::Sse),
                |s| s.transport = Some(TransportKind::Sse),
            ),
            (
                Statistics::new(State::Disconnected).connected_since(at),
                |s| {
                    s.connected_since = Some(SystemTime::UNIX_EPOCH);
                },
            ),
            (Statistics::new(State::Disconnected).connects(1), |s| {
                s.connects = 1;
            }),
            (
                Statistics::new(State::Disconnected).failed_attempts(2),
                |s| {
                    s.failed_attempts = 2;
                },
            ),
            (Statistics::new(State::Disconnected).bytes_in(3), |s| {
                s.bytes_in = 3;
            }),
            (Statistics::new(State::Disconnected).bytes_out(4), |s| {
                s.bytes_out = 4;
            }),
            (Statistics::new(State::Disconnected).messages_in(5), |s| {
                s.messages_in = 5;
            }),
            (Statistics::new(State::Disconnected).messages_out(6), |s| {
                s.messages_out = 6;
            }),
            (
                Statistics::new(State::Disconnected).publications_delivered(7),
                |s| s.publications_delivered = 7,
            ),
            (Statistics::new(State::Disconnected).subscriptions(8), |s| {
                s.subscriptions = 8;
            }),
            (
                Statistics::new(State::Disconnected).server_subscriptions(9),
                |s| s.server_subscriptions = 9,
            ),
        ];
        for (built, assign) in cases {
            let mut expected = Statistics::new(State::Disconnected);
            assign(&mut expected);
            assert_eq!(built, expected);
        }
        let empty = Statistics::new(State::Disconnected);
        assert_eq!(
            (empty.transport, empty.connected_since, empty.connects),
            (None, None, 0)
        );
    }

    #[test]
    fn each_delta_statistics_setter_sets_its_field_only() {
        let cases: [Case<DeltaStatistics>; 5] = [
            (DeltaStatistics::default().publications(1), |s| {
                s.publications = 1;
            }),
            (DeltaStatistics::default().full_payloads(2), |s| {
                s.full_payloads = 2;
            }),
            (DeltaStatistics::default().delta_payloads(3), |s| {
                s.delta_payloads = 3;
            }),
            (DeltaStatistics::default().bytes_received(4), |s| {
                s.bytes_received = 4;
            }),
            (DeltaStatistics::default().bytes_decoded(5), |s| {
                s.bytes_decoded = 5;
            }),
        ];
        for (built, assign) in cases {
            let mut expected = DeltaStatistics::default();
            assign(&mut expected);
            assert_eq!(built, expected);
        }
    }

    #[test]
    fn the_connected_event_keeps_what_it_is_given() {
        assert_eq!(
            ClientEvent::connected("c1", "websocket", Some(b"d".to_vec())),
            ClientEvent::Connected {
                client_id: "c1".into(),
                transport: "websocket".into(),
                data: Some(b"d".to_vec()),
            }
        );
    }
}
