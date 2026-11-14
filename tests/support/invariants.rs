//! The invariant checks the adapter runner (Block 16) and the real-server
//! runner (Block 51) share: pure functions over what each runner recorded.
//! The records come from each runner's own sources (the fake server's
//! connections or the channels API, the in-process panic hook or the
//! child's panic record), never from here.

use std::time::Duration;

use ferrifuge::{State, SubscriptionState};

/// Where an object is in its lifecycle, for I1: a client's `disconnected`,
/// `connecting` and `connected`, a subscription's `unsubscribed`,
/// `subscribing` and `subscribed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    Off,
    Pending,
    On,
}

impl From<State> for Lifecycle {
    fn from(state: State) -> Self {
        match state {
            State::Disconnected => Self::Off,
            State::Connecting => Self::Pending,
            State::Connected => Self::On,
        }
    }
}

impl From<SubscriptionState> for Lifecycle {
    fn from(state: SubscriptionState) -> Self {
        match state {
            SubscriptionState::Unsubscribed => Self::Off,
            SubscriptionState::Subscribing => Self::Pending,
            SubscriptionState::Subscribed => Self::On,
        }
    }
}

/// I1: the state event `next` of an object whose previous state event was
/// `last` differs from it, and does not go from off to on (`disconnected`
/// to `connected`, `unsubscribed` to `subscribed`) without the state
/// between.
///
/// # Errors
///
/// What the violation is.
pub(crate) fn state_chain(last: Lifecycle, next: Lifecycle) -> Result<(), String> {
    if last == next {
        return Err(format!("{next:?} twice in a row"));
    }
    if last == Lifecycle::Off && next == Lifecycle::On {
        return Err("off went to on without the state between".to_owned());
    }
    Ok(())
}

/// I2: no panic, given the panics a runner counted.
///
/// # Errors
///
/// How many panics there were.
pub(crate) fn no_panic(panics: usize) -> Result<(), String> {
    match panics {
        0 => Ok(()),
        panics => Err(format!("{panics} panics")),
    }
}

/// I5: after `disconnect()`, the connections made since and those still
/// open, both 0.
///
/// # Errors
///
/// What was made or left open.
pub(crate) fn cleanup_after_disconnect(made: usize, open: usize) -> Result<(), String> {
    if made > 0 || open > 0 {
        return Err(format!(
            "{made} connections made and {open} open after disconnect()"
        ));
    }
    Ok(())
}

/// I6: a call that took `took` settled within its `bound`.
///
/// # Errors
///
/// How long it took.
pub(crate) fn call_settled(took: Duration, bound: Duration) -> Result<(), String> {
    if took > bound {
        return Err(format!(
            "a call settled after {took:?}, its bound {bound:?}"
        ));
    }
    Ok(())
}
