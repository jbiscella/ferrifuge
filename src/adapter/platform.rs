//! The seam between the shared adapter layer and the runtime it runs on,
//! chosen at compile time with no trait object and no generic parameter on
//! a public type (Block 44, D2): the runtime handle, spawning, timers, the
//! clocks, and the boxed future and callback types of Block 44 D3. Tokio
//! natively; the browser target adds its own (Block 44b).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

pub(super) use ::tokio::runtime::Handle;
pub(super) use ::tokio::task::JoinHandle;
pub(super) use ::tokio::time::{Instant, Sleep, sleep};

/// The monotonic clock the sleep detection compares with the wall clock
/// (Block 25, D1); its time stops in a suspend.
pub(super) type Monotonic = std::time::Instant;

/// The wall clock and the monotonic clock, read together.
pub(super) fn clocks() -> (SystemTime, Monotonic) {
    (wall_clock(), Monotonic::now())
}

/// The wall clock.
pub(super) fn wall_clock() -> SystemTime {
    SystemTime::now()
}

/// The runtime this code runs in, if any.
pub(super) fn current_runtime() -> Option<Handle> {
    Handle::try_current().ok()
}

/// Runs `future` on `runtime` (Block 28, D1: every spawn goes through the
/// client's runtime handle).
pub(super) fn spawn<F>(runtime: &Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    runtime.spawn(future)
}

/// Lets the other tasks of the runtime run.
pub(super) async fn yield_now() {
    ::tokio::task::yield_now().await;
}

/// Runs `call`, which may block. On a worker of a multi-thread runtime the
/// worker hands its other tasks over first (`block_in_place`), so that a
/// handler that blocks holds no task woken before it behind it (Block 41,
/// D6: the driver among them); on a current-thread runtime nothing else
/// runs until it returns.
pub(super) fn hand_over_worker<R>(call: impl FnOnce() -> R) -> R {
    let multi_thread = Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == ::tokio::runtime::RuntimeFlavor::MultiThread
    });
    if multi_thread {
        ::tokio::task::block_in_place(call)
    } else {
        call()
    }
}

/// A boxed future the driver awaits: `Send` natively, as futures'
/// `BoxFuture` (Block 44, D3).
pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An application callback the client keeps, with no argument: `Send` and
/// `Sync` natively (Block 44, D3).
pub(super) type Callback<R> = Arc<dyn Fn() -> R + Send + Sync>;

/// An application callback the client keeps, given a subscription's
/// channel: `Send` and `Sync` natively (Block 44, D3).
pub(super) type ChannelCallback<R> = Arc<dyn Fn(String) -> R + Send + Sync>;
