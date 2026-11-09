//! The seam between the shared adapter layer and the runtime it runs on,
//! chosen at compile time with no trait object and no generic parameter on
//! a public type (Block 44, D2): the runtime handle, spawning, timers, the
//! clocks, and the boxed future and callback types of Block 44 D3. Tokio
//! natively; the browser's event loop, `setTimeout`, `performance.now()`
//! and `Date.now()` on the browser target (Block 44, D4).

#[cfg(adapter_tokio)]
pub(super) use native::*;

#[cfg(adapter_browser)]
pub(super) use browser::*;

/// The boxed future and callback types of the client (Block 44, D3).
pub(super) use types::*;

#[cfg(adapter_tokio)]
mod native {
    use std::future::Future;
    use std::time::SystemTime;

    pub(in crate::adapter) use ::tokio::runtime::Handle;
    pub(in crate::adapter) use ::tokio::task::JoinHandle;
    pub(in crate::adapter) use ::tokio::time::{Instant, Sleep, sleep};

    /// The monotonic clock the sleep detection compares with the wall clock
    /// (Block 25, D1); its time stops in a suspend.
    pub(in crate::adapter) type Monotonic = std::time::Instant;

    /// The wall clock and the monotonic clock, read together.
    pub(in crate::adapter) fn clocks() -> (SystemTime, Monotonic) {
        (wall_clock(), Monotonic::now())
    }

    /// The wall clock.
    pub(in crate::adapter) fn wall_clock() -> SystemTime {
        SystemTime::now()
    }

    /// The runtime this code runs in, if any.
    pub(in crate::adapter) fn current_runtime() -> Option<Handle> {
        Handle::try_current().ok()
    }

    /// Runs `future` on `runtime` (Block 28, D1: every spawn goes through the
    /// client's runtime handle).
    pub(in crate::adapter) fn spawn<F>(runtime: &Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        runtime.spawn(future)
    }

    /// Lets the other tasks of the runtime run.
    pub(in crate::adapter) async fn yield_now() {
        ::tokio::task::yield_now().await;
    }

    /// Runs `call`, which may block. On a worker of a multi-thread runtime the
    /// worker hands its other tasks over first (`block_in_place`), so that a
    /// handler that blocks holds no task woken before it behind it (Block 41,
    /// D6: the driver among them); on a current-thread runtime nothing else
    /// runs until it returns.
    pub(in crate::adapter) fn hand_over_worker<R>(call: impl FnOnce() -> R) -> R {
        let multi_thread = Handle::try_current().is_ok_and(|runtime| {
            runtime.runtime_flavor() == ::tokio::runtime::RuntimeFlavor::MultiThread
        });
        if multi_thread {
            ::tokio::task::block_in_place(call)
        } else {
            call()
        }
    }
}

#[cfg(adapter_tokio)]
mod types {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    /// A boxed future the driver awaits: `Send` natively, as futures'
    /// `BoxFuture` (Block 44, D3).
    pub(in crate::adapter) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

    /// An application callback the client keeps, with no argument: `Send` and
    /// `Sync` natively (Block 44, D3).
    pub(in crate::adapter) type Callback<R> = Arc<dyn Fn() -> R + Send + Sync>;

    /// An application callback the client keeps, given a subscription's
    /// channel: `Send` and `Sync` natively (Block 44, D3).
    pub(in crate::adapter) type ChannelCallback<R> = Arc<dyn Fn(String) -> R + Send + Sync>;
}

#[cfg(adapter_browser)]
mod types {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    /// A boxed future the driver awaits: not `Send` on the browser target,
    /// where JavaScript handles are not, as futures' `LocalBoxFuture`
    /// (Block 44, D3).
    pub(in crate::adapter) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

    /// An application callback the client keeps, with no argument.
    pub(in crate::adapter) type Callback<R> = Arc<dyn Fn() -> R>;

    /// An application callback the client keeps, given a subscription's
    /// channel.
    pub(in crate::adapter) type ChannelCallback<R> = Arc<dyn Fn(String) -> R>;
}

#[cfg(adapter_browser)]
mod browser {
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use wasm_bindgen::JsCast;

    /// No runtime to name on the browser target: tasks run on the page's
    /// event loop (Block 44, D4).
    #[derive(Debug, Clone, Default)]
    pub(in crate::adapter) struct Handle;

    /// The handle of a task: awaiting it gives the task's output; dropping
    /// it lets the task run on, as tokio's does.
    pub(in crate::adapter) struct JoinHandle<T> {
        output: ::tokio::sync::oneshot::Receiver<T>,
        finished: Rc<Cell<bool>>,
    }

    impl<T> JoinHandle<T> {
        /// Whether the task has ended.
        pub(in crate::adapter) fn is_finished(&self) -> bool {
            self.finished.get()
        }
    }

    impl<T> Future for JoinHandle<T> {
        /// `Err` when the task ended without an output.
        type Output = Result<T, ()>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.output).poll(cx).map_err(drop)
        }
    }

    /// A point of the page's monotonic clock, `performance.now()`, read from
    /// `globalThis.performance` so that workers have it too (Block 44, D4).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(in crate::adapter) struct Instant(Duration);

    impl Instant {
        pub(in crate::adapter) fn now() -> Self {
            let performance = js_sys::Reflect::get(&js_sys::global(), &"performance".into())
                .ok()
                .and_then(|performance| performance.dyn_into::<web_sys::Performance>().ok());
            let millis = performance.map_or(0.0, |performance| performance.now());
            Self(Duration::from_secs_f64(millis.max(0.0) / 1000.0))
        }

        pub(in crate::adapter) fn checked_sub(self, by: Duration) -> Option<Self> {
            self.0.checked_sub(by).map(Self)
        }

        pub(in crate::adapter) fn saturating_duration_since(self, earlier: Self) -> Duration {
            self.0.saturating_sub(earlier.0)
        }
    }

    impl std::ops::Add<Duration> for Instant {
        type Output = Self;

        fn add(self, delay: Duration) -> Self {
            Self(self.0.saturating_add(delay))
        }
    }

    /// The monotonic clock the sleep detection compares with the wall clock
    /// (Block 25, D1; Block 44, D16).
    pub(in crate::adapter) type Monotonic = Instant;

    /// The wall clock and the monotonic clock, read together.
    pub(in crate::adapter) fn clocks() -> (SystemTime, Monotonic) {
        (wall_clock(), Monotonic::now())
    }

    /// The wall clock, `Date.now()`: `SystemTime::now()` traps on this
    /// target, while building one from `UNIX_EPOCH` does not (Block 44, D4).
    pub(in crate::adapter) fn wall_clock() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs_f64(js_sys::Date::now().max(0.0) / 1000.0)
    }

    /// Runs `future` on the page's event loop (`spawn_local`).
    pub(in crate::adapter) fn spawn<F>(_: &Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (sender, output) = ::tokio::sync::oneshot::channel();
        let finished = Rc::new(Cell::new(false));
        let done = Rc::clone(&finished);
        wasm_bindgen_futures::spawn_local(async move {
            let _ = sender.send(future.await);
            done.set(true);
        });
        JoinHandle { output, finished }
    }

    /// Lets the page's other tasks run: pending once, woken at once.
    pub(in crate::adapter) async fn yield_now() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    /// Runs `call`: the page has no worker to hand over.
    pub(in crate::adapter) fn hand_over_worker<R>(call: impl FnOnce() -> R) -> R {
        call()
    }

    /// The longest delay `setTimeout` takes: gloo-timers passes its `u32`
    /// as an `i32`, so a longer one would wrap and fire at once (Block 44,
    /// D4).
    const LONGEST_TIMEOUT: Duration = Duration::from_millis(i32::MAX as u64);

    /// A timer to a deadline of [`Instant`], over `setTimeout`, re-armed
    /// when it fires before its deadline, as one longer than
    /// [`LONGEST_TIMEOUT`] does.
    pub(in crate::adapter) struct Sleep {
        deadline: Instant,
        timer: Option<gloo_timers::future::TimeoutFuture>,
    }

    impl Sleep {
        pub(in crate::adapter) fn deadline(&self) -> Instant {
            self.deadline
        }

        pub(in crate::adapter) fn reset(mut self: Pin<&mut Self>, deadline: Instant) {
            self.deadline = deadline;
            self.timer = None;
        }
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            loop {
                let left = self.deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Poll::Ready(());
                }
                let timer = self.timer.get_or_insert_with(|| {
                    let millis = left.min(LONGEST_TIMEOUT).as_millis().max(1);
                    gloo_timers::future::TimeoutFuture::new(
                        u32::try_from(millis).unwrap_or(u32::MAX),
                    )
                });
                match Pin::new(timer).poll(cx) {
                    Poll::Ready(()) => self.timer = None,
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    /// A timer of `delay` from now.
    pub(in crate::adapter) fn sleep(delay: Duration) -> Sleep {
        Sleep {
            deadline: Instant::now() + delay,
            timer: None,
        }
    }
}
