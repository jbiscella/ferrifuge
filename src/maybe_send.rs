//! `Send` and `Sync` where the target has threads (Block 44, D3).
//!
//! Natively they are `Send` and `Sync`, under a name that a bound of the
//! client's API can share with the browser target, where JavaScript handles
//! (`JsValue`, a `fetch` promise) are neither and the bounds ask nothing.
//! Their blanket implementations leave no type to implement them for.

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod markers {
    /// `Send` natively; nothing on the browser target (Block 44, D3).
    pub trait MaybeSend: Send {}

    impl<T: Send + ?Sized> MaybeSend for T {}

    /// `Sync` natively; nothing on the browser target (Block 44, D3).
    pub trait MaybeSync: Sync {}

    impl<T: Sync + ?Sized> MaybeSync for T {}
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod markers {
    /// `Send` natively; nothing on the browser target (Block 44, D3).
    pub trait MaybeSend {}

    impl<T: ?Sized> MaybeSend for T {}

    /// `Sync` natively; nothing on the browser target (Block 44, D3).
    pub trait MaybeSync {}

    impl<T: ?Sized> MaybeSync for T {}
}

pub use markers::{MaybeSend, MaybeSync};
