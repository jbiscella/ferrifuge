//! Android setup (Block 24): the one call that lets the operating system's
//! certificate verifier work, made through Ferrifuge so that it reaches the
//! copy of `rustls-platform-verifier` Ferrifuge links, and so that
//! Ferrifuge knows it ran.
//!
//! **Exempt from semver:** this module exposes `jni` 0.22, which is before
//! 1.0; a minor release of Ferrifuge may follow a new `jni` line.
//!
//! The app's Gradle build must include the verifier's Kotlin component, at
//! the version of `rustls-platform-verifier-android` in `Cargo.lock`, kept
//! by the `ProGuard` rule of the verifier's README. Call one of these once per
//! process, before building a client that uses TLS: until then such a
//! client fails `build()`, or its handshakes fail, naming this call.

pub use jni;
pub use rustls_platform_verifier::android::Runtime;

use jni::objects::{Global, JClassLoader, JObject};
use jni::{Env, JavaVM};

use super::tls::set_android_ready;

/// Sets the verifier up from an NDK environment and the app's context.
///
/// # Errors
///
/// The JNI error when the context's class loader cannot be read.
pub fn init_with_env(env: &mut Env, context: JObject) -> Result<(), jni::errors::Error> {
    rustls_platform_verifier::android::init_with_env(env, context)?;
    set_android_ready();
    Ok(())
}

/// Sets the verifier up from global references the app already holds.
pub fn init_with_refs(
    java_vm: JavaVM,
    context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
) {
    rustls_platform_verifier::android::init_with_refs(java_vm, context, loader);
    set_android_ready();
}

/// Sets the verifier up with a runtime serving the JVM, the context and
/// the class loader on demand.
pub fn init_with_runtime(runtime: &'static dyn Runtime) {
    rustls_platform_verifier::android::init_with_runtime(runtime);
    set_android_ready();
}
