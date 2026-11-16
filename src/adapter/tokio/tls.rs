//! TLS settings of the tokio adapter: the build's crypto provider and how
//! servers are verified, chosen in one place.
// Block 6; Block 22.

use super::super::body::{FailureKind, TransportFailure};
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::Instant;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};
use rustls::{DigitallySignedStruct, SignatureScheme};

#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
compile_error!(
    "the tokio adapter needs a TLS crypto provider: enable the `ring` (default), `aws-lc-rs` or `fips` feature"
);

/// The crypto provider chosen by the build's features. Features are
/// additive, so the strongest enabled one wins: fips > aws-lc-rs > ring.
// Block 6.
#[cfg(feature = "fips")]
pub(in crate::adapter) fn crypto_provider() -> CryptoProvider {
    rustls::crypto::default_fips_provider()
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "fips")))]
pub(in crate::adapter) fn crypto_provider() -> CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
pub(in crate::adapter) fn crypto_provider() -> CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// Whom a client trusts.
// Block 22.
#[derive(Clone, Default)]
pub(crate) enum Trust {
    /// The platform's roots, or its verifier.
    #[default]
    Platform,
    /// The platform's, and these certificate authorities.
    ExtraRoots(Arc<[CertificateDer<'static>]>),
    /// The application's configuration, as given but for its ALPN.
    Raw(Arc<ClientConfig>),
    /// A stub of the operating system's verifier, wrapped as the
    /// platform's is: the tests' way to that path on every platform.
    #[cfg(test)]
    OffThread(Arc<dyn ServerCertVerifier>),
}

impl std::fmt::Debug for Trust {
    // Neither certificates nor configurations: their number, or whether one
    // is set (Block 22, D5).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Platform => f.write_str("Platform"),
            Self::ExtraRoots(roots) => write!(f, "ExtraRoots(<{}>)", roots.len()),
            Self::Raw(_) => f.write_str("Raw(..)"),
            #[cfg(test)]
            Self::OffThread(_) => f.write_str("OffThread(..)"),
        }
    }
}

/// The TLS settings of one client: the process-wide ones when it trusts the
/// platform alone, its own otherwise.
#[derive(Debug, Default)]
pub(in crate::adapter) struct Tls {
    trust: Trust,
    /// Its own TLS settings, built at its first secure attempt.
    config: BuildOnce<Settings>,
}

/// TLS settings, and the verifier wrapper they verify through, if any: a
/// WebTransport handshake copies the settings with the wrapper bound to
/// its deadline.
// Block 62, D1.
#[derive(Clone, Debug)]
pub(in crate::adapter) struct Settings {
    pub(in crate::adapter) config: Arc<ClientConfig>,
    #[cfg_attr(
        not(feature = "unstable-webtransport"),
        allow(dead_code, reason = "read by the WebTransport transport")
    )]
    pub(in crate::adapter) off_thread: Option<OffThread>,
}

impl Settings {
    /// Settings verifying as `config` does, through no wrapper.
    fn plain(config: ClientConfig) -> Self {
        Self {
            config: Arc::new(config),
            off_thread: None,
        }
    }
}

impl Tls {
    /// Whether the client trusts the platform alone, sharing the
    /// process-wide settings.
    pub(in crate::adapter) fn is_platform(&self) -> bool {
        matches!(self.trust, Trust::Platform)
    }

    pub(in crate::adapter) fn new(trust: Trust) -> Self {
        Self {
            trust,
            ..Self::default()
        }
    }

    /// The settings of `wss://` connections.
    pub(in crate::adapter) fn config(&self) -> Result<Arc<ClientConfig>, TransportFailure> {
        self.setup().map(|(config, _)| config)
    }

    /// [`Tls::config`], and whether it is kept: a read of the platform's
    /// root certificates that reported errors serves the attempt but is
    /// read again by the next one.
    // Block 6, review item C7.
    pub(in crate::adapter) fn setup(&self) -> Result<(Arc<ClientConfig>, bool), TransportFailure> {
        self.settings_kept()
            .map(|(settings, keep)| (settings.config, keep))
    }

    /// [`Tls::config`], with the verifier wrapper it verifies through.
    #[cfg(feature = "unstable-webtransport")]
    pub(in crate::adapter) fn settings(&self) -> Result<Settings, TransportFailure> {
        self.settings_kept().map(|(settings, _)| settings)
    }

    fn settings_kept(&self) -> Result<(Settings, bool), TransportFailure> {
        match &self.trust {
            Trust::Platform => platform_setup(),
            Trust::ExtraRoots(roots) => self.config.get(|| verified_config(roots)),
            Trust::Raw(raw) => self
                .config
                .get(|| Ok((Settings::plain(http1_only(raw)), true))),
            #[cfg(test)]
            Trust::OffThread(stub) => self.config.get(|| {
                off_thread_settings(Arc::clone(stub), Arc::new(crypto_provider()))
                    .map(|settings| (settings, true))
            }),
        }
    }
}

/// TLS settings of an HTTP client that only ever speaks plain `http://`:
/// no root certificates, so it works on hosts without platform ones, and
/// still the build's provider, as the HTTP client takes no default one.
pub(in crate::adapter) fn plain_tls_config() -> ClientConfig {
    ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("the provider supports the default protocol versions")
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth()
}

/// A copy of the application's configuration offering HTTP/1.1 only, the
/// one protocol of the transports: one offering `h2` would fail them.
// Block 22, D3.
fn http1_only(raw: &ClientConfig) -> ClientConfig {
    let mut config = raw.clone();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

/// The process-wide settings of the clients trusting the platform alone.
///
/// The crypto provider is set explicitly rather than taken from the process
/// default: when another crate also enables another rustls provider, rustls
/// cannot pick a default and panics.
fn platform_setup() -> Result<(Settings, bool), TransportFailure> {
    static CONFIG: BuildOnce<Settings> = BuildOnce::new();
    CONFIG.get(|| verified_config(&[]))
}

/// How servers are verified in this build and on this platform, for tests.
// Block 22, D1.
#[cfg(test)]
pub(in crate::adapter) const VERIFIER: &str = if cfg!(all(
    not(feature = "fips"),
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "android"
    )
)) {
    "platform"
} else {
    "webpki"
};

/// TLS settings verifying servers as this build does on this platform, also
/// trusting `extra`, and whether to keep them.
///
/// The operating system's verifier on macOS, iOS, Windows and Android, but
/// in the fips build, whose chains must be checked inside the FIPS module.
// Block 22, D1.
#[cfg(all(
    not(feature = "fips"),
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "android"
    )
))]
fn verified_config(
    extra: &[CertificateDer<'static>],
) -> Result<(Settings, bool), TransportFailure> {
    let provider = Arc::new(crypto_provider());
    let verifier = platform_verifier(extra, &provider)?;
    // The operating system reads its store at each verification.
    off_thread_settings(verifier, provider).map(|settings| (settings, true))
}

/// TLS settings verifying servers as this build does on this platform, also
/// trusting `extra`, and whether to keep them.
///
/// Webpki over the platform's root certificates: on Linux this is what the
/// platform verifier does, but the read's errors must be seen, so that a
/// read with errors is tried again at the next attempt; in the fips build,
/// on every platform.
// Block 22, D1; C7.
#[cfg(not(all(
    not(feature = "fips"),
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "android"
    )
)))]
fn verified_config(
    extra: &[CertificateDer<'static>],
) -> Result<(Settings, bool), TransportFailure> {
    let found = rustls_native_certs::load_native_certs();
    roots_config(found.certs, extra, found.errors.is_empty())
}

/// The operating system's verifier, also trusting `extra`.
#[cfg(all(
    not(feature = "fips"),
    any(target_os = "macos", target_os = "ios", target_os = "windows")
))]
fn platform_verifier(
    extra: &[CertificateDer<'static>],
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, TransportFailure> {
    let verifier = rustls_platform_verifier::Verifier::new_with_extra_roots(
        extra.iter().cloned(),
        Arc::clone(provider),
    )
    .map_err(|e| TransportFailure::new(FailureKind::Tls, e.to_string()))?;
    Ok(Arc::new(verifier))
}

/// Android's verifier, behind the setup gate. It takes no extra roots:
/// `ClientBuilder::build` refuses them there.
// Block 24, D5; D3.
#[cfg(all(not(feature = "fips"), target_os = "android"))]
fn platform_verifier(
    _extra: &[CertificateDer<'static>],
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, TransportFailure> {
    let verifier = rustls_platform_verifier::Verifier::new(Arc::clone(provider))
        .map_err(|e| TransportFailure::new(FailureKind::Tls, e.to_string()))?;
    Ok(Arc::new(Guarded::new(verifier, android_ready)))
}

/// Set once one of `ferrifuge::android`'s setup calls succeeded: the
/// verifier offers no way to ask.
// Block 24, D4.
static ANDROID_READY: AtomicBool = AtomicBool::new(false);

/// Whether Android's verifier was set up through Ferrifuge.
pub(crate) fn android_ready() -> bool {
    ANDROID_READY.load(Ordering::Acquire)
}

#[cfg_attr(
    not(target_os = "android"),
    allow(dead_code, reason = "the guard of the Android verifier")
)]
pub(crate) fn set_android_ready() {
    ANDROID_READY.store(true, Ordering::Release);
}

/// The message of a TLS use before Android's verifier is set up.
pub(crate) const ANDROID_SETUP_MISSING: &str = "the Android certificate verifier is not set up: call ferrifuge::android::init_with_env once before building a client";

/// A verifier failing its handshakes until `ready` and turning a panic of
/// the one it wraps into an error: Android's panics at its first check
/// without the setup, and with a Kotlin component of another version,
/// inside the client's task.
// Block 24, D5.
#[cfg_attr(
    not(target_os = "android"),
    allow(dead_code, reason = "the guard of the Android verifier")
)]
struct Guarded<V> {
    inner: V,
    ready: fn() -> bool,
}

#[cfg_attr(
    not(target_os = "android"),
    allow(dead_code, reason = "the guard of the Android verifier")
)]
impl<V> Guarded<V> {
    fn new(inner: V, ready: fn() -> bool) -> Self {
        Self { inner, ready }
    }
}

impl<V> std::fmt::Debug for Guarded<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guarded").finish_non_exhaustive()
    }
}

/// Runs `check` unless it panics, which becomes an error.
#[cfg_attr(
    not(target_os = "android"),
    allow(dead_code, reason = "the guard of the Android verifier")
)]
fn unpanicked<T>(check: impl FnOnce() -> Result<T, rustls::Error>) -> Result<T, rustls::Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)).unwrap_or_else(|_| {
        Err(rustls::Error::General(
            "the platform certificate verifier panicked".to_owned(),
        ))
    })
}

impl<V: ServerCertVerifier> ServerCertVerifier for Guarded<V> {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if !(self.ready)() {
            return Err(rustls::Error::General(ANDROID_SETUP_MISSING.to_owned()));
        }
        unpanicked(|| {
            self.inner.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            )
        })
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        unpanicked(|| self.inner.verify_tls12_signature(message, cert, dss))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        unpanicked(|| self.inner.verify_tls13_signature(message, cert, dss))
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The message of a certificate verification that did not finish by the
/// handshake's deadline, or that more late verifications than
/// [`MAX_LATE`] refused at once.
// Block 62, D1.
pub(in crate::adapter) const VERIFICATION_TIMEOUT: &str =
    "handshake timeout: certificate verification did not finish";

/// How many verification threads may still run past their deadline in the
/// process; past them, a verification fails at once.
// Block 62, D1.
const MAX_LATE: usize = 8;

/// The verification threads still running past their deadline.
#[cfg_attr(
    not(all(
        not(feature = "fips"),
        any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows",
            target_os = "android"
        )
    )),
    allow(dead_code, reason = "the wrapper of the platform verifier")
)]
static LATE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The handshake deadline of the attempt or the emulation request this
    /// thread polls, while it polls it.
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Runs `poll` with `deadline` as this thread's handshake deadline, the one
/// set before restored afterwards, even when `poll` panics.
// Block 62, D1.
pub(in crate::adapter) fn with_deadline<T>(
    deadline: Option<Instant>,
    poll: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<Instant>);
    impl Drop for Restore {
        fn drop(&mut self) {
            DEADLINE.set(self.0);
        }
    }
    let _restore = Restore(DEADLINE.replace(deadline));
    poll()
}

/// A layer of the HTTP client's connector: each connection takes the
/// handshake deadline of the thread that starts it, and has it set on every
/// poll, on whichever task polls it. hyper finishes a connection that an
/// idle one beat on a task of its own.
// Block 62, D1; hyper-util 0.1.21 client/legacy/client.rs:427-447.
#[derive(Clone, Copy, Debug)]
pub(in crate::adapter) struct KeepDeadline;

impl<S> tower_layer::Layer<S> for KeepDeadline {
    type Service = KeptDeadline<S>;

    fn layer(&self, inner: S) -> Self::Service {
        KeptDeadline(inner)
    }
}

/// The connector under [`KeepDeadline`].
#[derive(Clone, Debug)]
pub(in crate::adapter) struct KeptDeadline<S>(S);

impl<S: tower_service::Service<R>, R> tower_service::Service<R> for KeptDeadline<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = WithDeadline<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let deadline = deadline();
        WithDeadline {
            inner: Box::pin(with_deadline(deadline, || self.0.call(request))),
            deadline,
        }
    }
}

/// A future polled with `deadline` as its thread's handshake deadline.
pub(in crate::adapter) struct WithDeadline<F> {
    inner: std::pin::Pin<Box<F>>,
    deadline: Option<Instant>,
}

impl<F> WithDeadline<F> {
    /// `inner`, polled with `deadline` as its thread's handshake deadline.
    // Block 64, D1.
    pub(in crate::adapter) fn new(deadline: Option<Instant>, inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
            deadline,
        }
    }
}

impl<F: Future> Future for WithDeadline<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let deadline = self.deadline;
        with_deadline(deadline, || self.inner.as_mut().poll(cx))
    }
}

/// The handshake deadline of the attempt or the emulation request this
/// thread polls, if any.
pub(in crate::adapter) fn deadline() -> Option<Instant> {
    DEADLINE.get()
}

/// The operating system's verifier, run on a thread of its own: it may
/// fetch over the network and block for seconds, which must not hold the
/// runtime's thread past the handshake's deadline.
///
/// Each check of a server's certificate runs on a new thread, which the
/// handshake waits for until its deadline: its own, or else the one of the
/// attempt or the emulation request the calling thread polls; without
/// either, until the check ends. On a multi-thread runtime it waits inside `block_in_place`, so
/// that the runtime's other tasks, timers and sockets go on on another
/// thread. Past the deadline the check fails, and its thread runs on until
/// the operating system returns; while [`MAX_LATE`] such threads run, a
/// check fails at once.
// Block 62, D1.
#[derive(Clone)]
pub(in crate::adapter) struct OffThread {
    inner: Arc<dyn ServerCertVerifier>,
    /// The deadline of the handshake that copied it, for a handshake run
    /// on a task other than the attempt's.
    deadline: Option<Instant>,
    /// The late threads counted against [`MAX_LATE`]: the process's.
    late: &'static AtomicUsize,
}

impl OffThread {
    #[cfg_attr(
        not(all(
            not(feature = "fips"),
            any(
                target_os = "macos",
                target_os = "ios",
                target_os = "windows",
                target_os = "android"
            )
        )),
        allow(dead_code, reason = "the wrapper of the platform verifier")
    )]
    fn new(inner: Arc<dyn ServerCertVerifier>) -> Self {
        Self {
            inner,
            deadline: None,
            late: &LATE,
        }
    }

    /// A copy waiting until `deadline`, whichever thread calls it.
    #[cfg_attr(
        not(feature = "unstable-webtransport"),
        allow(dead_code, reason = "used by the WebTransport transport")
    )]
    pub(in crate::adapter) fn until(&self, deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            ..self.clone()
        }
    }
}

impl std::fmt::Debug for OffThread {
    // Its name only: the verifier it wraps may print its extra roots
    // (Block 22, D5).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffThread").finish_non_exhaustive()
    }
}

/// TLS settings verifying servers through `inner`, run off the runtime's
/// thread.
// Block 62, D1.
#[cfg_attr(
    not(all(
        not(feature = "fips"),
        any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows",
            target_os = "android"
        )
    )),
    allow(dead_code, reason = "the wrapper of the platform verifier")
)]
fn off_thread_settings(
    inner: Arc<dyn ServerCertVerifier>,
    provider: Arc<CryptoProvider>,
) -> Result<Settings, TransportFailure> {
    let off_thread = OffThread::new(inner);
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| TransportFailure::new(FailureKind::Tls, e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(off_thread.clone()))
        .with_no_client_auth();
    Ok(Settings {
        config: Arc::new(config),
        off_thread: Some(off_thread),
    })
}

/// The error of a verification past its deadline.
fn verification_timeout() -> rustls::Error {
    rustls::Error::General(VERIFICATION_TIMEOUT.to_owned())
}

type Verified = Result<ServerCertVerified, rustls::Error>;

/// Where a verification thread leaves its result for the handshake waiting
/// on it.
#[derive(Default)]
struct Check {
    state: Mutex<CheckState>,
    done: Condvar,
}

#[derive(Default)]
enum CheckState {
    #[default]
    Running,
    /// The verifier returned, or panicked.
    Done(std::thread::Result<Verified>),
    /// The handshake stopped waiting: the thread counts as late until it
    /// ends.
    Late,
}

impl Check {
    fn state(&self) -> std::sync::MutexGuard<'_, CheckState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run by the verification thread once the verifier returned.
    fn finish(&self, verified: std::thread::Result<Verified>, late: &AtomicUsize) {
        let mut state = self.state();
        if matches!(*state, CheckState::Late) {
            late.fetch_sub(1, Ordering::AcqRel);
        } else {
            *state = CheckState::Done(verified);
            self.done.notify_one();
        }
    }

    /// The verifier's result, or the timeout once `deadline` passed, the
    /// thread then counted in `late`. A panic of the verifier goes on here.
    fn wait(&self, deadline: Option<Instant>, late: &AtomicUsize) -> Verified {
        let mut state = self.state();
        loop {
            if matches!(*state, CheckState::Done(_)) {
                let CheckState::Done(verified) = std::mem::take(&mut *state) else {
                    unreachable!("matched above")
                };
                return verified.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            }
            state = match deadline {
                None => self
                    .done
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        *state = CheckState::Late;
                        late.fetch_add(1, Ordering::AcqRel);
                        return Err(verification_timeout());
                    }
                    self.done
                        .wait_timeout(state, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }
}

/// Runs `wait`, which blocks: inside `block_in_place` on a multi-thread
/// runtime, so that its other tasks move to another thread; as it is
/// otherwise, holding a current-thread runtime until it returns.
// Block 62, D1.
fn off_runtime<T>(wait: impl FnOnce() -> T) -> T {
    let multi_thread = tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    });
    if multi_thread {
        tokio::task::block_in_place(wait)
    } else {
        wait()
    }
}

impl ServerCertVerifier for OffThread {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let deadline = self.deadline.or_else(deadline);
        if self.late.load(Ordering::Acquire) >= MAX_LATE
            || deadline.is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(verification_timeout());
        }
        let check = Arc::new(Check::default());
        let job = {
            let check = Arc::clone(&check);
            let inner = Arc::clone(&self.inner);
            let late = self.late;
            let end_entity = end_entity.clone().into_owned();
            let intermediates: Vec<CertificateDer<'static>> = intermediates
                .iter()
                .map(|certificate| certificate.clone().into_owned())
                .collect();
            let server_name = server_name.to_owned();
            let ocsp_response = ocsp_response.to_vec();
            move || {
                let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    inner.verify_server_cert(
                        &end_entity,
                        &intermediates,
                        &server_name,
                        &ocsp_response,
                        now,
                    )
                }));
                check.finish(verified, late);
            }
        };
        std::thread::Builder::new()
            .name("ferrifuge-verify".to_owned())
            .spawn(job)
            .map_err(|e| {
                rustls::Error::General(format!(
                    "certificate verification could not start a thread: {e}"
                ))
            })?;
        off_runtime(|| check.wait(deadline, self.late))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.inner.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[rustls::DistinguishedName]> {
        self.inner.root_hint_subjects()
    }
}

/// The TLS settings trusting `platform` and `extra`, kept when `complete`:
/// read without errors. Unreadable platform certificates are skipped, as
/// browsers do; the extra ones were checked by `ClientBuilder::build`.
#[cfg_attr(
    all(
        not(feature = "fips"),
        any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows",
            target_os = "android"
        )
    ),
    allow(
        dead_code,
        reason = "these targets verify through the platform verifier, without these settings"
    )
)]
fn roots_config(
    platform: Vec<CertificateDer<'static>>,
    extra: &[CertificateDer<'static>],
    complete: bool,
) -> Result<(Settings, bool), TransportFailure> {
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(platform);
    let (extra_added, _) = roots.add_parsable_certificates(extra.iter().cloned());
    if added + extra_added == 0 {
        return Err(TransportFailure::new(
            FailureKind::Tls,
            "no platform root certificates found",
        ));
    }
    let config = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| TransportFailure::new(FailureKind::Tls, e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((Settings::plain(config), complete))
}

/// The index of the first certificate of `roots` webpki cannot take as a
/// root.
// Block 22, D2.
pub(in crate::adapter) fn unparsable_root(roots: &[CertificateDer<'static>]) -> Option<usize> {
    roots
        .iter()
        .position(|root| RootCertStore::empty().add(root.clone()).is_err())
}

/// A value built at first use and shared; a build that fails, or that its
/// builder says not to keep, is not kept, so the next use builds again.
// Block 6, review item C7.
#[derive(Debug)]
pub(in crate::adapter) struct BuildOnce<T>(OnceLock<T>);

impl<T> Default for BuildOnce<T> {
    fn default() -> Self {
        Self(OnceLock::new())
    }
}

impl<T: Clone> BuildOnce<T> {
    pub(in crate::adapter) const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// The value, and whether it is kept: built by `build`, which also says
    /// whether to keep it, unless an earlier build was kept. Two first uses
    /// racing may both build; one value is kept.
    pub(in crate::adapter) fn get(
        &self,
        build: impl FnOnce() -> Result<(T, bool), TransportFailure>,
    ) -> Result<(T, bool), TransportFailure> {
        if let Some(value) = self.0.get() {
            return Ok((value.clone(), true));
        }
        let (value, keep) = build()?;
        if !keep {
            return Ok((value, false));
        }
        Ok((self.0.get_or_init(|| value).clone(), true))
    }
}

#[cfg(test)]
pub(in crate::adapter) mod tests {
    use std::sync::Arc;

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

    use super::*;

    /// A CA and a `localhost` server certificate it signed, valid for a
    /// short while around now, with the extensions Apple's and Windows'
    /// verifiers want of a server certificate: the CA, the server's chain
    /// and its key.
    pub(in crate::adapter) fn private_chain() -> (
        CertificateDer<'static>,
        CertificateDer<'static>,
        PrivateKeyDer<'static>,
    ) {
        let now = time::OffsetDateTime::now_utc();
        let ca_key = KeyPair::generate().expect("CA key");
        let mut ca = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca.distinguished_name
            .push(DnType::CommonName, "ferrifuge private test CA");
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca.not_before = now - time::Duration::days(1);
        ca.not_after = now + time::Duration::days(30);
        let ca_cert = ca.self_signed(&ca_key).expect("CA certificate");
        let issuer = Issuer::new(ca, ca_key);
        let server_key = KeyPair::generate().expect("server key");
        let mut server =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("server parameters");
        server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        server.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server.not_before = now - time::Duration::days(1);
        server.not_after = now + time::Duration::days(30);
        let server_cert = server
            .signed_by(&server_key, &issuer)
            .expect("server certificate");
        (
            ca_cert.der().clone(),
            server_cert.der().clone(),
            PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        )
    }

    /// Runs a TLS handshake in memory between `client` and a server
    /// presenting `chain`: the client's error, if it refuses the server.
    fn handshake(
        client: Arc<ClientConfig>,
        chain: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    ) -> Result<(), rustls::Error> {
        let server = ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![chain], key)
            .expect("server configuration");
        let name = ServerName::try_from("localhost").expect("a name");
        let mut client = ClientConnection::new(client, name).expect("client connection");
        let mut server = ServerConnection::new(Arc::new(server)).expect("server connection");
        while client.is_handshaking() || server.is_handshaking() {
            let mut to_server = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut to_server).expect("client write");
            }
            server
                .read_tls(&mut to_server.as_slice())
                .expect("server read");
            // The client's alert ends the server too: the client's error
            // is the one asked for.
            let server_side = server.process_new_packets();
            let mut to_client = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut to_client).expect("server write");
            }
            client
                .read_tls(&mut to_client.as_slice())
                .expect("client read");
            client.process_new_packets()?;
            server_side.expect("the server side of the handshake");
            assert!(
                !to_server.is_empty() || !to_client.is_empty(),
                "the handshake stalled"
            );
        }
        Ok(())
    }

    /// A stub verifier accepting every chain, or panicking.
    #[derive(Debug)]
    struct Stub {
        panics: bool,
    }

    impl ServerCertVerifier for Stub {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            assert!(!self.panics, "the stub panics");
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            Vec::new()
        }
    }

    fn verify(verifier: &impl ServerCertVerifier) -> Result<ServerCertVerified, rustls::Error> {
        let (_, chain, _) = private_chain();
        verifier.verify_server_cert(
            &chain,
            &[],
            &ServerName::try_from("localhost").expect("a name"),
            &[],
            UnixTime::now(),
        )
    }

    /// How long the slow stub's check takes, and the handshake timeout of
    /// the clients using it (Block 62, structural acceptance).
    pub(in crate::adapter) const SLOW_CHECK: Duration = Duration::from_secs(2);
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
    /// The bound of a failed attempt, and of another task's timer.
    const BOUND: Duration = Duration::from_millis(1500);

    use std::time::Duration;

    /// A stub of the operating system's verifier taking [`SLOW_CHECK`] to
    /// accept every chain, recording when each check started.
    #[derive(Debug, Default)]
    pub(in crate::adapter) struct Slow {
        started: Mutex<Vec<Instant>>,
    }

    impl Slow {
        /// When the checks started.
        pub(in crate::adapter) fn started(&self) -> Vec<Instant> {
            self.started.lock().unwrap().clone()
        }
    }

    impl ServerCertVerifier for Slow {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            self.started.lock().unwrap().push(Instant::now());
            std::thread::sleep(SLOW_CHECK);
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            crypto_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    /// Whether `verified` is the timeout of a verification.
    pub(in crate::adapter) fn timed_out<T>(verified: &Result<T, rustls::Error>) -> bool {
        matches!(verified, Err(rustls::Error::General(message)) if message == VERIFICATION_TIMEOUT)
    }

    /// A ninth verification while eight run past their deadline fails at
    /// once, without a thread of its own; the late threads free their
    /// places as their checks end (Block 62, D1).
    #[test]
    fn a_ninth_verification_past_its_deadline_fails_at_once() {
        static LATE_HERE: AtomicUsize = AtomicUsize::new(0);
        let slow = Arc::new(Slow::default());
        let wrapper = OffThread {
            inner: Arc::clone(&slow) as Arc<dyn ServerCertVerifier>,
            deadline: Some(Instant::now() + HANDSHAKE_TIMEOUT),
            late: &LATE_HERE,
        };
        let (_, chain, _) = private_chain();
        let checks: Vec<_> = (0..MAX_LATE)
            .map(|_| {
                let wrapper = wrapper.clone();
                let chain = chain.clone();
                std::thread::spawn(move || verify_chain(&wrapper, &chain))
            })
            .collect();
        let late: Vec<_> = checks
            .into_iter()
            .map(|check| check.join().expect("a verification"))
            .collect();
        let ninth = wrapper.until(Some(Instant::now() + HANDSHAKE_TIMEOUT));

        let start = Instant::now();
        let refused = verify_chain(&ninth, &chain);
        let took = start.elapsed();

        assert!(late.iter().all(timed_out), "{late:?}");
        assert!(timed_out(&refused), "{refused:?}");
        assert!(took < Duration::from_millis(100), "{took:?}");
        assert_eq!(slow.started().len(), MAX_LATE);
        let freed = Instant::now() + SLOW_CHECK + BOUND;
        while LATE_HERE.load(Ordering::Acquire) > 0 && Instant::now() < freed {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(LATE_HERE.load(Ordering::Acquire), 0);
    }

    /// The wrapper returns what the verifier it wraps returned, and goes on
    /// with its panic.
    #[test]
    fn the_wrapper_returns_the_verifiers_result_and_its_panic() {
        let accepting = OffThread::new(Arc::new(Stub { panics: false }));
        let panicking = OffThread::new(Arc::new(Stub { panics: true }));

        let accepted = verify(&accepting);
        let panicked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(&panicking)));

        assert!(accepted.is_ok(), "{accepted:?}");
        assert!(panicked.is_err());
    }

    fn verify_chain(
        verifier: &impl ServerCertVerifier,
        chain: &CertificateDer<'static>,
    ) -> Result<ServerCertVerified, rustls::Error> {
        verifier.verify_server_cert(
            chain,
            &[],
            &ServerName::try_from("localhost").expect("a name"),
            &[],
            UnixTime::now(),
        )
    }

    /// A TLS server on 127.0.0.1 presenting a private chain, handshaking
    /// with each connection and nothing more: its port.
    pub(in crate::adapter) async fn tls_server() -> u16 {
        let (_, chain, key) = private_chain();
        let config = ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![chain], key)
            .expect("server configuration");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a listener");
        let port = listener.local_addr().expect("its address").port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let _ = acceptor.accept(tcp).await;
                });
            }
        });
        port
    }

    fn wss(port: u16) -> String {
        format!("wss://127.0.0.1:{port}/connection/websocket")
    }

    /// A client of `endpoint` whose TLS settings verify through the wrapper
    /// around `slow`, with a handshake timeout of [`HANDSHAKE_TIMEOUT`].
    fn slow_client(endpoint: String, slow: &Arc<Slow>) -> crate::Client {
        let settings = off_thread_settings(
            Arc::clone(slow) as Arc<dyn ServerCertVerifier>,
            Arc::new(crypto_provider()),
        )
        .expect("TLS settings");
        crate::Client::builder(endpoint)
            .config(crate::Config::default().handshake_timeout(HANDSHAKE_TIMEOUT))
            .tls_config(settings.config)
            .build()
            .expect("a client")
    }

    /// The message of the client's first error event.
    async fn first_error(client: &crate::Client) -> String {
        use futures_util::StreamExt;
        let mut events = client.events();
        while let Some(event) = events.next().await {
            if let crate::ClientEvent::Error(error) = event {
                return error.message;
            }
        }
        panic!("the events ended without an error");
    }

    /// A task sleeping 100 ms: when it woke.
    fn ticker() -> tokio::task::JoinHandle<Instant> {
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Instant::now()
        })
    }

    /// On a current-thread runtime a slow verification holds the thread
    /// until the attempt's handshake deadline only: the attempt fails with
    /// the verification's timeout, and another task's timer fires late but
    /// within the same bound (Block 62, D1).
    #[test]
    fn a_slow_verification_holds_a_current_thread_runtime_until_the_handshake_deadline() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let slow = Arc::new(Slow::default());
            let client = slow_client(wss(tls_server().await), &slow);
            let start = Instant::now();
            let timer = ticker();

            client.connect();
            let message = tokio::time::timeout(SLOW_CHECK * 2, first_error(&client))
                .await
                .expect("an error event");
            let failed = start.elapsed();
            let ticked = timer.await.expect("the ticker") - start;

            assert!(message.contains(VERIFICATION_TIMEOUT), "{message}");
            assert_eq!(slow.started().len(), 1);
            assert!(failed < BOUND, "{failed:?}");
            assert!(ticked < BOUND, "{ticked:?}");
            client.disconnect();
        });
    }

    /// On a multi-thread runtime the verification waits off the runtime:
    /// another task's timer, armed before the connect and firing every
    /// 25 ms, fires while it runs (Block 62, D1). The check is on the
    /// verification's window, wherever it falls: a slow device's handshake
    /// can reach the verifier after a single 100 ms timer has fired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_verification_leaves_a_multi_thread_runtime_running() {
        let slow = Arc::new(Slow::default());
        let client = slow_client(wss(tls_server().await), &slow);
        let start = Instant::now();
        let ticks = Arc::new(Mutex::new(Vec::new()));
        let ticker = tokio::spawn({
            let ticks = Arc::clone(&ticks);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    ticks.lock().unwrap().push(Instant::now());
                }
            }
        });

        client.connect();
        let message = tokio::time::timeout(SLOW_CHECK * 2, first_error(&client))
            .await
            .expect("an error event");
        let failed = Instant::now();
        ticker.abort();
        let ticks = ticks.lock().unwrap().clone();
        let started = slow.started();

        assert!(message.contains(VERIFICATION_TIMEOUT), "{message}");
        assert_eq!(started.len(), 1);
        assert!(failed - start < BOUND, "{:?}", failed - start);
        assert!(
            ticks.iter().any(|&tick| started[0] < tick && tick < failed),
            "no tick during the verification, which started {:?} after the start and ended {:?} after it; ticks at {:?}",
            started[0] - start,
            failed - start,
            ticks.iter().map(|&tick| tick - start).collect::<Vec<_>>()
        );
        client.disconnect();
    }

    /// Android's verifier is guarded (Block 24, D5): without the setup the
    /// handshake fails naming the call, with it the check is the wrapped
    /// verifier's, and its panic becomes an error.
    #[test]
    fn the_guarded_verifier_needs_the_setup_and_survives_a_panic() {
        let unready = Guarded::new(Stub { panics: false }, || false);
        let ready = Guarded::new(Stub { panics: false }, || true);
        let panicking = Guarded::new(Stub { panics: true }, || true);

        let refused = verify(&unready);
        let accepted = verify(&ready);
        let panicked = verify(&panicking);

        assert!(
            matches!(&refused, Err(rustls::Error::General(message)) if message == ANDROID_SETUP_MISSING),
            "{refused:?}"
        );
        assert!(accepted.is_ok(), "{accepted:?}");
        assert!(
            matches!(&panicked, Err(rustls::Error::General(message)) if message.contains("panicked")),
            "{panicked:?}"
        );
    }

    /// The verifier this build uses on this platform (Block 22, D1), and
    /// whether its settings run it off the runtime's thread: the platform's
    /// always is (Block 62, D1).
    #[test]
    fn the_build_verifies_with_its_platforms_verifier() {
        let expected = if cfg!(feature = "fips") {
            "webpki"
        } else if cfg!(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows",
            target_os = "android"
        )) {
            "platform"
        } else {
            "webpki"
        };
        let settings = Tls::default().settings_kept().expect("TLS settings").0;
        let wrapped = format!("{:?}", settings.config).contains("OffThread {");

        println!("verifier: {VERIFIER}, off the runtime's thread: {wrapped}");
        assert_eq!(VERIFIER, expected);
        assert_eq!(wrapped, expected == "platform");
        assert_eq!(settings.off_thread.is_some(), wrapped);
    }

    /// The chosen verifier trusts an extra root and refuses a server whose
    /// issuer it does not know, on every platform (Block 22, D1 and D2).
    /// On Android, where the unit tests run on an emulator without the app's
    /// setup (Block 58, D4), both handshakes are refused, naming it: the
    /// verifier there takes no extra roots and waits for
    /// `ferrifuge::android::init_with_env` (Block 24, D3 and D5).
    #[test]
    fn an_extra_root_is_trusted_and_an_unknown_issuer_refused() {
        let (ca, chain, key) = private_chain();
        let trusting = Tls::new(Trust::ExtraRoots(vec![ca].into()))
            .config()
            .expect("TLS settings");
        let platform = Tls::default().config().expect("TLS settings");

        let trusted = handshake(trusting, chain.clone(), key.clone_key());
        let refused = handshake(platform, chain, key);

        if cfg!(all(target_os = "android", not(feature = "fips"))) {
            let missing = Err(rustls::Error::General(ANDROID_SETUP_MISSING.to_owned()));
            assert_eq!(trusted, missing);
            assert_eq!(refused, missing);
        } else {
            assert_eq!(trusted, Ok(()));
            assert!(
                matches!(&refused, Err(rustls::Error::InvalidCertificate(_))),
                "{refused:?}"
            );
        }
    }

    /// A raw configuration is used as given, but its copies offer
    /// HTTP/1.1 only (Block 22, D3).
    #[test]
    fn a_raw_configuration_offers_http1_only() {
        let (ca, chain, key) = private_chain();
        let mut roots = RootCertStore::empty();
        roots.add(ca).expect("a root");
        let mut raw = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        raw.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls = Tls::new(Trust::Raw(Arc::new(raw)));

        let config = tls.config().expect("TLS settings");

        assert_eq!(config.alpn_protocols, [b"http/1.1".to_vec()]);
        assert_eq!(handshake(config, chain, key), Ok(()));
    }

    // Review item C7: a transient failure (a CA bundle being replaced, no
    // file descriptor left) must not break every later secure connection.
    #[test]
    fn a_failed_tls_setup_is_retried() {
        let setup = BuildOnce::new();

        let failed = setup.get(|| {
            Err(TransportFailure::new(
                FailureKind::Tls,
                "no platform root certificates found",
            ))
        });
        let retried = setup.get(|| Ok((1, true)));

        assert!(failed.is_err());
        assert_eq!(retried, Ok((1, true)));
    }

    #[test]
    fn a_tls_setup_its_builder_does_not_keep_is_built_again() {
        let setup = BuildOnce::new();

        let first = setup.get(|| Ok((1, false)));
        let second = setup.get(|| Ok((2, true)));
        let third = setup.get(|| Ok((3, true)));

        assert_eq!(
            (first, second, third),
            (Ok((1, false)), Ok((2, true)), Ok((2, true)))
        );
    }

    #[test]
    fn root_certificates_read_with_errors_serve_the_attempt_but_are_not_kept() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .unwrap()
            .cert
            .der()
            .clone();

        let partial = roots_config(vec![certificate.clone()], &[], false);
        let complete = roots_config(vec![certificate], &[], true);

        assert!(matches!(partial, Ok((_, false))));
        assert!(matches!(complete, Ok((_, true))));
    }

    #[test]
    fn a_built_tls_setup_is_reused() {
        let setup = BuildOnce::new();
        let builds = std::cell::Cell::new(0);
        let build = || {
            builds.set(builds.get() + 1);
            Ok((builds.get(), true))
        };

        let first = setup.get(build);
        let second = setup.get(build);

        assert_eq!(
            (first, second, builds.get()),
            (Ok((1, true)), Ok((1, true)), 1)
        );
    }

    #[test]
    fn tls_config_builds_even_with_several_rustls_providers_enabled() {
        // Test builds enable aws-lc-rs too (through reqwest), which made the
        // process-default provider ambiguous and panic.
        let config = Tls::default().config();

        assert!(config.is_ok(), "{config:?}");
    }

    /// The build's strongest provider feature selects the provider
    /// (Block 6): fips > aws-lc-rs > ring.
    #[test]
    fn tls_config_uses_the_build_provider() {
        let config = Tls::default().config().expect("TLS configuration");

        let key_provider = format!("{:?}", config.crypto_provider().key_provider);

        let expected = if cfg!(feature = "aws-lc-rs") {
            "AwsLcRs"
        } else {
            "Ring"
        };
        assert_eq!(key_provider, expected);
        // rustls also checks TLS-level FIPS requirements here, beyond the
        // provider's primitives.
        assert_eq!(config.fips(), cfg!(feature = "fips"));
    }

    /// A dependency update must not switch the FIPS build to another
    /// module generation unnoticed (Block 6: AWS-LC-FIPS 4.x).
    #[cfg(feature = "fips")]
    #[test]
    fn fips_build_links_the_4x_fips_module() {
        // The FIPS module generation, not the AWS-LC release
        // (`awslc_version`), which aws-lc-rs documents as unfit for it.
        let generation = aws_lc_rs::fips_version();

        assert_eq!(generation, Some(4), "FIPS module generation");
        assert!(aws_lc_rs::try_fips_mode().is_ok(), "not in FIPS mode");
    }
}
