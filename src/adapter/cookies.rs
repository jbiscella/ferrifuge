//! The cookie jar of the tokio adapter (Block 43): cookies the server
//! sets on a response, sent back on the later requests they match.

use std::sync::{PoisonError, RwLock};

use cookie_store::{CookieStore, RawCookie, StoreAction};
use http::header::{COOKIE, SET_COOKIE};
use http::{HeaderMap, HeaderValue};
use url::Url;

/// Where the client keeps the cookies servers set, with the method set of
/// reqwest's `cookie::CookieStore`, so that a jar an application shares
/// with its own reqwest client is a newtype forwarding each method:
///
/// ```
/// # use std::sync::Arc;
/// # use ferrifuge::{CookieJar, MemoryJar};
/// struct Shared(Arc<MemoryJar>);
///
/// impl CookieJar for Shared {
///     fn set_cookies(
///         &self,
///         cookie_headers: &mut dyn Iterator<Item = &ferrifuge::http::HeaderValue>,
///         url: &ferrifuge::url::Url,
///     ) {
///         self.0.set_cookies(cookie_headers, url);
///     }
///
///     fn cookies(&self, url: &ferrifuge::url::Url) -> Option<ferrifuge::http::HeaderValue> {
///         self.0.cookies(url)
///     }
/// }
/// ```
///
/// The client gives it the `Set-Cookie` values of each response and asks
/// it for the cookies of each request, with the request's URL as `http://`
/// or `https://` (a websocket's `ws://` and `wss://` mapped to them) and
/// without its query and fragment (Block 43, D4).
pub trait CookieJar: Send + Sync {
    /// Stores the cookies of `cookie_headers`, the `Set-Cookie` values of a
    /// response to a request for `url`.
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url);

    /// The `Cookie` header value for a request to `url`, if any cookie
    /// matches it.
    fn cookies(&self, url: &Url) -> Option<HeaderValue>;
}

/// The largest `Set-Cookie` value [`MemoryJar`] takes, in bytes (RFC 6265
/// §6.1; Block 43, D7).
const MAX_COOKIE_BYTES: usize = 4096;

/// The most cookies [`MemoryJar`] holds (RFC 6265 §6.1; Block 43, D7).
const MAX_COOKIES: usize = 3000;

/// A [`CookieJar`] in memory, for the life of the process, on
/// [cookie_store](https://docs.rs/cookie_store): it matches domains,
/// paths, `Secure`, `HttpOnly` and expiry as RFC 6265 says.
///
/// It does no public-suffix check, as reqwest's `Jar` does none: a server
/// may set a cookie for a public suffix such as `co.uk`. An application
/// that needs the check implements [`CookieJar`] over `cookie_store` with a
/// list it supplies (Block 43, D1).
///
/// It ignores a `Set-Cookie` value over 4096 bytes, and holds at most 3000
/// cookies: at that count it drops its expired ones, then ignores a new
/// cookie, while still replacing one it holds (RFC 6265 §6.1's minimums).
/// It logs no cookie (Block 43, D6).
#[derive(Default)]
pub struct MemoryJar {
    store: RwLock<CookieStore>,
}

impl MemoryJar {
    /// An empty jar.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores one `Set-Cookie` value, within the bounds; without a log
    /// record, as `CookieStore::store_response_cookies` would write one.
    fn store(store: &mut CookieStore, value: &str, url: &Url) {
        if value.len() > MAX_COOKIE_BYTES {
            return;
        }
        let Ok(raw) = RawCookie::parse(value) else {
            return;
        };
        if store.iter_any().count() >= MAX_COOKIES {
            let held = cookie_store::Cookie::try_from_raw_cookie(&raw, url).is_ok_and(|cookie| {
                cookie.domain.as_cow().is_some_and(|domain| {
                    store.contains_any(&domain, &String::from(&cookie.path), cookie.name())
                })
            });
            if !held {
                drop_expired(store);
                if store.iter_any().count() >= MAX_COOKIES {
                    return;
                }
            }
        }
        // A cookie the store refuses (a domain that does not match, an
        // `HttpOnly` one from a non-HTTP scheme) changes nothing.
        let _: Result<StoreAction, _> = store.insert_raw(&raw, url);
    }
}

/// Removes the expired cookies of `store`.
fn drop_expired(store: &mut CookieStore) {
    let expired: Vec<(String, String, String)> = store
        .iter_any()
        .filter(|cookie| cookie.is_expired())
        .filter_map(|cookie| {
            Some((
                cookie.domain.as_cow()?.into_owned(),
                String::from(&cookie.path),
                cookie.name().to_owned(),
            ))
        })
        .collect();
    for (domain, path, name) in expired {
        store.remove(&domain, &path, &name);
    }
}

impl CookieJar for MemoryJar {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        let mut store = lock_read_write(self.store.write());
        for value in cookie_headers {
            if let Ok(value) = value.to_str() {
                Self::store(&mut store, value, url);
            }
        }
    }

    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        let store = lock_read_write(self.store.read());
        let pairs: Vec<String> = store
            .get_request_values(url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        if pairs.is_empty() {
            return None;
        }
        HeaderValue::from_str(&pairs.join("; ")).ok()
    }
}

impl std::fmt::Debug for MemoryJar {
    // The number of cookies only: names and values are secrets (Block 43,
    // D6).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cookies = lock_read_write(self.store.read()).iter_unexpired().count();
        f.debug_struct("MemoryJar")
            .field("cookies", &cookies)
            .finish()
    }
}

/// A guard of the jar's lock, whatever a panicking holder left: the store
/// stays consistent between its own calls.
fn lock_read_write<G>(result: Result<G, PoisonError<G>>) -> G {
    result.unwrap_or_else(PoisonError::into_inner)
}

/// The URL a jar sees for a request to `url`: `ws://` and `wss://` as
/// `http://` and `https://`, which `cookie_store` counts as HTTP and secure
/// schemes, without the query, which carries the sse token, and the
/// fragment (Block 43, D4).
pub(super) fn jar_url(url: &str) -> Option<Url> {
    let mut url = Url::parse(url).ok()?;
    let scheme = match url.scheme() {
        "ws" => Some("http"),
        "wss" => Some("https"),
        _ => None,
    };
    if let Some(scheme) = scheme {
        // `set_scheme` refuses a special scheme to another only when one
        // of them is not special; ws, wss, http and https all are.
        url.set_scheme(scheme).ok()?;
    }
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

/// The `Cookie` header of a request to `url`: the application's own pairs
/// from `app`, in their order, then the jar's, leaving out a jar cookie
/// whose name is among the application's, in one header (RFC 6265 §5.4;
/// Block 43, D3). `None` when neither has a cookie.
pub(super) fn cookie_header(
    jar: &dyn CookieJar,
    url: &str,
    app: Option<&HeaderValue>,
) -> Option<HeaderValue> {
    let app: Vec<&str> = app
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .map(str::trim)
                .filter(|pair| !pair.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let names: Vec<&str> = app
        .iter()
        .map(|pair| pair.split_once('=').map_or(*pair, |(name, _)| name))
        .collect();
    let jar_value = jar_url(url).and_then(|url| jar.cookies(&url));
    let from_jar: Vec<&str> = jar_value
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split("; ")
                .filter(|pair| {
                    let name = pair.split_once('=').map_or(*pair, |(name, _)| name);
                    !names.contains(&name)
                })
                .collect()
        })
        .unwrap_or_default();
    let pairs: Vec<&str> = app.into_iter().chain(from_jar).collect();
    if pairs.is_empty() {
        return None;
    }
    HeaderValue::from_str(&pairs.join("; ")).ok()
}

/// Sets the request's `Cookie` header in `headers` for a request to `url`
/// (Block 43, D2, D3).
pub(super) fn add_cookies(jar: &dyn CookieJar, url: &str, headers: &mut HeaderMap) {
    match cookie_header(jar, url, headers.get(COOKIE)) {
        Some(value) => {
            headers.insert(COOKIE, value);
        }
        None => {
            headers.remove(COOKIE);
        }
    }
}

/// Gives the jar the `Set-Cookie` values of a response with `status` to a
/// request to `url`; a 407 is a proxy's own answer, whose cookies are not
/// the server's (Block 43, D2, D5).
pub(super) fn store_cookies(jar: &dyn CookieJar, url: &str, status: u16, headers: &HeaderMap) {
    if status == 407 {
        return;
    }
    let Some(url) = jar_url(url) else {
        return;
    };
    let mut values = headers.get_all(SET_COOKIE).iter();
    jar.set_cookies(&mut values, &url);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(jar: &MemoryJar, url: &str, cookies: &[&str]) {
        let mut headers = HeaderMap::new();
        for cookie in cookies {
            headers.append(SET_COOKIE, HeaderValue::from_str(cookie).unwrap());
        }
        store_cookies(jar, url, 200, &headers);
    }

    fn sent(jar: &MemoryJar, url: &str) -> Option<String> {
        cookie_header(jar, url, None).map(|value| value.to_str().unwrap().to_owned())
    }

    // D4: an HttpOnly cookie set over ws:// is an HTTP one for the jar.
    #[test]
    fn an_http_only_cookie_from_ws_is_kept_and_sent() {
        let jar = MemoryJar::new();
        set(
            &jar,
            "ws://edge.example.com/connection/websocket",
            &["a=1; Path=/; HttpOnly"],
        );

        assert_eq!(
            sent(&jar, "ws://edge.example.com/connection/websocket").as_deref(),
            Some("a=1")
        );
    }

    // D4: a Secure cookie set over wss:// goes to wss:// only.
    #[test]
    fn a_secure_cookie_from_wss_is_sent_to_wss_only() {
        let jar = MemoryJar::new();
        set(
            &jar,
            "wss://edge.example.com/connection/websocket",
            &["s=1; Path=/; Secure"],
        );

        assert_eq!(
            sent(&jar, "wss://edge.example.com/connection/websocket").as_deref(),
            Some("s=1")
        );
        assert_eq!(
            sent(&jar, "ws://edge.example.com/connection/websocket"),
            None
        );
    }

    // D4: the jar never sees the query, which carries the sse token.
    #[test]
    fn the_jar_sees_no_query() {
        assert_eq!(
            jar_url("https://edge.example.com/connection/sse?cf_connect=secret#x")
                .unwrap()
                .as_str(),
            "https://edge.example.com/connection/sse"
        );
        assert_eq!(
            jar_url("wss://edge.example.com/connection/websocket?a=b")
                .unwrap()
                .as_str(),
            "https://edge.example.com/connection/websocket"
        );
    }

    // D3: one header, the application's pairs first, a jar cookie of the
    // same name left out.
    #[test]
    fn the_app_s_cookie_and_the_jar_s_are_one_header() {
        let jar = MemoryJar::new();
        set(
            &jar,
            "ws://edge.example.com/",
            &["affinity=node-1; Path=/", "lb=l1; Path=/"],
        );
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_static("app=a1; affinity=mine"));

        add_cookies(&jar, "ws://edge.example.com/", &mut headers);

        let values: Vec<&HeaderValue> = headers.get_all(COOKIE).iter().collect();
        assert_eq!(
            values,
            [&HeaderValue::from_static("app=a1; affinity=mine; lb=l1")]
        );
    }

    // D5: a proxy's 407 sets no cookie of the server's.
    #[test]
    fn a_407_s_cookies_are_not_stored() {
        let jar = MemoryJar::new();
        let mut headers = HeaderMap::new();
        headers.append(SET_COOKIE, HeaderValue::from_static("p=1; Path=/"));

        store_cookies(&jar, "http://edge.example.com/", 407, &headers);

        assert_eq!(sent(&jar, "http://edge.example.com/"), None);
    }

    // D7: a value over 4096 bytes is ignored.
    #[test]
    fn a_cookie_over_4096_bytes_is_ignored() {
        let jar = MemoryJar::new();
        let long = format!("big={}; Path=/", "x".repeat(MAX_COOKIE_BYTES));
        let fits = format!("fit={}", "x".repeat(MAX_COOKIE_BYTES - 4));

        set(&jar, "http://edge.example.com/", &[&long, &fits]);

        let sent = sent(&jar, "http://edge.example.com/").unwrap();
        assert!(sent.starts_with("fit="), "{}", &sent[..10]);
        assert!(!sent.contains("big="));
    }

    // D7: at 3000 cookies a new one is ignored and a held one replaced.
    #[test]
    fn the_3001st_cookie_is_ignored_while_a_replacement_is_taken() {
        let jar = MemoryJar::new();
        let url = "http://edge.example.com/";
        let cookies: Vec<String> = (0..MAX_COOKIES)
            .map(|n| format!("c{n}=v; Path=/"))
            .collect();
        let refs: Vec<&str> = cookies.iter().map(String::as_str).collect();
        set(&jar, url, &refs);

        set(&jar, url, &["new=v; Path=/", "c0=replaced; Path=/"]);

        let sent = sent(&jar, url).unwrap();
        assert!(!sent.contains("new=v"));
        assert!(sent.contains("c0=replaced"));
        assert_eq!(
            lock_read_write(jar.store.read()).iter_any().count(),
            MAX_COOKIES
        );
    }

    // D7: at 3000 cookies, the expired ones make room.
    #[test]
    fn expired_cookies_make_room() {
        let jar = MemoryJar::new();
        let url = "http://edge.example.com/";
        let mut cookies: Vec<String> = (1..MAX_COOKIES)
            .map(|n| format!("c{n}=v; Path=/"))
            .collect();
        cookies.push("short=v; Path=/; Max-Age=1".to_owned());
        let refs: Vec<&str> = cookies.iter().map(String::as_str).collect();
        set(&jar, url, &refs);
        std::thread::sleep(std::time::Duration::from_millis(1100));

        set(&jar, url, &["new=v; Path=/"]);

        let sent = sent(&jar, url).unwrap();
        assert!(sent.contains("new=v"));
        assert!(!sent.contains("short=v"));
    }

    // D6: Debug shows the number of cookies, no name or value.
    #[test]
    fn debug_shows_no_cookie() {
        let jar = MemoryJar::new();
        set(
            &jar,
            "http://edge.example.com/",
            &["secret=c-secret; Path=/"],
        );

        let shown = format!("{jar:?}");

        assert_eq!(shown, "MemoryJar { cookies: 1 }");
    }
}
