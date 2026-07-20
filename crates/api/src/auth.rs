//! Single-administrator authentication and session management.

use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use axum::{
    Json,
    extract::{ConnectInfo, Request, State},
    http::{Method, header},
    middleware::Next,
    response::Response,
};
use axum_extra::extract::{
    CookieJar,
    cookie::{Cookie, SameSite},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use crate::error::ApiError;

const SESSION_COOKIE: &str = "etl_session";
const CSRF_COOKIE: &str = "csrf_token";
const CSRF_HEADER: &str = "x-csrf-token";
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: usize = 5;
const MAX_FAILURE_ADDRESSES: usize = 10_000;
const MAX_SESSIONS: usize = 10_000;

#[derive(Clone)]
pub struct AuthState {
    inner: Arc<AuthInner>,
}

struct AuthInner {
    username: String,
    password_hash: SecretString,
    secure_cookies: bool,
    session_ttl: Duration,
    sessions: Mutex<HashMap<[u8; 32], Session>>,
    failures: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthState([REDACTED])")
    }
}

#[derive(Debug, Clone)]
struct Session {
    csrf_token: String,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
pub struct SessionIdentity {
    pub username: String,
    csrf_token: String,
    expires_in_seconds: u64,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    username: String,
    password: SecretString,
}

#[derive(Debug, Serialize)]
pub struct SessionResponse {
    username: String,
    csrf_token: String,
    expires_in_seconds: u64,
}

impl AuthState {
    #[must_use]
    pub fn new(
        username: String,
        password_hash: SecretString,
        secure_cookies: bool,
        session_ttl: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(AuthInner {
                username,
                password_hash,
                secure_cookies,
                session_ttl,
                sessions: Mutex::new(HashMap::new()),
                failures: Mutex::new(HashMap::new()),
            }),
        }
    }

    async fn is_rate_limited(&self, address: IpAddr) -> bool {
        let now = Instant::now();
        let mut failures = self.inner.failures.lock().await;
        failures.retain(|_, attempts| {
            attempts.retain(|instant| now.duration_since(*instant) <= LOGIN_WINDOW);
            !attempts.is_empty()
        });
        let attempts = failures.entry(address).or_default();
        attempts.len() >= MAX_LOGIN_FAILURES
    }

    async fn register_failure(&self, address: IpAddr) {
        let mut failures = self.inner.failures.lock().await;
        if failures.len() >= MAX_FAILURE_ADDRESSES
            && !failures.contains_key(&address)
            && let Some(oldest) = failures.keys().next().copied()
        {
            failures.remove(&oldest);
        }
        failures
            .entry(address)
            .or_default()
            .push_back(Instant::now());
    }

    async fn clear_failures(&self, address: IpAddr) {
        self.inner.failures.lock().await.remove(&address);
    }

    async fn verify(&self, username: &str, password: &SecretString) -> bool {
        let username_matches = self
            .inner
            .username
            .as_bytes()
            .ct_eq(username.as_bytes())
            .into();
        let hash = self.inner.password_hash.expose_secret().to_owned();
        let password = password.expose_secret().to_owned();
        let password_matches = tokio::task::spawn_blocking(move || {
            PasswordHash::new(&hash).ok().is_some_and(|parsed| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .is_ok()
            })
        })
        .await
        .unwrap_or(false);
        username_matches && password_matches
    }

    async fn create_session(&self) -> (String, Session) {
        let token = random_token();
        let session = Session {
            csrf_token: random_token(),
            expires_at: Instant::now() + self.inner.session_ttl,
        };
        let now = Instant::now();
        let mut sessions = self.inner.sessions.lock().await;
        sessions.retain(|_, existing| existing.expires_at > now);
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = sessions.keys().next().copied()
        {
            sessions.remove(&oldest);
        }
        sessions.insert(hash_token(&token), session.clone());
        (token, session)
    }

    async fn authenticate(&self, token: &str) -> Option<SessionIdentity> {
        let key = hash_token(token);
        let now = Instant::now();
        let mut sessions = self.inner.sessions.lock().await;
        sessions.retain(|_, session| session.expires_at > now);
        sessions.get(&key).map(|session| SessionIdentity {
            username: self.inner.username.clone(),
            csrf_token: session.csrf_token.clone(),
            expires_in_seconds: session.expires_at.saturating_duration_since(now).as_secs(),
        })
    }

    async fn revoke(&self, token: &str) {
        self.inner.sessions.lock().await.remove(&hash_token(token));
    }
}

pub async fn login(
    State(auth): State<AuthState>,
    ConnectInfo(connection): ConnectInfo<SocketAddr>,
    jar: CookieJar,
    Json(request): Json<LoginRequest>,
) -> Result<(CookieJar, Json<SessionResponse>), ApiError> {
    let address = connection.ip();
    if auth.is_rate_limited(address).await {
        return Err(ApiError::too_many_requests());
    }
    if !auth.verify(&request.username, &request.password).await {
        auth.register_failure(address).await;
        return Err(ApiError::unauthorized());
    }
    auth.clear_failures(address).await;

    let (token, session) = auth.create_session().await;
    let session_cookie = Cookie::build((SESSION_COOKIE, token))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Strict)
        .secure(auth.inner.secure_cookies)
        .build();
    let csrf_cookie = Cookie::build((CSRF_COOKIE, session.csrf_token.clone()))
        .path("/")
        .http_only(false)
        .same_site(SameSite::Strict)
        .secure(auth.inner.secure_cookies)
        .build();
    Ok((
        jar.add(session_cookie).add(csrf_cookie),
        Json(SessionResponse {
            username: auth.inner.username.clone(),
            csrf_token: session.csrf_token,
            expires_in_seconds: auth.inner.session_ttl.as_secs(),
        }),
    ))
}

pub async fn logout(
    State(auth): State<AuthState>,
    jar: CookieJar,
) -> (CookieJar, axum::http::StatusCode) {
    if let Some(cookie) = jar.get(SESSION_COOKIE) {
        auth.revoke(cookie.value()).await;
    }
    let session = Cookie::build(SESSION_COOKIE).path("/").build();
    let csrf = Cookie::build(CSRF_COOKIE).path("/").build();
    (
        jar.remove(session).remove(csrf),
        axum::http::StatusCode::NO_CONTENT,
    )
}

pub async fn current_session(
    axum::extract::Extension(identity): axum::extract::Extension<SessionIdentity>,
) -> Json<SessionResponse> {
    Json(SessionResponse {
        username: identity.username,
        csrf_token: identity.csrf_token,
        expires_in_seconds: identity.expires_in_seconds,
    })
}

pub async fn require_session(
    State(auth): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let token =
        cookie_value(request.headers(), SESSION_COOKIE).ok_or_else(ApiError::unauthorized)?;
    let identity = auth
        .authenticate(token)
        .await
        .ok_or_else(ApiError::unauthorized)?;

    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        let supplied = request
            .headers()
            .get(CSRF_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ApiError::forbidden("missing CSRF token"))?;
        if !bool::from(identity.csrf_token.as_bytes().ct_eq(supplied.as_bytes())) {
            return Err(ApiError::forbidden("invalid CSRF token"));
        }
    }

    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

fn cookie_value<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(candidate, value)| (candidate == name).then_some(value))
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cookie_without_prefix_confusion() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "other=1; etl_session=secret; x=2".parse().unwrap(),
        );
        assert_eq!(cookie_value(&headers, SESSION_COOKIE), Some("secret"));
    }

    #[test]
    fn generated_tokens_have_full_entropy_length() {
        let token = random_token();
        assert_eq!(URL_SAFE_NO_PAD.decode(token).unwrap().len(), 32);
    }
}
