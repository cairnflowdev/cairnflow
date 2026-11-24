// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared OIDC for tests: an in-process fake issuer and the console login flow up to
//! the `__Host-cairn_ui` session cookie.
//!
//! The fake issuer ([`FakeIssuer`]) is a real axum server on loopback: discovery,
//! JWKS (RS256, a fixed test key), `/authorize` consenting by itself for the "next
//! user" the test sets, `/token` checking `code`, `client_id`, `redirect_uri` and
//! PKCE S256 like a control-plane provider. Used in `tests/oidc.rs` (the protocol)
//! and `tests/tenant_isolation.rs` (`/query` with an SSO session).

use {
    anyhow::{anyhow, ensure, Context, Result},
    axum::{
        extract::{Query, State},
        http::StatusCode as AxStatus,
        response::{IntoResponse, Redirect, Response},
        routing::{get, post},
        Json, Router,
    },
    base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine},
    reqwest::{header, Client, StatusCode},
    ring::signature::{KeyPair, RsaKeyPair, RSA_PKCS1_SHA256},
    serde_json::{json, Value},
    sha2::{Digest, Sha256},
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    },
};

/// An RSA 2048 key **for tests only** (PKCS#8 DER, base64) — the fake issuer signs
/// `id_token`s with RS256 like a real provider. Not used anywhere else.
pub const TEST_RSA_PK8: &str = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDDGccG3V9JdLwe3zLSLw2LVyyyuLxBUbpXkAJR8Dj9Ry6q\
t2zp0SFzX03lmdI7kpcGKK000PESk/n+VQDAzdG+/pt6LyCrmjrZMQ7+If7JQxcaQV7iBXLDQdbJg0GDAGOOd4Mw3wN4vow2\
vKKA9NErFpxfd0syXCDbqO4nNwJBhJQ1ntttpWzvu3vl+MDgkO2y+JgNbSCKFpl0vI1j8u8CxDoVEnZgE4Y+gq92iVJ3UP/g\
qinSmGtl7thfTKcyXccR4t+9VtCfm3gzIuuqf0Ifb+/2rX2L4/sIEx5oDyCQPfNeEYSYQVt6WvicKRr/vNRznPnFTvucz8jH\
iuXlVljRAgMBAAECggEBAJL7L4rHacrwK+1iH2pjUmPgR+dY71KSXL0s2+KZBIYjg0/PojjAed/uM8Q+zzhcJnTYXizFIGjr\
nFHz/f5C0LqAG+e/62/8YEtR/0aLm+RKK/R2vSGwExZw2nyz6r3jpB0ngmlmCbqEayOtlwtp5M7+WM4/FCKbb5GswcY/Ve6+\
vwp87r4yCt2XC2hMkb8FUwPn2NvRf35/hMT2NFyafMhu/0VJoBGgYxuJgRt8DUWvfHTVZ/K+bYty4o4lwxxR0uPjOt9RqX06\
Zcgz+0URmPELv4zbuKS++meF510OWtvrE86B0RZmX6jH3MRjtV/pEyrQeUAbrRgs2b9aEzANamUCgYEA5Ko6XvZWYcqDmPVD\
HxGJhhh/jSNj15KMYtLmD0P5gFtBfG1aNxS9C4pSFpFeFDB6X4cBT4o4x98pYTsKua/RiyccI55bWiBEgW7PoPs4XH1Dx4Gt\
StLfENiS8Endp1xluOEpP3G9KJ4UrJ0mqfyIxJopPZ1zONNYQXgL8D/rZ6sCgYEA2mxkNHtUCVElcYF8Wlh/vl+foXqCg61M\
+2wEehjd0Jg3TjZISlpGA1hYKn2Y6bM04QF6iTgl0F39KwUqn/0AtD3+yUHMArTESOuhCJH6IfqFi3Dsh1xOzPB+9QKljl3J\
Xp/d3aqszA0Sw+n6BFtx5JNo1fK6WPofmKCgeliBVXMCgYAnbUKLRSY3zldKdF+SZ8+yk17K0yNP9DDIiVypnSrT+ugbsC8x\
+B1wc/Ubdw0lpgYzmm+ba1XHAK992FRNGAelWqT7bz6JF684MgcS1r4tFFwvdiiPEjTsDqunStUxGjlf21LlYZMgAcHG0YHt\
JWHfpEUgHpnTXn2FLFHCpwLL6QKBgDx3q1n4FnCp3ULKBQIB5ANw94E9qCoJ7Pjhk2+1uJqJuS3WyzaCPUWpzGae9Jkl+mqb\
/S9Fsvo3OuIRARbfdLVxQDq5E2BgaMKDMMZtmyCS+St8LI7gmBQWNB9iBzEdd6TRfxthp4qpx9siyAsywZn00R4Bn2MIg3y+\
Yw7bt5pPAoGAYEd1noKlCIbA0Xyai7EhKyJVJT9+kb1jI6w8rrc0CLXXNqy4h8BhlpGRnCPh8hPmdXTmxZgPU2Ku5RhuIPBh\
noJgadqiaZxwHR3S+N9VZNM2kfbkHHuwLrZj1cBBp/+dY/xpRmbVLdFW5Szk3m1cs0ls0vZNN81Xk2x4Mil5fOA=";

pub fn b64(v: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(v)
}

pub fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ───────────────────────────────────────────────────────────── fake issuer

/// The next user `/authorize` consents for — the `id_token` claims the test sets.
/// `Some` fields override the computed claims (`iss`, `aud`, `nonce`, `exp`).
#[derive(Clone, Default)]
pub struct NextUser {
    pub sub: String,
    pub role: Option<String>,
    pub tenant_claim: Option<String>,
    /// Claims overridden/added verbatim (`aud`, `nonce`, `exp`, `iss`…).
    pub overrides: serde_json::Map<String, Value>,
    /// `/authorize` returns `error=access_denied` instead of a code.
    pub deny: bool,
    /// The callback URL's `iss` (RFC 9207); `None` = the real issuer.
    pub callback_iss: Option<String>,
}

pub struct Pending {
    pub client_id: String,
    pub redirect_uri: String,
    pub challenge: String,
    pub nonce: String,
    pub user: NextUser,
}

#[derive(Default)]
pub struct IssuerState {
    pub next: NextUser,
    pub codes: BTreeMap<String, Pending>,
    /// Every endpoint returns `503`.
    pub down: bool,
    /// The number of `/token` requests received, and the last authorize parameters.
    pub tokens: u32,
    pub last_authorize: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct FakeIssuer {
    pub url: String,
    pub state: Arc<Mutex<IssuerState>>,
    pub key: Arc<RsaKeyPair>,
}

impl FakeIssuer {
    pub async fn start() -> Result<Self> {
        let der = base64::engine::general_purpose::STANDARD.decode(TEST_RSA_PK8)?;
        let key = Arc::new(RsaKeyPair::from_pkcs8(&der).map_err(|e| anyhow!("test key: {e}"))?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let me = FakeIssuer {
            url,
            state: Arc::new(Mutex::new(IssuerState::default())),
            key,
        };
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/authorize", get(authorize))
            .route("/token", post(token))
            .with_state(me.clone());
        tokio::spawn(async move { axum::serve(listener, app).await });
        me.set_user("user-1", Some("admin"));
        Ok(me)
    }

    pub fn set_user(&self, sub: &str, role: Option<&str>) {
        self.state.lock().unwrap().next = NextUser {
            sub: sub.into(),
            role: role.map(str::to_string),
            ..Default::default()
        };
    }

    pub fn edit_user(&self, f: impl FnOnce(&mut NextUser)) {
        f(&mut self.state.lock().unwrap().next);
    }

    pub fn set_down(&self, down: bool) {
        self.state.lock().unwrap().down = down;
    }

    pub fn sign(&self, claims: &Value) -> String {
        let h = b64(json!({"alg": "RS256", "kid": "test-1", "typ": "JWT"})
            .to_string()
            .as_bytes());
        let p = b64(claims.to_string().as_bytes());
        let signed = format!("{h}.{p}");
        let mut sig = vec![0u8; self.key.public().modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                signed.as_bytes(),
                &mut sig,
            )
            .unwrap();
        format!("{signed}.{}", b64(&sig))
    }
}

fn down(s: &FakeIssuer) -> Option<Response> {
    s.state
        .lock()
        .unwrap()
        .down
        .then(|| AxStatus::SERVICE_UNAVAILABLE.into_response())
}

async fn discovery(State(s): State<FakeIssuer>) -> Response {
    if let Some(r) = down(&s) {
        return r;
    }
    Json(json!({
        "issuer": s.url,
        "authorization_endpoint": format!("{}/authorize", s.url),
        "token_endpoint": format!("{}/token", s.url),
        "jwks_uri": format!("{}/jwks", s.url),
        "response_types_supported": ["code"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "code_challenge_methods_supported": ["S256"],
    }))
    .into_response()
}

async fn jwks(State(s): State<FakeIssuer>) -> Response {
    if let Some(r) = down(&s) {
        return r;
    }
    // The public key's modulus and exponent, unsigned big-endian.
    let der = s.key.public_key().as_ref();
    let (n, e) = rsa_public_components(der);
    Json(
        json!({"keys": [{"kty": "RSA", "kid": "test-1", "alg": "RS256", "use": "sig",
        "n": b64(&n), "e": b64(&e)}]}),
    )
    .into_response()
}

/// `RSAPublicKey ::= SEQUENCE { n INTEGER, e INTEGER }` (DER) ⇒ (n, e) without the
/// leading 0 byte.
fn rsa_public_components(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    fn len(b: &[u8], i: &mut usize) -> usize {
        let first = b[*i];
        *i += 1;
        if first < 0x80 {
            return first as usize;
        }
        let k = (first & 0x7f) as usize;
        let mut l = 0;
        for _ in 0..k {
            l = (l << 8) | b[*i] as usize;
            *i += 1;
        }
        l
    }
    let mut i = 1;
    len(der, &mut i);
    let mut int = || {
        assert_eq!(der[i], 0x02);
        i += 1;
        let l = len(der, &mut i);
        let mut v = der[i..i + l].to_vec();
        i += l;
        while v.first() == Some(&0) {
            v.remove(0);
        }
        v
    };
    let n = int();
    let e = int();
    (n, e)
}

async fn authorize(
    State(s): State<FakeIssuer>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Response {
    if let Some(r) = down(&s) {
        return r;
    }
    let mut st = s.state.lock().unwrap();
    st.last_authorize = q.clone();
    let redirect = q.get("redirect_uri").cloned().unwrap_or_default();
    let state = q.get("state").cloned().unwrap_or_default();
    if st.next.deny {
        return Redirect::to(&format!("{redirect}?error=access_denied&state={state}"))
            .into_response();
    }
    // RFC 9207: a control-plane provider sends `iss` with the callback.
    let iss = st
        .next
        .callback_iss
        .clone()
        .unwrap_or_else(|| s.url.clone());
    if q.get("code_challenge_method").map(String::as_str) != Some("S256")
        || q.get("response_type").map(String::as_str) != Some("code")
    {
        return (AxStatus::BAD_REQUEST, "code + S256 required").into_response();
    }
    let code = format!("code-{}", st.codes.len() + 1);
    let user = st.next.clone();
    st.codes.insert(
        code.clone(),
        Pending {
            client_id: q.get("client_id").cloned().unwrap_or_default(),
            redirect_uri: redirect.clone(),
            challenge: q.get("code_challenge").cloned().unwrap_or_default(),
            nonce: q.get("nonce").cloned().unwrap_or_default(),
            user,
        },
    );
    Redirect::to(&format!(
        "{redirect}?code={code}&state={state}&iss={}",
        urlencode(&iss)
    ))
    .into_response()
}

async fn token(State(s): State<FakeIssuer>, body: String) -> Response {
    let f: BTreeMap<String, String> = form_urlencoded::parse(body.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if let Some(r) = down(&s) {
        return r;
    }
    let pending = {
        let mut st = s.state.lock().unwrap();
        st.tokens += 1;
        f.get("code").and_then(|c| st.codes.remove(c))
    };
    let bad = || {
        (
            AxStatus::BAD_REQUEST,
            Json(json!({"error": "invalid_grant"})),
        )
            .into_response()
    };
    let Some(p) = pending else { return bad() };
    let verifier = f.get("code_verifier").cloned().unwrap_or_default();
    if f.get("grant_type").map(String::as_str) != Some("authorization_code")
        || f.get("client_id") != Some(&p.client_id)
        || f.get("redirect_uri") != Some(&p.redirect_uri)
        || b64(&Sha256::digest(verifier.as_bytes())) != p.challenge
    {
        return bad();
    }
    let now = now_s();
    let mut claims = json!({
        "iss": s.url,
        "aud": p.client_id,
        "sub": p.user.sub,
        "email": format!("{}@example.test", p.user.sub),
        "name": format!("User {}", p.user.sub),
        "nonce": p.nonce,
        "iat": now,
        "exp": now + 300,
    });
    if let Some(r) = &p.user.role {
        claims["cairn_role"] = r.clone().into();
    }
    if let Some(t) = &p.user.tenant_claim {
        claims["cairn_tenant"] = t.clone().into();
    }
    for (k, v) in &p.user.overrides {
        claims[k] = v.clone();
    }
    Json(json!({"access_token": "at", "token_type": "Bearer", "id_token": s.sign(&claims)}))
        .into_response()
}

pub fn http() -> Client {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// The `name=value` of cookie `name` among the `Set-Cookie`s, with the whole line.
pub fn set_cookie<'a>(r: &'a reqwest::Response, name: &str) -> Option<(String, &'a str)> {
    r.headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find(|v| v.starts_with(&format!("{name}=")))
        .map(|line| (line.split(';').next().unwrap_or_default().to_string(), line))
}

pub fn location(r: &reqwest::Response) -> String {
    r.headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// The outcome of one login: the session cookie (on success) and the callback's
/// final `Location`.
pub struct Login {
    pub cookie: Option<String>,
    pub landed: String,
    pub authorize: String,
}

/// Step 1: `GET /ui/login/oidc` ⇒ the authorize URL + the temporary cookie.
pub async fn start_login(node: &str, return_to: Option<&str>) -> Result<(String, String)> {
    let mut url = format!("{node}/ui/login/oidc");
    if let Some(r) = return_to {
        url.push_str(&format!("?return_to={}", urlencode(r)));
    }
    let r = http().get(url).send().await?;
    ensure!(
        r.status() == StatusCode::SEE_OTHER,
        "start login: {}",
        r.status()
    );
    let (flow, line) = set_cookie(&r, "__Host-cairn_oidc").context("no temporary cookie")?;
    for part in ["HttpOnly", "Secure", "SameSite=Lax", "Path=/"] {
        ensure!(line.contains(part), "temporary cookie lacks {part}: {line}");
    }
    ensure!(!line.contains("Domain"), "{line}");
    Ok((location(&r), flow))
}

pub fn urlencode(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Steps 2–3: issuer ⇒ callback (with the `flow` cookie) ⇒ outcome.
pub async fn finish_login(callback: &str, flow: &str) -> Result<(Option<String>, String)> {
    let r = http()
        .get(callback)
        .header(header::COOKIE, flow)
        .send()
        .await?;
    ensure!(
        r.status() == StatusCode::SEE_OTHER,
        "callback: {}",
        r.status()
    );
    let cookie = match set_cookie(&r, "__Host-cairn_ui") {
        Some((c, line)) => {
            for part in [
                "HttpOnly",
                "Secure",
                "SameSite=Strict",
                "Path=/",
                "Max-Age=",
            ] {
                ensure!(line.contains(part), "session cookie lacks {part}: {line}");
            }
            ensure!(!line.contains("Domain"), "{line}");
            Some(c)
        }
        None => None,
    };
    Ok((cookie, location(&r)))
}

pub async fn login(node: &str, return_to: Option<&str>) -> Result<Login> {
    let (authorize, flow) = start_login(node, return_to).await?;
    let r = http().get(&authorize).send().await?;
    let callback = location(&r);
    ensure!(
        callback.starts_with(&format!("{node}/ui/login/oidc/callback")),
        "issuer did not return to the callback: {} {callback}",
        r.status()
    );
    let (cookie, landed) = finish_login(&callback, &flow).await?;
    Ok(Login {
        cookie,
        landed,
        authorize,
    })
}
