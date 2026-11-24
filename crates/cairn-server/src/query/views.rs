// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Source of the view tables (`cairn_query::view`): runs `GET` on **the node's
//! own ingress router**, in process, carrying the exact identity of the
//! `/query` statement ([`Credentials`]: `Authorization` or the console session
//! cookie) — roles and tenant filtering are those of REST.

use {
    axum::{body::Body, http::Request},
    cairn_query::{view::ViewSource, QueryError},
    std::sync::{Arc, OnceLock, Weak},
};

/// Byte cap of one REST response read as a view table.
const VIEW_MAX_BYTES: usize = 32 << 20;

/// The node's ingress router — set after construction (the router needs
/// `AppState`, and `AppState` carries the query service). Only `Weak`: the
/// strong copy lives in the server task, so there is no `Arc` cycle keeping
/// the node alive after shutdown.
#[derive(Clone, Default)]
pub struct RouterSlot(Arc<OnceLock<Weak<axum::Router>>>);

impl RouterSlot {
    pub fn set(&self, r: Weak<axum::Router>) {
        let _ = self.0.set(r);
    }

    pub(crate) fn get(&self) -> Option<axum::Router> {
        self.0.get()?.upgrade().map(|r| (*r).clone())
    }
}

/// Headers carrying the caller's identity, copied into the internal `GET`
/// call: `Authorization`, and the console session cookie — a question asked
/// with a cookie has no `Authorization`. `GET` is a safe method, so the call
/// does not need to prove its origin.
///
/// Same **effective** `Host` as the original request (the trusted proxy's, if
/// any — [`crate::auth::Auth::request_host`]): the SSO cookie only opens
/// requests to the exact hostname of the OIDC client, and an in-process call
/// has no peer to trust `X-Forwarded-Host` from.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    authorization: Option<axum::http::HeaderValue>,
    cookie: Vec<axum::http::HeaderValue>,
    host: Option<axum::http::HeaderValue>,
}

impl Credentials {
    pub fn from_headers(h: &axum::http::HeaderMap) -> Self {
        Self {
            authorization: h.get(axum::http::header::AUTHORIZATION).cloned(),
            cookie: h
                .get_all(axum::http::header::COOKIE)
                .iter()
                .cloned()
                .collect(),
            host: h.get(axum::http::header::HOST).cloned(),
        }
    }

    /// Like [`Self::from_headers`], with `Host` set to the effective hostname
    /// `host` (already through `--trust-proxy`) when known.
    pub fn of_request(
        h: &axum::http::HeaderMap,
        auth: &crate::auth::Auth,
        ext: &axum::http::Extensions,
    ) -> Self {
        let mut c = Self::from_headers(h);
        if let Some(v) = auth
            .request_host(h, ext)
            .and_then(|host| axum::http::HeaderValue::from_str(&host).ok())
        {
            c.host = Some(v);
        }
        c
    }
}

pub(crate) struct RouterSource {
    pub slot: RouterSlot,
    pub credentials: Credentials,
}

#[async_trait::async_trait]
impl ViewSource for RouterSource {
    async fn get(&self, path: &str) -> Result<serde_json::Value, QueryError> {
        use tower_service::Service;
        let mut router = self
            .slot
            .get()
            .ok_or_else(|| QueryError::Internal("ingress router is not ready yet".into()))?;
        let mut req = Request::get(path);
        if let Some(a) = &self.credentials.authorization {
            req = req.header(axum::http::header::AUTHORIZATION, a.clone());
        }
        for c in &self.credentials.cookie {
            req = req.header(axum::http::header::COOKIE, c.clone());
        }
        if let Some(h) = &self.credentials.host {
            req = req.header(axum::http::header::HOST, h.clone());
        }
        let req = req
            .body(Body::empty())
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let resp = router
            .call(req)
            .await
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), VIEW_MAX_BYTES)
            .await
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        if status.is_client_error() {
            return Err(QueryError::Sql(format!(
                "GET {path}: {status} {}",
                String::from_utf8_lossy(&body)
            )));
        }
        if !status.is_success() {
            return Err(QueryError::Internal(format!("GET {path}: {status}")));
        }
        serde_json::from_slice(&body).map_err(|e| QueryError::Internal(e.to_string()))
    }
}
