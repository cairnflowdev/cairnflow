// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `/admin/oidc…`: the cell's OIDC relying party configuration — `auth/oidc` (issuer,
//! session TTL, clients by hostname → tenant) and `auth/revoked`. Permission
//! `Provision` (control plane) or `Cluster`.
//!
//! Write commands go through the `registry_sync::on_change` layer like key revocation:
//! `204` when every node has applied it, `202 {pending}` when not yet — removing a
//! client, changing the issuer, revoking must all take effect synchronously. Repeating a
//! write with the same content is idempotent.

use {
    super::*,
    crate::auth::oidc::{self, OidcClient, OidcConfig, Revocation},
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ClientView {
    client_id: String,
    hostname: String,
    tenant: String,
    max_role: &'static str,
    portal_url: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RevokedView {
    client: String,
    sub: String,
    before_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OidcView {
    issuer: Option<String>,
    session_ttl_ms: u64,
    display_name: Option<String>,
    clients: Vec<ClientView>,
    revoked: Vec<RevokedView>,
}

fn view(cfg: OidcConfig, revoked: Vec<Revocation>) -> OidcView {
    OidcView {
        issuer: cfg.issuer,
        session_ttl_ms: cfg.session_ttl_ms,
        display_name: cfg.display_name,
        clients: cfg
            .clients
            .into_iter()
            .map(|c| ClientView {
                client_id: c.client_id,
                hostname: c.hostname,
                tenant: c.tenant.to_string(),
                max_role: c.max_role.as_str(),
                portal_url: c.portal_url,
            })
            .collect(),
        revoked: revoked
            .into_iter()
            .map(|r| RevokedView {
                client: r.client,
                sub: r.sub,
                before_ms: r.before_ms,
            })
            .collect(),
    }
}

fn store_error(e: String) -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, e)
}

/// `GET /admin/oidc` — the registry **just read from the bucket** (the control plane
/// reconciles against the real registry, not the node's cache). No object ⇒
/// `issuer: null`, empty.
pub(super) async fn get_oidc(State(st): State<AppState>) -> ApiResult<Json<OidcView>> {
    let (cfg, revoked) = st.auth.keys.oidc().read().await.map_err(store_error)?;
    Ok(Json(view(cfg, revoked)))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PutOidc {
    issuer: String,
    #[serde(default)]
    session_ttl_ms: Option<u64>,
    #[serde(default)]
    display_name: Option<String>,
}

/// The error of one registry edit: conflict (`409`), store (`503`).
enum EditError {
    Conflict(String),
    Store(String),
}

impl From<String> for EditError {
    fn from(e: String) -> Self {
        Self::Store(e)
    }
}

impl From<EditError> for ApiError {
    fn from(e: EditError) -> Self {
        match e {
            EditError::Conflict(m) => ApiError(StatusCode::CONFLICT, m),
            EditError::Store(m) => store_error(m),
        }
    }
}

/// `PUT /admin/oidc {issuer, sessionTtlMs?, displayName?}` — issuer, session TTL, the
/// IdP name on the login button (absent ⇒ "SSO"); clients unchanged. The issuer must
/// pass the node's egress policy (SaaS profile: `https://`). Changing the issuer ⇒
/// every old SSO session dies (the cookie carries `iss`).
pub(super) async fn put_oidc(
    State(st): State<AppState>,
    Json(body): Json<PutOidc>,
) -> ApiResult<StatusCode> {
    oidc::check_issuer(&body.issuer).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    if let Ok(u) = reqwest::Url::parse(&body.issuer) {
        st.tenant_http
            .policy()
            .check_url(&u)
            .map_err(|d| ApiError(StatusCode::BAD_REQUEST, format!("issuer: {d}")))?;
    }
    let ttl = body.session_ttl_ms.unwrap_or(oidc::DEFAULT_SESSION_TTL_MS);
    oidc::check_session_ttl_ms(ttl).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    if let Some(n) = &body.display_name {
        oidc::check_display_name(n).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    }
    st.auth
        .keys
        .oidc()
        .update_config(|c| {
            let changed = c.issuer.as_deref() != Some(body.issuer.as_str())
                || c.session_ttl_ms != ttl
                || c.display_name != body.display_name;
            c.issuer = Some(body.issuer.clone());
            c.session_ttl_ms = ttl;
            c.display_name = body.display_name.clone();
            Ok::<_, EditError>(((), changed))
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PutClient {
    hostname: String,
    tenant: String,
    #[serde(default)]
    max_role: Option<String>,
    #[serde(default)]
    portal_url: Option<String>,
}

/// `PUT /admin/oidc/clients/{clientId}` — adds or replaces a client. A hostname that
/// already belongs to another client ⇒ `409`. Does not accept `clientSecret`: public
/// clients + PKCE, the cell keeps no IdP secret.
pub(super) async fn put_client(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<PutClient>,
) -> ApiResult<StatusCode> {
    let bad = |e: String| ApiError(StatusCode::BAD_REQUEST, e);
    let client = OidcClient {
        client_id: id.clone(),
        hostname: body.hostname,
        tenant: cairn_store::TenantId::new(&body.tenant).map_err(|e| bad(e.to_string()))?,
        max_role: oidc::check_max_role(body.max_role.as_deref().unwrap_or("admin")).map_err(bad)?,
        portal_url: body.portal_url,
    };
    oidc::check_client(&client).map_err(bad)?;
    st.auth
        .keys
        .oidc()
        .update_config(|c| {
            if let Some(other) = c
                .clients
                .iter()
                .find(|o| o.hostname == client.hostname && o.client_id != client.client_id)
            {
                return Err(EditError::Conflict(format!(
                    "hostname {} already belongs to client {}",
                    client.hostname, other.client_id
                )));
            }
            match c
                .clients
                .iter_mut()
                .find(|o| o.client_id == client.client_id)
            {
                Some(o) if *o == client => Ok(((), false)),
                Some(o) => {
                    *o = client.clone();
                    Ok(((), true))
                }
                None => {
                    c.clients.push(client.clone());
                    Ok(((), true))
                }
            }
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /admin/oidc/clients/{clientId}` — every session of the client dies at the
/// next check (the client must still be in the registry). Idempotent: an already absent
/// client is also `204` — the answer (`204`/`202 {pending}`) is the ACK of the
/// **current** registry, so the control plane repeats until `204` to know the removal
/// has reached every node.
pub(super) async fn delete_client(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    st.auth
        .keys
        .oidc()
        .update_config(|c| match c.clients.iter().position(|o| o.client_id == id) {
            Some(i) => {
                c.clients.remove(i);
                Ok(((), true))
            }
            None => Ok::<_, EditError>(((), false)),
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PostRevoked {
    client: String,
    sub: String,
    #[serde(default)]
    before_ms: Option<u64>,
}

/// `POST /admin/oidc/revoked {client, sub, beforeMs?}` — sessions of `sub` at `client`
/// with `iat` before the mark are refused. The mark is stored per
/// [`oidc::merge_revocation`]: a line that already has a mark ≥ `beforeMs` ⇒ unchanged
/// (a repeated send does not slide the mark), otherwise `max(beforeMs, cell time + max
/// skew)` — a slow control plane clock cannot save a session just minted.
/// `beforeMs` further ahead than [`oidc::MAX_REVOKE_AHEAD_MS`] ⇒ `400`.
pub(super) async fn post_revoked(
    State(st): State<AppState>,
    Json(body): Json<PostRevoked>,
) -> ApiResult<StatusCode> {
    let bad = |e: String| ApiError(StatusCode::BAD_REQUEST, e);
    oidc::check_client_id(&body.client).map_err(bad)?;
    oidc::check_sub(&body.sub).map_err(bad)?;
    let now = cairn_store::entropy::now_ms();
    let before = body.before_ms.unwrap_or(0);
    if before > now + oidc::MAX_REVOKE_AHEAD_MS {
        return Err(bad(format!(
            "beforeMs is more than {} ms away from the cell's clock",
            oidc::MAX_REVOKE_AHEAD_MS
        )));
    }
    st.auth
        .keys
        .oidc()
        .revoke(
            Revocation {
                client: body.client,
                sub: body.sub,
                before_ms: before,
            },
            now,
        )
        .await
        .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}
