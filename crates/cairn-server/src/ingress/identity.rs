// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `GET /identity`: the tenant's public request-identity keys — the `kid`s a customer
//! endpoint passes to `serve({identityKeys})`. `admin` key, the key's tenant
//! (`cluster-admin`: `?tenant=`).

use super::*;

#[derive(Serialize)]
pub(super) struct IdentityView {
    tenant: String,
    /// Whether the node has an identity master (signs requests to the SDK).
    signing: bool,
    /// The generations being published; the one signing is marked `active`. Empty when
    /// the node does not sign.
    keys: Vec<crate::identity::PublishedKey>,
}

pub(super) async fn published(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<IdentityView>> {
    let tenant = acting_tenant(&who, &q)?;
    Ok(Json(IdentityView {
        tenant: tenant.as_str().to_string(),
        signing: st.identity.signs(),
        keys: st.identity.published(&tenant),
    }))
}
