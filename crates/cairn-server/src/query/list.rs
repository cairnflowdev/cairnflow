// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`GET /invocations?cursor=…`**: the invocation list on the query layer's
//! scan path, paginated by key.
//!
//! With a `cursor` parameter (empty on the first page) this path is taken;
//! without one it is the old list (array + `x-cairn-*` headers) — old clients
//! see no change.
//!
//! Parameters: `limit` (default 50, cap 1 000), `class` (comma-separated
//! list), `service`, `handler`, `key`, `root` (`true`/`false`), `tag`
//! (`k=v`). Order `[created_at DESC, id ASC]`; `cursor` is the `<ms>.<id>` of
//! the previous page's last row — the next page is `created_at ≤ ms` minus the
//! rows `(ms, ≤ id)` (keyset pagination, no OFFSET).
//!
//! Returns `{invocations, nextCursor, partial, warnings}`; an absent
//! `nextCursor` means the end.

use {
    super::http::{err, warnings_json},
    axum::{http::StatusCode, response::IntoResponse, Json},
    cairn_query::TenantScope,
    cairn_store::entropy::HashMap,
};

/// Default and maximum page size.
pub const DEFAULT_LIMIT: usize = 50;
pub const MAX_LIMIT: usize = 1_000;

/// SQL string literal: single quotes doubled.
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn bad(msg: impl Into<String>) -> axum::response::Response {
    err(StatusCode::BAD_REQUEST, msg)
}

/// The SQL statement of one page.
pub(crate) fn page_sql(q: &HashMap<String, String>) -> Result<(String, usize), String> {
    let limit = match q.get("limit") {
        None => DEFAULT_LIMIT,
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| format!("limit {v:?} is not a number"))?
            .clamp(1, MAX_LIMIT),
    };
    let mut conds: Vec<String> = Vec::new();
    if let Some(cs) = q.get("class") {
        let mut names = Vec::new();
        for c in cs.split(',').filter(|c| !c.is_empty()) {
            if cairn_store::query_index::Class::from_name(c).is_none() {
                return Err(format!(
                    "class {c:?} does not exist; choose active, paused, completed, failed"
                ));
            }
            names.push(quote(c));
        }
        if !names.is_empty() {
            conds.push(format!("class IN ({})", names.join(", ")));
        }
    }
    for col in ["service", "handler", "key"] {
        if let Some(v) = q.get(col) {
            conds.push(format!("{col} = {}", quote(v)));
        }
    }
    match q.get("root").map(String::as_str) {
        None => {}
        Some("true") => conds.push("root = true".into()),
        Some("false") => conds.push("root = false".into()),
        Some(other) => return Err(format!("root {other:?} must be true or false")),
    }
    if let Some(t) = q.get("tag") {
        let (k, v) = t
            .split_once('=')
            .ok_or_else(|| format!("tag {t:?} must have the form k=v"))?;
        cairn_store::query_index::check_tag(k, v)?;
        // The `tags` column is a JSON object; look for exactly the `"k":"v"`
        // fragment (no wildcard characters — `strpos`, not `LIKE`).
        let needle = format!(
            "{}:{}",
            serde_json::Value::from(k),
            serde_json::Value::from(v)
        );
        conds.push(format!("strpos(tags, {}) > 0", quote(&needle)));
    }
    match q.get("cursor").map(String::as_str) {
        None | Some("") => {}
        Some(c) => {
            let (ms, id) = c
                .split_once('.')
                .and_then(|(ms, id)| Some((ms.parse::<u64>().ok()?, id)))
                .filter(|(_, id)| cairn_store::id_from_hex(id).is_some())
                .ok_or_else(|| format!("cursor {c:?} is malformed"))?;
            conds.push(format!(
                "created_at <= to_timestamp_millis({ms}) \
                 AND NOT (created_at = to_timestamp_millis({ms}) AND id <= {})",
                quote(id)
            ));
        }
    }
    let mut sql = "SELECT id, service, handler, key, class, status, root, \
                   CAST(created_at AS BIGINT) AS created_ms, \
                   CAST(transitioned_at AS BIGINT) AS transitioned_ms, tags \
                   FROM sys_invocation"
        .to_string();
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    // One extra row: if it is there, a next page exists.
    sql.push_str(&format!(
        " ORDER BY created_at DESC, id ASC LIMIT {}",
        limit + 1
    ));
    Ok((sql, limit))
}

/// Page entries from the statement's rows (column order of [`page_sql`]).
fn rows(rows: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    rows.into_iter()
        .map(|r| {
            let s = |c: usize| r[c].as_str().map(str::to_string);
            let tags: serde_json::Value = s(9)
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            serde_json::json!({
                "id": s(0),
                "service": s(1),
                "handler": s(2),
                "key": s(3).filter(|k| !k.is_empty()),
                "class": s(4),
                "status": s(5),
                "root": r[6].as_bool().unwrap_or(false),
                "createdAt": r[7].as_i64().unwrap_or(0),
                "transitionedAt": r[8].as_i64().unwrap_or(0),
                "tags": tags,
            })
        })
        .collect()
}

/// One page. `tenant` is the acting tenant (already checked at ingress).
pub(crate) async fn page(
    st: &crate::ingress::AppState,
    tenant: String,
    credentials: super::views::Credentials,
    q: &HashMap<String, String>,
) -> axum::response::Response {
    let Some(svc) = st.query().cloned() else {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "query layer is not enabled on this node",
        );
    };
    let (sql, limit) = match page_sql(q) {
        Ok(x) => x,
        Err(e) => return bad(e),
    };
    let answer = match svc
        .execute(
            st,
            super::http::Author::Server,
            TenantScope::One(tenant),
            credentials,
            &sql,
            &[],
            None,
        )
        .await
    {
        Ok(a) => a,
        Err((code, msg)) => return err(code, msg),
    };
    let mut items = match answer.rows() {
        Ok(r) => rows(r),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let next = (items.len() > limit).then(|| {
        items.truncate(limit);
        let last = &items[limit - 1];
        format!(
            "{}.{}",
            last["createdAt"],
            last["id"].as_str().unwrap_or_default()
        )
    });
    let mut body = serde_json::json!({
        "invocations": items,
        "partial": !answer.warnings.is_empty(),
        "warnings": warnings_json(&answer.warnings),
    });
    if let Some(c) = next {
        body["nextCursor"] = c.into();
    }
    Json(body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn page_sql_escapes_and_validates() {
        let (sql, n) = page_sql(&q(&[("service", "a'b"), ("limit", "5000")])).unwrap();
        assert_eq!(n, MAX_LIMIT);
        assert!(sql.contains("service = 'a''b'"), "{sql}");
        assert!(sql.ends_with(&format!("LIMIT {}", MAX_LIMIT + 1)), "{sql}");
        assert!(page_sql(&q(&[("class", "running")])).is_err());
        assert!(page_sql(&q(&[("cursor", "12.zz")])).is_err());
        assert!(page_sql(&q(&[("root", "yes")])).is_err());
        assert!(page_sql(&q(&[("tag", "Bad=x")])).is_err());
        let (sql, _) = page_sql(&q(&[("tag", "team=a'%")])).unwrap();
        assert!(
            sql.contains(r#"strpos(tags, '"team":"a''%"') > 0"#),
            "{sql}"
        );
    }
}
