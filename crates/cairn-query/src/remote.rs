// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Remote scans**.
//!
//! The node that receives the query coordinates: ranges open on it are read
//! through read handles; ranges led by another node go through one
//! [`RemoteScan`] per node (in the server: `POST /internal/query/scan`, HMAC
//! signed, tenant in the signed body). The remote node runs **exactly the same
//! leaf code** ([`serve`]) on its ranges — choose the stream, stop at `fetch`,
//! read the extra projected columns, sort in the declared order — then returns
//! rows; the coordinating node merges, re-sorts and cuts at `fetch`.
//!
//! [`ScanRequest`] carries the **name + version of the path's index
//! descriptor**: a remote node whose descriptor has a different version
//! (rolling upgrade) scans metadata + warning `indexing`. A remote node that
//! does not answer ⇒ warning `partial` for all its ranges; a remote node over
//! the read cap ⇒ the whole query fails with `query_budget`.

use {
    crate::{
        ctx::{QueryCtx, TenantScope, Warning},
        filter::Pred,
        invocation::{self, Order},
        QueryError,
    },
    cairn_store::{query_index::Class, range_read::RangeReadHandle},
    serde::{Deserialize, Serialize},
};

/// Version of the `ScanRequest`/`ScanResponse` body (JSON).
pub const WIRE_VERSION: u32 = 1;

/// Scan path of a remote request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WirePath {
    Point,
    /// Scan the metadata table (fallback).
    Meta,
    Index {
        name: String,
        version: u16,
    },
}

/// The table scanned.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "table", rename_all = "snake_case")]
pub enum Scan {
    Invocation {
        pred: Box<Pred>,
        order: Order,
        path: WirePath,
        fetch: Option<usize>,
        projection: Vec<usize>,
    },
    Counts,
    /// `sys_artifact_bytes`. An old-version node does not know this tag ⇒
    /// error ⇒ its ranges are `partial` (missing from the total — the soft cap
    /// lets it through), with no change to `WIRE_VERSION`.
    ArtifactBytes,
    /// A table by key ([`crate::keyed`]): table name and key values.
    Keyed {
        name: String,
        args: crate::keyed::Args,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanRequest {
    pub v: u32,
    /// `None` = every tenant (`cluster-admin`).
    pub tenant: Option<String>,
    /// Ranges the remote node must scan (by the coordinating node's routing table).
    pub ranges: Vec<u32>,
    /// Read units the query has left.
    pub read_budget: u64,
    pub scan: Scan,
}

impl ScanRequest {
    pub(crate) fn new(ctx: &QueryCtx, ranges: &[u32], scan: Scan) -> Self {
        Self {
            v: WIRE_VERSION,
            tenant: match &ctx.tenant {
                TenantScope::All => None,
                TenantScope::One(t) => Some(t.clone()),
            },
            ranges: ranges.to_vec(),
            read_budget: ctx.read_budget.saturating_sub(ctx.stats.units()),
            scan,
        }
    }
}

/// One counter group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCount {
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub class: String,
    pub root: bool,
    pub n: u64,
}

/// Live artifact bytes of one tenant on the remote node's ranges.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireArtifactBytes {
    pub tenant: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScanResponse {
    pub v: u32,
    #[serde(default)]
    pub rows: Vec<invocation::WireRow>,
    #[serde(default)]
    pub counts: Vec<WireCount>,
    /// `sys_artifact_bytes`.
    #[serde(default)]
    pub artifact_bytes: Vec<WireArtifactBytes>,
    /// Rows of a table by key.
    #[serde(default)]
    pub cells: Vec<Vec<crate::keyed::Cell>>,
    #[serde(default)]
    pub warnings: Vec<Warning>,
    /// Read units the remote node used.
    pub read: u64,
}

/// Another node holding some of the query's ranges.
#[async_trait::async_trait]
pub trait RemoteScan: Send + Sync {
    /// Node name — for warnings.
    fn node(&self) -> &str;
    /// Ranges this node leads by the coordinating node's routing table.
    fn ranges(&self) -> &[u32];
    /// Send the request. `Err(Budget)` ⇒ the whole query is over the cap; any
    /// other error ⇒ this node's ranges are `partial`.
    async fn scan(&self, req: ScanRequest) -> Result<ScanResponse, QueryError>;
}

/// Send `scan` to every remote node in parallel; merge warnings and read units
/// into `ctx`. Returns the responses of the nodes that answered.
pub(crate) async fn fan_out(
    ctx: &QueryCtx,
    scan: impl Fn() -> Scan,
) -> Result<Vec<ScanResponse>, QueryError> {
    let asks = ctx.remotes.iter().map(|r| {
        let req = ScanRequest::new(ctx, r.ranges(), scan());
        async move { (r, r.scan(req).await) }
    });
    let mut out = Vec::new();
    for (r, res) in futures::future::join_all(asks).await {
        match res {
            Ok(resp) => {
                for w in &resp.warnings {
                    ctx.warn(w.range, w.reason.clone());
                }
                ctx.charge(resp.read, 0)?;
                out.push(resp);
            }
            Err(e @ QueryError::Budget(_)) => return Err(e),
            Err(e) => {
                for range in r.ranges() {
                    ctx.warn_partial(Some(*range), &format_args!("node {}: {e}", r.node()));
                }
            }
        }
    }
    Ok(out)
}

/// Remote-node side: run `req` on its local read handles.
pub async fn serve(
    handles: Vec<RangeReadHandle>,
    req: ScanRequest,
) -> Result<ScanResponse, QueryError> {
    if req.v != WIRE_VERSION {
        return Err(QueryError::Internal(format!(
            "unknown ScanRequest v{} (known: v{WIRE_VERSION})",
            req.v
        )));
    }
    let tenant = match req.tenant {
        None => TenantScope::All,
        Some(t) => TenantScope::One(t),
    };
    let mut mine = Vec::new();
    let mut ctx_warn = Vec::new();
    for r in &req.ranges {
        match handles.iter().find(|h| h.range().id.0 == *r) {
            Some(h) => mine.push(h.clone()),
            None => ctx_warn.push(*r),
        }
    }
    let mut ctx = QueryCtx::new(tenant, mine);
    ctx.read_budget = req.read_budget;
    for r in ctx_warn {
        ctx.warn(
            Some(r),
            "partial: the range is no longer open on the asked node",
        );
    }
    let mut resp = ScanResponse {
        v: WIRE_VERSION,
        ..Default::default()
    };
    match req.scan {
        Scan::Counts => {
            for (k, n) in crate::counts::local_sums(&ctx) {
                resp.counts.push(WireCount {
                    tenant: k.0,
                    service: k.1,
                    handler: k.2,
                    class: k.3.to_string(),
                    root: k.4,
                    n,
                });
            }
        }
        Scan::ArtifactBytes => {
            for (tenant, bytes) in crate::artifact_bytes::local_sums(&ctx) {
                resp.artifact_bytes
                    .push(WireArtifactBytes { tenant, bytes });
            }
        }
        Scan::Keyed { name, args } => {
            let spec = crate::keyed::spec(&name)
                .ok_or_else(|| QueryError::Internal(format!("no table {name}")))?;
            resp.cells = crate::keyed::local(&ctx, spec, &args).await?;
        }
        Scan::Invocation {
            pred,
            order,
            path,
            fetch,
            projection,
        } => {
            resp.rows = invocation::serve_rows(&ctx, *pred, order, path, fetch, projection).await?;
        }
    }
    resp.warnings = ctx.warnings();
    resp.read = ctx.stats.units();
    Ok(resp)
}

/// `Class` by its wire name — `cairn-store` does not depend on serde.
pub(crate) mod class_names {
    use {
        super::Class,
        serde::{Deserialize, Deserializer, Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(v: &Option<Vec<Class>>, s: S) -> Result<S::Ok, S::Error> {
        v.as_ref()
            .map(|cs| cs.iter().map(|c| c.name()).collect::<Vec<_>>())
            .serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<Class>>, D::Error> {
        let names = Option::<Vec<String>>::deserialize(d)?;
        names
            .map(|ns| {
                ns.iter()
                    .map(|n| {
                        Class::from_name(n)
                            .ok_or_else(|| serde::de::Error::custom(format!("unknown class {n:?}")))
                    })
                    .collect()
            })
            .transpose()
    }
}
