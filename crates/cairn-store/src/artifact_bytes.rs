// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Live artifact bytes per tenant, in the RAM of one range** — the source of the
//! `max-artifact-bytes=` cap.
//!
//! An `Artifacts` row `(id, sha)` is **counted** when it decodes
//! ([`keyspace::ArtifactRow::decode`]) **and** invocation `id` has metadata
//! (`Table::InvocationMeta`) — the tenant comes from there. A tenant's total is Σ `size` over
//! every counted row. Rows of an id without metadata yet (the copies `restart` makes before the
//! commit that creates the new invocation) are not counted yet; the commit that creates the new
//! invocation adds them.
//!
//! Same pattern as `QueryCounts`:
//!
//! - **Changes are applied after the batch is submitted** (the writer's hook). Each change is
//!   keyed by the artifact row key it touches, with the tenant and the bytes added/subtracted —
//!   whoever builds the batch reads the prior state (old row, metadata) in the same engine hold
//!   as the commit.
//! - **Rebuild** by scanning the range's hot zone in chunks (a background loop of the term, one
//!   engine hold per chunk). During the rebuild, changes to keys `≤` the cursor are applied;
//!   the rest is left for the scan to see the final state. Not finished ⇒
//!   [`ArtifactBytes::snapshot`] returns `None` ⇒ the range is missing from the total ⇒ the
//!   soft cap lets things through.
//!
//! Nothing durable is written: the worst error here is a skewed cap, never data. The lock is
//! the range's own, not global; only the engine thread writes.

use crate::keyspace;

/// One change to the counter: `bytes` (negative = subtract) for `tenant`, caused by the
/// artifact row at key `key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDelta {
    pub key: Vec<u8>,
    pub tenant: String,
    pub bytes: i64,
}

/// Rebuild progress.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Scan {
    /// Not started: changes are ignored.
    NotStarted,
    /// The hot zone has been scanned up to this key (inclusive). Empty = no key yet.
    At(Vec<u8>),
    Done,
}

#[derive(Debug)]
struct Inner {
    tenants: crate::entropy::HashMap<String, i64>,
    scan: Scan,
}

/// A range's counter of live artifact bytes — see the module docs.
#[derive(Debug)]
pub struct ArtifactBytes {
    inner: std::sync::Mutex<Inner>,
}

impl Default for ArtifactBytes {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner {
                tenants: Default::default(),
                scan: Scan::NotStarted,
            }),
        }
    }
}

impl Inner {
    fn add(&mut self, tenant: &str, d: i64) {
        if d == 0 {
            return;
        }
        let n = self.tenants.entry(tenant.to_string()).or_insert(0);
        *n += d;
        if *n == 0 {
            self.tenants.remove(tenant);
        }
    }
}

impl ArtifactBytes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // The hook never panics in the middle of a change; a poisoned lock is still readable.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Applies the changes of a submitted batch.
    pub fn apply(&self, deltas: &[ArtifactDelta]) {
        let mut g = self.lock();
        for d in deltas {
            let counted = match &g.scan {
                Scan::NotStarted => false,
                Scan::At(cursor) => !cursor.is_empty() && d.key <= *cursor,
                Scan::Done => true,
            };
            if counted {
                g.add(&d.tenant, d.bytes);
            }
        }
    }

    /// Starts (or restarts) the rebuild: clears every total, cursor before every key.
    pub fn begin(&self) {
        let mut g = self.lock();
        g.tenants.clear();
        g.scan = Scan::At(Vec::new());
    }

    /// `None` = not started; `Some(None)` = finished; `Some(Some(k))` = rebuilding, scanned up
    /// to `k` (empty = no key yet).
    pub fn cursor(&self) -> Option<Option<Vec<u8>>> {
        match &self.lock().scan {
            Scan::NotStarted => None,
            Scan::At(c) => Some(Some(c.clone())),
            Scan::Done => Some(None),
        }
    }

    /// Adds a chunk of counted rows `(tenant, size)` of the hot zone scanned up to `last`
    /// (inclusive), then moves the cursor there.
    pub fn absorb(&self, rows: &[(String, u64)], last: Vec<u8>) {
        let mut g = self.lock();
        for (tenant, size) in rows {
            g.add(tenant, i64::try_from(*size).unwrap_or(i64::MAX));
        }
        g.scan = Scan::At(last);
    }

    /// The scan has finished.
    pub fn finish(&self) {
        self.lock().scan = Scan::Done;
    }

    /// Drops everything: the next rebuild starts over.
    pub fn reset(&self) {
        let mut g = self.lock();
        g.tenants.clear();
        g.scan = Scan::NotStarted;
    }

    /// Non-zero `(tenant, bytes)`, by tenant; `None` while the rebuild has not finished.
    pub fn snapshot(&self) -> Option<Vec<(String, u64)>> {
        let g = self.lock();
        if g.scan != Scan::Done {
            return None;
        }
        let mut out: Vec<(String, u64)> = g
            .tenants
            .iter()
            .map(|(t, n)| (t.clone(), (*n).max(0) as u64))
            .collect();
        out.sort();
        Some(out)
    }
}

/// The change for one artifact row: its key, `tenant`, `bytes`. `bytes == 0` ⇒ nothing.
pub fn delta(
    id: &crate::InvocationId,
    sha: &[u8; 32],
    tenant: &str,
    bytes: i64,
) -> Option<ArtifactDelta> {
    (bytes != 0).then(|| ArtifactDelta {
        key: keyspace::artifact(id, sha),
        tenant: tenant.to_string(),
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(key: &[u8], tenant: &str, bytes: i64) -> ArtifactDelta {
        ArtifactDelta {
            key: key.to_vec(),
            tenant: tenant.into(),
            bytes,
        }
    }

    /// Not rebuilt: changes are dropped, `snapshot` is `None`. Rebuilding: only changes to keys
    /// `≤` the cursor are applied. Finished: every change is applied; a tenant whose total
    /// reaches 0 disappears; `reset` goes back to not rebuilt.
    #[test]
    fn deltas_follow_the_cursor() {
        let b = ArtifactBytes::default();
        b.apply(&[d(b"a", "t", 5)]);
        assert_eq!(b.snapshot(), None);
        b.begin();
        b.apply(&[d(b"a", "t", 5)]);
        b.absorb(&[("t".into(), 7)], b"m".to_vec());
        b.apply(&[d(b"a", "t", 3), d(b"z", "t", 100)]);
        assert_eq!(b.snapshot(), None, "rebuilding");
        b.absorb(&[("u".into(), 4)], b"zz".to_vec());
        b.finish();
        assert_eq!(b.snapshot(), Some(vec![("t".into(), 10), ("u".into(), 4)]));
        b.apply(&[d(b"q", "u", -4)]);
        assert_eq!(b.snapshot(), Some(vec![("t".into(), 10)]));
        b.reset();
        assert_eq!(b.cursor(), None);
        assert_eq!(b.snapshot(), None);
    }
}
