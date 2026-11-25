// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Delivering segmented outputs outside the engine.
//!
//! The engine never assembles: the `completed` state row, the `Reply` outbox row and the
//! inter-range `Completion` message carry a [`SegmentedOutput`] (refs + tail + the source
//! owner). The two operations here run **outside** the engine — ingress calls them
//! directly, the turn shell calls them inside `Lease::released` (D16):
//!
//! - [`assemble`]: GET each segment, check length and sha, append the tail — bounded
//!   (ingress: [`cairn_engine::MAX_ASSEMBLE`]).
//! - [`copy`]: copy each segment to the receiver's owner under a new `gen`, PUT
//!   `Create` (rule 7: `ValueStore::put` resolves ambiguity by reading back) **before** the
//!   receiver's completion commits. Dying between the two steps ⇒ an orphan COPY, which the
//!   value sweep round cleans up (like a sealing PUT); the source row stays, the next
//!   delivery COPYs again.
//!
//! Never log URLs or segment paths at any level above `info`.

use {
    bytes::{Bytes, BytesMut},
    cairn_engine::SegmentedOutput,
    cairn_protocol::value::{Logical, SegmentRef},
    cairn_store::value::{Owner, Segment, ValueStore},
    cairn_store::{PartitionKey, StoreError, TenantId},
};

/// Why assembling or copying failed.
#[derive(Debug)]
pub enum OutputError {
    /// The logical output is larger than the assembler's limit.
    TooLarge { len: u64, max: u64 },
    /// A segment missing, of the wrong length or the wrong sha — never send corrupt bytes.
    Corrupt(String),
    /// A store error (retryable).
    Store(StoreError),
}

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge { len, max } => write!(
                f,
                "value_too_large: output of {len} bytes exceeds the limit of {max} bytes"
            ),
            Self::Corrupt(what) => write!(f, "value: {what}"),
            Self::Store(e) => write!(f, "value: {e}"),
        }
    }
}

impl From<StoreError> for OutputError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

impl From<OutputError> for StoreError {
    fn from(e: OutputError) -> Self {
        match e {
            OutputError::Store(e) => e,
            other => StoreError::Io(other.to_string()),
        }
    }
}

/// The bytes of segment `r` under `o`'s source owner, length and sha checked.
async fn fetch(
    values: &ValueStore,
    o: &SegmentedOutput,
    r: &SegmentRef,
) -> Result<Bytes, OutputError> {
    let seg = o.segment(r);
    let got = values.get(&seg).await.map_err(OutputError::Store)?;
    match got {
        Some(b) if b.len() as u64 == r.len && cairn_store::artifact::sha256(&b) == r.sha256 => {
            Ok(b)
        }
        Some(_) => Err(OutputError::Corrupt(
            "segment does not match its sha256".into(),
        )),
        None => Err(OutputError::Corrupt("segment is missing".into())),
    }
}

/// Assemble the whole output `o` (≤ `max` bytes).
pub async fn assemble(
    values: &ValueStore,
    o: &SegmentedOutput,
    max: u64,
) -> Result<Bytes, OutputError> {
    let len = o.len();
    if len > max {
        return Err(OutputError::TooLarge { len, max });
    }
    let mut full = BytesMut::with_capacity(len as usize);
    for r in o.refs() {
        full.extend_from_slice(&fetch(values, o, r).await?);
    }
    full.extend_from_slice(o.tail());
    if full.len() as u64 != len {
        return Err(OutputError::Corrupt(
            "assembled length differs from the value".into(),
        ));
    }
    Ok(full.freeze())
}

/// Copy every segment of `o` to the receiver's `(tenant, pk, owner)` under a new
/// `gen`. Returns the new value (new refs + the same tail) and the `(sha, gen)` just PUT —
/// the list the engine receives through `stage_copied`.
pub async fn copy(
    values: &ValueStore,
    o: &SegmentedOutput,
    to: (&TenantId, PartitionKey, Owner),
) -> Result<(Logical, Vec<([u8; 32], u64)>), OutputError> {
    let (tenant, pk, owner) = to;
    let mut refs = Vec::with_capacity(o.refs().len());
    let mut fresh = Vec::with_capacity(o.refs().len());
    for r in o.refs() {
        let bytes = fetch(values, o, r).await?;
        let seg = Segment {
            tenant: tenant.as_str().to_string(),
            pk,
            owner,
            sha: r.sha256,
            gen: fresh_gen(),
        };
        values.put(&seg, bytes).await.map_err(OutputError::Store)?;
        crate::metrics::agent::bump(&crate::metrics::agent::VALUE_COPIES);
        refs.push(SegmentRef {
            sha256: r.sha256,
            len: r.len,
            gen: seg.gen,
        });
        fresh.push((seg.sha, seg.gen));
    }
    // The gap "the COPY is durable, the receiver's completion has not committed" (AP-1).
    cairn_store::failpoint!("value::copy_after_put", err);
    Ok((
        Logical::Segmented {
            refs,
            tail: o.tail().clone(),
            len: o.len(),
        },
        fresh,
    ))
}

fn fresh_gen() -> u64 {
    let mut b = [0u8; 8];
    cairn_store::entropy::fill(&mut b);
    u64::from_be_bytes(b)
}
