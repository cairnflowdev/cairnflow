// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`sys_artifact_bytes`**: live artifact bytes per tenant — the RAM counters
//! of every range (`cairn_store::artifact_bytes`), summed. **Does not read the
//! store.** A range whose counter is not built yet ⇒ that range is skipped +
//! warning `indexing` (answer `partial`), like `sys_invocation_counts`.
//!
//! Ranges on another node: that node's counters through [`crate::remote`]
//! (still RAM). Source of the `max-artifact-bytes=` cap (`tenant_usage`): a
//! missing range ⇒ a lower total ⇒ the soft cap lets it through.

use {
    crate::ctx::QueryCtx,
    datafusion::{
        arrow::{
            array::{ArrayRef, RecordBatch, StringArray, UInt64Array},
            datatypes::{DataType, Field, Schema, SchemaRef},
        },
        catalog::{Session, TableProvider},
        common::Result,
        datasource::{memory::MemorySourceConfig, TableType},
        logical_expr::Expr,
        physical_plan::ExecutionPlan,
    },
    std::{collections::BTreeMap, fmt, sync::Arc},
};

/// Table name.
pub const NAME: &str = "sys_artifact_bytes";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("bytes", DataType::UInt64, false),
    ]))
}

/// Sum over every local range the asker can see: `tenant → bytes`.
pub(crate) fn local_sums(ctx: &QueryCtx) -> BTreeMap<String, u64> {
    let mut sum: BTreeMap<String, u64> = BTreeMap::new();
    for h in &ctx.ranges {
        match h.artifact_bytes().snapshot() {
            None => ctx.warn(Some(h.range().id.0), "indexing"),
            Some(rows) => {
                for (tenant, n) in rows {
                    if ctx.sees(&tenant) {
                        let s = sum.entry(tenant).or_default();
                        *s = s.saturating_add(n);
                    }
                }
            }
        }
    }
    sum
}

#[derive(Clone)]
pub struct ArtifactBytesTable {
    ctx: QueryCtx,
}

impl ArtifactBytesTable {
    pub fn new(ctx: QueryCtx) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for ArtifactBytesTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(NAME)
    }
}

#[async_trait::async_trait]
impl TableProvider for ArtifactBytesTable {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut sum = local_sums(&self.ctx);
        let remote = crate::remote::fan_out(&self.ctx, || crate::remote::Scan::ArtifactBytes)
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        for r in remote.into_iter().flat_map(|r| r.artifact_bytes) {
            if self.ctx.sees(&r.tenant) {
                let s = sum.entry(r.tenant).or_default();
                *s = s.saturating_add(r.bytes);
            }
        }
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from_iter_values(sum.keys())) as ArrayRef,
                Arc::new(UInt64Array::from_iter_values(sum.values().copied())),
            ],
        )?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema(),
            projection.cloned(),
        )?)
    }
}
