// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`sys_invocation_counts`**: the RAM counters of
//! every local range, summed by `(tenant, service, handler, class, root)`.
//! **Does not read the store.** A range whose counter is not built yet ⇒ that
//! range is skipped + warning `indexing` (answer `partial`).
//!
//! Ranges on another node: that node's counters through [`crate::remote`]
//! (still RAM, no store read anywhere).
//!
//! RAM tables are kept apart from durable tables: no view joins the two kinds
//! (a view mixing them would make every query of it pay for a store read and
//! mix two consistency levels in one answer).

use {
    crate::ctx::QueryCtx,
    cairn_store::query_index::Class,
    datafusion::{
        arrow::{
            array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array},
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

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("handler", DataType::Utf8, false),
        Field::new("class", DataType::Utf8, false),
        Field::new("root", DataType::Boolean, false),
        Field::new("count", DataType::UInt64, false),
    ]))
}

/// Summed group key: `(tenant, service, handler, class, root)`.
pub(crate) type Group = (String, String, String, &'static str, bool);

/// Sum of the counters of every local range the asker can see.
pub(crate) fn local_sums(ctx: &QueryCtx) -> BTreeMap<Group, u64> {
    let mut sum: BTreeMap<Group, u64> = BTreeMap::new();
    for h in &ctx.ranges {
        match h.counts().snapshot() {
            None => ctx.warn(Some(h.range().id.0), "indexing"),
            Some(groups) => {
                for (k, n) in groups {
                    if ctx.sees(&k.tenant) {
                        *sum.entry((k.tenant, k.service, k.handler, k.class.name(), k.root))
                            .or_default() += n;
                    }
                }
            }
        }
    }
    sum
}

#[derive(Clone)]
pub struct CountsTable {
    ctx: QueryCtx,
}

impl CountsTable {
    pub fn new(ctx: QueryCtx) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for CountsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sys_invocation_counts")
    }
}

#[async_trait::async_trait]
impl TableProvider for CountsTable {
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
        let remote = crate::remote::fan_out(&self.ctx, || crate::remote::Scan::Counts)
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        for c in remote.into_iter().flat_map(|r| r.counts) {
            let Some(class) = Class::from_name(&c.class) else {
                continue;
            };
            if self.ctx.sees(&c.tenant) {
                *sum.entry((c.tenant, c.service, c.handler, class.name(), c.root))
                    .or_default() += c.n;
            }
        }
        let col = |f: &dyn Fn(&Group) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(sum.keys().map(f)))
        };
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                col(&|k| k.0.clone()),
                col(&|k| k.1.clone()),
                col(&|k| k.2.clone()),
                col(&|k| k.3.to_string()),
                Arc::new(BooleanArray::from_iter(sum.keys().map(|k| Some(k.4)))),
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
