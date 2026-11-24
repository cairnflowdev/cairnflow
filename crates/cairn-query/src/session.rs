// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **A locked-down session per query**.
//!
//! - `SQLOptions` forbid DDL (including `CREATE EXTERNAL TABLE`), DML + `COPY`,
//!   statements (`SET`, `PREPARE`), subqueries checked too.
//! - No table factory, no table function (`generate_series`, `range`).
//! - `repeat`/`lpad`/`rpad` (allocate outside the pool) and the `file://`
//!   object store are removed; scalar array-generating functions are not
//!   registered (`nested_expressions` off).
//! - Results are read batch by batch: the row cap stops the query at row
//!   `max_rows + 1`, results reserve space in the pool, each batch checks the
//!   deadline and yields the thread (security review).
//! - `enable_recursive_ctes = false`, `recursion_limit = 50`,
//!   `sort_pushdown_buffer_capacity = 8 MiB` (default 1 GiB).
//! - A `GreedyMemoryPool` of 128 MiB per query, no spill
//!   (`DiskManagerMode::Disabled` ⇒ `ResourcesExhausted`).
//! - `target_partitions = 2`, `batch_size = 256`; `information_schema` on.

use {
    crate::{
        counts::CountsTable,
        ctx::{QueryCtx, Warning},
        invocation::InvocationTable,
        QueryError,
    },
    datafusion::{
        arrow::{array::RecordBatch, datatypes::SchemaRef},
        execution::{
            disk_manager::{DiskManagerBuilder, DiskManagerMode},
            memory_pool::{GreedyMemoryPool, MemoryConsumer},
            object_store::ObjectStoreUrl,
            runtime_env::RuntimeEnvBuilder,
            SessionStateBuilder,
        },
        prelude::{SQLOptions, SessionConfig, SessionContext},
    },
    futures::StreamExt,
    std::{sync::Arc, time::Duration},
};

/// Cap on the text length of one query: checked **before** any parsing, even
/// before the tenant's slot.
pub const MAX_SQL_BYTES: usize = 64 << 10;

/// Cap on the number of operator tokens in one query.
///
/// sqlparser builds a chain of operators of equal precedence (`1+1+…`, `a OR a OR …`,
/// `x::t::t…`) with a loop into a left-deep tree as deep as the operator count,
/// never touching `recursion_limit` (which only counts nesting). Planning, `Drop`
/// and `Display` of that tree are recursive: measured (debug build, 2 MiB
/// thread) it survives 1 500 operators and **aborts the process** with a stack
/// overflow at 2 000 — and planning is quadratic in the length, synchronous CPU
/// that the deadline cannot cut (1 500 `+` ≈ 2 s). The cap of 500 is far below
/// that threshold; the query runtime also runs on a large stack
/// (`cairn-server::query::http::QUERY_STACK`). Real queries rarely exceed a few dozen.
pub const MAX_SQL_OPERATORS: usize = 500;

/// Cap on the number of joined relations in one query (security review): each
/// `JOIN`/`APPLY` and each comma separating relations at the `FROM` level.
///
/// DataFusion folds a chain of relations into a left-deep join tree as deep as
/// the relation count, each level merging the schemas of every level below —
/// never touching `recursion_limit` (which only counts bracket nesting).
/// Measured (debug, 64 MiB thread, a query filling 64 KiB): commas in `FROM`
/// (8 325 relations) and `CROSS JOIN` (3 505) take > 300 s to plan. Real
/// queries rarely join more than a few tables.
pub const MAX_SQL_RELATIONS: usize = 64;

/// Cap on the number of query blocks in one query: each `SELECT`/`VALUES`/`TABLE`
/// and each set operation (`UNION`, `INTERSECT`, `EXCEPT`, `MINUS`).
///
/// sqlparser builds a chain of set operations with a loop into a left-deep
/// tree; a chain of CTEs where each reads the previous one is also one plan
/// level per CTE. Measured: `UNION ALL` with 3 448 arms plans in 10.6 s,
/// `INTERSECT` in 76 s — synchronous CPU the query deadline cannot cut.
pub const MAX_SQL_BLOCKS: usize = 64;

/// Check the shape of a query **before** handing it to DataFusion, in two
/// layers. (1) On tokens, before parsing: length ≤ [`MAX_SQL_BYTES`], operator
/// tokens ≤ [`MAX_SQL_OPERATORS`], joined relations ≤ [`MAX_SQL_RELATIONS`],
/// blocks ≤ [`MAX_SQL_BLOCKS`] — the tokenizer is linear, not recursive, and
/// blocks deep trees before sqlparser builds them. (2) On the AST, after CTE
/// inlining ([`crate::sql_check`]): blocks, relations, expressions, width,
/// cells and grouping sets. Errors are the asker's errors.
pub fn check_sql(sql: &str) -> Result<(), QueryError> {
    check_sql_text(sql)?;
    check_sql_ast(sql)
}

/// Layer (1) of [`check_sql`]: only on the token sequence — one pass, not
/// recursive, no AST built. This is the part the parent node of `/query` runs
/// before handing the query to the child process: the ingress thread never
/// parses the asker's query recursively.
pub fn check_sql_text(sql: &str) -> Result<(), QueryError> {
    use datafusion::sql::sqlparser::{
        dialect::GenericDialect,
        keywords::Keyword,
        tokenizer::{Token, Tokenizer},
    };
    if sql.len() > MAX_SQL_BYTES {
        return Err(QueryError::Sql(format!(
            "query text is {} bytes; the limit is {MAX_SQL_BYTES} bytes",
            sql.len()
        )));
    }
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize()
        .map_err(|e| QueryError::Sql(format!("cannot read the query: {e}")))?;
    let operators = tokens
        .iter()
        .filter(|t| match t {
            Token::Word(w) => matches!(
                w.keyword,
                Keyword::AND
                    | Keyword::OR
                    | Keyword::XOR
                    | Keyword::NOT
                    | Keyword::IS
                    | Keyword::LIKE
                    | Keyword::ILIKE
                    | Keyword::SIMILAR
                    | Keyword::BETWEEN
                    | Keyword::IN
                    | Keyword::COLLATE
                    | Keyword::AT
                    | Keyword::DIV
                    | Keyword::OVERLAPS
            ),
            // Builds no expression level: names, constants, separators, brackets
            // (nesting already has `recursion_limit`), parameters.
            Token::EOF
            | Token::Number(..)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::Comma
            | Token::Whitespace(_)
            | Token::LParen
            | Token::RParen
            | Token::RBracket
            | Token::RBrace
            | Token::Period
            | Token::SemiColon
            | Token::Placeholder(_) => false,
            // Every operator sign, `::` cast, `[` index, unknown character.
            _ => true,
        })
        .count();
    if operators > MAX_SQL_OPERATORS {
        return Err(QueryError::Sql(format!(
            "query has {operators} operators; the limit is {MAX_SQL_OPERATORS} — split long AND/OR/+ chains (use IN (…) for lists of values)"
        )));
    }
    // Before parsing: a tree deep by token count.
    let (relations, blocks) = relations_and_blocks(&tokens);
    if relations > MAX_SQL_RELATIONS {
        return Err(QueryError::Sql(format!(
            "query joins {relations} relations; the limit is {MAX_SQL_RELATIONS}"
        )));
    }
    if blocks > MAX_SQL_BLOCKS {
        return Err(QueryError::Sql(format!(
            "query has {blocks} SELECT/VALUES blocks and set operations; the limit is {MAX_SQL_BLOCKS}"
        )));
    }
    Ok(())
}

/// Layer (2) of [`check_sql`]: on the AST, after CTE inlining. Parses and walks
/// recursively — runs on a large-stack thread (the query runtime or the child
/// process), not on the ingress thread.
fn check_sql_ast(sql: &str) -> Result<(), QueryError> {
    let cost = crate::sql_check::query_cost(sql)?;
    let after = "counting each use of a WITH query in full";
    if cost.relations > MAX_SQL_RELATIONS as u64 {
        return Err(QueryError::Sql(format!(
            "query joins {} relations ({after}); the limit is {MAX_SQL_RELATIONS}",
            cost.relations
        )));
    }
    if cost.blocks > MAX_SQL_BLOCKS as u64 {
        return Err(QueryError::Sql(format!(
            "query has {} SELECT/VALUES blocks and set operations ({after}); the limit is {MAX_SQL_BLOCKS}",
            cost.blocks
        )));
    }
    if cost.expressions > crate::sql_check::MAX_SQL_EXPRESSIONS {
        return Err(QueryError::Sql(format!(
            "query has {} expressions ({after}); the limit is {}",
            cost.expressions,
            crate::sql_check::MAX_SQL_EXPRESSIONS
        )));
    }
    if cost.cells > crate::sql_check::MAX_SQL_CELLS {
        return Err(QueryError::Sql(format!(
            "query is {} columns wide in total across joins and VALUES ({after}); the limit is {}",
            cost.cells,
            crate::sql_check::MAX_SQL_CELLS
        )));
    }
    Ok(())
}

/// Filter **before parsing**: count joined relations and blocks on the token
/// sequence, one pass, not recursive — blocks a left-deep set-operation/join
/// tree before sqlparser builds it (building, walking and `Drop` of the AST are
/// all recursive). The size after CTE inlining is computed on the AST by
/// [`crate::sql_check`]. Relations: `JOIN`, `APPLY`, and commas at the same
/// bracket depth as an open `FROM`; blocks: `SELECT`/`VALUES`/`TABLE` and set
/// operations. Overcounting is only safe.
fn relations_and_blocks(tokens: &[datafusion::sql::sqlparser::tokenizer::Token]) -> (usize, usize) {
    use datafusion::sql::sqlparser::{keywords::Keyword, tokenizer::Token};
    let (mut relations, mut blocks) = (0usize, 0usize);
    // Open `FROM` at each bracket depth; the last element is the current depth.
    let mut from_open = vec![false];
    for t in tokens {
        match t {
            Token::LParen => from_open.push(false),
            Token::RParen => {
                if from_open.len() > 1 {
                    from_open.pop();
                }
            }
            Token::SemiColon => {
                from_open.clear();
                from_open.push(false);
            }
            Token::Comma => {
                if from_open.last().copied().unwrap_or(false) {
                    relations += 1;
                }
            }
            Token::Word(w) => {
                let here = from_open.last_mut();
                match w.keyword {
                    Keyword::SELECT | Keyword::VALUES | Keyword::TABLE => blocks += 1,
                    Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT | Keyword::MINUS => {
                        blocks += 1;
                        if let Some(h) = here {
                            *h = false;
                        }
                    }
                    Keyword::JOIN | Keyword::APPLY => relations += 1,
                    Keyword::FROM => {
                        if let Some(h) = here {
                            *h = true;
                        }
                    }
                    Keyword::WHERE
                    | Keyword::GROUP
                    | Keyword::HAVING
                    | Keyword::ORDER
                    | Keyword::LIMIT
                    | Keyword::OFFSET
                    | Keyword::FETCH
                    | Keyword::WINDOW
                    | Keyword::QUALIFY => {
                        if let Some(h) = here {
                            *h = false;
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    (relations, blocks)
}

/// Caps of one query (the numbers were settled after measurement).
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub memory_bytes: usize,
    pub deadline: Duration,
    pub max_rows: usize,
    pub read_budget: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 128 << 20,
            deadline: Duration::from_secs(10),
            max_rows: 10_000,
            read_budget: crate::ctx::DEFAULT_READ_BUDGET,
        }
    }
}

/// The result of one query.
#[derive(Debug)]
pub struct QueryOutput {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub warnings: Vec<Warning>,
    /// Some range is missing or an index is being built: the answer may lack rows.
    pub partial: bool,
    /// Read units used (index rows + point reads).
    pub read: u64,
}

/// Names of every registered table — the table catalogue and the coverage check.
pub fn tables() -> Vec<&'static str> {
    let mut t = vec![
        "sys_invocation",
        "sys_invocation_counts",
        crate::artifact_bytes::NAME,
    ];
    t.extend(crate::keyed::TABLES.iter().map(|s| s.name));
    t.extend(crate::view::VIEWS.iter().map(|s| s.name));
    t
}

/// Build a locked-down session with the tables of `ctx`.
pub fn session(ctx: &QueryCtx, limits: &Limits) -> Result<SessionContext, QueryError> {
    let mut config = SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(256)
        .with_information_schema(true);
    {
        let o = config.options_mut();
        o.execution.enable_recursive_ctes = false;
        o.execution.sort_pushdown_buffer_capacity = 8 << 20;
    }
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(limits.memory_bytes)))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()
        .map_err(QueryError::from)?;
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_table_factories(empty())
        .with_table_functions(empty())
        .build();
    let sc = SessionContext::new_with_state(state);
    // Functions that allocate by argument, outside the pool. The array-generating
    // functions (`range`, `generate_series`, `array_repeat`, `array_resize` and
    // the `list_*` aliases) are not registered today (feature
    // `nested_expressions` off) — removed in case it gets turned on (security
    // review; `scalar_range_is_locked`).
    for f in [
        "repeat",
        "lpad",
        "rpad",
        "range",
        "generate_series",
        "array_repeat",
        "array_resize",
    ] {
        sc.deregister_udf(f);
    }
    let _ = sc.deregister_object_store(ObjectStoreUrl::local_filesystem().as_ref());
    sc.register_table(
        "sys_invocation",
        Arc::new(InvocationTable::new(ctx.clone())),
    )
    .map_err(QueryError::from)?;
    sc.register_table(
        "sys_invocation_counts",
        Arc::new(CountsTable::new(ctx.clone())),
    )
    .map_err(QueryError::from)?;
    sc.register_table(
        crate::artifact_bytes::NAME,
        Arc::new(crate::artifact_bytes::ArtifactBytesTable::new(ctx.clone())),
    )
    .map_err(QueryError::from)?;
    for spec in crate::keyed::TABLES {
        sc.register_table(
            spec.name,
            Arc::new(crate::keyed::KeyedTable::new(spec, ctx.clone())),
        )
        .map_err(QueryError::from)?;
    }
    if let Some(source) = &ctx.views {
        for spec in crate::view::VIEWS {
            sc.register_table(
                spec.name,
                Arc::new(crate::view::ViewTable::new(spec, source.clone())),
            )
            .map_err(QueryError::from)?;
        }
    }
    Ok(sc)
}

/// An empty map for DataFusion's API (which demands std's `HashMap`): nothing
/// iterated, no key hashed — no nondeterminism gets in.
#[allow(
    clippy::disallowed_types,
    reason = "DataFusion's API demands std's HashMap; the map is empty"
)]
fn empty<V>() -> std::collections::HashMap<String, V> {
    std::collections::HashMap::new()
}

fn sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// A bind value for `$n`: values the user types reach the query **only**
/// through here, never spliced into the SQL string. Four types, matching JSON.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Param {
    Str(String),
    Int(i64),
    Bool(bool),
    Null,
}

impl Param {
    fn scalar(&self) -> datafusion::scalar::ScalarValue {
        use datafusion::scalar::ScalarValue as S;
        match self {
            Self::Str(s) => S::Utf8(Some(s.clone())),
            Self::Int(i) => S::Int64(Some(*i)),
            Self::Bool(b) => S::Boolean(Some(*b)),
            Self::Null => S::Null,
        }
    }
}

/// Replace `$1…$n` of `df` with `params` — **on the unoptimised logical plan**,
/// so the optimiser and filter pushdown into tables see literals as in a
/// hand-written query (same read cost). Only consecutive positional parameters,
/// exactly as many as values: missing or extra ones are the asker's error, not
/// silently ignored.
fn bind(
    df: datafusion::dataframe::DataFrame,
    params: &[Param],
) -> Result<datafusion::dataframe::DataFrame, QueryError> {
    let mut names: Vec<String> = df
        .logical_plan()
        .get_parameter_types()
        .map_err(QueryError::from)?
        .into_keys()
        .collect();
    names.sort();
    let n = names.len();
    for name in &names {
        let positional = name
            .strip_prefix('$')
            .and_then(|d| d.parse::<usize>().ok())
            .is_some_and(|i| (1..=n).contains(&i));
        if !positional {
            return Err(QueryError::Sql(format!(
                "parameter {name}: only consecutive positional parameters $1…${n} are accepted"
            )));
        }
    }
    if params.len() != n {
        return Err(QueryError::Sql(format!(
            "the query has {n} parameters but {} values were given",
            params.len()
        )));
    }
    if n == 0 {
        return Ok(df);
    }
    df.with_param_values(params.iter().map(Param::scalar).collect::<Vec<_>>())
        .map_err(QueryError::from)
}

/// Run one read-only SQL query on the ranges of `ctx`.
pub async fn run(ctx: QueryCtx, sql: &str, limits: Limits) -> Result<QueryOutput, QueryError> {
    run_with_params(ctx, sql, &[], limits).await
}

/// Like [`run`], with values for `$1…$n` ([`Param`]).
pub async fn run_with_params(
    mut ctx: QueryCtx,
    sql: &str,
    params: &[Param],
    limits: Limits,
) -> Result<QueryOutput, QueryError> {
    check_sql(sql)?;
    ctx.read_budget = limits.read_budget;
    let sc = session(&ctx, &limits)?;
    // One deadline for both planning and running, as before parameters existed.
    let until = tokio::time::Instant::now() + limits.deadline;
    let late = || QueryError::Budget(format!("query ran over {:?}", limits.deadline));
    let df = tokio::time::timeout_at(until, sc.sql_with_options(sql, sql_options()))
        .await
        .map_err(|_| late())?
        .map_err(QueryError::from)?;
    let df = bind(df, params)?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    // Read **batch by batch** (security review), not a whole `collect()`: each
    // batch checks the row cap (stopping at row `max_rows + 1`), reserves space
    // in the query's pool (results count against the memory cap), checks the
    // deadline and yields the thread — an operator that does not yield by
    // itself (`CrossJoinStream`) cannot hold a worker past the deadline.
    let mut stream = tokio::time::timeout_at(until, df.execute_stream())
        .await
        .map_err(|_| late())?
        .map_err(QueryError::from)?;
    let held = MemoryConsumer::new("cairn-query-result").register(&sc.runtime_env().memory_pool);
    let (mut batches, mut rows) = (Vec::new(), 0usize);
    loop {
        let next = tokio::time::timeout_at(until, stream.next())
            .await
            .map_err(|_| late())?;
        let Some(batch) = next.transpose().map_err(QueryError::from)? else {
            break;
        };
        rows += batch.num_rows();
        if rows > limits.max_rows {
            return Err(QueryError::Budget(format!(
                "the query returns more than {} rows — add LIMIT",
                limits.max_rows
            )));
        }
        held.try_grow(batch.get_array_memory_size())
            .map_err(QueryError::from)?;
        batches.push(batch);
        tokio::task::yield_now().await;
    }
    let warnings = ctx.warnings();
    Ok(QueryOutput {
        schema,
        batches,
        partial: !warnings.is_empty(),
        warnings,
        read: ctx.stats.units(),
    })
}

/// Physical `EXPLAIN` of one query (query-plan shape tests, `plan_shape_*`).
pub async fn explain(ctx: QueryCtx, sql: &str) -> Result<String, QueryError> {
    let sc = session(&ctx, &Limits::default())?;
    let df = sc.sql_with_options(sql, sql_options()).await?;
    let plan = df.create_physical_plan().await?;
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    Ok(text)
}
