// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Query-shape checker on the AST** (security review; replaces the token
//! counters for the relation/block part).
//!
//! The query is parsed with **exactly** the parser the DataFusion session uses
//! (`DFParserBuilder`, dialect and `recursion_limit` taken from the same
//! `ConfigOptions`), then the AST is walked with an **allow list**: any
//! structure the checker does not know ⇒ refused (safe by default). CTEs are
//! inlined the way DataFusion inlines them: the definition key is
//! `IdentNormalizer::normalize(alias)`, the lookup key is
//! `object_name_to_table_reference(name).to_string()` — the same two functions
//! DataFusion calls in `cte.rs` and `relation/mod.rs` — so every way of writing
//! a name (no `AS`, single quotes, dotted, keywords) resolves exactly as the
//! planner resolves it. Each reference adds the **full weight** of the body
//! (DataFusion copies the CTE plan into every use; every walk follows the
//! inlined tree). A definition also counts in full once (planning the body may
//! walk the inlined tree).
//!
//! The caps apply to the size **after inlining**: blocks, relations,
//! expression nodes, cells (the accumulated width of the join schema plus
//! `VALUES` cells plus projected columns), and the number of grouping sets of
//! `GROUP BY` (`CUBE`/`ROLLUP`/`GROUPING SETS`, counted **before** DataFusion
//! expands them — it allocates 2^n before its own cap). Saturating sums.

use {
    crate::QueryError,
    datafusion::{
        config::ConfigOptions,
        sql::{
            parser::{DFParserBuilder, Statement as DFStatement},
            planner::{object_name_to_table_reference, IdentNormalizer},
            sqlparser::{
                ast::{
                    Expr, GroupByExpr, GroupByWithModifier, JoinConstraint, JoinOperator,
                    LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr, SetQuantifier,
                    Statement, TableFactor, TableWithJoins, Visit, Visitor,
                },
                dialect::dialect_from_str,
            },
        },
    },
    std::ops::ControlFlow,
};

/// Cap on expression nodes after CTE inlining: a body of 500 operators inlined
/// 62 times is 31 000 nodes.
pub const MAX_SQL_EXPRESSIONS: u64 = 20_000;
/// Cap on cells after inlining: Σ of the accumulated schema width of each join,
/// `VALUES` cells (rows × columns) and projected columns. `VALUES` of 20 000
/// columns joined 62 times is ~3.9·10⁷.
pub const MAX_SQL_CELLS: u64 = 20_000;
/// Cap on the width (column count) of any schema — `VALUES`, a join result, a
/// projection list. Measured with the referee harness: a schema of ~1 800
/// columns through a few joins and a CTE used three times plans in 2.1 s
/// (release) although the total cells are only ~3·10⁴ — DataFusion's cost in
/// the width is superlinear.
pub const MAX_SQL_WIDTH: u64 = 256;
/// Cap on the grouping sets of one `GROUP BY` after expansion: `CUBE` of n
/// columns is 2^n — DataFusion (55.2) allocates all 2^n before its cap of 4 096.
pub const MAX_SQL_GROUPING_SETS: u64 = 256;
/// Estimated width of a base table (`sys_*`, `information_schema`): the widest
/// table of the catalogue (`sys_invocation`) has 34 columns
/// (`docs/api/query-tables.md`); 48 for headroom. Joining 6 or more base tables
/// exceeds [`MAX_SQL_WIDTH`].
const BASE_TABLE_WIDTH: u64 = 48;

/// The cost of a subtree, after CTE inlining.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    pub blocks: u64,
    pub relations: u64,
    pub expressions: u64,
    pub cells: u64,
}

impl Cost {
    fn add(&mut self, o: Cost) {
        self.blocks = self.blocks.saturating_add(o.blocks);
        self.relations = self.relations.saturating_add(o.relations);
        self.expressions = self.expressions.saturating_add(o.expressions);
        self.cells = self.cells.saturating_add(o.cells);
    }

    fn max(self, o: Cost) -> Cost {
        Cost {
            blocks: self.blocks.max(o.blocks),
            relations: self.relations.max(o.relations),
            expressions: self.expressions.max(o.expressions),
            cells: self.cells.max(o.cells),
        }
    }
}

/// A defined CTE: full weight and output width.
#[derive(Clone)]
struct Cte {
    name: String,
    cost: Cost,
    width: u64,
}

fn refuse(why: impl Into<String>) -> QueryError {
    QueryError::Sql(why.into())
}

/// Parse `sql` like the DataFusion session and return the cost after CTE
/// inlining. A query that does not parse, several statements, not a query, or
/// one carrying a structure outside the allow list ⇒ a client error.
pub fn query_cost(sql: &str) -> Result<Cost, QueryError> {
    let options = ConfigOptions::default();
    let dialect = dialect_from_str(options.sql_parser.dialect)
        .ok_or_else(|| refuse("unsupported SQL dialect"))?;
    let mut statements = DFParserBuilder::new(sql)
        .with_dialect(dialect.as_ref())
        .with_recursion_limit(options.sql_parser.recursion_limit.get())
        .build()
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| refuse(format!("cannot read the query: {e}")))?;
    if statements.len() != 1 {
        return Err(refuse("send exactly one SQL statement"));
    }
    // `EXPLAIN [ANALYZE] [VERBOSE] <query>` wraps exactly one query: check the
    // inner query as usual (DataFusion accepts it before the AST checker;
    // `ANALYZE` executes the query — like running it).
    let statement = match statements.pop_front() {
        Some(DFStatement::Explain(e)) => *e.statement,
        other => match other {
            Some(s) => s,
            None => return Err(refuse("send exactly one SQL statement")),
        },
    };
    let query = match statement {
        DFStatement::Statement(s) => match *s {
            Statement::Query(q) => q,
            _ => return Err(refuse("only SELECT queries are allowed")),
        },
        _ => return Err(refuse("only SELECT queries are allowed")),
    };
    let checker = Checker {
        normalizer: IdentNormalizer::new(options.sql_parser.enable_ident_normalization),
        normalize: options.sql_parser.enable_ident_normalization,
    };
    let mut scopes = Vec::new();
    let (cost, _) = checker.query(&query, &mut scopes)?;
    Ok(cost)
}

struct Checker {
    normalizer: IdentNormalizer,
    normalize: bool,
}

impl Checker {
    fn query(&self, q: &Query, scopes: &mut Vec<Vec<Cte>>) -> Result<(Cost, u64), QueryError> {
        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks,
            for_clause,
            settings,
            format_clause,
            pipe_operators,
        } = q;
        if !locks.is_empty()
            || for_clause.is_some()
            || settings.is_some()
            || format_clause.is_some()
            || !pipe_operators.is_empty()
        {
            return Err(refuse("query uses a clause the query API does not support"));
        }
        let mut cost = Cost::default();
        scopes.push(Vec::new());
        let out = (|| {
            if let Some(w) = with {
                if w.recursive {
                    return Err(refuse("recursive WITH queries are not supported"));
                }
                for cte in &w.cte_tables {
                    if cte.from.is_some() {
                        return Err(refuse("WITH … FROM is not supported"));
                    }
                    let (c, mut width) = self.query(&cte.query, scopes)?;
                    if !cte.alias.columns.is_empty() {
                        width = cte.alias.columns.len() as u64;
                    }
                    // A definition counts **in full** once, even when nobody uses
                    // it: planning the body may walk the inlined tree (e.g. the
                    // type coercion of `UNION` rewrites the child plan —
                    // `make_mut` copies the DAG into a tree). The referee caught
                    // this case (seed 9 664: a two-branch CTE 17 levels deep used
                    // only in the body of a CTE nobody reads ⇒ RSS > 2 GiB).
                    cost.add(c);
                    let name = self.normalizer.normalize(cte.alias.name.clone());
                    scopes.last_mut().expect("just pushed").push(Cte {
                        name,
                        cost: c,
                        width,
                    });
                }
            }
            let (c, width) = self.set_expr(body, scopes)?;
            cost.add(c);
            if let Some(o) = order_by {
                if o.interpolate.is_some() {
                    return Err(refuse("ORDER BY … INTERPOLATE is not supported"));
                }
                match &o.kind {
                    OrderByKind::Expressions(es) => {
                        for e in es {
                            cost.add(self.exprs(&e.expr, scopes)?);
                        }
                    }
                    OrderByKind::All(_) => {}
                }
            }
            if let Some(l) = limit_clause {
                match l {
                    LimitClause::LimitOffset {
                        limit,
                        offset,
                        limit_by,
                    } => {
                        if !limit_by.is_empty() {
                            return Err(refuse("LIMIT … BY is not supported"));
                        }
                        if let Some(e) = limit {
                            cost.add(self.exprs(e, scopes)?);
                        }
                        if let Some(o) = offset {
                            cost.add(self.exprs(&o.value, scopes)?);
                        }
                    }
                    LimitClause::OffsetCommaLimit { offset, limit } => {
                        cost.add(self.exprs(offset, scopes)?);
                        cost.add(self.exprs(limit, scopes)?);
                    }
                }
            }
            if let Some(f) = fetch {
                if let Some(e) = &f.quantity {
                    cost.add(self.exprs(e, scopes)?);
                }
            }
            Ok((cost, width))
        })();
        scopes.pop();
        out
    }

    fn set_expr(&self, s: &SetExpr, scopes: &mut Vec<Vec<Cte>>) -> Result<(Cost, u64), QueryError> {
        match s {
            SetExpr::Select(sel) => self.select(sel, scopes),
            SetExpr::Query(q) => self.query(q, scopes),
            SetExpr::SetOperation {
                left,
                right,
                set_quantifier,
                ..
            } => {
                // `BY NAME` takes the union of the names of both sides — a width
                // beyond the left side that the count below returns. Console
                // queries do not use it.
                if matches!(
                    set_quantifier,
                    SetQuantifier::ByName
                        | SetQuantifier::AllByName
                        | SetQuantifier::DistinctByName
                ) {
                    return Err(refuse(
                        "set operations BY NAME are not supported; list the columns of each side",
                    ));
                }
                let (mut cost, width) = self.set_expr(left, scopes)?;
                let (r, _) = self.set_expr(right, scopes)?;
                cost.add(r);
                cost.blocks = cost.blocks.saturating_add(1);
                Ok((cost, width))
            }
            SetExpr::Values(v) => {
                let mut cost = Cost {
                    blocks: 1,
                    ..Cost::default()
                };
                let width = v
                    .rows
                    .first()
                    .map(|r| row_exprs(r).len() as u64)
                    .unwrap_or(0);
                wide(width)?;
                let cells: u64 = v.rows.iter().map(|r| row_exprs(r).len() as u64).sum();
                cost.cells = cells;
                for row in &v.rows {
                    for e in row_exprs(row) {
                        cost.add(self.exprs(e, scopes)?);
                    }
                }
                Ok((cost, width))
            }
            SetExpr::Table(_)
            | SetExpr::Insert(_)
            | SetExpr::Update(_)
            | SetExpr::Delete(_)
            | SetExpr::Merge(_) => Err(refuse("only SELECT queries are allowed")),
        }
    }

    fn select(&self, s: &Select, scopes: &mut Vec<Vec<Cte>>) -> Result<(Cost, u64), QueryError> {
        let Select {
            select_token: _,
            optimizer_hints,
            distinct,
            select_modifiers,
            top,
            top_before_distinct: _,
            projection,
            exclude,
            into,
            from,
            lateral_views,
            prewhere,
            selection,
            connect_by,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window,
            qualify,
            window_before_qualify: _,
            value_table_mode,
            flavor: _,
        } = s;
        if !optimizer_hints.is_empty()
            || select_modifiers.is_some()
            || top.is_some()
            || exclude.is_some()
            || into.is_some()
            || !lateral_views.is_empty()
            || prewhere.is_some()
            || !connect_by.is_empty()
            || !cluster_by.is_empty()
            || !distribute_by.is_empty()
            || !sort_by.is_empty()
            || value_table_mode.is_some()
        {
            return Err(refuse("query uses a clause the query API does not support"));
        }
        let mut cost = Cost {
            blocks: 1,
            ..Cost::default()
        };
        // Left-deep join schema: node k holds the width of the first k relations.
        let mut from_width = 0u64;
        for twj in from {
            self.table_with_joins(twj, scopes, &mut cost, &mut from_width)?;
        }
        // Relations = joins: the first relation of `FROM` does not count (as in the token filter).
        if !from.is_empty() {
            cost.relations = cost.relations.saturating_sub(1);
        }
        if let Some(datafusion::sql::sqlparser::ast::Distinct::On(es)) = distinct {
            for e in es {
                cost.add(self.exprs(e, scopes)?);
            }
        }
        let mut width = 0u64;
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    width = width.saturating_add(1);
                    cost.add(self.exprs(e, scopes)?);
                }
                SelectItem::ExprWithAliases { expr, aliases } => {
                    width = width.saturating_add(aliases.len() as u64);
                    cost.add(self.exprs(expr, scopes)?);
                }
                SelectItem::Wildcard(opts) | SelectItem::QualifiedWildcard(_, opts) => {
                    // `REPLACE` expressions do not go through the count.
                    if opts.opt_replace.is_some() {
                        return Err(refuse(
                            "SELECT * REPLACE (…) is not supported; list the columns instead",
                        ));
                    }
                    width = width.saturating_add(from_width.max(1));
                }
            }
        }
        wide(width)?;
        cost.cells = cost.cells.saturating_add(width);
        for e in [selection, having, qualify].into_iter().flatten() {
            cost.add(self.exprs(e, scopes)?);
        }
        for w in named_window {
            cost.add(self.exprs_in(w, scopes)?);
        }
        let sets = match group_by {
            GroupByExpr::All(mods) => self.modifiers(mods, 0, scopes, &mut cost)?,
            GroupByExpr::Expressions(es, mods) => {
                let mut sets = 1u64;
                for e in es {
                    sets = sets.saturating_mul(grouping_sets(e));
                    cost.add(self.exprs(e, scopes)?);
                }
                sets.saturating_mul(self.modifiers(mods, es.len(), scopes, &mut cost)?)
            }
        };
        if sets > MAX_SQL_GROUPING_SETS {
            return Err(refuse(format!(
                "GROUP BY expands to {sets} grouping sets (CUBE/ROLLUP/GROUPING SETS); the limit is {MAX_SQL_GROUPING_SETS}"
            )));
        }
        Ok((cost, width))
    }

    fn modifiers(
        &self,
        mods: &[GroupByWithModifier],
        n: usize,
        scopes: &mut Vec<Vec<Cte>>,
        cost: &mut Cost,
    ) -> Result<u64, QueryError> {
        let mut sets = 1u64;
        for m in mods {
            sets = sets.saturating_mul(match m {
                GroupByWithModifier::Rollup => n as u64 + 1,
                GroupByWithModifier::Cube => pow2(n),
                GroupByWithModifier::Totals => 2,
                GroupByWithModifier::GroupingSets(e) => {
                    cost.add(self.exprs(e, scopes)?);
                    grouping_sets(e)
                }
            });
        }
        Ok(sets)
    }

    fn table_with_joins(
        &self,
        twj: &TableWithJoins,
        scopes: &mut Vec<Vec<Cte>>,
        cost: &mut Cost,
        from_width: &mut u64,
    ) -> Result<(), QueryError> {
        self.factor(&twj.relation, scopes, cost, from_width)?;
        for j in &twj.joins {
            self.factor(&j.relation, scopes, cost, from_width)?;
            let constraint = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c)
                | JoinOperator::Semi(c)
                | JoinOperator::LeftSemi(c)
                | JoinOperator::RightSemi(c)
                | JoinOperator::Anti(c)
                | JoinOperator::LeftAnti(c)
                | JoinOperator::RightAnti(c) => c,
                _ => return Err(refuse("query uses a join the query API does not support")),
            };
            if let JoinConstraint::On(e) = constraint {
                cost.add(self.exprs(e, scopes)?);
            }
        }
        Ok(())
    }

    /// One relation of `FROM`: its cost, and the accumulated schema width of the
    /// left-deep join up to it (cells).
    fn factor(
        &self,
        f: &TableFactor,
        scopes: &mut Vec<Vec<Cte>>,
        cost: &mut Cost,
        from_width: &mut u64,
    ) -> Result<(), QueryError> {
        let (c, w) = self.table_factor(f, scopes)?;
        cost.add(c);
        *from_width = from_width.saturating_add(w);
        wide(*from_width)?;
        cost.cells = cost.cells.saturating_add(*from_width);
        Ok(())
    }

    fn table_factor(
        &self,
        f: &TableFactor,
        scopes: &mut Vec<Vec<Cte>>,
    ) -> Result<(Cost, u64), QueryError> {
        let mut cost = Cost {
            relations: 1,
            ..Cost::default()
        };
        match f {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some()
                    || !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                    || sample.is_some()
                    || !index_hints.is_empty()
                {
                    return Err(refuse(
                        "query uses a table form the query API does not support",
                    ));
                }
                // Lookup key as in DataFusion's `relation/mod.rs`.
                let key = object_name_to_table_reference(name.clone(), self.normalize)
                    .map_err(|e| refuse(format!("cannot read the query: {e}")))?
                    .to_string();
                // Every visible definition with the same key: take the largest
                // (safe even if the planner's shadowing rules differ).
                let hit = scopes.iter().flatten().filter(|c| c.name == key).fold(
                    None::<(Cost, u64)>,
                    |acc, c| match acc {
                        None => Some((c.cost, c.width)),
                        Some((ac, aw)) => Some((ac.max(c.cost), aw.max(c.width))),
                    },
                );
                let width = match hit {
                    Some((c, w)) => {
                        cost.add(c);
                        w
                    }
                    None => BASE_TABLE_WIDTH,
                };
                Ok((cost, alias_width(alias.as_ref()).unwrap_or(width)))
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
                sample,
            } => {
                if sample.is_some() {
                    return Err(refuse(
                        "query uses a table form the query API does not support",
                    ));
                }
                let (c, w) = self.query(subquery, scopes)?;
                cost.add(c);
                Ok((cost, alias_width(alias.as_ref()).unwrap_or(w)))
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let mut w = 0;
                self.table_with_joins(table_with_joins, scopes, &mut cost, &mut w)?;
                Ok((cost, alias_width(alias.as_ref()).unwrap_or(w)))
            }
            _ => Err(refuse(
                "query uses a table form the query API does not support",
            )),
        }
    }

    /// Cost of the expressions in `e`: the node count at this level (allow
    /// list), and the full cost of every subquery inside (walked again with
    /// [`Self::query`] in the same CTE scope).
    fn exprs(&self, e: &Expr, scopes: &mut Vec<Vec<Cte>>) -> Result<Cost, QueryError> {
        self.exprs_in(e, scopes)
    }

    fn exprs_in<V: Visit>(&self, node: &V, scopes: &mut Vec<Vec<Cte>>) -> Result<Cost, QueryError> {
        let mut v = ExprWalk::default();
        if let ControlFlow::Break(why) = node.visit(&mut v) {
            return Err(refuse(why));
        }
        let mut cost = Cost {
            expressions: v.nodes,
            ..Cost::default()
        };
        for q in &v.subqueries {
            let (c, _) = self.query(q, scopes)?;
            cost.add(c);
        }
        Ok(cost)
    }
}

/// The expressions of one `VALUES` row — the `content` field of sqlparser's
/// `Parens`, **not** `pb::Value` (the `OTHER_CONTENT` line of
/// `cairn-server/tests/value_source.rs`). The only place it is read.
fn row_exprs(row: &datafusion::sql::sqlparser::ast::Parens<Vec<Expr>>) -> &[Expr] {
    &row.content
}

/// A schema wider than [`MAX_SQL_WIDTH`] ⇒ refused.
fn wide(width: u64) -> Result<(), QueryError> {
    if width > MAX_SQL_WIDTH {
        return Err(refuse(format!(
            "query builds a {width}-column row (VALUES, join or select list); the limit is {MAX_SQL_WIDTH}"
        )));
    }
    Ok(())
}

/// The number of columns an alias names, if any.
fn alias_width(a: Option<&datafusion::sql::sqlparser::ast::TableAlias>) -> Option<u64> {
    a.filter(|a| !a.columns.is_empty())
        .map(|a| a.columns.len() as u64)
}

fn pow2(n: usize) -> u64 {
    if n >= 63 {
        u64::MAX
    } else {
        1u64 << n
    }
}

/// The number of grouping sets one `GROUP BY` item produces (DataFusion
/// `enumerate_grouping_sets`): `CUBE` 2^n, `ROLLUP` n + 1, `GROUPING SETS` n.
fn grouping_sets(e: &Expr) -> u64 {
    match e {
        Expr::Cube(v) => pow2(v.len()),
        Expr::Rollup(v) => v.len() as u64 + 1,
        Expr::GroupingSets(v) => v.len() as u64,
        _ => 1,
    }
}

/// Walk one expression node: count the nodes at the root level, collect the
/// root-level subqueries (copies — walked again in a CTE scope by
/// [`Checker::query`]), refuse expressions outside the allow list.
#[derive(Default)]
struct ExprWalk {
    depth: usize,
    nodes: u64,
    subqueries: Vec<Query>,
}

impl Visitor for ExprWalk {
    type Break = String;

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<String> {
        if self.depth == 0 {
            self.subqueries.push(q.clone());
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<String> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<String> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        self.nodes = self.nodes.saturating_add(1);
        match e {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::CompoundFieldAccess { .. }
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::BinaryOp { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::RLike { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Cast { .. }
            | Expr::AtTimeZone { .. }
            | Expr::Extract { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Substring { .. }
            | Expr::Trim { .. }
            | Expr::Overlay { .. }
            | Expr::Collate { .. }
            | Expr::Nested(_)
            | Expr::Value(_)
            | Expr::TypedString(_)
            | Expr::Function(_)
            | Expr::Case { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(_)
            | Expr::GroupingSets(_)
            | Expr::Cube(_)
            | Expr::Rollup(_)
            | Expr::Tuple(_)
            | Expr::Array(_)
            | Expr::Interval(_)
            | Expr::Wildcard(_)
            | Expr::QualifiedWildcard(..) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(
                "query uses an expression the query API does not support".to_string(),
            ),
        }
    }
}
