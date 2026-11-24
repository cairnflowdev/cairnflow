// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Filters pushed down into `sys_invocation` ("`Exact` only for" the shapes below).
//!
//! DataFusion **trusts an `Exact` filter absolutely** — it removes the
//! `FilterExec`. So every shape here does two jobs at once: chooses the scan
//! path, **and** is re-applied to each row at the leaf ([`Pred::admits`]). An
//! unrecognised shape ⇒ `Unsupported`, DataFusion filters afterwards — slower,
//! not wrong.
//!
//! Recognised shapes (on exactly one column):
//! - `col = lit`, `lit = col`;
//! - `col IN (…)` (not `NOT`), an `OR` chain of `=` on the **same** column
//!   (DataFusion splits an `IN` of ≤ 3 elements into an `OR` chain);
//! - ranges `<`, `<=`, `>`, `>=`, `=` on `created_at`/`transitioned_at`
//!   (the column may be wrapped in `Cast`/`TryCast`);
//! - `LIKE 'prefix%'` (no `_`, no `\`, case-sensitive) on
//!   `service`/`handler`;
//! - `duration_ms >= lit`.

use {
    cairn_store::query_index::Class,
    datafusion::{
        common::ScalarValue,
        logical_expr::{BinaryExpr, Expr, Operator},
    },
};

/// Filterable columns of `sys_invocation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Col {
    Id,
    Tenant,
    Service,
    Handler,
    Key,
    Kind,
    Class,
    Root,
    ParentId,
    Schedule,
    Deployment,
    RestartedFrom,
    RootId,
    CreatedAt,
    TransitionedAt,
    DurationMs,
}

impl Col {
    fn of(name: &str) -> Option<Self> {
        Some(match name {
            "id" => Self::Id,
            "tenant" => Self::Tenant,
            "service" => Self::Service,
            "handler" => Self::Handler,
            "key" => Self::Key,
            "kind" => Self::Kind,
            "class" => Self::Class,
            "root" => Self::Root,
            "parent_id" => Self::ParentId,
            "schedule" => Self::Schedule,
            "deployment" => Self::Deployment,
            "restarted_from" => Self::RestartedFrom,
            "root_id" => Self::RootId,
            "created_at" => Self::CreatedAt,
            "transitioned_at" => Self::TransitionedAt,
            "duration_ms" => Self::DurationMs,
            _ => return None,
        })
    }

    fn is_time(self) -> bool {
        matches!(self, Self::CreatedAt | Self::TransitionedAt)
    }
}

/// A time range, ms, both ends inclusive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Span {
    pub lo: Option<u64>,
    pub hi: Option<u64>,
}

impl Span {
    fn narrow(&mut self, lo: Option<u64>, hi: Option<u64>) {
        if let Some(l) = lo {
            self.lo = Some(self.lo.map_or(l, |x| x.max(l)));
        }
        if let Some(h) = hi {
            self.hi = Some(self.hi.map_or(h, |x| x.min(h)));
        }
    }

    pub fn contains(&self, t: u64) -> bool {
        self.lo.is_none_or(|l| t >= l) && self.hi.is_none_or(|h| t <= h)
    }

    pub fn is_open(&self) -> bool {
        self.lo.is_none() && self.hi.is_none()
    }

    /// Empty (lo > hi).
    pub fn is_empty(&self) -> bool {
        matches!((self.lo, self.hi), (Some(l), Some(h)) if l > h)
    }
}

/// Every condition pushed down, combined with AND.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Pred {
    /// `Some(set)` = the column must be in the set (the intersection of every condition on the column).
    pub ids: Option<Vec<String>>,
    pub tenants: Option<Vec<String>>,
    pub services: Option<Vec<String>>,
    pub handlers: Option<Vec<String>>,
    pub keys: Option<Vec<String>>,
    pub kinds: Option<Vec<String>>,
    #[serde(with = "crate::remote::class_names")]
    pub classes: Option<Vec<Class>>,
    pub root: Option<Vec<bool>>,
    pub parent_ids: Option<Vec<String>>,
    pub schedules: Option<Vec<String>>,
    pub deployments: Option<Vec<String>>,
    pub restarted_from: Option<Vec<String>>,
    /// Root of the call tree (`ByAttr[$root]`). Absent on old nodes.
    #[serde(default)]
    pub root_ids: Option<Vec<String>>,
    pub service_prefix: Option<String>,
    pub handler_prefix: Option<String>,
    pub created: Span,
    pub transitioned: Span,
    pub min_duration_ms: Option<i64>,
    /// Two contradicting conditions (empty intersection): no row.
    pub impossible: bool,
}

/// One recognised single condition.
#[derive(Clone, Debug, PartialEq)]
enum Cond {
    In(Col, Vec<ScalarValue>),
    Range(Col, Option<u64>, Option<u64>),
    Prefix(Col, String),
    MinDuration(i64),
}

fn column_of(e: &Expr) -> Option<Col> {
    match e {
        Expr::Column(c) => Col::of(&c.name),
        Expr::Cast(c) => match c.expr.as_ref() {
            Expr::Column(col) => Col::of(&col.name).filter(|c| c.is_time()),
            _ => None,
        },
        Expr::TryCast(c) => match c.expr.as_ref() {
            Expr::Column(col) => Col::of(&col.name).filter(|c| c.is_time()),
            _ => None,
        },
        _ => None,
    }
}

fn literal_of(e: &Expr) -> Option<ScalarValue> {
    match e {
        Expr::Literal(v, _) => Some(v.clone()),
        Expr::Cast(c) => literal_of(&c.expr).and_then(|v| v.cast_to(c.field.data_type()).ok()),
        _ => None,
    }
}

/// The ms timestamp of a time literal (any unit); `None` = not a time or
/// null.
fn time_ms(v: &ScalarValue) -> Option<i64> {
    Some(match v {
        ScalarValue::TimestampMillisecond(Some(t), _) => *t,
        ScalarValue::TimestampSecond(Some(t), _) => t.checked_mul(1000)?,
        ScalarValue::TimestampMicrosecond(Some(t), _) => t.div_euclid(1000),
        ScalarValue::TimestampNanosecond(Some(t), _) => t.div_euclid(1_000_000),
        ScalarValue::Int64(Some(t)) => *t,
        _ => return None,
    })
}

/// A ms timestamp used as a bound: `strict` (`<`/`>`) moves it by one ms; odd
/// micro/nano parts round towards the safe side. Below 0 ⇒ 0.
fn bound(v: &ScalarValue, op: Operator) -> Option<(Option<u64>, Option<u64>)> {
    let ms = time_ms(v)?;
    // Sub-ms part (micro/nano) — `t > 10.5ms` means `t >= 11`.
    let frac = match v {
        ScalarValue::TimestampMicrosecond(Some(t), _) => t.rem_euclid(1000) != 0,
        ScalarValue::TimestampNanosecond(Some(t), _) => t.rem_euclid(1_000_000) != 0,
        _ => false,
    };
    // Negative lower bound ⇒ 0 (every timestamp is ≥ 0); negative upper bound ⇒ empty range `[1, 0]`.
    let lo = |x: i64| Some(x.max(0) as u64);
    let hi = |x: i64| -> (Option<u64>, Option<u64>) {
        if x < 0 {
            (Some(1), Some(0))
        } else {
            (None, Some(x as u64))
        }
    };
    Some(match op {
        Operator::Gt => (lo(ms + 1), None),
        Operator::GtEq => (lo(if frac { ms + 1 } else { ms }), None),
        Operator::Lt => hi(if frac { ms } else { ms - 1 }),
        Operator::LtEq => hi(ms),
        Operator::Eq if !frac && ms >= 0 => (lo(ms), Some(ms as u64)),
        Operator::Eq => (Some(1), Some(0)),
        _ => return None,
    })
}

fn flip(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

fn parse(e: &Expr) -> Option<Cond> {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Or => {
            // An `OR` chain of `=` on one column.
            let (Some(Cond::In(a, mut va)), Some(Cond::In(b, vb))) = (parse(left), parse(right))
            else {
                return None;
            };
            (a == b).then(|| {
                va.extend(vb);
                Cond::In(a, va)
            })
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (col, lit, op) = match (column_of(left), literal_of(right)) {
                (Some(c), Some(v)) => (c, v, *op),
                _ => match (column_of(right), literal_of(left)) {
                    (Some(c), Some(v)) => (c, v, flip(*op)?),
                    _ => return None,
                },
            };
            if col.is_time() {
                let (lo, hi) = bound(&lit, op)?;
                return Some(Cond::Range(col, lo, hi));
            }
            match (col, op) {
                (Col::DurationMs, Operator::GtEq) => match lit {
                    ScalarValue::Int64(Some(v)) => Some(Cond::MinDuration(v)),
                    _ => None,
                },
                (_, Operator::Eq) if !col.is_time() && col != Col::DurationMs => {
                    (!lit.is_null()).then(|| Cond::In(col, vec![lit]))
                }
                _ => None,
            }
        }
        Expr::InList(l) if !l.negated => {
            let col = column_of(&l.expr).filter(|c| !c.is_time() && *c != Col::DurationMs)?;
            let vals: Option<Vec<ScalarValue>> = l.list.iter().map(literal_of).collect();
            let vals = vals?;
            (!vals.iter().any(ScalarValue::is_null)).then_some(Cond::In(col, vals))
        }
        Expr::Like(l) if !l.negated && !l.case_insensitive && l.escape_char.is_none() => {
            let col = column_of(&l.expr).filter(|c| matches!(c, Col::Service | Col::Handler))?;
            let ScalarValue::Utf8(Some(p)) = literal_of(&l.pattern)? else {
                return None;
            };
            let prefix = p.strip_suffix('%')?;
            (!prefix.contains(['%', '_', '\\'])).then(|| Cond::Prefix(col, prefix.to_string()))
        }
        _ => None,
    }
}

/// `Exact` if the shape is recognised (and will be applied at the leaf), otherwise `Unsupported`.
pub fn supported(e: &Expr) -> bool {
    parse(e).is_some()
}

fn text(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Intersect the set `slot` with `vals` (already normalised).
fn intersect<T: PartialEq + Clone>(slot: &mut Option<Vec<T>>, vals: Vec<T>, impossible: &mut bool) {
    let next: Vec<T> = match slot.take() {
        None => vals,
        Some(old) => old.into_iter().filter(|x| vals.contains(x)).collect(),
    };
    if next.is_empty() {
        *impossible = true;
    }
    *slot = Some(next);
}

impl Pred {
    /// Element count of every list and prefix — the size the leaf check
    /// ([`crate::invocation`]) pays per row. The parent node of `/query` blocks
    /// oversized filters from the child process.
    pub fn entries(&self) -> usize {
        let strings = [
            &self.ids,
            &self.tenants,
            &self.services,
            &self.handlers,
            &self.keys,
            &self.kinds,
            &self.parent_ids,
            &self.schedules,
            &self.deployments,
            &self.restarted_from,
            &self.root_ids,
        ];
        strings
            .iter()
            .map(|v| v.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
            + self.classes.as_ref().map_or(0, Vec::len)
            + self.root.as_ref().map_or(0, Vec::len)
            + usize::from(self.service_prefix.is_some())
            + usize::from(self.handler_prefix.is_some())
    }

    /// Combine every recognised filter (unrecognised filters are dropped:
    /// DataFusion keeps a `FilterExec` for them).
    pub fn from_filters(filters: &[Expr]) -> Self {
        let mut p = Pred::default();
        for f in filters {
            let Some(c) = parse(f) else { continue };
            match c {
                Cond::In(col, vals) => p.add_in(col, vals),
                Cond::Range(Col::CreatedAt, lo, hi) => p.created.narrow(lo, hi),
                Cond::Range(_, lo, hi) => p.transitioned.narrow(lo, hi),
                Cond::Prefix(Col::Service, s) => p.service_prefix = Some(s),
                Cond::Prefix(_, s) => p.handler_prefix = Some(s),
                Cond::MinDuration(d) => {
                    p.min_duration_ms = Some(p.min_duration_ms.map_or(d, |x| x.max(d)))
                }
            }
        }
        if p.created.is_empty() || p.transitioned.is_empty() {
            p.impossible = true;
        }
        p
    }

    fn add_in(&mut self, col: Col, vals: Vec<ScalarValue>) {
        let imp = &mut self.impossible;
        let strs = || vals.iter().filter_map(text).collect::<Vec<_>>();
        match col {
            Col::Id => intersect(
                &mut self.ids,
                strs().into_iter().map(|s| s.to_ascii_lowercase()).collect(),
                imp,
            ),
            Col::Tenant => intersect(&mut self.tenants, strs(), imp),
            Col::Service => intersect(&mut self.services, strs(), imp),
            Col::Handler => intersect(&mut self.handlers, strs(), imp),
            Col::Key => intersect(&mut self.keys, strs(), imp),
            Col::Kind => intersect(&mut self.kinds, strs(), imp),
            Col::ParentId => intersect(&mut self.parent_ids, strs(), imp),
            Col::Schedule => intersect(&mut self.schedules, strs(), imp),
            Col::Deployment => intersect(&mut self.deployments, strs(), imp),
            Col::RestartedFrom => intersect(&mut self.restarted_from, strs(), imp),
            Col::RootId => intersect(
                &mut self.root_ids,
                strs().into_iter().map(|s| s.to_ascii_lowercase()).collect(),
                imp,
            ),
            Col::Class => intersect(
                &mut self.classes,
                strs().iter().filter_map(|s| Class::from_name(s)).collect(),
                imp,
            ),
            Col::Root => intersect(
                &mut self.root,
                vals.iter()
                    .filter_map(|v| match v {
                        ScalarValue::Boolean(Some(b)) => Some(*b),
                        _ => None,
                    })
                    .collect(),
                imp,
            ),
            Col::CreatedAt | Col::TransitionedAt | Col::DurationMs => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        datafusion::logical_expr::{col, lit},
    };

    #[test]
    fn recognises_exact_forms() {
        let ts = |ms: i64| {
            lit(ScalarValue::TimestampMillisecond(
                Some(ms),
                Some("UTC".into()),
            ))
        };
        for e in [
            col("service").eq(lit("Svc")),
            lit("Svc").eq(col("service")),
            col("class").in_list(vec![lit("failed"), lit("paused")], false),
            col("class")
                .eq(lit("failed"))
                .or(col("class").eq(lit("paused"))),
            col("created_at").gt_eq(ts(10)),
            col("transitioned_at").lt(ts(10)),
            col("root").eq(lit(true)),
            col("duration_ms").gt_eq(lit(5i64)),
        ] {
            assert!(supported(&e), "{e}");
        }
        for e in [
            col("service").not_eq(lit("Svc")),
            col("class").in_list(vec![lit("failed")], true),
            col("class")
                .eq(lit("failed"))
                .or(col("service").eq(lit("x"))),
            col("output").eq(lit("x")),
        ] {
            assert!(!supported(&e), "{e}");
        }
    }

    #[test]
    fn folds_ranges_and_sets() {
        let ts = |ms: i64| {
            lit(ScalarValue::TimestampMillisecond(
                Some(ms),
                Some("UTC".into()),
            ))
        };
        let p = Pred::from_filters(&[
            col("created_at").gt(ts(10)),
            col("created_at").lt_eq(ts(20)),
            col("class").in_list(vec![lit("failed"), lit("paused")], false),
            col("class").eq(lit("failed")),
        ]);
        assert_eq!(
            p.created,
            Span {
                lo: Some(11),
                hi: Some(20)
            }
        );
        assert_eq!(p.classes, Some(vec![Class::Failed]));
        assert!(!p.impossible);
        let p = Pred::from_filters(&[
            col("class").eq(lit("failed")),
            col("class").eq(lit("paused")),
        ]);
        assert!(p.impossible);
    }
}
