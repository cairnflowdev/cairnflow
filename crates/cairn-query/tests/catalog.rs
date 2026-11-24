// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Table catalogue: generated docs, keyspace coverage.

use cairn_query::catalog::{markdown, Covered, FEATURES, INVOCATION_DOCS, TABLE_COVERAGE};

const DOC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/api/query-tables.md"
);

/// **The table doc matches the declarations.** Regenerate:
/// `CAIRNFLOW_REGENERATE_QUERY_DOCS=1 cargo test -p cairn-query --test catalog`.
#[test]
fn query_tables_doc_is_current() {
    let want = markdown();
    if std::env::var("CAIRNFLOW_REGENERATE_QUERY_DOCS").is_ok() {
        std::fs::write(DOC, &want).unwrap();
    }
    let have = std::fs::read_to_string(DOC).unwrap_or_default();
    assert!(
        have == want,
        "docs/api/query-tables.md is stale — CAIRNFLOW_REGENERATE_QUERY_DOCS=1 cargo test -p cairn-query --test catalog"
    );
}

/// **Every keyspace table is queryable or excluded with a reason**, and the
/// query tables it names exist.
#[test]
fn every_table_is_queryable_or_excluded() {
    let tables = cairn_query::tables();
    for t in cairn_store::keys::Table::ALL {
        let Some((_, c)) = TABLE_COVERAGE.iter().find(|(x, _)| *x == t) else {
            panic!("keyspace table {t:?} is missing from TABLE_COVERAGE — add a query table or an exclusion reason");
        };
        match c {
            Covered::By(names) => {
                for n in *names {
                    assert!(
                        tables.contains(n),
                        "{t:?} points at table {n}, which does not exist"
                    );
                }
            }
            Covered::Excluded(why) => assert!(!why.is_empty(), "{t:?}: empty reason"),
        }
    }
    assert_eq!(
        TABLE_COVERAGE.len(),
        cairn_store::keys::Table::ALL.len(),
        "duplicate rows"
    );
}

/// Every column of `sys_invocation` has a doc.
#[test]
fn every_invocation_column_is_documented() {
    let schema = cairn_query::invocation::schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let docs: Vec<&str> = INVOCATION_DOCS.iter().map(|(n, _)| *n).collect();
    assert_eq!(names, docs);
}

/// **Feature coverage matrix**: every table the matrix names exists; every
/// query table appears in at least one feature (a new table must say which
/// feature it serves); a row without tables has a reason; F1…F50 are contiguous.
#[test]
fn every_feature_names_real_tables() {
    let tables = cairn_query::tables();
    for f in FEATURES {
        for t in f.tables {
            assert!(tables.contains(t), "{}: table {t} does not exist", f.id);
        }
        assert!(
            !f.tables.is_empty() || !f.note.is_empty(),
            "{}: no table and no reason",
            f.id
        );
    }
    for t in &tables {
        if *t == "sys_invocation_counts" {
            continue;
        }
        assert!(
            FEATURES.iter().any(|f| f.tables.contains(t)),
            "table {t} serves no feature in FEATURES"
        );
    }
    let ids: Vec<String> = FEATURES.iter().map(|f| f.id.to_string()).collect();
    let want: Vec<String> = (1..=FEATURES.len()).map(|i| format!("F{i}")).collect();
    assert_eq!(ids, want);
}
