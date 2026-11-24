// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Every read of `pb::Value.content` goes through `cairn_protocol::value`.**
//!
//! The `content` of a `Value` is the **tail**: a segmented value carries most
//! of its bytes in segments. Reading `content` directly treats the tail as the
//! whole value — a silent corruption that no behaviour test catches
//! completely. So this is a source rule: every access to a field named
//! `content` (`x.content`, also inside a macro body) and every struct literal
//! with a `content` field in `crates/*/src` is a FAIL, except:
//!
//! - `crates/cairn-protocol/src/value.rs` (the only place allowed to know the
//!   layout);
//! - test code (`#[cfg(test)]`, `#[test]`, `tests/` directories);
//! - [`OTHER_CONTENT`]: the `content` field of a message **other than**
//!   `Value` (counted exactly per file — a new use means editing the table,
//!   and the reviewer checks whether it is a `Value`).
//!
//! | Case | Must see |
//! |---|---|
//! | every file `crates/*/src/**/*.rs` | no `.content` access outside the list |
//! | sample source snippets (`scanner_*`) | catches `.content` in expressions, macros, struct literals; skips test code |

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
    },
    syn::visit::Visit,
};

/// The only place allowed to read `Value.content`.
const VALUE_MODULE: &str = "crates/cairn-protocol/src/value.rs";

/// `(file, count, message)`: the `content` field of a message that is not `Value`.
const OTHER_CONTENT: &[(&str, usize, &str)] = &[
    (
        "crates/cairn-server/src/turn/sdk.rs",
        6,
        "ArtifactPutCommandMessage.content",
    ),
    (
        "crates/cairn-query/src/sql_check.rs",
        1,
        "sqlparser Parens<Vec<Expr>>.content (VALUES rows, row_exprs)",
    ),
];

fn test_only(attrs: &[syn::Attribute]) -> bool {
    fn idents(ts: proc_macro2::TokenStream, out: &mut Vec<String>) {
        for t in ts {
            match t {
                proc_macro2::TokenTree::Ident(i) => out.push(i.to_string()),
                proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
                _ => {}
            }
        }
    }
    attrs.iter().any(|a| {
        let p = a.path();
        if p.segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        if !p.is_ident("cfg") {
            return false;
        }
        let mut words = Vec::new();
        if let syn::Meta::List(l) = &a.meta {
            idents(l.tokens.clone(), &mut words);
        }
        words.iter().any(|w| w == "test") && !words.iter().any(|w| w == "not")
    })
}

#[derive(Default)]
struct Scan {
    hits: usize,
}

impl Scan {
    fn tokens(&mut self, ts: proc_macro2::TokenStream) {
        let mut prev_dot = false;
        for t in ts {
            match t {
                proc_macro2::TokenTree::Punct(p) => {
                    prev_dot = p.as_char() == '.';
                    continue;
                }
                proc_macro2::TokenTree::Ident(i) if prev_dot && i == "content" => {
                    self.hits += 1;
                }
                proc_macro2::TokenTree::Group(g) => self.tokens(g.stream()),
                _ => {}
            }
            prev_dot = false;
        }
    }
}

macro_rules! skip_tests {
    ($($method:ident: $ty:ty),* $(,)?) => {$(
        fn $method(&mut self, i: &'ast $ty) {
            if !test_only(&i.attrs) {
                syn::visit::$method(self, i);
            }
        }
    )*};
}

impl<'ast> Visit<'ast> for Scan {
    skip_tests!(
        visit_item_fn: syn::ItemFn,
        visit_item_mod: syn::ItemMod,
        visit_item_impl: syn::ItemImpl,
        visit_item_const: syn::ItemConst,
        visit_item_static: syn::ItemStatic,
        visit_impl_item_fn: syn::ImplItemFn,
    );

    fn visit_expr_field(&mut self, f: &'ast syn::ExprField) {
        if let syn::Member::Named(n) = &f.member {
            if n == "content" {
                self.hits += 1;
            }
        }
        syn::visit::visit_expr_field(self, f);
    }

    fn visit_field_value(&mut self, f: &'ast syn::FieldValue) {
        if let syn::Member::Named(n) = &f.member {
            if n == "content" {
                self.hits += 1;
            }
        }
        syn::visit::visit_field_value(self, f);
    }

    fn visit_field_pat(&mut self, f: &'ast syn::FieldPat) {
        if let syn::Member::Named(n) = &f.member {
            if n == "content" {
                self.hits += 1;
            }
        }
        syn::visit::visit_field_pat(self, f);
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        self.tokens(m.tokens.clone());
    }
}

fn scan_source(src: &str) -> usize {
    let file = syn::parse_file(src).expect("valid Rust source");
    let mut s = Scan::default();
    s.visit_file(&file);
    s.hits
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn rust_files(p: &Path, out: &mut Vec<PathBuf>) {
    if p.is_file() {
        if p.extension().is_some_and(|e| e == "rs") {
            out.push(p.to_path_buf());
        }
        return;
    }
    let mut entries: Vec<_> = std::fs::read_dir(p)
        .unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for e in entries {
        // `tests.rs` / `tests/` under `src/` are test code.
        if e.file_stem().is_some_and(|s| s == "tests") {
            continue;
        }
        rust_files(&e, out);
    }
}

#[test]
fn value_content_goes_through_logical() {
    let root = workspace();
    let mut crates: Vec<_> = std::fs::read_dir(root.join("crates"))
        .unwrap()
        .map(|e| e.unwrap().path().join("src"))
        .filter(|p| p.is_dir())
        .collect();
    crates.sort();
    let mut files = Vec::new();
    for c in &crates {
        rust_files(c, &mut files);
    }
    let allowed: BTreeMap<&str, usize> = OTHER_CONTENT.iter().map(|(f, n, _)| (*f, *n)).collect();
    let mut bad = Vec::new();
    for f in &files {
        let rel = f
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == VALUE_MODULE {
            continue;
        }
        let src = std::fs::read_to_string(f).unwrap();
        let hits = scan_source(&src);
        let want = allowed.get(rel.as_str()).copied().unwrap_or(0);
        if hits != want {
            // Location hint: text lines containing `content` (the scanner counts on
            // the syntax tree).
            let lines: Vec<usize> = src
                .lines()
                .enumerate()
                .filter(|(_, l)| l.contains("content"))
                .map(|(i, _)| i + 1)
                .collect();
            bad.push(format!(
                "{rel}: {hits} `.content` uses (allowed {want}); lines with the word `content`: {lines:?}"
            ));
        }
    }
    assert!(
        bad.is_empty(),
        "direct read of `Value.content` — go through `cairn_protocol::value::Logical` \
         (or edit OTHER_CONTENT if it is another message):\n{}",
        bad.join("\n")
    );
}

#[test]
fn scanner_finds_field_access_macros_and_literals() {
    let src = r#"
        fn f(v: pb::Value) -> Bytes { let pb::Value { content, .. } = v; content }
        fn g(v: &pb::Value) { tracing::debug!(n = v.content.len()); }
        fn h(b: Bytes) -> pb::Value { pb::Value { content: b } }
        fn i(v: &pb::Value) -> usize { v.content.len() }
    "#;
    assert_eq!(scan_source(src), 4);
}

#[test]
fn scanner_skips_test_code() {
    let src = r#"
        #[cfg(test)]
        mod tests { fn g(v: &pb::Value) -> usize { v.content.len() } }
        #[test]
        fn t() { let _ = pb::Value { content: Bytes::new() }; }
        fn content() {}
        fn ok(x: &X) -> usize { x.contents.len() }
    "#;
    assert_eq!(scan_source(src), 0);
}
