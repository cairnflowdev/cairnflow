// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Text users see at runtime is English** (the English public docs quote it
//! verbatim): HTTP error bodies, `cairnflow --help` and the startup lines, the output
//! of `cairn-ctl`, metric HELP text.
//!
//! A source rule, not a running test: not every HTTP error path can be rebuilt in a
//! test, whereas literals can all be counted. The test parses the files in [`SCOPE`]
//! with `syn` and fails when a **string literal** carrying Vietnamese text appears in:
//!
//! - expressions and macro bodies (`format!`, `json!`, `println!`, `tracing` logs
//!   too — operators read these files' logs together with the HTTP errors);
//! - `#[error("…")]` attributes (`thiserror`: that is the error's `Display`).
//!
//! Not counted: comments and doc comments (not user-facing), other attributes
//! (`#[allow(reason = …)]`), items marked `#[cfg(test)]` / `#[test]`, `tests.rs` files
//! and `tests/` directories.
//!
//! "Vietnamese text" = an accented Latin letter (U+00C0–U+024F, U+1EA0–U+1EF9) or a
//! combining mark (U+0300–U+036F), as in `cli_text.rs`, except `×`/`÷`. Arrows, `…`,
//! `—`, `≤` are fine.
//!
//! | Action | Must see |
//! |---|---|
//! | every file of [`SCOPE`] | no Vietnamese literal outside comments and test code |
//! | every file of [`ENGINE_SCOPE`] | same as above |
//! | sample source (`scanner_*`) | the scanner catches literals in expressions, macros, `#[error]`; skips comments, `#[cfg(test)]`, `#[test]`, `#[allow(reason)]` |
//!
//! [`ENGINE_SCOPE`] covers the engine side: turn and engine error messages that can
//! reach a tenant (HTTP bodies, an invocation's `lastFailure`, failure messages
//! recorded in the journal) — `turn/`, `cairn-engine`, `cairn-runtime`,
//! `cairn-lifecycle`, `actor`/`actor_types.rs`, `invoker.rs`.
//!
//! Out of scope (later work, not guarded by this test): `cairn-store` error messages
//! outside `error.rs`/`scope.rs`, the query table descriptions of `cairn-query`
//! (`view.rs`, `keyed.rs` — the error messages to the asker there were translated by
//! hand), the logs of other files.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    std::path::{Path, PathBuf},
    syn::visit::Visit,
};

/// Files (or directories, scanned recursively) relative to the workspace root.
const SCOPE: &[&str] = &[
    // The HTTP error bodies of ingress/admin and the places that build their text.
    "crates/cairn-server/src/ingress",
    "crates/cairn-server/src/query",
    "crates/cairn-server/src/auth.rs",
    "crates/cairn-server/src/auth",
    "crates/cairn-server/src/capture.rs",
    "crates/cairn-server/src/replay.rs",
    "crates/cairn-server/src/discovery.rs",
    "crates/cairn-server/src/egress.rs",
    "crates/cairn-server/src/schedules.rs",
    "crates/cairn-server/src/ui.rs",
    "crates/cairn-store/src/scope.rs",
    "crates/cairn-store/src/error.rs",
    // `cairnflow --help`, the startup lines, store refusals at startup.
    "crates/cairn-server/src/main.rs",
    "crates/cairn-server/src/store_check.rs",
    "crates/cairn-server/src/node/setup.rs",
    "crates/cairn-server/src/autosplit.rs",
    // CLIs: `cairn` (already English, kept so it does not drift) and `cairn-ctl`.
    "crates/cairn-server/src/bin",
    "crates/cairn-server/src/ctl.rs",
    "crates/cairn-server/src/ctl",
    // Metric HELP text.
    "crates/cairn-server/src/metrics.rs",
    "crates/cairn-server/src/l0.rs",
    "crates/cairn-server/src/store_profile.rs",
];

/// The engine side: turn errors, engine errors and failure messages reaching a tenant
/// through an HTTP body, `lastFailure` or the journal.
const ENGINE_SCOPE: &[&str] = &[
    "crates/cairn-server/src/turn",
    "crates/cairn-server/src/actor_types.rs",
    "crates/cairn-server/src/actor",
    "crates/cairn-server/src/invoker.rs",
    "crates/cairn-engine/src",
    "crates/cairn-runtime/src",
    "crates/cairn-lifecycle/src",
    // `ProtocolError` ⇒ `violation()` ⇒ failure 571 ⇒ `lastFailure`; `SpecError`
    // ⇒ the `400` body of `PUT /schedules`.
    "crates/cairn-protocol/src",
    "crates/cairn-schedule/src",
];

fn vietnamese(s: &str) -> Option<char> {
    s.chars().find(|c| {
        // `×`, `÷` sit inside the Latin-1 range but are math signs ("2× the median").
        (('\u{00C0}'..='\u{024F}').contains(c) && !matches!(c, '×' | '÷'))
            || ('\u{1EA0}'..='\u{1EF9}').contains(c)
            || ('\u{0300}'..='\u{036F}').contains(c)
    })
}

/// Items that exist only in test builds: `#[test]`, `#[tokio::test]`, `#[cfg(test)]`,
/// `#[cfg(any(test, …))]` — but not `#[cfg(not(test))]`.
fn test_only(attrs: &[syn::Attribute]) -> bool {
    fn idents(ts: proc_macro2::TokenStream, out: &mut Vec<String>) {
        for t in ts {
            match t {
                proc_macro2::TokenTree::Group(g) => idents(g.stream(), out),
                proc_macro2::TokenTree::Ident(i) => out.push(i.to_string()),
                _ => {}
            }
        }
    }
    attrs.iter().any(|a| {
        if a.path().segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        let syn::Meta::List(l) = &a.meta else {
            return false;
        };
        if !a.path().is_ident("cfg") {
            return false;
        }
        let mut ids = Vec::new();
        idents(l.tokens.clone(), &mut ids);
        ids.iter().any(|i| i == "test") && !ids.iter().any(|i| i == "not")
    })
}

#[derive(Default)]
struct Scan {
    hits: Vec<String>,
}

impl Scan {
    fn check(&mut self, s: &str) {
        if let Some(c) = vietnamese(s) {
            let mut short: String = s.chars().take(100).collect();
            if short.len() < s.len() {
                short.push('…');
            }
            self.hits.push(format!("{c:?} in {short:?}"));
        }
    }

    fn tokens(&mut self, ts: proc_macro2::TokenStream) {
        for t in ts {
            match t {
                proc_macro2::TokenTree::Group(g) => self.tokens(g.stream()),
                proc_macro2::TokenTree::Literal(l) => {
                    if let syn::Lit::Str(s) = syn::Lit::new(l) {
                        self.check(&s.value());
                    }
                }
                _ => {}
            }
        }
    }
}

macro_rules! skip_test_items {
    ($($method:ident: $ty:ty => $default:ident;)*) => {$(
        fn $method(&mut self, i: &'ast $ty) {
            if !test_only(&i.attrs) {
                syn::visit::$default(self, i);
            }
        }
    )*};
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_attribute(&mut self, a: &'ast syn::Attribute) {
        // thiserror's `#[error("…")]` is the error's `Display`; every other attribute
        // (doc comments, `allow(reason)`, `serde(rename)`) is not text for users.
        if a.path().is_ident("error") {
            if let syn::Meta::List(l) = &a.meta {
                self.tokens(l.tokens.clone());
            }
        }
    }

    fn visit_lit_str(&mut self, s: &'ast syn::LitStr) {
        self.check(&s.value());
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        self.tokens(m.tokens.clone());
    }

    skip_test_items! {
        visit_item_fn: syn::ItemFn => visit_item_fn;
        visit_item_mod: syn::ItemMod => visit_item_mod;
        visit_item_impl: syn::ItemImpl => visit_item_impl;
        visit_item_const: syn::ItemConst => visit_item_const;
        visit_item_static: syn::ItemStatic => visit_item_static;
        visit_item_struct: syn::ItemStruct => visit_item_struct;
        visit_item_enum: syn::ItemEnum => visit_item_enum;
        visit_item_macro: syn::ItemMacro => visit_item_macro;
        visit_item_use: syn::ItemUse => visit_item_use;
        visit_impl_item_fn: syn::ImplItemFn => visit_impl_item_fn;
        visit_impl_item_const: syn::ImplItemConst => visit_impl_item_const;
        visit_expr_closure: syn::ExprClosure => visit_expr_closure;
        visit_local: syn::Local => visit_local;
        visit_variant: syn::Variant => visit_variant;
        visit_arm: syn::Arm => visit_arm;
    }
}

fn scan_source(src: &str) -> Vec<String> {
    let file = syn::parse_file(src).expect("parse source");
    let mut s = Scan::default();
    s.visit_file(&file);
    s.hits
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn rust_files(p: &Path, out: &mut Vec<PathBuf>) {
    if p.is_dir() {
        if p.file_name().is_some_and(|n| n == "tests") {
            return;
        }
        let mut entries: Vec<_> = std::fs::read_dir(p)
            .expect("read directory")
            .map(|e| e.expect("entry").path())
            .collect();
        entries.sort();
        for e in entries {
            rust_files(&e, out);
        }
    } else if p.extension().is_some_and(|e| e == "rs") && !p.ends_with("tests.rs") {
        out.push(p.to_path_buf());
    }
}

#[test]
fn runtime_text_in_scope_is_english() {
    assert_english(SCOPE);
}

/// The error messages of a `/query` to the asker (found in a security review): in
/// child-process mode the child's deadline fires before the node's kill bound, so
/// `session.rs`'s deadline message is a common one. `view.rs`, `keyed.rs` carry the
/// catalog's column descriptions — the error messages to the asker there are
/// translated by hand, not guarded by this rule.
const QUERY_SCOPE: &[&str] = &[
    "crates/cairn-query/src/session.rs",
    "crates/cairn-query/src/ctx.rs",
    "crates/cairn-query/src/worker.rs",
];

#[test]
fn query_text_in_scope_is_english() {
    assert_english(QUERY_SCOPE);
}

#[test]
fn engine_text_in_scope_is_english() {
    assert_english(ENGINE_SCOPE);
}

fn assert_english(scope: &[&str]) {
    let root = workspace();
    let mut files = Vec::new();
    for s in scope {
        let p = root.join(s);
        assert!(p.exists(), "SCOPE points at {s}, which does not exist");
        rust_files(&p, &mut files);
    }
    let mut report = String::new();
    let mut total = 0;
    for f in &files {
        let src = std::fs::read_to_string(f).expect("read source");
        let hits = scan_source(&src);
        if hits.is_empty() {
            continue;
        }
        total += hits.len();
        let rel = f.strip_prefix(&root).unwrap_or(f).display().to_string();
        report.push_str(&format!("\n{rel}: {} literal\n", hits.len()));
        for h in hits {
            report.push_str(&format!("  {h}\n"));
        }
    }
    assert!(
        total == 0,
        "{total} user-facing Vietnamese string literals:{report}"
    );
}

#[test]
fn scanner_finds_literals_in_code_macros_and_error_attrs() {
    let src = r##"
        #[derive(thiserror::Error, Debug)]
        enum E { #[error("lỗi một")] A }
        fn f() -> String { let _ = "hai đây"; format!("bà {}", json!({"k": "bốn"})) }
        const C: &str = "năm";
    "##;
    let hits = scan_source(src);
    assert_eq!(hits.len(), 5, "{hits:#?}");
}

#[test]
fn scanner_skips_comments_tests_and_other_attributes() {
    let src = r##"
        //! doc tiếng Việt
        /// chú thích "trong ngoặc"
        #[allow(clippy::x, reason = "lý do")]
        fn f() -> &'static str { /* "chú thích" */ "english" }
        #[cfg(test)]
        mod tests { fn g() { let _ = "tiếng Việt"; } }
        #[test]
        fn t() { assert_eq!(1, 1, "thông điệp"); }
        #[cfg(any(test, feature = "testkit"))]
        fn h() -> &'static str { "chỉ test" }
    "##;
    let hits = scan_source(src);
    assert!(hits.is_empty(), "{hits:#?}");
}
