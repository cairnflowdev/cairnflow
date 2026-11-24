// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The Cairnflow web console, embedded into the binary.
//!
//! `build.rs` picks the embedded folder: `ui/dist` if it has been built,
//! otherwise the placeholder page ([`built`] returns `false`). This crate knows
//! nothing about the node's auth or router: [`Ui::respond`] takes
//! `method path headers` and returns `None` when the request is not the UI's —
//! `cairn-server` mounts it as the outermost layer of the ingress port.
//!
//! Serving rules (under the base path, default `/ui`; `GET`/`HEAD` only):
//!
//! | Request | Response |
//! |---|---|
//! | `GET /` with `Accept: text/html` | `302 <base>/` |
//! | `GET <base>` with `text/html` | `302 <base>/` |
//! | existing file | the file; `assets/*` `immutable` for a year, the rest `no-cache`; `ETag` + `If-None-Match` ⇒ `304`; prebuilt `.br`/`.gz` variant by `Accept-Encoding`, with `Vary` |
//! | missing `assets/*` | `404` |
//! | other path with `text/html` | the rewritten `index.html`, `no-store` |
//! | other path without `text/html` | `404` |
//!
//! Every response carries a nonce-free CSP ([`CSP`]) and three protective headers.
//!
//! `index.html` is rewritten **once**, when the [`Ui`] is built: `"./` ⇒
//! `"<base>/` (Vite builds with `base: './'`), and `<meta name="cairn-base">`
//! and `cairn-version` are inserted. The base path comes only from
//! configuration, never from a header (`X-Forwarded-Prefix`): that value goes
//! into HTML.

use {
    axum::{
        body::{Body, Bytes},
        http::{header, HeaderMap, HeaderValue, Method, Response, StatusCode},
    },
    sha2::Digest,
    std::{borrow::Cow, collections::BTreeMap},
};

#[derive(rust_embed::RustEmbed)]
#[folder = "$CAIRN_UI_FOLDER"]
struct Dist;

/// `true` when the binary embeds the real `ui/dist`, `false` for the placeholder page.
pub fn built() -> bool {
    env!("CAIRN_UI_BUILT") == "true"
}

/// CSP of every UI response. No inline script; `style-src-attr` allows the
/// `style=` attribute (React, floating-ui), `<style>` elements stay blocked.
pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; \
     style-src-attr 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; \
     font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";

/// Checks base path syntax: `^(/[A-Za-z0-9._~-]+)+$`, no `.`/`..` segment.
/// The base path goes into HTML and into `Location`, so only a character set
/// that needs no escaping in either place is accepted.
pub fn check_base_path(s: &str) -> Result<(), String> {
    let ok = s.len() > 1
        && s.starts_with('/')
        && s[1..].split('/').all(|seg| {
            !seg.is_empty()
                && seg != "."
                && seg != ".."
                && seg
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b))
        });
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid UI base path {s:?} — expected /a or /a/b, each segment only A-Z a-z 0-9 . _ ~ -"
        ))
    }
}

/// A servable file.
struct Asset {
    data: Cow<'static, [u8]>,
    etag: HeaderValue,
    mime: String,
}

/// A node's UI: base path, rewritten `index.html`, file table.
pub struct Ui {
    base: String,
    index: Bytes,
    files: BTreeMap<String, Asset>,
}

impl std::fmt::Debug for Ui {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ui")
            .field("base", &self.base)
            .field("files", &self.files.len())
            .finish()
    }
}

impl Ui {
    /// UI from the embedded build.
    pub fn new(base: &str, version: &str) -> Result<Self, String> {
        let files = Dist::iter()
            .filter_map(|name| {
                let f = Dist::get(&name)?;
                Some((
                    name.to_string(),
                    Asset {
                        etag: etag(&f.metadata.sha256_hash()),
                        mime: mime_of(&name),
                        data: f.data,
                    },
                ))
            })
            .collect();
        Self::assemble(base, version, files)
    }

    /// UI from a given file set (`relative name`, bytes) — for tests, to check
    /// assets, compression and caching regardless of whether `ui/dist` was built.
    pub fn from_files<'a>(
        base: &str,
        version: &str,
        files: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    ) -> Result<Self, String> {
        let files = files
            .into_iter()
            .map(|(name, data)| {
                let sha: [u8; 32] = sha2::Sha256::digest(data).into();
                (
                    name.to_string(),
                    Asset {
                        etag: etag(&sha),
                        mime: mime_of(name),
                        data: Cow::Owned(data.to_vec()),
                    },
                )
            })
            .collect();
        Self::assemble(base, version, files)
    }

    fn assemble(base: &str, version: &str, files: BTreeMap<String, Asset>) -> Result<Self, String> {
        check_base_path(base)?;
        let index = files.get("index.html").ok_or("UI has no index.html")?;
        let html = std::str::from_utf8(&index.data)
            .map_err(|e| format!("UI index.html is not UTF-8: {e}"))?;
        Ok(Self {
            index: Bytes::from(rewrite_index(html, base, version)),
            base: base.to_string(),
            files,
        })
    }

    /// The checked base path, without a trailing `/`.
    pub fn base_path(&self) -> &str {
        &self.base
    }

    /// Response for `method path`, or `None` when the request is not the UI's
    /// (the caller passes it on to the API).
    pub fn respond(
        &self,
        method: &Method,
        path: &str,
        headers: &HeaderMap,
    ) -> Option<Response<Body>> {
        if method != Method::GET && method != Method::HEAD {
            return None;
        }
        let html = accepts_html(headers);
        let root = format!("{}/", self.base);
        if path == "/" {
            return html.then(|| redirect(&root));
        }
        let rest = path.strip_prefix(&self.base)?;
        if rest.is_empty() {
            return Some(if html { redirect(&root) } else { not_found() });
        }
        let rel = rest.strip_prefix('/')?;
        Some(self.serve(rel, headers, html))
    }

    fn serve(&self, rel: &str, headers: &HeaderMap, html: bool) -> Response<Body> {
        if rel == "index.html" {
            return self.index();
        }
        if let Some(asset) = self.files.get(rel) {
            return self.file(rel, asset, headers);
        }
        if html && !rel.starts_with("assets/") {
            return self.index();
        }
        not_found()
    }

    fn index(&self) -> Response<Body> {
        let mut r = Response::new(Body::from(self.index.clone()));
        let h = r.headers_mut();
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        secure(r)
    }

    fn file(&self, rel: &str, asset: &Asset, headers: &HeaderMap) -> Response<Body> {
        // Prebuilt compressed variant: br before gzip. `Vary` whenever any
        // variant exists — intermediate caches must split by `Accept-Encoding`
        // even when this response is the original.
        let mut chosen: Option<(&'static str, &Asset)> = None;
        let mut varies = false;
        for (enc, ext) in [("br", "br"), ("gzip", "gz")] {
            if let Some(v) = self.files.get(&format!("{rel}.{ext}")) {
                varies = true;
                if chosen.is_none() && accepts_encoding(headers, enc) {
                    chosen = Some((enc, v));
                }
            }
        }
        let body = chosen.map_or(asset, |(_, v)| v);
        let fresh = if_none_match(headers, &body.etag);
        let mut r = if fresh {
            let mut r = Response::new(Body::empty());
            *r.status_mut() = StatusCode::NOT_MODIFIED;
            r
        } else {
            Response::new(Body::from(body.data.clone()))
        };
        let h = r.headers_mut();
        if !fresh {
            if let Ok(v) = HeaderValue::from_str(&asset.mime) {
                h.insert(header::CONTENT_TYPE, v);
            }
            if let Some((enc, _)) = chosen {
                h.insert(header::CONTENT_ENCODING, HeaderValue::from_static(enc));
            }
        }
        h.insert(header::ETAG, body.etag.clone());
        h.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(if rel.starts_with("assets/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            }),
        );
        if varies {
            h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
        }
        secure(r)
    }
}

fn mime_of(name: &str) -> String {
    mime_guess::from_path(name)
        .first_or_octet_stream()
        .essence_str()
        .to_string()
}

fn etag(sha: &[u8; 32]) -> HeaderValue {
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    HeaderValue::from_str(&format!("\"{hex}\"")).expect("hex is always a valid header")
}

/// `"./` ⇒ `"<base>/`, then inserts two meta tags before `</head>`.
fn rewrite_index(html: &str, base: &str, version: &str) -> String {
    let html = html.replace("\"./", &format!("\"{base}/"));
    let meta = format!(
        "<meta name=\"cairn-base\" content=\"{}\"><meta name=\"cairn-version\" content=\"{}\">",
        attr(base),
        attr(version)
    );
    match html.find("</head>") {
        Some(i) => format!("{}{meta}{}", &html[..i], &html[i..]),
        None => format!("{meta}{html}"),
    }
}

/// Escapes an HTML attribute value.
fn attr(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn accepts_html(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.contains("text/html"))
}

/// Whether `enc` is in `Accept-Encoding` with a non-zero `q`.
fn accepts_encoding(headers: &HeaderMap, enc: &str) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|item| {
            let mut parts = item.split(';');
            let name = parts.next().unwrap_or("").trim();
            let refused = parts.any(|p| {
                p.trim()
                    .strip_prefix("q=")
                    .and_then(|q| q.trim().parse::<f32>().ok())
                    == Some(0.0)
            });
            name.eq_ignore_ascii_case(enc) && !refused
        })
}

/// Whether `If-None-Match` matches `etag` (weak comparison: drop `W/`; `*` matches everything).
fn if_none_match(headers: &HeaderMap, etag: &HeaderValue) -> bool {
    let want = etag.as_bytes();
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim())
        .any(|t| t == "*" || t.trim_start_matches("W/").as_bytes() == want)
}

fn redirect(to: &str) -> Response<Body> {
    let mut r = Response::new(Body::empty());
    *r.status_mut() = StatusCode::FOUND;
    if let Ok(v) = HeaderValue::from_str(to) {
        r.headers_mut().insert(header::LOCATION, v);
    }
    secure(r)
}

fn not_found() -> Response<Body> {
    let mut r = Response::new(Body::from("not found\n"));
    *r.status_mut() = StatusCode::NOT_FOUND;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    secure(r)
}

/// CSP and three protective headers, on every UI response.
fn secure(mut r: Response<Body>) -> Response<Body> {
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    h.insert(
        "cross-origin-opener-policy",
        HeaderValue::from_static("same-origin"),
    );
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_path_syntax() {
        for ok in ["/ui", "/a/b", "/console-2", "/x.y_z~"] {
            assert!(check_base_path(ok).is_ok(), "{ok}");
        }
        for bad in [
            "", "/", "ui", "/ui/", "//ui", "/u i", "/ui?x", "/ui#a", "/a/../b", "/.", "/ui\"",
            "/\u{fc}",
        ] {
            assert!(check_base_path(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn index_is_rewritten_once() {
        let html = r##"<html><head><script type="module" src="./assets/a.js"></script><link href="./assets/a.css"></head><body><a href="#main">x</a></body></html>"##;
        let out = rewrite_index(html, "/console", "1.2.3");
        assert!(out.contains(r#"src="/console/assets/a.js""#), "{out}");
        assert!(out.contains(r#"href="/console/assets/a.css""#), "{out}");
        assert!(out.contains(r##"href="#main""##), "{out}");
        assert!(
            out.contains(r#"<meta name="cairn-base" content="/console"><meta name="cairn-version" content="1.2.3"></head>"#),
            "{out}"
        );
        assert!(!out.contains("<base"), "{out}");
    }

    #[test]
    fn accept_encoding_honours_q_zero() {
        let h = |v: &str| {
            let mut m = HeaderMap::new();
            m.insert(header::ACCEPT_ENCODING, HeaderValue::from_str(v).unwrap());
            m
        };
        assert!(accepts_encoding(&h("gzip, deflate, br"), "br"));
        assert!(accepts_encoding(&h("gzip;q=0.5, BR;q=1"), "br"));
        assert!(!accepts_encoding(&h("br;q=0, gzip"), "br"));
        assert!(accepts_encoding(&h("br;q=0, gzip"), "gzip"));
        assert!(!accepts_encoding(&h("identity"), "gzip"));
    }

    /// The embedded folder `build.rs` picks must be a path **relative** to the
    /// crate: rust-embed joins it with the `CARGO_MANIFEST_DIR` of the
    /// compilation. An absolute path is cached by cargo along with the build
    /// script output — moving the repo, or two worktrees sharing `target/`
    /// (cargo hashes workspace crates by relative path), leaves `RustEmbed`
    /// pointing at a folder that no longer exists (seen as `folder
    /// '…/<other worktree>/crates/cairn-ui/placeholder' does not exist`).
    #[test]
    fn embed_folder_is_relative() {
        let folder = env!("CAIRN_UI_FOLDER");
        assert!(
            std::path::Path::new(folder).is_relative(),
            "absolute CAIRN_UI_FOLDER: {folder}"
        );
    }

    #[test]
    fn embedded_dist_has_index() {
        let ui = Ui::new("/ui", "0.0.0").expect("the embedded build must have index.html");
        assert!(std::str::from_utf8(&ui.index)
            .unwrap()
            .contains(r#"<meta name="cairn-base" content="/ui">"#));
    }
}
