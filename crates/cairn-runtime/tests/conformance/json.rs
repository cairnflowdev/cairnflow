// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! JSON with a fixed key order — enough for conformance traces, without pulling
//! in serde.
//!
//! Traces are files committed to the repository and the drift check compares
//! them **character by character**, so the key order must be the order we
//! write (`serde_json` sorts alphabetically by default, and enabling
//! `preserve_order` changes it for every crate sharing it in the workspace).

pub enum J {
    Bool(bool),
    Num(i128),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(&'static str, J)>),
}

impl J {
    pub fn str(s: impl Into<String>) -> Self {
        J::Str(s.into())
    }

    pub fn num(n: impl Into<i128>) -> Self {
        J::Num(n.into())
    }

    fn scalar(&self) -> bool {
        !matches!(self, J::Arr(_) | J::Obj(_))
    }

    /// Pretty printing: nested objects/arrays go on new lines, but an object
    /// **containing only scalars** (one frame, one journal entry) stays on one
    /// line — one line per frame is what `git diff` can read.
    pub fn pretty(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, depth: usize) {
        match self {
            J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            J::Num(n) => out.push_str(&n.to_string()),
            J::Str(s) => escape(s, out),
            J::Arr(items) if items.is_empty() => out.push_str("[]"),
            J::Arr(items) if items.iter().all(J::scalar) => {
                out.push('[');
                for (i, it) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    it.write(out, depth);
                }
                out.push(']');
            }
            J::Arr(items) => {
                out.push_str("[\n");
                for (i, it) in items.iter().enumerate() {
                    indent(out, depth + 1);
                    it.write(out, depth + 1);
                    if i + 1 < items.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                indent(out, depth);
                out.push(']');
            }
            J::Obj(fields) if fields.is_empty() => out.push_str("{}"),
            J::Obj(fields) if fields.iter().all(|(_, v)| v.scalar()) && depth > 0 => {
                out.push_str("{ ");
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    escape(k, out);
                    out.push_str(": ");
                    v.write(out, depth);
                }
                out.push_str(" }");
            }
            J::Obj(fields) => {
                out.push_str("{\n");
                for (i, (k, v)) in fields.iter().enumerate() {
                    indent(out, depth + 1);
                    escape(k, out);
                    out.push_str(": ");
                    v.write(out, depth + 1);
                    if i + 1 < fields.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                indent(out, depth);
                out.push('}');
            }
        }
    }
}

fn indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("  ");
    }
}

fn escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
