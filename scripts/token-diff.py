#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Compares the token multiset of a group of Rust files between two revisions.

Used to prove that splitting a file is a "pure move":

    scripts/token-diff.py <rev_before> <rev_after> <path>...

`rev` is a commit (`HEAD~1`, a hash) or `WORKTREE` (the working tree). Each path is a
file or a directory; every `.rs` under it (at that revision) is collected. Example:

    scripts/token-diff.py HEAD~1 HEAD crates/cairn-server/src/flow.rs crates/cairn-server/src/flow

Left out of the code comparison:
- `use …;` declarations (at every level) — printed separately as a normalised text set;
- `mod x;` declarations — printed separately;
- visibility keywords (`pub`, `pub(crate)`, `pub(super)`, `pub(in …)`) — counts printed separately;
- a trailing comma right before `)`/`]`/`}` (rustfmt adds/removes it with line length).
Comments (`//`, `///`, `//!`, `/* */`) are compared separately, per comment line (leading
and trailing whitespace stripped). Every remaining difference is printed; a "pure move"
means the code differs only in the tokens of the module frame (`mod x {`, the old
module's `}`, the `impl` header of the new block).
"""

import collections
import os
import re
import subprocess
import sys


def files_at(rev, paths):
    out = []
    for p in paths:
        if rev == "WORKTREE":
            if os.path.isdir(p):
                for root, _, names in os.walk(p):
                    out += [os.path.join(root, n) for n in names if n.endswith(".rs")]
            elif os.path.exists(p):
                out.append(p)
        else:
            r = subprocess.run(
                ["git", "ls-tree", "-r", "--name-only", rev, "--", p],
                capture_output=True, text=True, check=True,
            )
            out += [f for f in r.stdout.split() if f.endswith(".rs")]
    return sorted(set(out))


def read(rev, f):
    if rev == "WORKTREE":
        with open(f, encoding="utf-8") as h:
            return h.read()
    return subprocess.run(
        ["git", "show", f"{rev}:{f}"], capture_output=True, text=True, check=True
    ).stdout


IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[0-9][A-Za-z0-9_]*")


def lex(src):
    """Returns (tokens, comments). Tokens are strings; literals are kept verbatim."""
    toks, comments = [], []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
        elif src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            comments.append(src[i:j].strip())
            i = j
        elif src.startswith("/*", i):
            depth, j = 1, i + 2
            while depth and j < n:
                if src.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif src.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            for line in src[i:j].splitlines():
                comments.append(line.strip())
            i = j
        else:
            m = re.match(r'(b?r)(#*)"', src[i:])
            if m:
                close = '"' + m.group(2)
                j = src.find(close, i + m.end())
                j = j + len(close)
                toks.append(src[i:j])
                i = j
                continue
            if c == '"' or src.startswith('b"', i):
                j = i + (2 if c == "b" else 1)
                while src[j] != '"':
                    j += 2 if src[j] == "\\" else 1
                toks.append(src[i : j + 1])
                i = j + 1
                continue
            if c == "'" or src.startswith("b'", i):
                k = i + (2 if c == "b" else 1)
                if src[k] == "\\":
                    j = src.find("'", k + 2)
                    toks.append(src[i : j + 1])
                    i = j + 1
                    continue
                if k + 1 < n and src[k + 1] == "'":
                    toks.append(src[i : k + 2])
                    i = k + 2
                    continue
                m = IDENT.match(src, k)  # lifetime
                toks.append("'" + m.group(0))
                i = m.end()
                continue
            m = IDENT.match(src, i)
            if m:
                toks.append(m.group(0))
                i = m.end()
            else:
                toks.append(c)
                i += 1
    return toks, comments


def strip(toks):
    """Splits `use`, `mod x;` and visibility off the token stream."""
    code, uses, mods, vis = [], [], [], collections.Counter()
    i = 0
    while i < len(toks):
        t = toks[i]
        if t == "pub":
            if i + 1 < len(toks) and toks[i + 1] == "(":
                j = toks.index(")", i)
                vis["pub(" + " ".join(toks[i + 2 : j]) + ")"] += 1
                i = j + 1
            else:
                vis["pub"] += 1
                i += 1
            continue
        if t == "use" and (not code or code[-1] in (";", "{", "}", "]")):
            j = i
            depth = 0
            while not (toks[j] == ";" and depth == 0):
                depth += {"{": 1, "}": -1}.get(toks[j], 0)
                j += 1
            uses.append(" ".join(toks[i + 1 : j]))
            i = j + 1
            continue
        if t == "mod" and i + 2 < len(toks) and toks[i + 2] == ";":
            mods.append(toks[i + 1])
            i += 3
            continue
        code.append(t)
        i += 1
    # rustfmt adds/removes a trailing comma when a line changes length (e.g. adding
    # `pub(super) ` pushes a signature past 100 columns): a comma right before a
    # closing bracket carries no meaning.
    code = [t for k, t in enumerate(code) if not (t == "," and k + 1 < len(code) and code[k + 1] in ")]}")]
    return code, uses, mods, vis


def collect(rev, paths):
    code, uses, mods, comments = collections.Counter(), [], [], collections.Counter()
    vis = collections.Counter()
    files = files_at(rev, paths)
    for f in files:
        t, c = lex(read(rev, f))
        c2, u, m, v = strip(t)
        code.update(c2)
        uses += u
        mods += m
        vis.update(v)
        comments.update(c)
    return files, code, uses, mods, vis, comments


def show(title, before, after):
    gone = before - after
    new = after - before
    print(f"== {title}: -{sum(gone.values())} +{sum(new.values())}")
    for k, v in sorted(gone.items()):
        print(f"  - {v} × {k}")
    for k, v in sorted(new.items()):
        print(f"  + {v} × {k}")


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    a, b, paths = sys.argv[1], sys.argv[2], sys.argv[3:]
    fa, ca, ua, ma, va, cma = collect(a, paths)
    fb, cb, ub, mb, vb, cmb = collect(b, paths)
    print(f"files {a}: {len(fa)}, {b}: {len(fb)}")
    show("code (no use/mod/visibility)", ca, cb)
    show("comments (per line)", cma, cmb)
    show("use", collections.Counter(ua), collections.Counter(ub))
    show("mod x;", collections.Counter(ma), collections.Counter(mb))
    print("== visibility:", dict(va), "→", dict(vb))


if __name__ == "__main__":
    main()
