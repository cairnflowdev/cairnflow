# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Shared helpers for the publication tooling in `scripts/oss/`.

Python 3.9 standard library only. Everything here works on the *tracked* file
set (`git ls-files -z`), never on a walk of the working tree: the working tree
has untracked files, symlinks and ignored build output that must never reach
an export.

Contents:

- `tracked_files(root)` -- tracked paths with their git mode (regular file,
  executable, symlink, gitlink), read NUL-separated so names with spaces or
  non-ASCII characters survive.
- `glob_to_regex(pattern)` -- path globs used by the manifest and allow lists:
  `**` spans any number of path segments, `*` and `?` stay inside one segment,
  `{a,b}` is alternation.
- `parse_toml(text)` -- a small TOML subset parser (tables, arrays of tables,
  strings, booleans, integers, string arrays). `tomllib` only exists from
  Python 3.11.
- `Manifest` -- loads `scripts/oss/manifest.toml` and classifies a path
  (first matching rule wins) and resolves its license (first matching
  license rule wins).
- `VIETNAMESE_CHAR` -- characters that mark Vietnamese text with diacritics.
- `area_of(path)` -- the reporting bucket of a path (crate, package or
  top-level directory) so per-area counts show translation progress.
"""

import os
import re
import subprocess

# git ls-files -s modes.
MODE_SYMLINK = "120000"
MODE_GITLINK = "160000"

# Latin letters with diacritics (Latin-1 Supplement, Latin Extended-A/B,
# Latin Extended Additional) and combining marks, minus the math signs that
# live in the Latin-1 block. Same definition as the existing `english_text`
# source rule of cairn-server, so the two lints agree.
def _char_range(lo, hi):
    return chr(lo) + "-" + chr(hi)


VIETNAMESE_CHAR = re.compile(
    "["
    + _char_range(0x00C0, 0x00D6)  # Latin-1 letters before the multiplication sign
    + _char_range(0x00D8, 0x00F6)  # ... before the division sign
    + _char_range(0x00F8, 0x024F)  # ... and Latin Extended-A/B
    + _char_range(0x1EA0, 0x1EF9)  # Latin Extended Additional (Vietnamese)
    + _char_range(0x0300, 0x036F)  # combining diacritical marks
    + "]"
)


class TrackedFile:
    __slots__ = ("path", "mode")

    def __init__(self, path, mode):
        self.path = path
        self.mode = mode

    @property
    def is_symlink(self):
        return self.mode == MODE_SYMLINK

    @property
    def is_gitlink(self):
        return self.mode == MODE_GITLINK

    def __repr__(self):
        return "TrackedFile(%r, %r)" % (self.path, self.mode)


def git_root(start="."):
    out = subprocess.run(
        ["git", "-C", start, "rev-parse", "--show-toplevel"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout
    return out.decode("utf-8").strip()


def tracked_files(root, rev=None):
    """Every tracked path of `root` with its mode, sorted by path: the index
    by default, or the tree of commit `rev` (`git ls-tree -r`)."""
    if rev is None:
        cmd = ["git", "-C", root, "ls-files", "-s", "-z"]
    else:
        cmd = ["git", "-C", root, "ls-tree", "-r", "-z", "--full-tree", rev]
    raw = subprocess.run(cmd, check=True, stdout=subprocess.PIPE).stdout
    files = []
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        meta, _, path = entry.partition(b"\t")
        mode = meta.split(b" ")[0].decode("ascii")
        files.append(TrackedFile(path.decode("utf-8", "surrogateescape"), mode))
    # `ls-files -s` lists one row per stage during a conflict; keep the first.
    seen = set()
    unique = []
    for f in files:
        if f.path not in seen:
            seen.add(f.path)
            unique.append(f)
    unique.sort(key=lambda f: f.path)
    return unique


def show(root, rev, path):
    """Content of `path` at commit `rev`."""
    return subprocess.run(
        ["git", "-C", root, "show", "%s:%s" % (rev, path)], check=True, stdout=subprocess.PIPE
    ).stdout.decode("utf-8")


# ─────────────────────────────────────────────────────────────── globs

_GLOB_CACHE = {}


def glob_to_regex(pattern):
    """Compile a path glob to an anchored regex.

    - `**/` matches zero or more whole segments, `/**` at the end matches
      everything below, a lone `**` matches anything;
    - `*` matches within one segment, `?` one character of a segment;
    - `{a,b,c}` is alternation (no nesting);
    - a pattern ending in `/` matches everything below that directory.
    """
    cached = _GLOB_CACHE.get(pattern)
    if cached is not None:
        return cached
    pat = pattern
    if pat.endswith("/"):
        pat += "**"
    out = []
    i = 0
    n = len(pat)
    while i < n:
        c = pat[i]
        if pat.startswith("**/", i):
            out.append("(?:[^/]*/)*")
            i += 3
        elif pat.startswith("/**", i) and i + 3 == n:
            out.append("(?:/.*)?")
            i += 3
        elif pat.startswith("**", i):
            out.append(".*")
            i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c == "{":
            j = pat.index("}", i)
            alts = pat[i + 1 : j].split(",")
            out.append("(?:" + "|".join(re.escape(a) for a in alts) + ")")
            i = j + 1
        else:
            out.append(re.escape(c))
            i += 1
    rx = re.compile("^" + "".join(out) + "$")
    _GLOB_CACHE[pattern] = rx
    return rx


def glob_match(pattern, path):
    return glob_to_regex(pattern).match(path) is not None


# ─────────────────────────────────────────────────────────────── TOML subset


class TomlError(ValueError):
    pass


_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")
_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f"}


class _Reader:
    def __init__(self, text):
        self.s = text
        self.i = 0
        self.line = 1

    def peek(self):
        return self.s[self.i] if self.i < len(self.s) else ""

    def skip_ws(self, newlines=False):
        while self.i < len(self.s):
            c = self.s[self.i]
            if c in " \t\r":
                self.i += 1
            elif c == "\n" and newlines:
                self.i += 1
                self.line += 1
            elif c == "#":
                while self.i < len(self.s) and self.s[self.i] != "\n":
                    self.i += 1
            else:
                break

    def error(self, msg):
        raise TomlError("line %d: %s" % (self.line, msg))

    def expect(self, ch):
        if self.peek() != ch:
            self.error("expected %r, found %r" % (ch, self.peek()))
        self.i += 1

    def key(self):
        self.skip_ws()
        c = self.peek()
        if c == '"':
            return self.basic_string()
        if c == "'":
            return self.literal_string()
        m = _BARE_KEY.match(self.s, self.i)
        if not m:
            self.error("expected a key")
        self.i = m.end()
        return m.group(0)

    def basic_string(self):
        self.expect('"')
        out = []
        while True:
            if self.i >= len(self.s):
                self.error("unterminated string")
            c = self.s[self.i]
            if c == '"':
                self.i += 1
                return "".join(out)
            if c == "\n":
                self.error("newline in string")
            if c == "\\":
                e = self.s[self.i + 1 : self.i + 2]
                if e in _ESCAPES:
                    out.append(_ESCAPES[e])
                    self.i += 2
                elif e == "u":
                    out.append(chr(int(self.s[self.i + 2 : self.i + 6], 16)))
                    self.i += 6
                elif e == "U":
                    out.append(chr(int(self.s[self.i + 2 : self.i + 10], 16)))
                    self.i += 10
                else:
                    self.error("bad escape \\%s" % e)
                continue
            out.append(c)
            self.i += 1

    def literal_string(self):
        if self.s.startswith("'''", self.i):
            j = self.s.find("'''", self.i + 3)
            if j < 0:
                self.error("unterminated multi-line literal string")
            v = self.s[self.i + 3 : j]
            self.line += v.count("\n")
            self.i = j + 3
            return v[1:] if v.startswith("\n") else v
        self.expect("'")
        j = self.s.find("'", self.i)
        if j < 0 or "\n" in self.s[self.i : j]:
            self.error("unterminated literal string")
        v = self.s[self.i : j]
        self.i = j + 1
        return v

    def value(self):
        self.skip_ws()
        c = self.peek()
        if c == '"':
            return self.basic_string()
        if c == "'":
            return self.literal_string()
        if c == "[":
            self.i += 1
            items = []
            while True:
                self.skip_ws(newlines=True)
                if self.peek() == "]":
                    self.i += 1
                    return items
                items.append(self.value())
                self.skip_ws(newlines=True)
                if self.peek() == ",":
                    self.i += 1
                elif self.peek() == "]":
                    continue
                else:
                    self.error("expected ',' or ']' in array")
        m = re.compile(r"true|false|[+-]?[0-9][0-9_]*").match(self.s, self.i)
        if not m:
            self.error("unsupported value")
        self.i = m.end()
        tok = m.group(0)
        if tok == "true":
            return True
        if tok == "false":
            return False
        return int(tok.replace("_", ""))


def parse_toml(text):
    """Parse the TOML subset used by the files in `scripts/oss/`."""
    r = _Reader(text)
    doc = {}
    current = doc
    while True:
        r.skip_ws(newlines=True)
        if r.i >= len(r.s):
            return doc
        if r.peek() == "[":
            array = r.s.startswith("[[", r.i)
            r.i += 2 if array else 1
            name = r.key()
            r.skip_ws()
            r.expect("]")
            if array:
                r.expect("]")
                current = {}
                doc.setdefault(name, []).append(current)
            else:
                current = doc.setdefault(name, {})
            continue
        k = r.key()
        r.skip_ws()
        r.expect("=")
        v = r.value()
        if k in current:
            r.error("duplicate key %r" % k)
        current[k] = v
        r.skip_ws()
        if r.peek() not in ("\n", ""):
            r.error("trailing characters after value")


# ─────────────────────────────────────────────────────────────── manifest

CLASSES = ("export", "internal", "drop")
LICENSES = ("BUSL-1.1", "Apache-2.0", "MIT")
MANIFEST_PATH = "scripts/oss/manifest.toml"


class Rule:
    def __init__(self, index, table, kind):
        self.index = index
        self.kind = kind
        globs = table.get("paths")
        if globs is None and "path" in table:
            globs = [table["path"]]
        if not globs or not all(isinstance(g, str) for g in globs):
            raise TomlError("%s rule %d: needs `path` or `paths`" % (kind, index))
        self.globs = list(globs)
        self.reason = table.get("reason", "")
        if not self.reason:
            raise TomlError("%s rule %d (%s): needs a `reason`" % (kind, index, self.globs[0]))
        self.optional = bool(table.get("optional", False))
        self.cls = table.get("class")
        self.license = table.get("license")
        if kind == "rule" and self.cls not in CLASSES:
            raise TomlError("rule %d (%s): class must be one of %s" % (index, self.globs[0], CLASSES))
        if kind == "license" and self.license not in LICENSES:
            raise TomlError("license rule %d (%s): license must be one of %s" % (index, self.globs[0], LICENSES))

    def matches(self, path):
        return any(glob_match(g, path) for g in self.globs)

    def label(self):
        return ", ".join(self.globs)


class Manifest:
    """The export manifest: ordered rules, first match wins."""

    def __init__(self, text):
        doc = parse_toml(text)
        self.rules = [Rule(i, t, "rule") for i, t in enumerate(doc.get("rule", []))]
        self.licenses = [Rule(i, t, "license") for i, t in enumerate(doc.get("license", []))]
        self.public_doc_names = list(doc.get("refs", {}).get("public_doc_names", []))

    @classmethod
    def load(cls, root):
        with open(os.path.join(root, MANIFEST_PATH), encoding="utf-8") as fh:
            return cls(fh.read())

    def rule_for(self, path):
        for r in self.rules:
            if r.matches(path):
                return r
        return None

    def classify(self, path):
        r = self.rule_for(path)
        return r.cls if r else None

    def license_rule_for(self, path):
        for r in self.licenses:
            if r.matches(path):
                return r
        return None

    def license_of(self, path):
        r = self.license_rule_for(path)
        return r.license if r else None


def load_manifest_if_present(root):
    if os.path.exists(os.path.join(root, MANIFEST_PATH)):
        return Manifest.load(root)
    return None


def scan_set(root, use_manifest=True):
    """Files a lint should scan in `root`.

    In the private repository (manifest present) that is the `export` class:
    internal documents stay Vietnamese and keep their internal references
    after they move out. In an export tree (no manifest) it is every tracked
    file. Gitlinks (submodules) are never scanned.
    """
    files = [f for f in tracked_files(root) if not f.is_gitlink]
    manifest = load_manifest_if_present(root) if use_manifest else None
    if manifest is not None:
        files = [f for f in files if manifest.classify(f.path) == "export"]
    return files, manifest


# ─────────────────────────────────────────────────────────────── reading


def read_bytes(root, path):
    with open(os.path.join(root, path), "rb") as fh:
        return fh.read()


def is_binary(data):
    return b"\0" in data[:8192]


_PRINTABLE_RUN = re.compile(r"[^\x00-\x08\x0b-\x1f\x7f\ufffd]{4,}")


def printable_strings(data):
    """Printable text runs of a binary file (`strings`, but UTF-8 aware so
    diacritics inside a binary value are seen too)."""
    text = data.decode("utf-8", "replace")
    return [m.group(0) for m in _PRINTABLE_RUN.finditer(text)]


def text_lines(root, path, binary_globs=()):
    """Lines to scan for `path`, or None when the file is a binary we skip.

    Binary files are scanned only when they match one of `binary_globs`
    (data fixtures such as SST files): then their printable runs stand in
    for lines.
    """
    data = read_bytes(root, path)
    if is_binary(data):
        if any(glob_match(g, path) for g in binary_globs):
            return printable_strings(data)
        return None
    return data.decode("utf-8", "replace").split("\n")


# ─────────────────────────────────────────────────────────────── reporting


def area_of(path):
    parts = path.split("/")
    if parts[0] in ("crates", "verify") and len(parts) > 2:
        return "/".join(parts[:2])
    if parts[0] == "sdk" and len(parts) > 2:
        return "/".join(parts[:2])
    if parts[0] == "bench" and len(parts) > 2:
        return "/".join(parts[:2])
    if parts[0] == "docs" and len(parts) > 2:
        return "/".join(parts[:2])
    if len(parts) == 1:
        return "(root)"
    return parts[0]


# ─────────────────────────────────────────────────────────────── allow lists


class AllowEntry:
    __slots__ = ("path", "counts", "keep", "reason", "lineno")

    def __init__(self, path, counts, keep, reason, lineno):
        self.path = path
        self.counts = counts
        self.keep = keep
        self.reason = reason
        self.lineno = lineno


def parse_allow(text, fields):
    """Parse a frozen allow list.

    Two kinds of lines (blank lines and `#` comments ignored):

        keep <glob>  # reason        permanent: the path may hold any count
        <n1> <n2> ... <path>         frozen: per-family maxima, shrink-only

    `fields` is the number of counts on a frozen line. The path is the rest
    of the line, so it may contain spaces.
    """
    entries = []
    for lineno, raw in enumerate(text.split("\n"), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("keep "):
            body = line[5:].strip()
            glob, _, reason = body.partition(" #")
            reason = reason.strip()
            if not reason:
                raise ValueError("line %d: a `keep` entry needs a `# reason`" % lineno)
            entries.append(AllowEntry(glob.strip(), None, True, reason, lineno))
            continue
        parts = line.split(None, fields)
        if len(parts) != fields + 1:
            raise ValueError("line %d: expected %d counts and a path" % (lineno, fields))
        try:
            counts = [int(p) for p in parts[:fields]]
        except ValueError:
            raise ValueError("line %d: counts must be integers" % lineno)
        entries.append(AllowEntry(parts[fields], counts, False, "", lineno))
    return entries


def read_allow(path, fields):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return parse_allow(fh.read(), fields)
