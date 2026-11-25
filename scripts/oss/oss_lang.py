#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""oss-lang: no Vietnamese in tracked files outside an allow list.

Two detectors, both run over every scanned file -- sources, documents and
generated data alike (conformance traces, fixture `EXPECTED.json`, and the
printable strings of binary fixtures such as SST files):

1. **Diacritics.** A line holding a Latin letter with diacritics or a
   combining mark (the same character class as the `english_text` source
   rule of cairn-server) counts once.

2. **Undiacritic Vietnamese.** Text is cut into tokens (`[\\w-]+`,
   ASCII only); a token is split into words at `_`, `-` and camelCase
   boundaries, while a digit ends a run. A token counts once when it has
     - two or more consecutive words from the syllable list of
       `lang-words.txt` and at least one of them is not in its English list
       (so `to_le_bytes` or `can_do` stay English), or
     - two or more non-English syllables anywhere in the token, even with
       English words between them, or
     - at least two words, one of which is a *distinctive* syllable: in the
       syllable list, not English, and spelled in a way English words are
       not (`kh`/`ng`/`nh` onset, `uo`, `ie`, `uy`, `oa`, `oe`, `ue`) -- this
       catches a Vietnamese word glued to an English one by a dash.
   Hash-like parts (a digit and 8+ characters, or 8+ hex digits), `0x`
   literals and lockfiles are skipped for this detector. Example tokens live
   in `scripts/oss/tests/test_lang.py`, which is not exported.

Which files: in the private repository the `export` class of
`scripts/oss/manifest.toml` (internal documents stay Vietnamese); in an
export tree (no manifest) every tracked file. Gitlinks are skipped, binary
files are skipped unless they match `BINARY_FIXTURES`.

Allow list `scripts/lint-lang.allow` (see `osslib.parse_allow`):

    keep <glob>  # reason       permanent (locale file, glossary, i18n
                                fixtures, frozen format-10 buckets)
    <lines> <tokens> <path>     frozen counts: diacritic lines, undiacritic
                                tokens; may only shrink

Modes:

    oss_lang.py                 frozen: FAIL when a file exceeds its counts
                                or an unlisted file has findings
    oss_lang.py --strict        hard: only `keep` entries count (export tree)
    oss_lang.py --report        table per area, exit 0
    oss_lang.py --hits [PATH..] print every finding (calibration, lanes)
    oss_lang.py --write-allow   rewrite the frozen counts from the tree;
                                refuses to raise a count unless --bootstrap
"""

import argparse
import collections
import os
import re
import sys

sys.dont_write_bytecode = True  # no __pycache__ next to the scripts
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import osslib  # noqa: E402

ALLOW_PATH = "scripts/lint-lang.allow"
WORDS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lang-words.txt")

# Binary files scanned through their printable strings.
BINARY_FIXTURES = ("**/tests/fixtures/**",)
# Files where the undiacritic detector only finds noise (checksums, names of
# third-party packages). The diacritic detector still runs on them.
SKIP_UNDIACRITIC = ("**/Cargo.lock", "**/package-lock.json")

_TOKEN = re.compile(r"[\w-]+")
_PIECE = re.compile(r"[A-Za-z]+|[0-9]+|[_-]+")
_CAMEL = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+")
_HEXISH = re.compile(r"^[0-9a-fA-F]{8,}$")
_DISTINCT = re.compile(r"^(?:kh|ng|nh)|uo|ie|uy|oa|oe|ue")
_RUST_FN = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")


def _hash_like(part):
    """A hex digest, base64 run or random id rather than words."""
    return bool(_HEXISH.match(part)) or (len(part) >= 8 and any(c.isdigit() for c in part))


def load_words(path=WORDS_PATH):
    sections = collections.defaultdict(set)
    current = None
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("[") and line.endswith("]"):
                current = line[1:-1]
                continue
            if current is None:
                raise ValueError("%s: word outside a section" % path)
            sections[current].update(w.lower() for w in line.split())
    return sections["syllables"], sections["english"]


class Detector:
    def __init__(self, syllables=None, english=None):
        if syllables is None:
            syllables, english = load_words()
        self.syllables = syllables
        self.english = english
        self.strong = syllables - english
        self.distinct = {w for w in self.strong if len(w) >= 3 and _DISTINCT.search(w)}

    @staticmethod
    def segments(token):
        """Word runs of a token: separators keep a run going, digits end it,
        and a hash-like part between separators (`2rAxB2eNFyc…`) is dropped."""
        segs = [[]]
        for part in re.split(r"([_-]+)", token):
            if not part or part[0] in "_-":
                continue
            if _hash_like(part):
                segs.append([])
                continue
            for piece in _PIECE.findall(part):
                if piece[0].isdigit():
                    segs.append([])
                else:
                    segs[-1].extend(w.lower() for w in _CAMEL.findall(piece))
        return [s for s in segs if s]

    def token_hit(self, token):
        """True when `token` reads as undiacritic Vietnamese."""
        if not token.isascii() or token[:2] in ("0x", "0X"):
            return False
        segs = self.segments(token)
        nwords = sum(len(s) for s in segs)
        if nwords < 2:
            return False
        for seg in segs:
            run = []
            for w in seg + [None]:
                if w is not None and w in self.syllables:
                    run.append(w)
                    continue
                if len(run) >= 2 and any(x in self.strong for x in run):
                    return True
                run = []
        words = [w for seg in segs for w in seg]
        if sum(1 for w in words if w in self.strong) >= 2:
            return True
        return any(w in self.distinct for w in words)

    def line_hits(self, line):
        return [t for t in _TOKEN.findall(line) if self.token_hit(t)]


class FileResult:
    __slots__ = ("path", "vi_lines", "tokens", "examples", "fn_names")

    def __init__(self, path):
        self.path = path
        self.vi_lines = []  # line numbers
        self.tokens = []  # (line number, token)
        self.examples = []
        self.fn_names = []

    @property
    def counts(self):
        return [len(self.vi_lines), len(self.tokens)]

    def any(self):
        return bool(self.vi_lines or self.tokens)


def scan_file(root, path, detector):
    lines = osslib.text_lines(root, path, BINARY_FIXTURES)
    res = FileResult(path)
    if lines is None:
        return res, False
    undiacritic = not any(osslib.glob_match(g, path) for g in SKIP_UNDIACRITIC)
    is_rust = path.endswith(".rs")
    for no, line in enumerate(lines, 1):
        if osslib.VIETNAMESE_CHAR.search(line):
            res.vi_lines.append(no)
        if undiacritic:
            for tok in detector.line_hits(line):
                res.tokens.append((no, tok))
            if is_rust:
                for m in _RUST_FN.finditer(line):
                    if detector.token_hit(m.group(1)):
                        res.fn_names.append((no, m.group(1)))
    return res, True


def scan(root, use_manifest=True, only=None, detector=None):
    detector = detector or Detector()
    files, _ = osslib.scan_set(root, use_manifest)
    results = []
    skipped = 0
    for f in files:
        if f.is_symlink or f.path == ALLOW_PATH:
            continue
        if only and not any(f.path == p or f.path.startswith(p.rstrip("/") + "/") for p in only):
            continue
        res, scanned = scan_file(root, f.path, detector)
        if not scanned:
            skipped += 1
        results.append(res)
    return results, skipped


def keep_for(path, entries):
    for e in entries:
        if e.keep and osslib.glob_match(e.path, path):
            return e
    return None


def evaluate(results, entries, strict):
    """Return (failures, notes, kept) for a scan against an allow list."""
    frozen = {e.path: e for e in entries if not e.keep}
    failures, notes = [], []
    kept = collections.Counter()
    seen = set()
    for r in results:
        seen.add(r.path)
        k = keep_for(r.path, entries)
        if k is not None:
            if r.any():
                kept[k.path] += 1
            continue
        allowed = [0, 0] if strict or r.path not in frozen else frozen[r.path].counts
        have = r.counts
        if have[0] > allowed[0] or have[1] > allowed[1]:
            failures.append((r, allowed))
        elif not strict and r.path in frozen and have != allowed:
            notes.append("note: %s now has %d lines / %d tokens (allow list: %d / %d) -- lower it in %s"
                         % (r.path, have[0], have[1], allowed[0], allowed[1], ALLOW_PATH))
    if not strict:
        for p in frozen:
            if p not in seen:
                notes.append("note: %s is no longer scanned -- drop its line from %s" % (p, ALLOW_PATH))
    return failures, notes, kept


def area_table(results, entries):
    by_area = collections.defaultdict(lambda: [0, 0, 0, 0])
    for r in results:
        a = by_area[osslib.area_of(r.path)]
        if r.any():
            a[0] += 1
        a[1] += len(r.vi_lines)
        a[2] += len(r.tokens)
        if keep_for(r.path, entries) is not None:
            a[3] += len(r.vi_lines) + len(r.tokens)
    rows = ["%-32s %6s %9s %8s %8s" % ("area", "files", "vi-lines", "tokens", "kept")]
    tot = [0, 0, 0, 0]
    for area in sorted(by_area):
        a = by_area[area]
        if not any(a):
            continue
        rows.append("%-32s %6d %9d %8d %8d" % (area, a[0], a[1], a[2], a[3]))
        tot = [x + y for x, y in zip(tot, a)]
    rows.append("%-32s %6d %9d %8d %8d" % ("total", tot[0], tot[1], tot[2], tot[3]))
    return rows


def write_allow(root, results, entries, bootstrap):
    path = os.path.join(root, ALLOW_PATH)
    frozen = {e.path: e for e in entries if not e.keep}
    raised = []
    rows = []
    for r in sorted(results, key=lambda r: r.path):
        if not r.any() or keep_for(r.path, entries) is not None:
            continue
        old = frozen.get(r.path)
        if old is not None and (r.counts[0] > old.counts[0] or r.counts[1] > old.counts[1]) or (
            old is None and frozen
        ):
            raised.append(r.path)
        rows.append("%d %d %s" % (r.counts[0], r.counts[1], r.path))
    if raised and not bootstrap:
        for p in raised:
            print("FAIL oss-lang: refusing to raise the frozen count of %s (pass --bootstrap)" % p)
        return 1
    header = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if s and not s.startswith("#") and not s.startswith("keep "):
                    break
                header.append(line.rstrip("\n"))
    while header and not header[-1].strip():
        header.pop()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(header + [""] + rows) + "\n")
    print("oss-lang: wrote %d frozen entries to %s" % (len(rows), ALLOW_PATH))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="oss-lang: no Vietnamese outside the allow list")
    ap.add_argument("--root", default=None)
    ap.add_argument("--allow", default=None, help="allow list (default: %s)" % ALLOW_PATH)
    ap.add_argument("--all", action="store_true", help="scan every tracked file, ignore the manifest")
    ap.add_argument("--strict", action="store_true", help="only permanent `keep` entries count")
    ap.add_argument("--report", action="store_true", help="print the per-area table and exit 0")
    ap.add_argument("--hits", nargs="*", default=None, metavar="PATH", help="print findings")
    ap.add_argument("--write-allow", action="store_true")
    ap.add_argument("--bootstrap", action="store_true")
    args = ap.parse_args(argv)

    root = args.root or osslib.git_root()
    allow_path = args.allow or os.path.join(root, ALLOW_PATH)
    try:
        entries = osslib.read_allow(allow_path, 2)
    except ValueError as e:
        print("FAIL oss-lang: %s: %s" % (allow_path, e))
        return 1

    only = args.hits if args.hits else None
    results, skipped = scan(root, use_manifest=not args.all, only=only)

    if args.hits is not None:
        for r in results:
            for no in r.vi_lines:
                print("%s:%d: diacritics" % (r.path, no))
            for no, tok in r.tokens:
                print("%s:%d: undiacritic %s" % (r.path, no, tok))
        return 0

    if args.write_allow:
        return write_allow(root, results, entries, args.bootstrap)

    tokens = collections.Counter(t for r in results for _, t in r.tokens)
    fns = sorted({n for r in results for _, n in r.fn_names})
    summary = ("%d files scanned (%d binary skipped), %d diacritic lines in %d files, "
               "%d undiacritic tokens (%d distinct) in %d files, %d Rust fn names"
               % (len(results), skipped, sum(len(r.vi_lines) for r in results),
                  sum(1 for r in results if r.vi_lines), sum(tokens.values()), len(tokens),
                  sum(1 for r in results if r.tokens), len(fns)))

    if args.report:
        for row in area_table(results, entries):
            print(row)
        print("oss-lang: " + summary)
        return 0

    failures, notes, kept = evaluate(results, entries, args.strict)
    for r, allowed in failures:
        print("FAIL oss-lang: %s has %d Vietnamese lines / %d undiacritic tokens (allowed %d / %d)"
              % (r.path, r.counts[0], r.counts[1], allowed[0], allowed[1]))
        shown = 0
        for no in r.vi_lines[:3]:
            print("     %s:%d: diacritics" % (r.path, no))
            shown += 1
        for no, tok in r.tokens[:3]:
            print("     %s:%d: undiacritic `%s`" % (r.path, no, tok))
    for n in notes:
        print(n)
    if failures:
        print("     Translate to English (identifiers, literals and test names too), or -- only for a"
              " permanent reason -- add a `keep` line to %s." % ALLOW_PATH)
        print("FAIL oss-lang (%s): %d files over the allow list -- %s."
              % ("strict" if args.strict else "frozen", len(failures), summary))
        return 1
    mode = "strict" if args.strict else "frozen"
    print("oss-lang (%s): clean -- %s; %d kept paths." % (mode, summary, sum(kept.values())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
