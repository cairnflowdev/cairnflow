# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Source rule: string literals in ``src/`` are text users see (raised errors, errors sent
to the runtime, warnings), so they must be English; public docs quote them verbatim.

"Vietnamese text" = accented Latin (U+00C0-U+024F, U+1EA0-U+1EF9) or combining marks
(U+0300-U+036F). Typographic dashes and arrows are fine. Only literals are read (``ast``),
not comments.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "cairnflow"
VIETNAMESE = re.compile("[\u00c0-\u024f\u1ea0-\u1ef9\u0300-\u036f]")


class EnglishTextTest(unittest.TestCase):
    def test_user_visible_strings_are_english(self) -> None:
        bad = []
        for f in sorted(SRC.glob("*.py")):
            for node in ast.walk(ast.parse(f.read_text(), str(f))):
                if isinstance(node, ast.Constant) and isinstance(node.value, str) and VIETNAMESE.search(node.value):
                    bad.append(f"{f.name}:{node.lineno}: {node.value!r}")
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main()
