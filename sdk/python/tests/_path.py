# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Makes ``src/`` importable when the tests run from a checkout without installing."""

import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
