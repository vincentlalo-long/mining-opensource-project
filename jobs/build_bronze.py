#!/usr/bin/env python3
"""Spark / CLI job entrypoint for building bronze.events."""

import sys
from pathlib import Path

# Ensure src/ is in sys.path when executed directly or via spark-submit
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gh_ecosystem.bronze import main

if __name__ == "__main__":
    raise SystemExit(main())
