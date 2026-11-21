#!/usr/bin/env python3
"""Spark / CLI job entrypoint for building gold metric tables."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gh_ecosystem.gold.driver import main

if __name__ == "__main__":
    raise SystemExit(main())
