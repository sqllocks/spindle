#!/usr/bin/env python3
"""Re-run only the 5 domains that failed the first smoke test."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import smoke_test_domains as st

st.DOMAINS = ["retail", "financial", "iot", "hr", "education"]

if __name__ == "__main__":
    sys.exit(st.main())
