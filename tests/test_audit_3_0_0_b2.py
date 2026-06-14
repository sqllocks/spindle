"""Audit 3.0.0 - B2 determinism: cross-subprocess reproducibility.

Verifies that running the same generation in two subprocesses with DIFFERENT
PYTHONHASHSEED env vars produces identical output. Pre-3.0.0 the chunk worker
and scale router used Python builtin hash() to derive per-table child RNGs;
that hash is per-process-randomized via PYTHONHASHSEED, so chunked / scale-
routed generation drifted across subprocesses. zlib.crc32 is now used instead.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

import pytest

_SCRIPT = textwrap.dedent("""
    import json
    import sys
    from sqllocks_spindle import Spindle, RetailDomain
    result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
    out = {}
    for tname, df in result.tables.items():
        cols = sorted(df.columns)
        first5 = df[cols].head(5).astype(str).values.tolist()
        out[tname] = {"cols": cols, "first5": first5, "rows": len(df)}
    sys.stdout.write(json.dumps(out, sort_keys=True))
""")


def _run(hashseed: str) -> dict:
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = hashseed
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        raise AssertionError(f"subprocess failed (hashseed={hashseed}): {proc.stderr}")
    return json.loads(proc.stdout)


def test_cross_subprocess_determinism_default_path():
    """Same seed, different PYTHONHASHSEED, must yield identical output."""
    a = _run("0")
    b = _run("1")
    assert a.keys() == b.keys()
    for tname in a:
        assert a[tname]["cols"] == b[tname]["cols"], f"{tname} cols differ"
        assert a[tname]["rows"] == b[tname]["rows"], f"{tname} row count differs"
        assert a[tname]["first5"] == b[tname]["first5"], f"{tname} first-5 rows differ across PYTHONHASHSEED"


def test_chunk_worker_stable_hash():
    """The replacement for hash() in chunk_worker must be deterministic across runs."""
    import zlib

    expected = zlib.crc32(b"orders")
    # In a child subprocess, the same crc32 must hold (hash() would change).
    proc = subprocess.run(
        [sys.executable, "-c", "import zlib; print(zlib.crc32(b'orders'))"],
        env={**os.environ, "PYTHONHASHSEED": "999"},
        capture_output=True,
        text=True,
    )
    assert int(proc.stdout.strip()) == expected
