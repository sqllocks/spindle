# sqllocks_spindle/verify/loader.py
"""Shared data loader for spindle verify and compare commands."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

_SUPPORTED_FORMATS = ("csv", "parquet", "jsonl")


def load_tables(path: str, fmt: str) -> dict[str, pd.DataFrame]:
    """Load one or more data files into a dict of DataFrames keyed by table name.

    Args:
        path: Path to a single file or a directory containing data files.
        fmt:  File format — one of "csv", "parquet", "jsonl".

    Returns:
        Dict mapping stem name (filename without extension) to DataFrame.
        Empty dict if a directory contains no matching files.

    Raises:
        FileNotFoundError: If `path` does not exist.
        ValueError: If `fmt` is not supported.
    """
    if fmt not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Choose from: {_SUPPORTED_FORMATS}")

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    if p.is_file():
        return {p.stem: _read_file(p, fmt)}

    # Directory: glob all matching files
    ext_map = {"csv": "*.csv", "parquet": "*.parquet", "jsonl": "*.jsonl"}
    files = sorted(p.glob(ext_map[fmt]))
    return {fp.stem: _read_file(fp, fmt) for fp in files}


def _read_file(p: Path, fmt: str) -> pd.DataFrame:
    if fmt == "csv":
        return pd.read_csv(p)
    if fmt == "parquet":
        return pd.read_parquet(p)
    # jsonl
    return pd.read_json(p, lines=True)
