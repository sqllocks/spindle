"""Benchmark Spindle generation performance across scales and domains."""

from __future__ import annotations

import json
import os
import sys
import time
import tracemalloc
from pathlib import Path

# Ensure the package is importable from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain


def _bench_one(domain_cls, scale: str, seed: int = 42) -> dict:
    """Time a single generation run and return metrics."""
    tracemalloc.start()
    spindle = Spindle()
    domain = domain_cls()

    t0 = time.perf_counter()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)
    elapsed = time.perf_counter() - t0

    _, peak_mb = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    total_rows = sum(result.row_counts.values())
    per_table = {
        name: {"rows": count, "columns": len(result.tables[name].columns)}
        for name, count in result.row_counts.items()
    }

    return {
        "domain": domain_cls.__name__,
        "scale": scale,
        "total_rows": total_rows,
        "total_seconds": round(elapsed, 4),
        "rows_per_second": round(total_rows / max(elapsed, 0.001)),
        "peak_memory_mb": round(peak_mb / 1024 / 1024, 2),
        "per_table": per_table,
    }


def main():
    results = []

    # Retail at multiple scales
    for scale in ("small", "medium"):
        print(f"Benchmarking RetailDomain @ {scale}...")
        r = _bench_one(RetailDomain, scale)
        results.append(r)
        print(f"  {r['total_rows']:,} rows in {r['total_seconds']}s "
              f"({r['rows_per_second']:,} rows/s, {r['peak_memory_mb']} MB)")

    # Healthcare at small
    print("Benchmarking HealthcareDomain @ small...")
    r = _bench_one(HealthcareDomain, "small")
    results.append(r)
    print(f"  {r['total_rows']:,} rows in {r['total_seconds']}s "
          f"({r['rows_per_second']:,} rows/s, {r['peak_memory_mb']} MB)")

    # Determine output path
    benchmarks_dir = Path(__file__).resolve().parent.parent / "benchmarks"
    benchmarks_dir.mkdir(exist_ok=True)

    # Auto-detect version
    from sqllocks_spindle import __version__
    out_file = benchmarks_dir / f"baseline-v{__version__}.json"

    with open(out_file, "w") as f:
        json.dump({"version": __version__, "benchmarks": results}, f, indent=2)

    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
