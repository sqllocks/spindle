"""Scale benchmark — test Spindle generation and Delta write at 1M+ rows.

Usage:
    python scripts/scale_benchmark.py [--scale medium|large] [--delta]
"""

from __future__ import annotations

import argparse
import sys
import time
import tracemalloc

from sqllocks_spindle import Spindle, RetailDomain


def main():
    parser = argparse.ArgumentParser(description="Spindle scale benchmark")
    parser.add_argument("--scale", default="medium", choices=["medium", "large", "xlarge"])
    parser.add_argument("--delta", action="store_true", help="Also benchmark Delta Lake write")
    parser.add_argument("--output", default="./benchmark_output", help="Output dir for Delta")
    args = parser.parse_args()

    print(f"Spindle Scale Benchmark — scale={args.scale}")
    print("=" * 60)

    # --- Generation benchmark ---
    tracemalloc.start()
    domain = RetailDomain(schema_mode="3nf")
    spindle = Spindle()

    t0 = time.perf_counter()
    result = spindle.generate(domain=domain, scale=args.scale, seed=42)
    gen_time = time.perf_counter() - t0

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    total_rows = sum(len(df) for df in result.tables.values())
    gen_rate = total_rows / gen_time if gen_time > 0 else 0

    print()
    print(result.summary())
    print()

    # --- FK integrity ---
    errors = result.verify_integrity()
    print(f"FK integrity: {'PASS' if not errors else f'FAIL ({len(errors)} issues)'}")
    print()

    # --- Delta write benchmark ---
    write_time = 0.0
    write_rate = 0.0
    if args.delta:
        try:
            from sqllocks_spindle.output import DeltaWriter

            writer = DeltaWriter(output_dir=args.output)
            t0 = time.perf_counter()
            paths = writer.write_all(result.tables)
            write_time = time.perf_counter() - t0
            write_rate = total_rows / write_time if write_time > 0 else 0

            print(f"Delta write: {len(paths)} tables to {args.output}/")
        except ImportError:
            print("Delta write: SKIPPED (pip install sqllocks-spindle[fabric])")

    # --- Summary table ---
    print()
    print("| Metric | Value |")
    print("|---|---|")
    print(f"| Scale | {args.scale} |")
    print(f"| Tables | {len(result.tables)} |")
    print(f"| Total rows | {total_rows:,} |")
    print(f"| Generation time | {gen_time:.1f}s |")
    print(f"| Generation rate | {gen_rate:,.0f} rows/sec |")
    if args.delta and write_time > 0:
        print(f"| Delta write time | {write_time:.1f}s |")
        print(f"| Delta write rate | {write_rate:,.0f} rows/sec |")
        print(f"| Total time | {gen_time + write_time:.1f}s |")
    print(f"| Peak memory | {peak_mem / 1024 / 1024:.0f} MB |")
    print(f"| FK integrity | {'PASS' if not errors else 'FAIL'} |")


if __name__ == "__main__":
    main()
