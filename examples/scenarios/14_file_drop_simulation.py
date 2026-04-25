"""
Scenario 14 -- File-Drop Simulation
=====================================
Simulate an upstream source landing files into a Fabric Lakehouse landing
zone. Covers daily partitioning, manifest and done-flag generation, late
arrivals, duplicate files, and per-entity stats.

This mirrors the pattern used by ADF / Fabric Data Factory pipelines that
write raw files into Files/landing/<domain>/<entity>/dt=YYYY-MM-DD/.

Run:
    python examples/scenarios/14_file_drop_simulation.py
"""

import shutil
from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.simulation.file_drop import FileDropConfig, FileDropSimulator

OUTPUT_DIR = Path("./demo_file_drop")

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
print(f"Generated {len(result.tables)} tables")

# ── 1. Minimal daily batch ────────────────────────────────────────────────────
print("\n── 1. Minimal daily batch (customer + order, Jan 1–7) ──")

cfg_basic = FileDropConfig(
    domain="retail",
    base_path=str(OUTPUT_DIR / "basic"),
    cadence="daily",
    date_range_start="2025-01-01",
    date_range_end="2025-01-07",
    partitioning="dt=YYYY-MM-DD",
    formats=["parquet"],
    entities=["customer", "order"],
    manifest_enabled=True,
    done_flag_enabled=True,
)

sim = FileDropSimulator(tables=result.tables, config=cfg_basic)
r = sim.run()

print(f"  Files written:  {len(r.files_written)}")
print(f"  Manifests:      {len(r.manifest_paths)}")
print(f"  Done flags:     {len(r.done_flag_paths)}")
print(f"  Sample path:    {r.files_written[0].relative_to(OUTPUT_DIR) if r.files_written else '(none)'}")

# ── 2. Multiple formats ────────────────────────────────────────────────────────
print("\n── 2. Multiple formats (parquet + csv + jsonl) ──")

cfg_multi = FileDropConfig(
    domain="retail",
    base_path=str(OUTPUT_DIR / "multi_format"),
    cadence="daily",
    date_range_start="2025-01-01",
    date_range_end="2025-01-03",
    formats=["parquet", "csv", "jsonl"],
    entities=["order"],
    manifest_enabled=False,
    done_flag_enabled=False,
)

r2 = FileDropSimulator(tables=result.tables, config=cfg_multi).run()
print(f"  Files written: {len(r2.files_written)}")
print(f"  Extensions:    {sorted({f.suffix for f in r2.files_written})}")

# ── 3. Lateness + duplicates ──────────────────────────────────────────────────
print("\n── 3. Lateness + duplicate file simulation ──")

cfg_chaos = FileDropConfig(
    domain="retail",
    base_path=str(OUTPUT_DIR / "with_lateness"),
    cadence="daily",
    date_range_start="2025-01-01",
    date_range_end="2025-01-14",
    formats=["parquet"],
    entities=["order"],
    manifest_enabled=True,
    done_flag_enabled=True,
    lateness_enabled=True,
    lateness_probability=0.20,  # 20% of partitions arrive late
    max_days_late=3,
    duplicates_enabled=True,
    duplicate_probability=0.10, # 10% chance of a duplicate file per partition
    seed=99,
)

r3 = FileDropSimulator(tables=result.tables, config=cfg_chaos).run()
print(f"  Files written: {len(r3.files_written)}")
print(f"  Late partitions (from stats): {r3.stats.get('late_count', 'N/A')}")
print(f"  Duplicate files:              {r3.stats.get('duplicate_count', 'N/A')}")

# ── 4. Per-entity stats ───────────────────────────────────────────────────────
print("\n── 4. Per-entity stats ──")
cfg_all = FileDropConfig(
    domain="retail",
    base_path=str(OUTPUT_DIR / "all_entities"),
    cadence="daily",
    date_range_start="2025-01-01",
    date_range_end="2025-01-03",
    formats=["parquet"],
    manifest_enabled=True,
    done_flag_enabled=True,
)

r4 = FileDropSimulator(tables=result.tables, config=cfg_all).run()
print(f"  {'Entity':<20} {'Files':>6} {'Rows':>10}")
print(f"  {'-'*40}")
for entity, stats in r4.stats.items():
    if isinstance(stats, dict) and "files" in stats:
        print(f"  {entity:<20} {stats['files']:>6} {stats.get('rows_written', '?'):>10,}")

# ── 5. Hourly cadence ─────────────────────────────────────────────────────────
print("\n── 5. Hourly cadence ──")

cfg_hourly = FileDropConfig(
    domain="retail",
    base_path=str(OUTPUT_DIR / "hourly"),
    cadence="hourly",
    date_range_start="2025-01-01",
    date_range_end="2025-01-01",  # single day = 24 hourly drops
    partitioning="dt=YYYY-MM-DD/hour=HH",
    formats=["jsonl"],
    entities=["order"],
    manifest_enabled=True,
    done_flag_enabled=False,
)

r5 = FileDropSimulator(tables=result.tables, config=cfg_hourly).run()
print(f"  Files written: {len(r5.files_written)}")
sample_paths = [str(f.name) for f in r5.files_written[:3]]
print(f"  Sample files:  {sample_paths}")

# ── Cleanup ────────────────────────────────────────────────────────────────────
print(f"\nOutput directory: {OUTPUT_DIR.resolve()}")
print("Run `ls -R demo_file_drop` to inspect the generated partition layout.")
print("\nTo clean up:  shutil.rmtree('./demo_file_drop')")
