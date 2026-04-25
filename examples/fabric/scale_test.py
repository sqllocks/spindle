"""Fabric Notebook — Scale Testing.

Paste each section into a separate Fabric Notebook cell.
Generates retail data at large scale and benchmarks performance.
"""

# ── CELL 1: Install Spindle ─────────────────────────────────────────────
# %pip install sqllocks-spindle[fabric] -q

# ── CELL 2: Generate at large scale + benchmark ─────────────────────────
import time

from sqllocks_spindle import Spindle, RetailDomain

domain = RetailDomain(schema_mode="3nf")
spindle = Spindle()

# Large scale: ~500K customers, ~5M orders
t0 = time.perf_counter()
result = spindle.generate(domain=domain, scale="large", seed=42)
gen_time = time.perf_counter() - t0

total_rows = sum(len(df) for df in result.tables.values())
print(f"Generated {total_rows:,} rows across {len(result.tables)} tables in {gen_time:.1f}s")
print(f"Throughput: {total_rows / gen_time:,.0f} rows/sec")
print()
print(result.summary())

# ── CELL 3: Write to Delta + benchmark ──────────────────────────────────
from sqllocks_spindle.output import DeltaWriter

writer = DeltaWriter()

t0 = time.perf_counter()
paths = writer.write_all(result.tables)
write_time = time.perf_counter() - t0

print(f"Delta write: {total_rows:,} rows in {write_time:.1f}s")
print(f"Write throughput: {total_rows / write_time:,.0f} rows/sec")
print()
for p in paths:
    print(f"  {p}")

# ── CELL 4: Verify row counts ───────────────────────────────────────────
print(f"{'Table':<25} {'Expected':>12} {'Actual':>12} {'Status':>8}")
print("-" * 60)
for table_name, df in result.tables.items():
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").collect()[0].cnt  # noqa: F821
    expected = len(df)
    status = "OK" if count == expected else "MISMATCH"
    print(f"{table_name:<25} {expected:>12,} {count:>12,} {status:>8}")

# ── CELL 5: Summary ────────────────────────────────────────────────────
print(f"""
Scale Test Summary
==================
Total rows:       {total_rows:,}
Generation time:  {gen_time:.1f}s ({total_rows / gen_time:,.0f} rows/sec)
Delta write time: {write_time:.1f}s ({total_rows / write_time:,.0f} rows/sec)
Total time:       {gen_time + write_time:.1f}s
""")
