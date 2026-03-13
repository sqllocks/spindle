"""
Scenario 03 -- Scale Presets
============================
Spindle ships named scale presets for every domain. Choose the right
scale for your use case -- from a 2-second demo to a 10M-row warehouse load.

Presets (Retail domain as example):
  fabric_demo  ~200 customers,     ~1,000 orders    -- notebooks, demos
  small        ~1,000 customers,   ~5,000 orders    -- unit tests, local dev
  medium       ~50,000 customers,  ~250,000 orders  -- integration testing
  large        ~500,000 customers, ~2.5M orders     -- staging environments
  warehouse    ~1M customers,      ~10M orders      -- Fabric Data Warehouse loads
  xlarge       ~5M customers,      ~50M orders      -- extreme scale (use with Spark)

Run:
    python examples/scenarios/03_scale_presets.py
"""

import time
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()

# ------------------------------------------------------------------
# 1. Named presets -- fabric_demo and small are fast enough to run inline
# ------------------------------------------------------------------
for scale in ["fabric_demo", "small"]:
    t0 = time.time()
    r = spindle.generate(domain=RetailDomain(), scale=scale, seed=42)
    elapsed = time.time() - t0
    total = sum(r.row_counts.values())
    print(f"{scale:<12}  customers={r.row_counts['customer']:>7,}  "
          f"orders={r.row_counts['order']:>8,}  total={total:>9,}  {elapsed:.2f}s")

# ------------------------------------------------------------------
# 2. Custom scale -- override individual table row counts
# ------------------------------------------------------------------
print("\n--- Custom scale overrides ---")
custom_result = spindle.generate(
    domain=RetailDomain(),
    scale_overrides={
        "customer":         500,
        "product_category":  20,
        "product":          200,
        "store":             15,
        "promotion":         30,
    },
    seed=42,
)
print(custom_result)

# ------------------------------------------------------------------
# 3. Inspect scale presets programmatically
# ------------------------------------------------------------------
print("\n--- All scale presets for RetailDomain ---")
schema = RetailDomain().get_schema()
scales = schema.generation.scales
for preset_name, preset_counts in scales.items():
    customers = preset_counts.get("customer", "n/a")
    orders    = preset_counts.get("order", "n/a")
    print(f"  {preset_name:<12}  customer={customers!s:>8}  order={orders!s:>10}")

# ------------------------------------------------------------------
# 4. medium scale benchmark (skip if you want to keep runtime short)
# ------------------------------------------------------------------
run_medium = False  # set True to benchmark medium scale

if run_medium:
    t0 = time.time()
    r = spindle.generate(domain=RetailDomain(), scale="medium", seed=42)
    elapsed = time.time() - t0
    total = sum(r.row_counts.values())
    print(f"\nmedium  total={total:,} rows in {elapsed:.1f}s "
          f"({int(total/elapsed):,} rows/s)")
