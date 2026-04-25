"""
Scenario 13 -- Chaos Injection
===============================
Inject realistic data quality issues into generated data: schema drift,
value corruption, late arrivals, orphaned FKs, temporal anomalies,
and volume spikes.

Use chaos to simulate the messy reality of production data pipelines —
upstream vendors missing files, ETL bugs introducing nulls, clocks
drifting, and FK references arriving out of order.

Run:
    python examples/scenarios/13_chaos_injection.py
"""

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.chaos.config import ChaosConfig, ChaosCategory, ChaosOverride
from sqllocks_spindle.chaos.engine import ChaosEngine

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
orders = result["order"].copy()
print(f"Base orders: {len(orders):,} rows, {len(orders.columns)} columns")

# ── 1. Basic chaos config ─────────────────────────────────────────────────────
# intensity controls probability multipliers:
#   calm=0.25x | moderate=1.0x | stormy=2.5x | hurricane=5.0x
cfg = ChaosConfig(
    enabled=True,
    intensity="stormy",
    seed=99,
    warmup_days=7,        # no chaos before day 7 (let the pipeline settle)
    chaos_start_day=8,
    escalation="gradual", # ramp up probabilities over 30 days
    breaking_change_day=20,  # allow column drops/renames only after day 20
)

engine = ChaosEngine(cfg)

# ── 2. Day-by-day chaos simulation ───────────────────────────────────────────
print("\nDay-by-day chaos decisions:")
for day in [1, 5, 8, 12, 20, 25, 30]:
    decisions = {
        cat.value: engine.should_inject(day, cat.value)
        for cat in ChaosCategory
    }
    active = [k for k, v in decisions.items() if v]
    print(f"  Day {day:2d}: {active if active else '(none)'}")

# ── 3. Value corruption ───────────────────────────────────────────────────────
print("\nValue corruption (day 15):")
corrupted = engine.corrupt_dataframe(orders.copy(), day=15)
null_count_before = orders.isnull().sum().sum()
null_count_after  = corrupted.isnull().sum().sum()
print(f"  Nulls before: {null_count_before:,}")
print(f"  Nulls after:  {null_count_after:,}")

# ── 4. Schema drift ───────────────────────────────────────────────────────────
print("\nSchema drift (day 22 — post breaking_change_day):")
drifted = engine.drift_schema(orders.copy(), day=22)
added   = [c for c in drifted.columns if c not in orders.columns]
removed = [c for c in orders.columns  if c not in drifted.columns]
print(f"  Columns added:   {added}")
print(f"  Columns removed: {removed}")
print(f"  Shape: {orders.shape} -> {drifted.shape}")

# ── 5. Temporal chaos ─────────────────────────────────────────────────────────
print("\nTemporal chaos (day 18):")
date_cols = [c for c in orders.columns if "date" in c.lower()]
if date_cols:
    temporal = engine.inject_temporal_chaos(orders.copy(), date_columns=date_cols, day=18)
    print(f"  Date columns affected: {date_cols}")
    print(f"  Sample created_date before: {orders[date_cols[0]].iloc[0]}")
    print(f"  Sample created_date after:  {temporal[date_cols[0]].iloc[0]}")
else:
    print("  (no date columns found)")

# ── 6. Volume chaos ───────────────────────────────────────────────────────────
print("\nVolume chaos (day 10):")
volumed = engine.inject_volume_chaos(orders.copy(), day=10)
print(f"  Rows before: {len(orders):,}")
print(f"  Rows after:  {len(volumed):,}  (spike, empty, or singleton)")

# ── 7. Force-inject via ChaosOverride ─────────────────────────────────────────
print("\nForced override on day 14:")
cfg_with_override = ChaosConfig(
    enabled=True,
    intensity="moderate",
    seed=42,
    overrides=[
        ChaosOverride(day=14, category="value",  params={"severity": "high"}),
        ChaosOverride(day=14, category="volume", params={}),
    ],
)
engine_override = ChaosEngine(cfg_with_override)
overrides_day14 = cfg_with_override.overrides_for_day(14)
print(f"  Overrides: {[o.category for o in overrides_day14]}")

# ── 8. apply_all convenience ──────────────────────────────────────────────────
print("\napply_all (day 12):")
after_all = engine.apply_all(
    df=orders.copy(),
    day=12,
    tables_dict=result.tables,
    date_columns=date_cols if date_cols else None,
)
print(f"  Output shape: {after_all.shape}")
print("\nDone.")
