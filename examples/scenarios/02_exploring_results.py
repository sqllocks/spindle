"""
Scenario 02 -- Exploring Results
================================
The GenerationResult object exposes everything you need to inspect,
iterate, and validate the generated data.

Run:
    python examples/scenarios/02_exploring_results.py
"""

from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

# ------------------------------------------------------------------
# 1. Summary
# ------------------------------------------------------------------
print("=== summary() ===")
print(result.summary())

# ------------------------------------------------------------------
# 2. Row counts per table
# ------------------------------------------------------------------
print("\n=== row_counts ===")
for table, count in result.row_counts.items():
    print(f"  {table:<20} {count:>6,} rows")

# ------------------------------------------------------------------
# 3. Generation order (respects foreign key dependencies)
# ------------------------------------------------------------------
print("\n=== generation_order ===")
for i, table in enumerate(result.generation_order, 1):
    print(f"  {i}. {table}")

# ------------------------------------------------------------------
# 4. Access tables -- three equivalent ways
# ------------------------------------------------------------------
df1 = result["customer"]          # dict-style
df2 = result.tables["customer"]   # explicit .tables dict
print(f"\nresult['customer'] == result.tables['customer']: {df1.equals(df2)}")

# ------------------------------------------------------------------
# 5. Iterate all tables
# ------------------------------------------------------------------
print("\n=== all tables ===")
for table_name, df in result.tables.items():
    print(f"  {table_name:<20} {df.shape[0]:>6,} rows  {df.shape[1]:>3} cols")

# ------------------------------------------------------------------
# 6. Referential integrity check
# ------------------------------------------------------------------
errors = result.verify_integrity()
if errors:
    print(f"\nFK violations ({len(errors)}):")
    for e in errors:
        print(f"  {e}")
else:
    print("\nReferential integrity: PASS -- 0 FK violations")

# ------------------------------------------------------------------
# 7. Generation metadata
# ------------------------------------------------------------------
print(f"\nGeneration time:  {result.elapsed_seconds:.3f}s")
print(f"Total rows:       {sum(result.row_counts.values()):,}")
print(f"Tables generated: {len(result.tables)}")

# ------------------------------------------------------------------
# 8. Spot-check a few distributions
# ------------------------------------------------------------------
print("\n=== Loyalty tier distribution ===")
print(
    result["customer"]["loyalty_tier"]
    .value_counts(normalize=True)
    .mul(100)
    .round(1)
    .rename("pct %")
)

print("\n=== Order status distribution ===")
print(
    result["order"]["status"]
    .value_counts(normalize=True)
    .mul(100)
    .round(1)
    .rename("pct %")
)
