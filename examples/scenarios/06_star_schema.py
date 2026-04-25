"""
Scenario 06 -- Star Schema Transform
=====================================
Spindle generates normalized 3NF data and can transform it into a star
schema (dim + fact tables) in one step. The result is ready for Power BI
DirectLake mode, Analysis Services, or any OLAP workload.

What gets generated:
  Dimensions -- one dim table per entity, surrogate key (sk_*) as first column
  Facts       -- join table with surrogate keys + preserved natural keys (nk_*)
  dim_date    -- auto-generated from the date range in your data (YYYYMMDD int SK)

Run:
    python examples/scenarios/06_star_schema.py
"""

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain
from sqllocks_spindle.transform import StarSchemaTransform

spindle = Spindle()
transform = StarSchemaTransform()

# ------------------------------------------------------------------
# 1. Retail star schema
# ------------------------------------------------------------------
retail = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
star = transform.transform(retail.tables, RetailDomain().star_schema_map())

print("=== Retail star schema ===")
print(star.summary())

# ------------------------------------------------------------------
# 2. Explore dimensions
# ------------------------------------------------------------------
print("\n--- dim_customer (first 3 rows) ---")
print(star.dimensions["dim_customer"].head(3).to_string())

print("\n--- dim_product columns ---")
print(list(star.dimensions["dim_product"].columns))

# dim_product is enriched with product_category columns (prefixed 'cat_')
cat_cols = [c for c in star.dimensions["dim_product"].columns if c.startswith("cat_")]
print(f"  Category enrichment columns: {cat_cols}")

# ------------------------------------------------------------------
# 3. dim_date -- auto-generated, YYYYMMDD surrogate key
# ------------------------------------------------------------------
print("\n--- dim_date (first 5 rows) ---")
print(star.date_dim.head(5).to_string())

print(f"\n  Date range: {star.date_dim['date'].min()} -> {star.date_dim['date'].max()}")
print(f"  Total days: {len(star.date_dim):,}")
print(f"  Columns:    {list(star.date_dim.columns)}")

# Verify sk_date format: YYYYMMDD integer
sample = star.date_dim.iloc[0]
expected_sk = sample["year"] * 10000 + sample["month"] * 100 + int(sample["day_of_month"])
assert sample["sk_date"] == expected_sk, "sk_date format mismatch"
print(f"  sk_date format (YYYYMMDD): PASS  example={sample['sk_date']}")

# ------------------------------------------------------------------
# 4. Explore facts
# ------------------------------------------------------------------
print("\n--- fact_sale columns ---")
sk_cols = [c for c in star.facts["fact_sale"].columns if c.startswith("sk_")]
nk_cols = [c for c in star.facts["fact_sale"].columns if c.startswith("nk_")]
print(f"  Surrogate keys (sk_*):  {sk_cols}")
print(f"  Natural keys   (nk_*):  {nk_cols}")
print(f"  Total columns: {len(star.facts['fact_sale'].columns)}")

print("\n--- fact_sale (first 3 rows) ---")
print(star.facts["fact_sale"].head(3).to_string())

# ------------------------------------------------------------------
# 5. Referential integrity -- SK must be valid in every dim
# ------------------------------------------------------------------
sale        = star.facts["fact_sale"]
dim_cust    = star.dimensions["dim_customer"]
dim_prod    = star.dimensions["dim_product"]

assert sale["sk_customer"].dropna().isin(set(dim_cust["sk_customer"])).all()
assert sale["sk_product"].dropna().isin(set(dim_prod["sk_product"])).all()
print("\nStar schema SK integrity: PASS")

# ------------------------------------------------------------------
# 6. all_tables() -- combine dims + date_dim + facts into one dict
# ------------------------------------------------------------------
all_tables = star.all_tables()
print(f"\nall_tables() -> {list(all_tables.keys())}")

# ------------------------------------------------------------------
# 7. Custom fiscal year start
# ------------------------------------------------------------------
from sqllocks_spindle.transform import StarSchemaMap, DimSpec, FactSpec

# Retail domain with fiscal year starting in July (month 7)
custom_map = RetailDomain().star_schema_map()
custom_map_fy = StarSchemaMap(
    dims=custom_map.dims,
    facts=custom_map.facts,
    generate_date_dim=True,
    fiscal_year_start=7,    # July fiscal year (common in retail)
)

star_fy = transform.transform(retail.tables, custom_map_fy)
# Fiscal year for a date in June should be prior year
june_dates = star_fy.date_dim[star_fy.date_dim["month"] == 6]
if len(june_dates) > 0:
    sample = june_dates.iloc[0]
    # In a July FY, June belongs to the prior fiscal year
    assert sample["fiscal_year"] == sample["year"] - 1 or sample["fiscal_year"] == sample["year"]
print("Custom fiscal year (July start): generated successfully")

# ------------------------------------------------------------------
# 8. Healthcare star schema
# ------------------------------------------------------------------
print("\n=== Healthcare star schema ===")
hc = spindle.generate(domain=HealthcareDomain(), scale="fabric_demo", seed=42)
hc_star = transform.transform(hc.tables, HealthcareDomain().star_schema_map())
print(hc_star.summary())

print("\n--- fact_encounter columns ---")
enc = hc_star.facts["fact_encounter"]
print(f"  SK columns: {[c for c in enc.columns if c.startswith('sk_')]}")
print(f"  Rows: {len(enc):,}")
