# Tutorial 05: Star Schema

Transform normalized 3NF data into a dimensional star schema with surrogate keys, auto-generated date dimensions, and Parquet export.

## Prerequisites

- Completed [Tutorial 04: Output Formats](../beginner/04-output-formats.md) (or equivalent experience)
- Familiarity with `Spindle.generate()` and domain objects
- Basic understanding of dimensional modeling (dimensions vs. facts)

## What You'll Learn

- How to use `StarSchemaTransform` to convert 3NF tables into a star schema
- How surrogate keys (`sk_*`) and natural keys (`nk_*`) work
- How Spindle auto-generates `dim_date` from your data's date range
- How to export the star schema to Parquet files
- How to verify referential integrity across dimension and fact tables

## Time Estimate

**~20 minutes**

---

## Step 1 -- Generate 3NF Data and Transform to Star Schema

Spindle generates normalized third-normal-form data by default. Many analytics tools -- Power BI, Fabric DirectLake, traditional data warehouses -- work best with a star schema (dimension + fact tables). The `StarSchemaTransform` class bridges the gap.

Every domain ships with a `star_schema_map()` method that defines how 3NF tables map to dimensions and facts: which columns become surrogate keys, which become measures, and how date columns link to `dim_date`. One call handles the entire transformation.

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.transform import StarSchemaTransform

# Generate 3NF retail data
spindle = Spindle()
result = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

# Transform to star schema
transform = StarSchemaTransform()
star = transform.transform(result.tables, RetailDomain().star_schema_map())

print(star.summary())
```

The summary shows every dimension and fact table with row counts:

```
Star Schema Result
========================================
DIMENSIONS:
  dim_customer                     200 rows  9 cols
  dim_product                      100 rows  10 cols
  dim_store                        150 rows  6 cols
  dim_promotion                    200 rows  7 cols
  dim_date                       1,352 rows  14 cols
FACTS:
  fact_sale                      2,500 rows  20 cols
  fact_return                      170 rows  15 cols
```

## Step 2 -- Explore Dimension Tables

Each dimension table has a surrogate key (`sk_*`) as its first column. Surrogate keys are essential for star schema integrity -- they decouple the warehouse from source-system IDs. The original primary keys are preserved as natural keys so you can trace any row back to its origin.

```python
# Inspect dimension tables
for name, df in star.dimensions.items():
    sk_cols = [c for c in df.columns if c.startswith("sk_")]
    print(f"  {name:<20} {len(df):>6} rows  |  SK: {sk_cols}")
```

```
Dimension Tables:
  dim_customer            200 rows  |  SK: ['sk_customer']
  dim_product             100 rows  |  SK: ['sk_product']
  dim_store               150 rows  |  SK: ['sk_store']
  dim_promotion           200 rows  |  SK: ['sk_promotion']
```

Look at `dim_product` -- notice the `cat_` prefixed columns. When a dimension joins data from a parent table (like product categories), the parent columns are flattened into the dimension with a prefix:

```python
print(star.dimensions["dim_product"].head(3).to_string(index=False))
```

```
 sk_product  product_id  category_id  product_name  unit_price product_status  cost cat_category_name cat_parent_category_id  cat_level
          1           1            7  Jump Rope...       19.48         active  9.46    Bedding & Bath                   None          1
          2           2           40  Dried Mang...      16.21         active 10.97  Garden & Outdoor                     10          3
          3           3            8  Almond But...       4.75         active  2.82   Women's Apparel                   None          1
```

## Step 3 -- Explore Fact Tables

Fact tables contain surrogate keys for joining to dimensions, preserved natural keys for traceability, and measure columns (quantities, amounts). The `sk_date` column uses `YYYYMMDD` integer format, which enables efficient range filtering and direct joins to `dim_date`.

```python
for name, df in star.facts.items():
    sk_cols = [c for c in df.columns if c.startswith("sk_")]
    print(f"  {name:<20} {len(df):>6} rows  |  SK joins: {sk_cols}")
```

```
  fact_sale              2500 rows  |  SK joins: ['sk_customer', 'sk_product', 'sk_store', 'sk_promotion', 'sk_date']
  fact_return             170 rows  |  SK joins: ['sk_customer', 'sk_store', 'sk_date']
```

You can verify that every surrogate key in the fact table references a valid dimension record:

```python
sale     = star.facts["fact_sale"]
dim_cust = star.dimensions["dim_customer"]
dim_prod = star.dimensions["dim_product"]

assert sale["sk_customer"].dropna().isin(set(dim_cust["sk_customer"])).all()
assert sale["sk_product"].dropna().isin(set(dim_prod["sk_product"])).all()
print("Star schema SK integrity: PASS")
```

## Step 4 -- The Auto-Generated dim_date

`StarSchemaTransform` automatically generates a `dim_date` table from the date range found in your fact data. Every date dimension needs calendar attributes, and Spindle builds all of them for you:

```python
dim_date = star.date_dim

print(f"Date range: {dim_date['date'].min()} to {dim_date['date'].max()}")
print(f"Total days: {len(dim_date):,}")
print(f"Columns: {list(dim_date.columns)}")
```

```
Date range: 2022-05-06 to 2026-01-16
Total days: 1,352

Columns (14):
  ['sk_date', 'date', 'year', 'quarter', 'month', 'month_name',
   'week_of_year', 'day_of_month', 'day_of_week', 'day_of_week_name',
   'is_weekend', 'is_weekday', 'fiscal_year', 'fiscal_quarter']
```

The `sk_date` key uses `YYYYMMDD` integer format (e.g., `20220506`). You can customize the fiscal year start month:

```python
from sqllocks_spindle.transform import StarSchemaMap

custom_map = RetailDomain().star_schema_map()
custom_map_fy = StarSchemaMap(
    dims=custom_map.dims,
    facts=custom_map.facts,
    generate_date_dim=True,
    fiscal_year_start=7,    # July fiscal year (common in retail)
)

star_fy = transform.transform(result.tables, custom_map_fy)
```

With a July fiscal year, a date in June belongs to the *prior* fiscal year -- matching how many retail organizations report.

## Step 5 -- Export to Parquet

Write every dimension, fact, and date table to individual Parquet files. Parquet is the standard columnar format for Lakehouse, Spark, and Power BI DirectLake workloads.

```python
import os

output_dir = "./spindle_star_output"
os.makedirs(output_dir, exist_ok=True)

# Write all star schema tables to Parquet
for table_name, df in star.all_tables().items():
    path = os.path.join(output_dir, f"{table_name}.parquet")
    df.to_parquet(path, index=False)

files = sorted(os.listdir(output_dir))
print(f"Exported {len(files)} Parquet files to {output_dir}/")
for f in files:
    size = os.path.getsize(os.path.join(output_dir, f))
    print(f"  {f:<35} {size:>8,} bytes")
```

```
Exported 7 Parquet files to ./spindle_star_output/

  dim_customer.parquet                  15,112 bytes
  dim_date.parquet                      24,512 bytes
  dim_product.parquet                   11,291 bytes
  dim_promotion.parquet                 11,698 bytes
  dim_store.parquet                      8,498 bytes
  fact_return.parquet                   18,237 bytes
  fact_sale.parquet                     86,066 bytes
```

The `all_tables()` method returns every table (dimensions + facts + date) in one dictionary -- convenient for bulk operations.

## Step 6 -- Healthcare Star Schema

The star schema transform works with any domain that provides a `star_schema_map()`. Here is the same pattern applied to the healthcare domain:

```python
from sqllocks_spindle import HealthcareDomain

hc = spindle.generate(domain=HealthcareDomain(), scale="fabric_demo", seed=42)
hc_star = transform.transform(hc.tables, HealthcareDomain().star_schema_map())
print(hc_star.summary())
```

The healthcare star schema produces tables like `dim_patient`, `dim_provider`, `dim_facility`, and `fact_encounter` -- the same pattern, different domain.

---

> **Run It Yourself**
>
> - Notebook: [`T06_star_schema_export.ipynb`](../../../examples/notebooks/quickstart/T06_star_schema_export.ipynb)
> - Script: [`06_star_schema.py`](../../../examples/scenarios/06_star_schema.py)

## Related

- [Star Schema Guide](../../guides/star-schema.md) -- deep dive into dimension specs, fact specs, and advanced configuration

## Next Step

Continue to [Tutorial 06: Streaming](06-streaming.md) to learn how to emit events with rate limiting, burst windows, and anomaly injection.
