# Tutorial 13: Medallion Architecture

Build a complete Bronze/Silver/Gold lakehouse pipeline with synthetic data -- from messy landing zone to curated star schema.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle`
- Completed [Tutorial 10: Fabric Lakehouse](10-fabric-lakehouse.md)
- Basic understanding of the medallion architecture pattern (Bronze = raw, Silver = validated, Gold = analytics-ready)

## What You'll Learn

- How to generate medium-scale retail data and write it to a bronze landing zone
- How to inject chaos (schema drift, orphan FKs) to simulate real-world messy upstream data
- How to run validation gates to catch data quality issues
- How to quarantine bad records and promote clean data to silver
- How to transform silver data into a gold star schema with surrogate keys and a date dimension
- How to organize the full medallion folder structure

---

## Step 1: Generate Retail Data at Medium Scale

Generate a full retail dataset -- customers, products, stores, orders, and order lines -- at medium scale. Real medallion pipelines ingest messy upstream data, so Spindle gives us realistic relational data with proper FK relationships to build and test each tier.

```python
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()
result = spindle.generate(domain=RetailDomain(), scale="medium", seed=42)

print(result.summary())
print(f"\nFK integrity errors: {len(result.verify_integrity())}")
```

At medium scale this produces approximately 1.9 million rows across 9 tables, with zero FK integrity errors.

## Step 2: Bronze -- Write Raw Parquet with Chaos Injection

The bronze layer should contain data exactly as it arrived -- warts and all. First write the clean generated data to Parquet, then inject chaos to simulate real-world upstream issues. Testing your pipeline against only clean data gives you false confidence.

```python
from pathlib import Path
from sqllocks_spindle.chaos.config import ChaosConfig
from sqllocks_spindle.chaos.engine import ChaosEngine

# Set up medallion directory structure
base_dir = Path("medallion_demo")
bronze_dir = base_dir / "bronze" / "retail"
silver_dir = base_dir / "silver" / "retail"
gold_dir   = base_dir / "gold" / "retail"

for d in [bronze_dir, silver_dir, gold_dir]:
    d.mkdir(parents=True, exist_ok=True)

# Write clean data as raw Parquet (the "as-generated" landing)
paths = result.to_parquet(bronze_dir)
print(f"Bronze: wrote {len(paths)} Parquet files to {bronze_dir}/")
for p in paths:
    print(f"  {p.name} ({p.stat().st_size / 1024:.1f} KB)")
```

Now inject chaos. The `ChaosConfig` controls intensity and which categories fire. Here we enable schema drift (extra/missing columns) and referential chaos (orphan FK values):

```python
chaos_cfg = ChaosConfig(
    enabled=True,
    intensity="moderate",
    seed=99,
    warmup_days=0,
    chaos_start_day=0,
    categories={
        "value":       {"enabled": False, "weight": 0.0},
        "schema":      {"enabled": True, "weight": 0.15},
        "referential": {"enabled": True, "weight": 0.20},
        "temporal":    {"enabled": False, "weight": 0.0},
        "file":        {"enabled": False, "weight": 0.0},
        "volume":      {"enabled": False, "weight": 0.0},
    },
)
chaos = ChaosEngine(chaos_cfg)

# Apply schema chaos to each table
bronze_tables = {}
for table_name, df in result.tables.items():
    corrupted = chaos.drift_schema(df.copy(), day=10)
    bronze_tables[table_name] = corrupted
    extra_cols = len(corrupted.columns) - len(df.columns)
    print(f"  {table_name}: {len(corrupted)} rows, {extra_cols:+d} columns from schema drift")

# Inject referential chaos across the full table set
bronze_tables = chaos.inject_referential_chaos(bronze_tables, day=10)
print("\nBronze layer chaos injection complete (schema drift + orphan FKs).")
```

The chaos engine adds extra columns to some tables and introduces orphan foreign key values that reference non-existent parent rows -- exactly the kinds of issues you encounter with real upstream data.

## Step 3: Silver -- Clean and Validate with ValidationGates

The silver layer is your "validated, conformed" tier. Every record that makes it past the gates is trustworthy. Run the corrupted bronze data through Spindle's `GateRunner` with built-in validation gates.

```python
from sqllocks_spindle.validation.gates import (
    GateRunner, ValidationContext, GateResult
)

context = ValidationContext(
    tables=bronze_tables,
    schema=result.schema,
)

runner = GateRunner(gates=[
    "referential_integrity",
    "schema_conformance",
    "null_constraint",
    "unique_constraint",
])
gate_results = runner.run_all(context)

summary = GateRunner.summary(gate_results)
print(f"Gates run:   {summary['total_gates']}")
print(f"Passed:      {summary['passed']}")
print(f"Failed:      {summary['failed']}")
print(f"Total errors: {summary['total_errors']}")

for gr in gate_results:
    status = "PASS" if gr.passed else "FAIL"
    print(f"  [{status}] {gr.gate_name}: {len(gr.errors)} errors, {len(gr.warnings)} warnings")
```

Now quarantine bad records and promote clean data to silver. Drop chaos-injected extra columns to restore schema conformance, and isolate rows with null primary keys using the `QuarantineManager`.

```python
from sqllocks_spindle.validation.quarantine import QuarantineManager

quarantine_dir = base_dir / "quarantine"
qm = QuarantineManager(domain="retail")

silver_tables = {}
for table_name, bronze_df in bronze_tables.items():
    original_df = result.tables[table_name]
    original_cols = set(original_df.columns)
    bronze_cols = set(bronze_df.columns)

    # Drop chaos-injected extra columns
    extra_cols = bronze_cols - original_cols
    clean_df = bronze_df.drop(columns=list(extra_cols), errors="ignore")

    # Quarantine rows with null PKs
    pk_cols = result.schema.tables[table_name].primary_key
    if pk_cols:
        bad_mask = clean_df[pk_cols].isna().any(axis=1)
        if bad_mask.sum() > 0:
            bad_rows = clean_df[bad_mask]
            qm.quarantine_dataframe(
                bad_rows, quarantine_dir, run_id="bronze_v1",
                table_name=table_name, reason="Null primary key",
                gate_name="null_constraint",
            )
            clean_df = clean_df[~bad_mask]

    silver_tables[table_name] = clean_df
    print(f"  {table_name}: {len(bronze_df)} bronze -> {len(clean_df)} silver rows")

# Write silver to Parquet
for name, df in silver_tables.items():
    df.to_parquet(silver_dir / f"{name}.parquet", index=False)

print(f"\nSilver: wrote {len(silver_tables)} cleaned tables to {silver_dir}/")
```

## Step 4: Gold -- Transform to Star Schema

Use `StarSchemaTransform` to convert the cleaned silver tables into a dimensional model with surrogate keys, a date dimension, and fact/dim separation. This is optimized for Power BI and analytics queries.

```python
from sqllocks_spindle import (
    StarSchemaTransform, StarSchemaMap, DimSpec, FactSpec
)

schema_map = StarSchemaMap(
    dims={
        "dim_customer": DimSpec(
            source="customer", sk="sk_customer", nk="customer_id"
        ),
        "dim_product": DimSpec(
            source="product", sk="sk_product", nk="product_id"
        ),
        "dim_store": DimSpec(
            source="store", sk="sk_store", nk="store_id"
        ),
    },
    facts={
        "fact_order": FactSpec(
            primary="order",
            fk_map={
                "customer_id": "dim_customer",
                "store_id": "dim_store",
            },
            date_cols=["order_date"],
        ),
    },
    generate_date_dim=True,
)

transform = StarSchemaTransform()
star = transform.transform(silver_tables, schema_map)

print(star.summary())
```

This produces dimension tables (`dim_customer`, `dim_product`, `dim_store`, `dim_date`) and a `fact_order` table with surrogate keys replacing natural keys.

```python
# Write gold star schema tables to Parquet
gold_tables = star.all_tables()
for name, df in gold_tables.items():
    df.to_parquet(gold_dir / f"{name}.parquet", index=False)
    print(f"  {name}: {len(df):,} rows x {len(df.columns)} cols")

print(f"\nGold: wrote {len(gold_tables)} star schema tables to {gold_dir}/")
```

## Step 5: Inspect the Final Folder Structure

The completed medallion pipeline produces a clean folder layout:

```
medallion_demo/
  bronze/retail/    (9 raw Parquet files)
  silver/retail/    (9 cleaned Parquet files)
  gold/retail/      (5 star schema files: dim_customer, dim_product, dim_store, dim_date, fact_order)
  quarantine/       (bad records with full metadata for investigation)
```

```python
print("--- Medallion Directory Structure ---")
for tier in ["bronze", "silver", "gold", "quarantine"]:
    tier_path = base_dir / tier
    if tier_path.exists():
        files = list(tier_path.rglob("*"))
        data_files = [f for f in files if f.is_file() and not f.name.startswith(".")]
        print(f"  {tier}/  ({len(data_files)} files)")
        for f in sorted(data_files)[:5]:
            print(f"    {f.relative_to(base_dir)}")
```

---

> **Run It Yourself**
>
> - Notebook: [`F01_medallion_architecture.ipynb`](../../../examples/notebooks/fabric-scenarios/F01_medallion_architecture.ipynb)

---

## Related

- [Chaos guide](../../guides/chaos.md) -- full reference for all 6 chaos categories and intensity presets
- [Validation guide](../../guides/validation.md) -- all 8 built-in validation gates and the quarantine workflow
- [Star Schema guide](../../guides/star-schema.md) -- DimSpec, FactSpec, and date dimension configuration

---

## Next Step

[Tutorial 14: Scenario Packs](../advanced/14-scenario-packs.md) -- run pre-built YAML-defined end-to-end data generation workflows across 11 industry verticals.
