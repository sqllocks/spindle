# Tutorial 10: Fabric Lakehouse

Write synthetic data to a Microsoft Fabric Lakehouse as both Parquet files and Delta Lake tables.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle` and `pip install deltalake`
- Completed [Tutorial 01: Hello, Spindle!](../beginner/01-hello-spindle.md)
- A Microsoft Fabric workspace with a Lakehouse (for deployment; local simulation works without one)

## What You'll Learn

- How to generate retail data and write it to Parquet files (simulating the Lakehouse Files section)
- How to write the same data to Delta Lake format (simulating the Lakehouse Tables section)
- How to inspect the resulting file structure and verify round-trip fidelity
- How partitioning works in a medallion folder layout

---

## Step 1: Generate Retail Data

Spindle's `Spindle.generate()` produces a complete relational dataset. We will use the `small` scale, which gives enough data to see realistic file sizes without waiting long.

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

print(result.summary())
```

The `result` object holds every generated table. You can inspect table names with `result.table_names` and access DataFrames through `result.tables["customer"]`.

## Step 2: Write to Parquet (Lakehouse Files)

In a real Fabric Lakehouse, the **Files** section is the raw landing zone where upstream data arrives as Parquet or CSV. One call writes every table to its own Parquet file:

```python
files = result.to_parquet("./lakehouse_demo/Files/landing/retail/")

print(f"Wrote {len(files)} Parquet files.")
for f in files:
    print(f"  {f}")
```

The directory structure mirrors what you would see inside a Lakehouse: `Files/landing/retail/customer.parquet`, `Files/landing/retail/order.parquet`, and so on.

## Step 3: Inspect the File Structure

Understanding file sizes and layout helps you estimate storage costs, plan partition strategies, and verify that your data landed where expected.

```python
from pathlib import Path

print("=== Lakehouse File Structure ===")
for f in sorted(Path("./lakehouse_demo").rglob("*.parquet")):
    print(f"  {f.relative_to('./lakehouse_demo')}: {f.stat().st_size:,} bytes")
```

## Step 4: Write to Delta Lake (Lakehouse Tables)

Fabric Lakehouse **Tables** are Delta Lake tables. Delta adds ACID transactions, time travel, and schema enforcement on top of Parquet. Writing Delta locally lets you test downstream notebooks that rely on `MERGE`, `UPDATE`, or time-travel queries.

```python
delta_files = result.to_delta("./lakehouse_demo/Tables/retail/")

print(f"Wrote {len(delta_files)} Delta tables.")
for f in delta_files:
    print(f"  {f}")

# Show the Delta log structure for the first table
delta_dirs = sorted(Path("./lakehouse_demo/Tables/retail/").iterdir())
if delta_dirs:
    first_table = delta_dirs[0]
    print(f"\n=== Delta structure for {first_table.name} ===")
    for item in sorted(first_table.rglob("*")):
        print(f"  {item.relative_to(first_table)}")
```

Each Delta table directory contains a `_delta_log/` folder with JSON transaction logs alongside the Parquet data files.

## Step 5: Verify Round-Trip Fidelity

Read the Parquet files back into DataFrames and compare row counts to the originals. If you lose rows or corrupt data types during export, downstream tests will be unreliable.

```python
import pandas as pd
from pathlib import Path

print("=== Round-Trip Verification ===")
parquet_dir = Path("./lakehouse_demo/Files/landing/retail/")
for pf in sorted(parquet_dir.glob("*.parquet")):
    table_name = pf.stem
    df_read = pd.read_parquet(pf)
    original_rows = len(result.tables[table_name])
    read_rows = len(df_read)
    status = "MATCH" if original_rows == read_rows else "MISMATCH"
    print(f"  {table_name}: original={original_rows}, read_back={read_rows} [{status}]")

print("\nRound trip complete!")
```

## Step 6: Medallion Folder Layout

When building a full medallion pipeline, organize your Lakehouse into three tiers. This is the folder structure you would create:

```python
from pathlib import Path

base_dir = Path("medallion_demo")
bronze_dir = base_dir / "bronze" / "retail"
silver_dir = base_dir / "silver" / "retail"
gold_dir   = base_dir / "gold" / "retail"

for d in [bronze_dir, silver_dir, gold_dir]:
    d.mkdir(parents=True, exist_ok=True)

# Write raw data to bronze
paths = result.to_parquet(bronze_dir)
print(f"Bronze: wrote {len(paths)} Parquet files to {bronze_dir}/")
for p in paths:
    print(f"  {p.name} ({p.stat().st_size / 1024:.1f} KB)")
```

The bronze layer holds raw data as it arrived. Silver and gold tiers are covered in [Tutorial 13: Medallion Architecture](13-medallion.md).

---

> **Run It Yourself**
>
> - Notebook: [`T08_fabric_lakehouse.ipynb`](../../../examples/notebooks/intermediate/T08_fabric_lakehouse.ipynb)
> - Notebook: [`F01_medallion_architecture.ipynb`](../../../examples/notebooks/fabric-scenarios/F01_medallion_architecture.ipynb)

---

## Related

- [Fabric Lakehouse guide](../../guides/fabric-lakehouse.md) -- the condensed reference for Lakehouse writes

---

## Next Step

[Tutorial 11: Fabric Warehouse](11-fabric-warehouse.md) -- load synthetic data into a Fabric Warehouse using T-SQL DDL and INSERT statements.
