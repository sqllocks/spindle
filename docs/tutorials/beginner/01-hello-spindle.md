# Tutorial 01: Hello, Spindle!

Generate your first synthetic dataset, verify its integrity, and inspect the results.

---

## Prerequisites

- Python 3.10 or later
- Basic familiarity with pandas DataFrames

## What You'll Learn

- How to install Spindle from PyPI
- How to generate a complete relational dataset with one function call
- How to print a summary of the generated data
- How to access individual tables as pandas DataFrames
- How to verify that all foreign-key relationships hold

---

## Step 1: Install Spindle

Spindle is distributed as a single pip package. If you are running inside a Microsoft Fabric notebook, uncomment the `%pip` line; otherwise use a standard terminal.

```bash
pip install sqllocks-spindle
```

In a Fabric notebook cell:

```python
# %pip install sqllocks-spindle
```

## Step 2: Import and Generate

Spindle's entry point is the `Spindle` class, and each business domain is a separate class you pass in. For this first tutorial we will use `RetailDomain`, which models customers, products, orders, and more.

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle.generate(
    domain=RetailDomain(),
    scale="fabric_demo",
    seed=42,
)

print(result.summary())
```

A few things to notice:

- **`scale="fabric_demo"`** selects a small, fast preset -- perfect for tutorials and demos.
- **`seed=42`** makes the output fully reproducible. Run it twice and you get identical data.
- **`result`** is a `GenerationResult` that holds every generated table plus metadata.

The `summary()` call prints a table showing every generated table and its row count.

## Step 3: Access Tables

Every table is a standard pandas DataFrame. You can access them through the `result.tables` dictionary or with dict-style indexing directly on the result:

```python
customers = result.tables["customers"]

print("=== Customers -- First 5 Rows ===")
print(customers.head())
print(f"\nShape: {customers.shape[0]} rows x {customers.shape[1]} columns")
print(f"\n=== Column Data Types ===")
print(customers.dtypes)
```

Because these are plain DataFrames, you can immediately filter, group, join, or plot them with the full pandas API.

## Step 4: Verify Foreign-Key Integrity

Synthetic data is only useful if it is relationally valid. If an order references a customer ID that does not exist, downstream queries and dashboards will silently lose data. Spindle guarantees referential integrity by design, and `verify_integrity()` lets you prove it:

```python
violations = result.verify_integrity()

assert len(violations) == 0, f"Found {len(violations)} FK violations!"

print(f"Checked all foreign-key relationships.")
print(f"Violations found: {len(violations)}")
print("All FK relationships verified!")
```

An empty list means every foreign key in every table points to a valid parent row.

## Step 5: Export to CSV

One call writes every table to its own CSV file:

```python
import os

output_dir = "./spindle_output"
result.to_csv(output_dir)

files = sorted(os.listdir(output_dir))
print(f"Exported {len(files)} CSV files to {output_dir}/\n")
for f in files:
    size = os.path.getsize(os.path.join(output_dir, f))
    print(f"  {f} ({size:,} bytes)")
```

From here you can open the CSVs in Power BI, Excel, another notebook, or load them into a Lakehouse.

---

> **Run It Yourself**
>
> - Notebook: [`T01_hello_spindle.ipynb`](../../../examples/notebooks/quickstart/T01_hello_spindle.ipynb)
> - Script: [`01_hello_world.py`](../../../examples/scenarios/01_hello_world.py)

---

## Related

- [Quickstart guide](../../getting-started/quickstart.md) -- the condensed reference version of this workflow

---

## Next Step

[Tutorial 02: Explore All Domains](02-explore-domains.md) -- see all 13 built-in domains and compare their schemas.
