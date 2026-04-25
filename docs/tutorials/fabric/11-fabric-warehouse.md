# Tutorial 11: Fabric Warehouse

Load synthetic data into a Microsoft Fabric Warehouse using `FabricSqlDatabaseWriter` with auto-generated DDL and bulk inserts.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle[fabric-sql]` (includes `pyodbc` and `azure-identity`)
- ODBC Driver 18 for SQL Server installed
- Completed [Tutorial 10: Fabric Lakehouse](10-fabric-lakehouse.md)
- A Fabric Warehouse endpoint (or Azure SQL Database for local testing)
- `az login` completed for Azure CLI authentication

## What You'll Learn

- How to configure `FabricSqlDatabaseWriter` with different authentication methods
- How to preview auto-generated T-SQL DDL before executing it
- How to export SQL INSERT scripts for offline review or version control
- How to write directly to a Fabric Warehouse using `create_insert` and other write modes
- How Fabric Warehouse limitations (no IDENTITY, no enforced PKs) are handled automatically

---

## Step 1: Generate Retail Data

Start by generating a retail dataset. Before loading to a warehouse, it helps to understand the schema -- column types, primary keys, and foreign key relationships -- since this metadata drives DDL generation.

```python
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()
result = spindle.generate(domain=RetailDomain(), scale="small", seed=42)

print(result.summary())
print(f"\nTables: {result.table_names}")
print(f"FK integrity check: {len(result.verify_integrity())} errors")
```

## Step 2: Configure the Writer

Set up `FabricSqlDatabaseWriter` with a connection string pointing to your Fabric Warehouse. Authentication uses Azure CLI by default.

```python
from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

CONNECTION_STRING = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=YOUR_WAREHOUSE.datawarehouse.fabric.microsoft.com;"
    "Database=YOUR_WAREHOUSE;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

writer = FabricSqlDatabaseWriter(
    connection_string=CONNECTION_STRING,
    auth_method="cli",  # Change to "msi" in Fabric Notebooks
)
```

Spindle supports four authentication methods:

| Method | Best For | Usage |
|--------|----------|-------|
| `cli` | Local development | Uses your `az login` token |
| `msi` | Fabric Notebooks | Uses Managed Identity |
| `spn` | CI/CD pipelines | Uses Service Principal credentials |
| `sql` | On-prem SQL Server | Uses SQL username/password |

## Step 3: Preview the Generated DDL

Use `create_ddl()` to generate the T-SQL `CREATE TABLE` statements without executing them. This lets you review the DDL before running it against your warehouse.

```python
ddl = writer.create_ddl(result, schema_name="dbo", dialect="tsql")
print(ddl[:2000])
```

Spindle infers column types from its schema metadata (not just pandas dtypes), producing proper `NVARCHAR`, `DECIMAL(10,2)`, `DATETIME2`, and `BIT` types. Fabric Warehouse does not support `IDENTITY` columns or enforced `PRIMARY KEY` constraints, so Spindle emits plain `INT NOT NULL` instead of `INT IDENTITY(1,1)`.

## Step 4: Generate SQL INSERT Scripts

Use `to_sql()` to write INSERT statements to `.sql` files. This is useful for offline review, version control, or loading via other tools. The method handles NULL values, datetime formatting, and string escaping automatically.

```python
from pathlib import Path

sql_dir = Path("warehouse_sql_output")
sql_files = result.to_sql(sql_dir)

for f in sql_files:
    size_kb = f.stat().st_size / 1024
    print(f"  {f.name} ({size_kb:.1f} KB)")

# Preview the first file
first_file = sql_files[0]
with open(first_file) as fh:
    content = fh.read()
print(f"\n--- Preview of {first_file.name} (first 1000 chars) ---")
print(content[:1000])
```

Each `.sql` file contains the `CREATE TABLE` DDL followed by batched `INSERT INTO` statements.

## Step 5: Write to Fabric Warehouse

Execute `writer.write()` with `mode="create_insert"` to DROP existing tables, CREATE new ones, and INSERT all data. Tables are written in dependency order (parents before children).

```python
write_result = writer.write(
    result,
    schema_name="dbo",
    mode="create_insert",   # DROP + CREATE + INSERT (full reset)
    batch_size=1000,        # Rows per INSERT batch
)

print(write_result.summary())
print(f"\nSuccess: {write_result.success}")
```

Spindle provides four write modes for different scenarios:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `create_insert` | DROP + CREATE + INSERT | Full reset for dev/test |
| `insert_only` | INSERT into existing tables | Load into pre-created schema |
| `truncate_insert` | TRUNCATE + INSERT | Keep schema, reset data |
| `append` | INSERT without truncating | Incremental loads |

---

> **Run It Yourself**
>
> - Notebook: [`F02_warehouse_dimensional.ipynb`](../../../examples/notebooks/fabric-scenarios/F02_warehouse_dimensional.ipynb)

---

## Related

- [Fabric Warehouse guide](../../guides/fabric-warehouse.md) -- the condensed reference for warehouse writes and DDL generation

---

## Next Step

[Tutorial 12: Fabric Streaming](12-fabric-streaming.md) -- stream synthetic events to Fabric Eventstream with burst windows and anomaly injection.
