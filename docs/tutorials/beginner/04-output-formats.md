# Tutorial 04: Output Formats

Export synthetic data to CSV, TSV, JSON Lines, Parquet, Excel, and SQL INSERT statements.

---

## Prerequisites

- Completed [Tutorial 01: Hello Spindle](01-hello-spindle.md)
- For Parquet output: `pip install pyarrow`
- For Excel output: `pip install openpyxl`

## What You'll Learn

- How to use `PandasWriter` to export generated data in six formats
- The trade-offs of each format (when to use CSV vs. Parquet vs. SQL INSERT)
- How to preview exported files to verify their contents
- How to use CLI flags to choose output format without writing Python

---

## Overview: Six Output Formats

Spindle supports six output formats via the `PandasWriter` class. Each writes every table in the result to the specified directory.

| Format | Method | File pattern | Best for |
|--------|--------|-------------|----------|
| CSV | `to_csv()` | One `.csv` per table | Universal interchange |
| TSV | `to_tsv()` | One `.tsv` per table | `BULK INSERT` / `bcp` workflows |
| JSON Lines | `to_jsonl()` | One `.jsonl` per table | Event Hubs, Kafka, streaming |
| Parquet | `to_parquet()` | One `.parquet` per table | Fabric Lakehouse, analytics |
| Excel | `to_excel()` | Single `.xlsx` workbook | Sharing with non-technical users |
| SQL INSERT | `to_sql_inserts()` | One `.sql` per table | Direct database loading |

## Step 1: Generate the Data

Start by generating a retail dataset. We also import `PandasWriter`, which handles all format conversions.

```python
from pathlib import Path
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.output import PandasWriter

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
writer = PandasWriter()

output_dir = Path("./output")
```

## Step 2: CSV -- Universal Interchange

CSV is the simplest, most widely supported format. One file per table, comma-delimited.

```python
files = writer.to_csv(result.tables, output_dir / "csv")
print(f"CSV:        {len(files)} files")
print(f"            sample: {files[0].name}  ({files[0].stat().st_size:,} bytes)")
```

Use CSV when you need maximum compatibility -- virtually every tool on the planet reads CSV.

## Step 3: TSV -- Tab-Delimited

TSV is identical to CSV but uses tab characters as delimiters. This is the format expected by SQL Server `BULK INSERT` and the `bcp` utility.

```python
files = writer.to_tsv(result.tables, output_dir / "tsv")
print(f"TSV:        {len(files)} files")
```

## Step 4: JSON Lines -- Streaming-Friendly

JSON Lines stores one JSON object per line. Each line is a self-contained record, making it ideal for streaming systems like Azure Event Hubs and Kafka.

```python
files = writer.to_jsonl(result.tables, output_dir / "jsonl")
print(f"JSON Lines: {len(files)} files")

# Preview the first record from the orders file
orders_jsonl = next(f for f in files if "order" in f.stem and "line" not in f.stem)
first_line = orders_jsonl.read_text(encoding="utf-8").splitlines()[0]
print(f"            first record: {first_line[:120]}...")
```

## Step 5: Parquet -- Columnar Analytics

Parquet is a columnar format that is the standard for Fabric Lakehouse, Spark, and most analytics engines. It preserves data types natively and compresses far better than CSV.

```python
try:
    files = writer.to_parquet(result.tables, output_dir / "parquet")
    print(f"Parquet:    {len(files)} files")
    print(f"            sample: {files[0].name}  ({files[0].stat().st_size:,} bytes)")
except ImportError:
    print("Parquet:    skipped (pip install pyarrow)")
```

Requires `pyarrow`. If you are working in Microsoft Fabric, `pyarrow` is already installed.

## Step 6: Excel -- Single Workbook

Writes all tables as sheets in a single `.xlsx` workbook. Useful for sharing data with stakeholders who live in Excel.

```python
try:
    files = writer.to_excel(result.tables, output_dir / "excel", single_workbook=True)
    print(f"Excel:      {len(files)} file(s) -- {files[0].name}  ({files[0].stat().st_size:,} bytes)")
except ImportError:
    print("Excel:      skipped (pip install openpyxl)")
```

Requires `openpyxl`. The `single_workbook=True` flag puts every table on its own sheet inside one file.

## Step 7: SQL INSERT -- Direct Database Loading

Generates ANSI SQL `INSERT INTO` statements that work with SQL Server, PostgreSQL, and other relational databases. You can optionally specify a schema name to prefix the table names.

```python
files = writer.to_sql_inserts(result.tables, output_dir / "sql", schema_name="retail")
print(f"SQL INSERT: {len(files)} files")

# Preview the first few lines of the customer INSERT script
customer_sql = next(f for f in files if f.stem == "customer")
lines = customer_sql.read_text(encoding="utf-8").splitlines()
for line in lines[:5]:
    print(f"            {line}")
```

## CLI Alternative

You can also select the output format from the command line without writing any Python:

```bash
# CSV (default)
spindle generate retail --scale fabric_demo --seed 42 --format csv --output ./output/

# Parquet
spindle generate retail --scale fabric_demo --seed 42 --format parquet --output ./output/

# SQL INSERT with schema prefix
spindle generate retail --scale fabric_demo --seed 42 --format sql --output ./output/
```

The `--format` flag accepts: `csv`, `tsv`, `jsonl`, `parquet`, `excel`, `sql`.

---

> **Run It Yourself**
>
> - Script: [`04_output_formats.py`](../../../examples/scenarios/04_output_formats.py)

---

## Related

- [Quickstart guide](../../getting-started/quickstart.md) -- covers both Python and CLI generation workflows

---

## Next Step

[Tutorial 05: Star Schema](../intermediate/05-star-schema.md) -- transform 3NF data into a dimensional model with surrogate keys.
