"""
Scenario 04 -- Output Formats
==============================
Spindle supports six output formats via PandasWriter. All write every
table in the result to the specified directory.

Formats:
  CSV         -- universal, one file per table
  TSV         -- tab-delimited, compatible with BULK INSERT / bcp
  JSON Lines  -- one JSON object per row, ideal for Event Hubs / Kafka
  Parquet     -- columnar, optimal for Fabric Lakehouse and analytics
  Excel       -- single .xlsx workbook with one sheet per table
  SQL INSERT  -- standard ANSI INSERT statements (SQL Server, PostgreSQL, etc.)

Run:
    pip install sqllocks-spindle pyarrow openpyxl
    python examples/scenarios/04_output_formats.py
"""

import tempfile
from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.output import PandasWriter

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
writer = PandasWriter()

# Use a temp directory so this script leaves no files behind.
# Replace with a real path when you want to keep the output:
#   output_dir = Path("./output")

with tempfile.TemporaryDirectory() as tmp:
    base = Path(tmp)

    # ------------------------------------------------------------------
    # 1. CSV -- one .csv per table
    # ------------------------------------------------------------------
    files = writer.to_csv(result.tables, base / "csv")
    print(f"CSV:        {len(files)} files")
    print(f"            sample: {files[0].name}  ({files[0].stat().st_size:,} bytes)")

    # ------------------------------------------------------------------
    # 2. TSV -- tab-delimited
    # ------------------------------------------------------------------
    files = writer.to_tsv(result.tables, base / "tsv")
    print(f"TSV:        {len(files)} files")

    # ------------------------------------------------------------------
    # 3. JSON Lines -- one JSON object per row
    # ------------------------------------------------------------------
    files = writer.to_jsonl(result.tables, base / "jsonl")
    print(f"JSON Lines: {len(files)} files")
    # Preview the first line of the orders file
    orders_jsonl = next(f for f in files if "order" in f.stem and "line" not in f.stem)
    first_line = orders_jsonl.read_text(encoding="utf-8").splitlines()[0]
    print(f"            first record: {first_line[:120]}...")

    # ------------------------------------------------------------------
    # 4. Parquet -- requires pyarrow
    # ------------------------------------------------------------------
    try:
        files = writer.to_parquet(result.tables, base / "parquet")
        print(f"Parquet:    {len(files)} files")
        print(f"            sample: {files[0].name}  ({files[0].stat().st_size:,} bytes)")
    except ImportError:
        print("Parquet:    skipped (pip install pyarrow)")

    # ------------------------------------------------------------------
    # 5. Excel -- single workbook, one sheet per table
    # ------------------------------------------------------------------
    try:
        files = writer.to_excel(result.tables, base / "excel", single_workbook=True)
        print(f"Excel:      {len(files)} file(s) -- {files[0].name}  ({files[0].stat().st_size:,} bytes)")
    except ImportError:
        print("Excel:      skipped (pip install openpyxl)")

    # ------------------------------------------------------------------
    # 6. SQL INSERT -- ANSI SQL, works with SQL Server, PostgreSQL, etc.
    # ------------------------------------------------------------------
    files = writer.to_sql_inserts(result.tables, base / "sql", schema_name="retail")
    print(f"SQL INSERT: {len(files)} files")
    # Preview first few lines of the customer INSERT script
    customer_sql = next(f for f in files if f.stem == "customer")
    lines = customer_sql.read_text(encoding="utf-8").splitlines()
    for line in lines[:5]:
        print(f"            {line}")
