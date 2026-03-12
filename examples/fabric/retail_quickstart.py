"""Fabric Notebook — Retail Domain Quickstart.

Paste each section into a separate Fabric Notebook cell.
Generates retail data at small scale and writes to Lakehouse Delta tables.
"""

# ── CELL 1: Install Spindle ─────────────────────────────────────────────
# %pip install sqllocks-spindle[fabric] -q

# ── CELL 2: Generate retail data ────────────────────────────────────────
from sqllocks_spindle import Spindle, RetailDomain

domain = RetailDomain(schema_mode="3nf")
spindle = Spindle()
result = spindle.generate(domain=domain, scale="small", seed=42)

print(result.summary())

errors = result.verify_integrity()
print(f"\nFK integrity: {'PASS' if not errors else f'{len(errors)} issues'}")

# ── CELL 3: Write to Lakehouse Delta tables ─────────────────────────────
from sqllocks_spindle.output import DeltaWriter

# Auto-detects /lakehouse/default/Tables/ in Fabric
writer = DeltaWriter()
paths = writer.write_all(result.tables)

for p in paths:
    print(f"  Written: {p}")

# ── CELL 4: Verify with Spark SQL ───────────────────────────────────────
for table_name in result.tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").collect()[0].cnt  # noqa: F821
    expected = len(result.tables[table_name])
    status = "OK" if count == expected else "MISMATCH"
    print(f"  {table_name}: {count:,} rows [{status}]")

# ── CELL 5: Quick preview ───────────────────────────────────────────────
display(spark.sql("SELECT * FROM customer LIMIT 10"))  # noqa: F821
display(spark.sql("SELECT * FROM `order` LIMIT 10"))  # noqa: F821
