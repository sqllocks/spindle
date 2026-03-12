"""Fabric Notebook — Custom Domain with Partitioned Delta Output.

Paste each section into a separate Fabric Notebook cell.
Shows how to use a custom .spindle.json schema and write partitioned Delta tables.
"""

# ── CELL 1: Install Spindle ─────────────────────────────────────────────
# %pip install sqllocks-spindle[fabric] -q

# ── CELL 2: Load a custom schema and generate ──────────────────────────
from sqllocks_spindle import Spindle
from sqllocks_spindle.schema.parser import SchemaParser

# Point at your .spindle.json file in the Lakehouse Files area
# or embed it inline with parse_dict()
parser = SchemaParser()
schema = parser.parse_file("/lakehouse/default/Files/my_schema.spindle.json")

spindle = Spindle()
result = spindle.generate(schema=schema, scale="small", seed=42)

print(result.summary())

# ── CELL 3: Write with partitioning ─────────────────────────────────────
from sqllocks_spindle.output import DeltaWriter

# Partition large fact tables by date columns for query performance
writer = DeltaWriter(
    partition_by={
        "order": ["order_date:year", "order_date:month"],
        "order_line": ["order_date:year"],
    },
)
paths = writer.write_all(result.tables)

for p in paths:
    print(f"  Written: {p}")

# ── CELL 4: Verify partitioned output ──────────────────────────────────
# Check that Spark sees the partition columns
df = spark.sql("DESCRIBE EXTENDED `order`")  # noqa: F821
display(df.filter("col_name = 'order_date_year' OR col_name = 'order_date_month'"))  # noqa: F821

# Query a specific partition
display(spark.sql("""  # noqa: F821
    SELECT COUNT(*) AS orders_2025
    FROM `order`
    WHERE order_date_year = 2025
"""))
