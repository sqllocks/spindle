# Quickstart (Fabric Notebook)

Generate synthetic data directly in a Microsoft Fabric notebook and write it to your Lakehouse as Delta tables.

## Prerequisites

- A Microsoft Fabric workspace with a Lakehouse
- A Fabric notebook attached to that Lakehouse

## Step 1: Install Spindle

In the first cell of your Fabric notebook:

```python
%pip install sqllocks-spindle
```

## Step 2: Generate Data

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
print(result.summary())
# GenerationResult(9 tables, ~3,000 total rows, 0.1s)
```

## Step 3: Write to Lakehouse

```python
for table_name, df in result:
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Wrote {table_name}: {len(df)} rows")
```

## Step 4: Query with Spark SQL

```python
display(spark.sql("SELECT * FROM customer LIMIT 10"))
display(spark.sql("""
    SELECT c.loyalty_tier, COUNT(*) as order_count, AVG(ol.unit_price) as avg_price
    FROM customer c
    JOIN `order` o ON c.customer_id = o.customer_id
    JOIN order_line ol ON o.order_id = ol.order_id
    GROUP BY c.loyalty_tier
    ORDER BY order_count DESC
"""))
```

## Try Other Domains

```python
from sqllocks_spindle import HealthcareDomain, FinancialDomain

# Healthcare — 9 tables (patients, encounters, claims)
health = Spindle().generate(domain=HealthcareDomain(), scale="fabric_demo", seed=42)

# Financial — 10 tables (accounts, transactions, loans)
finance = Spindle().generate(domain=FinancialDomain(), scale="fabric_demo", seed=42)
```

## Scale Up

| Preset | Rows | Use case |
|--------|------|----------|
| `fabric_demo` | ~3,000 | Quick demos, notebooks |
| `small` | ~21,000 | Development |
| `medium` | ~1,000,000 | Integration testing |
| `large` | ~10,000,000 | Performance testing |
| `warehouse` | ~20,000,000 | Warehouse load testing |

For `large` and above, use Spark's distributed processing:

```python
result = Spindle().generate(domain=RetailDomain(), scale="large", seed=42)
for table_name, df in result:
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(table_name)
```

## What's Next?

- [Fabric Lakehouse Tutorial](../tutorials/fabric/10-fabric-lakehouse.md) — detailed walkthrough with star schema export
- [Fabric Warehouse Tutorial](../tutorials/fabric/11-fabric-warehouse.md) — load a dimensional warehouse
- [Medallion Architecture Tutorial](../tutorials/fabric/13-medallion.md) — build a Bronze/Silver/Gold pipeline
- [Notebook Index](https://github.com/sqllocks/spindle/tree/main/examples/notebooks) — all 35 notebooks
