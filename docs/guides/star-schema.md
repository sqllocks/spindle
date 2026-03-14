# Star Schema Export

Transform Spindle's normalized 3NF output into star schemas with surrogate keys, conformed dimensions, and auto-generated date dimensions.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.transform.star_schema import StarSchemaTransformer

domain = RetailDomain()
result = Spindle().generate(domain=domain, scale="small", seed=42)

transformer = StarSchemaTransformer(
    schema=domain.get_schema(),
    star_map=domain.star_schema_map(),
)
star = transformer.transform(result.tables)

# Access dimensions and facts
print(star.dimensions.keys())   # dim_customer, dim_product, dim_store, ...
print(star.facts.keys())        # fact_sale, fact_return
print(star.date_dim.head())     # Auto-generated date dimension
```

## How It Works

1. **Dimension tables** are built from source tables with surrogate keys (sequential integers).
2. **Fact tables** join source tables and replace natural keys with surrogate key references.
3. A **date dimension** is auto-generated from all `date_cols` referenced in fact specs.

## Domain Star Maps

Every Spindle domain provides a `star_schema_map()` method that defines its dimensions and facts:

```python
domain = RetailDomain()
star_map = domain.star_schema_map()

# Inspect the mapping
for dim_name, spec in star_map.dims.items():
    print(f"{dim_name}: source={spec.source}, sk={spec.sk}, nk={spec.nk}")

for fact_name, spec in star_map.facts.items():
    print(f"{fact_name}: primary={spec.primary}, date_cols={spec.date_cols}")
```

### Available Domains with Star Maps

All 13 Spindle domains include star schema mappings:

| Domain | Dimensions | Facts |
|--------|-----------|-------|
| Retail | customer, product, store, promotion | sale, return |
| Healthcare | patient, provider, facility | encounter, claim |
| Financial | customer, branch, account, category | transaction, loan_payment |
| HR | employee, department, position | compensation, performance, time_off |
| Education | student, course, instructor, department | enrollment, financial_aid |
| Insurance | policyholder, agent, policy_type, policy | claim, claim_payment, premium |
| IoT | device, location, sensor | reading, alert |
| Manufacturing | product, production_line, equipment | work_order, quality, downtime |
| Marketing | contact, campaign, lead_source | lead, opportunity, email |
| Real Estate | property, agent, listing | transaction, showing |
| Supply Chain | supplier, warehouse, material | po_line, shipment, inventory |
| Telecom | subscriber, plan, device_model, service_line | usage, billing, network |
| Capital Markets | company, exchange, sector | daily_price, dividend, earnings, insider_txn |

## Enriched Dimensions

Dimensions can be enriched by joining related tables:

```python
DimSpec(
    source="product",
    sk="sk_product",
    nk="product_id",
    enrich=[{
        "table": "product_category",
        "left_on": "category_id",
        "right_on": "category_id",
        "prefix": "cat_",
    }],
)
```

This produces a `dim_product` table with both product columns and prefixed category columns (e.g., `cat_category_name`).

## Date Dimension

The transformer auto-generates a date dimension from all date columns referenced in `date_cols`:

```python
star.date_dim.columns
# ['sk_date', 'calendar_date', 'year', 'quarter', 'month', 'month_name',
#  'week', 'day_of_week', 'day_name', 'is_weekend', 'fiscal_year', ...]
```

## CLI

```bash
# Generate star schema output
spindle generate retail --scale small --mode star --format parquet --output ./star_output/
```

## Writing to Fabric

```python
# Write star schema to Lakehouse
for name, df in star.dimensions.items():
    df.to_parquet(f"/lakehouse/default/Files/star/{name}.parquet", index=False)

for name, df in star.facts.items():
    df.to_parquet(f"/lakehouse/default/Files/star/{name}.parquet", index=False)

star.date_dim.to_parquet("/lakehouse/default/Files/star/dim_date.parquet", index=False)
```
