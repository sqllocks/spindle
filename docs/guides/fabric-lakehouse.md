# Fabric Lakehouse

Load Spindle-generated data into a Microsoft Fabric Lakehouse as Delta tables via the Files API or OneLake paths.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Write Parquet files to the Lakehouse Files area
result.to_parquet("/lakehouse/default/Files/spindle/retail")
```

Once uploaded, register the Parquet files as Delta tables using a Fabric notebook:

```python
for table_name in result.table_names:
    path = f"/lakehouse/default/Files/spindle/retail/{table_name}.parquet"
    df = spark.read.parquet(path)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
```

## Writing to Lakehouse Files

Spindle's `LakehouseFilesWriter` handles path resolution and format selection:

```python
from sqllocks_spindle.fabric import LakehouseFilesWriter

writer = LakehouseFilesWriter(
    lakehouse_path="/lakehouse/default",
    subfolder="spindle/retail",
    format="parquet",          # parquet | csv | jsonl
)
writer.write(result)
```

### OneLake Paths

When running outside a Fabric notebook, use full `abfss://` paths:

```python
writer = LakehouseFilesWriter(
    lakehouse_path="abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse",
    subfolder="spindle/retail",
)
```

## CLI

```bash
# Generate and write directly to Lakehouse Files
spindle generate retail --scale small --format parquet --output /lakehouse/default/Files/spindle/retail

# From a local machine, generate files then upload separately
spindle generate retail --scale medium --format parquet --output ./output/retail
```

## File Organization

Spindle writes one file per table, organized by domain:

```text
/lakehouse/default/Files/spindle/
└── retail/
    ├── customer.parquet
    ├── address.parquet
    ├── product.parquet
    ├── order.parquet
    ├── order_line.parquet
    └── ...
```

## Scale Recommendations

| Lakehouse Scale | Spindle Scale | Approx. Rows | Approx. Size |
|-----------------|---------------|-------------|--------------|
| Dev / POC       | `demo`        | ~5K         | < 1 MB       |
| Small           | `small`       | ~15K        | ~5 MB        |
| Medium          | `medium`      | ~1M         | ~200 MB      |
| Large           | `large`       | ~15M        | ~3 GB        |

## Tips

- Use `parquet` format for best Lakehouse performance and schema preservation.
- Set `seed=42` (or any fixed seed) for reproducible datasets across environments.
- For multi-domain loads, generate each domain into its own subfolder.
- After loading, create shortcuts to other Lakehouses or Warehouses as needed.

---

## See Also

- **Tutorial:** [10: Fabric Lakehouse](../tutorials/fabric/10-fabric-lakehouse.md) — step-by-step walkthrough
- **Tutorial:** [13: Medallion](../tutorials/fabric/13-medallion.md) — step-by-step walkthrough
- **Example script:** [`22_fabric_integration.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/22_fabric_integration.py)
- **Notebook:** [`T08_fabric_lakehouse.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/intermediate/T08_fabric_lakehouse.ipynb)
- **Notebook:** [`F01_medallion_architecture.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/fabric-scenarios/F01_medallion_architecture.ipynb)
