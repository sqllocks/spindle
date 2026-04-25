# Fabric Notebooks

Use Spindle inside Microsoft Fabric notebooks to generate synthetic data directly in your Lakehouse, Warehouse, or Eventstream.

## Installation

In the first cell of your Fabric notebook:

```python
%pip install sqllocks-spindle[fabric-sql]
```

For streaming scenarios:

```python
%pip install sqllocks-spindle[all]
```

## Generate to Lakehouse

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Write Parquet files to the default Lakehouse
for name, df in result.tables.items():
    df.to_parquet(f"/lakehouse/default/Files/spindle/{name}.parquet", index=False)
    print(f"Wrote {len(df)} rows to {name}.parquet")
```

### Register as Delta Tables

```python
for name in result.table_names:
    path = f"/lakehouse/default/Files/spindle/{name}.parquet"
    sdf = spark.read.parquet(path)
    sdf.write.format("delta").mode("overwrite").saveAsTable(name)
    print(f"Registered {name} as Delta table")
```

## Generate to SQL Database / Warehouse

```python
from sqllocks_spindle import Spindle, HealthcareDomain
from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

result = Spindle().generate(domain=HealthcareDomain(), scale="small", seed=42)

writer = FabricSqlDatabaseWriter(
    connection_string="your-server.database.fabric.microsoft.com",
    database="your_database",
    auth="msi",  # Use managed identity in Fabric notebooks
    write_mode="create_insert",
)
writer.write(result)
```

## Stream to Eventstream

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.fabric import FabricStreamWriter

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Stream order events to console (or Eventstream endpoint)
FabricStreamWriter.stream(
    tables=result.tables,
    table="order",
    max_events=500,
    events_per_second=50,
)
```

## Export Semantic Model

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.fabric import SemanticModelExporter

domain = RetailDomain()
result = Spindle().generate(domain=domain, scale="small", seed=42)

exporter = SemanticModelExporter(domain=domain, result=result)
bim = exporter.export(source_type="lakehouse")

with open("/lakehouse/default/Files/retail_model.bim", "w") as f:
    f.write(bim)
print("Exported .bim semantic model")
```

## Multi-Domain Notebook Pattern

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.healthcare import HealthcareDomain
from sqllocks_spindle.domains.financial import FinancialDomain

spindle = Spindle()
domains = [RetailDomain(), HealthcareDomain(), FinancialDomain()]

for domain in domains:
    result = spindle.generate(domain=domain, scale="small", seed=42)
    for name, df in result.tables.items():
        path = f"/lakehouse/default/Files/{domain.name}/{name}.parquet"
        df.to_parquet(path, index=False)
    print(f"{domain.name}: {len(result.table_names)} tables written")
```

## Tips

- Use `auth="msi"` in Fabric notebooks — managed identity is automatic.
- Set a fixed `seed` for reproducible results across notebook runs.
- Use `scale="demo"` for quick iteration, `scale="small"` for realistic demos.
- Fabric notebooks have a 20-minute idle timeout — large scales may need a keep-alive cell.
- Install Spindle once per session; the package persists until the Spark session restarts.

---

## See Also

- **Tutorial:** [10: Fabric Lakehouse](../tutorials/fabric/10-fabric-lakehouse.md) — step-by-step walkthrough
- **Quickstart:** [Fabric Quickstart](../getting-started/quickstart-fabric.md)
- **Notebooks:** All F-series notebooks in [`fabric-scenarios/`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/fabric-scenarios/)
