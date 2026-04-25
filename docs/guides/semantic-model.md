# Semantic Model Export

Export any Spindle domain schema as a Power BI / Fabric semantic model in `.bim` format (Tabular Model Definition Language). The generated file can be opened directly in Tabular Editor or deployed via XMLA.

## Quick Start

```bash
spindle export-model retail --output retail_model.bim --source-type lakehouse
```

```python
from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter
from sqllocks_spindle import RetailDomain

exporter = SemanticModelExporter()
path = exporter.export_bim(
    schema=RetailDomain().schema(),
    source_type="lakehouse",
    source_name="RetailLakehouse",
    output_path="retail_model.bim",
    include_measures=True,
)
print(f"Exported to {path}")
```

## Source Types

The M expression (Power Query) in the generated model adapts to the data source:

| Source Type | M Expression Target | Typical Use |
| --- | --- | --- |
| `lakehouse` | Lakehouse SQL endpoint | Default for Fabric Lakehouse |
| `warehouse` | Fabric Warehouse / Synapse | Dedicated SQL compute |
| `sql_database` | Fabric SQL Database | OLTP workloads |

## Auto-Generated DAX Measures

When `include_measures=True` (default), Spindle generates starter DAX measures for each table:

| Measure Pattern | Example |
| --- | --- |
| Row count | `Customer Count = COUNTROWS('customer')` |
| SUM for numeric columns | `Total Order Amount = SUM('order'[order_total])` |
| AVERAGE for numeric columns | `Avg Order Amount = AVERAGE('order'[order_total])` |

## Type Mapping

| Spindle Type | TOM Type |
| --- | --- |
| `integer` | `int64` |
| `string` | `string` |
| `decimal`, `float` | `decimal`, `double` |
| `timestamp`, `date` | `dateTime` |
| `boolean` | `boolean` |
| `uuid` | `string` |

## CLI Reference

```bash
spindle export-model DOMAIN_NAME [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `DOMAIN_NAME` | — | Domain to export (required) |
| `--output, -o` | `model.bim` | Output file path |
| `--source-type` | `lakehouse` | `lakehouse`, `warehouse`, or `sql_database` |
| `--source-name` | — | Data source name in the M expression |
| `--include-measures` | `True` | Generate starter DAX measures |
| `--schema-name` | `dbo` | SQL schema name |

## Workflow: Generate + Model

```bash
# 1. Generate data to Lakehouse
spindle publish retail --target lakehouse --base-path "abfss://..." --scale small

# 2. Export semantic model
spindle export-model retail --output retail.bim --source-type lakehouse \
  --source-name "RetailLakehouse"

# 3. Open retail.bim in Tabular Editor or deploy via XMLA
```
