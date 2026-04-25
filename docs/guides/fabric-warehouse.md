# Fabric Warehouse

Load Spindle-generated data into a Microsoft Fabric Warehouse using SQL DDL and INSERT statements, or write directly via the SQL Database Writer.

## Quick Start

```bash
# Generate SQL with CREATE TABLE + INSERT for Fabric Warehouse
spindle generate retail --scale small --format sql \
    --sql-dialect tsql --sql-ddl --sql-drop --sql-go \
    --output ./output/retail.sql
```

Then execute the `.sql` file in the Fabric Warehouse query editor.

## SQL Output Options

```bash
spindle generate retail --scale small --format sql \
    --schema-name dbo \
    --sql-dialect tsql \
    --sql-ddl \
    --sql-drop \
    --sql-go \
    --output ./output/retail.sql
```

| Flag | Description |
|------|-------------|
| `--schema-name` | Schema prefix (default: `dbo`) |
| `--sql-dialect` | `tsql`, `postgresql`, or `mysql` |
| `--sql-ddl` | Include `CREATE TABLE` statements |
| `--sql-drop` | Include `DROP TABLE IF EXISTS` |
| `--sql-go` | Add `GO` batch separators (required for SSMS / Fabric) |

### Fabric Warehouse Compatibility

Fabric Warehouse has specific constraints compared to SQL Server:

- No `IDENTITY` columns — Spindle uses plain `INT` for PKs
- No `PRIMARY KEY` constraints — omitted automatically with `--sql-dialect tsql`
- `DATETIME2` instead of `DATETIME`
- No user-defined types

Spindle's `tsql` dialect handles all of these automatically.

## Python API

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Generate SQL string
sql = result.to_sql_inserts(
    schema_name="dbo",
    dialect="tsql",
    include_ddl=True,
    include_drop=True,
    batch_separator="GO",
)
print(sql[:500])
```

## Direct Write

For programmatic loading without intermediate files, see the
[Fabric SQL Database](fabric-sql-database.md) guide — the same
`FabricSqlDatabaseWriter` works for both Warehouse and SQL Database endpoints.

```python
from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

writer = FabricSqlDatabaseWriter(
    connection_string="your-fabric-warehouse.datawarehouse.fabric.microsoft.com",
    database="your_warehouse",
    auth="cli",
    write_mode="create_insert",
)
writer.write(result)
```

## Scale Recommendations

| Warehouse Use Case | Spindle Scale | Approx. Rows |
|--------------------|---------------|--------------|
| Schema testing | `demo` | ~5K |
| Query development | `small` | ~15K |
| Performance testing | `medium` | ~1M |
| Load testing | `large` | ~15M |

---

## See Also

- **Tutorial:** [11: Fabric Warehouse](../tutorials/fabric/11-fabric-warehouse.md) — step-by-step walkthrough
- **Notebook:** [`F02_warehouse_dimensional.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/fabric-scenarios/F02_warehouse_dimensional.ipynb)
