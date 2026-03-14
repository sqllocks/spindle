# Fabric SQL Database

Write Spindle-generated data directly to a Fabric SQL Database endpoint using ODBC and Entra ID authentication.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

writer = FabricSqlDatabaseWriter(
    connection_string="your-server.database.fabric.microsoft.com",
    database="your_database",
    auth="cli",
)
writer.write(result)
```

## Prerequisites

Install the `fabric-sql` extra:

```bash
pip install sqllocks-spindle[fabric-sql]
```

This adds `pyodbc` and `azure-identity` as dependencies. You also need the
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

## Authentication

Fabric SQL Database uses Entra ID (Azure AD) for authentication.

| Method | `--auth` | Use Case |
|--------|----------|----------|
| Azure CLI | `cli` | Local dev (uses `az login` token) |
| Managed Identity | `msi` | Fabric notebooks, Azure VMs |
| Service Principal | `spn` | CI/CD pipelines |
| SQL Auth | `sql` | Legacy (not recommended for Fabric) |

### Azure CLI (Recommended for Local Dev)

```bash
az login
spindle generate retail --scale small \
    --format sql-database \
    --connection-string "your-server.database.fabric.microsoft.com" \
    --auth cli
```

### Managed Identity (Fabric Notebooks / Azure VMs)

```python
writer = FabricSqlDatabaseWriter(
    connection_string="your-server.database.fabric.microsoft.com",
    auth="msi",
)
```

### Service Principal (CI/CD)

```bash
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"

spindle generate retail --format sql-database \
    --connection-string "your-server.database.fabric.microsoft.com" \
    --auth spn
```

## Write Modes

| Mode | SQL Behavior |
|------|-------------|
| `create_insert` | `DROP` + `CREATE` + `INSERT` (default) |
| `insert_only` | `INSERT` into existing tables only |
| `truncate_insert` | `TRUNCATE` + `INSERT` |
| `append` | `INSERT` without clearing existing data |

Tables are written in dependency order — parent tables first, then children.

## CLI

```bash
spindle generate retail --scale small \
    --format sql-database \
    --connection-string "your-server.database.fabric.microsoft.com" \
    --auth cli \
    --write-mode create_insert \
    --batch-size 1000
```

Use an environment variable to avoid repeating the connection string:

```bash
export SPINDLE_SQL_CONNECTION="your-server.database.fabric.microsoft.com"
spindle generate healthcare --scale medium --format sql-database --auth cli
```

## Python API

```python
from sqllocks_spindle.fabric import FabricSqlDatabaseWriter, WriteResult

writer = FabricSqlDatabaseWriter(
    connection_string="your-server.database.fabric.microsoft.com",
    database="spindle_demo",
    auth="cli",
    write_mode="create_insert",
    batch_size=500,
    schema_name="dbo",
)

write_result: WriteResult = writer.write(result)

for table_name, stats in write_result.table_stats.items():
    print(f"{table_name}: {stats.rows_written} rows in {stats.elapsed_seconds:.1f}s")
```

## Troubleshooting

| Issue | Solution |
|-------|---------|
| `Login timeout expired` | Check firewall rules; ensure your IP is allowed |
| `AADSTS700016` | Verify `AZURE_TENANT_ID` matches your Fabric tenant |
| `Cannot open database` | Database name must match the SQL Database item name |
| `ODBC Driver not found` | Install Microsoft ODBC Driver 18 for SQL Server |
