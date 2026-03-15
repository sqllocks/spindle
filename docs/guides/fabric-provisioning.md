# Fabric Workspace Provisioning Guide

This guide walks you through setting up a Microsoft Fabric workspace for use
with Spindle's `publish` command and Fabric-native notebooks.

## Prerequisites

- **Microsoft Fabric capacity** (F2 or higher, or Trial capacity)
- **Azure Entra ID account** with Fabric access
- **Python 3.10+** with Spindle installed: `pip install sqllocks-spindle[fabric,streaming,fabric-sql]`

## 1. Create a Fabric Workspace

1. Navigate to [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Select **Workspaces** → **New workspace**
3. Name it (e.g. `spindle-demo`)
4. Assign it to your Fabric capacity under **Advanced** → **License mode** → **Fabric**
5. Note the **Workspace ID** from the URL: `app.fabric.microsoft.com/groups/{workspace-id}`

Set the environment variable for Spindle:
```bash
export SPINDLE_WORKSPACE_ID="your-workspace-id"
```

## 2. Create a Lakehouse

1. In your workspace, select **+ New item** → **Lakehouse**
2. Name it (e.g. `spindle_lakehouse`)
3. Note the **Lakehouse ID** from Settings → About

The OneLake path follows this pattern:
```
abfss://{workspace-id}@onelake.dfs.fabric.microsoft.com/{lakehouse-id}.Lakehouse
```

Set the environment variable:
```bash
export SPINDLE_LAKEHOUSE_PATH="abfss://{workspace-id}@onelake.dfs.fabric.microsoft.com/{lakehouse-id}.Lakehouse"
export SPINDLE_LAKEHOUSE_ID="your-lakehouse-id"
```

## 3. Publish Data to Lakehouse

```bash
# Generate and publish retail data
spindle publish retail --target lakehouse \
  --base-path "$SPINDLE_LAKEHOUSE_PATH" \
  --scale small --format parquet

# Dry run first to verify
spindle publish retail --target lakehouse \
  --base-path "$SPINDLE_LAKEHOUSE_PATH" \
  --dry-run
```

Files land in the Lakehouse `Files/` area under:
```
Files/landing/{domain}/{table}/latest/part-0001.parquet
```

## 4. Create a SQL Database (Optional)

For SQL-based workflows:

1. In your workspace, select **+ New item** → **SQL Database**
2. Name it (e.g. `spindle_sql`)
3. Copy the connection string from **Settings** → **Connection strings**

```bash
export SPINDLE_SQL_CONNECTION="Server=your-server.database.fabric.microsoft.com;Database=spindle_sql"

spindle publish retail --target sql-database \
  --connection-string "$SPINDLE_SQL_CONNECTION" \
  --auth cli
```

### Authentication Methods

| Method | Flag | When to Use |
|--------|------|-------------|
| `cli` | `--auth cli` | Local development (uses `az login` token) |
| `msi` | `--auth msi` | Inside Fabric notebooks or Azure VMs |
| `spn` | `--auth spn` | CI/CD pipelines (set `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`) |
| `sql` | `--auth sql` | SQL authentication (username/password in connection string) |

## 5. Create a Warehouse (Optional)

For dimensional modeling with star schemas:

1. **+ New item** → **Warehouse**
2. Use `spindle generate retail --mode star --format sql` to generate DDL
3. Run the DDL in the Warehouse SQL editor

Note: Fabric Warehouse does not support `IDENTITY` columns or primary key
constraints. Spindle's `tsql-fabric-warehouse` dialect handles this automatically:

```bash
spindle generate retail --mode star --format sql \
  --sql-dialect tsql-fabric-warehouse --output ./ddl/
```

## 6. Create an Eventhouse (Optional)

For streaming / real-time analytics:

1. **+ New item** → **Eventhouse**
2. Create a **KQL Database** inside it
3. Note the cluster URI (e.g. `https://your-eventhouse.z0.kusto.fabric.microsoft.com`)

```bash
export SPINDLE_EVENTHOUSE_URI="https://your-eventhouse.z0.kusto.fabric.microsoft.com"
export SPINDLE_EVENTHOUSE_DB="your-kql-db"

spindle publish retail --target eventhouse \
  --connection-string "$SPINDLE_EVENTHOUSE_URI" \
  --database "$SPINDLE_EVENTHOUSE_DB"
```

Requires: `pip install sqllocks-spindle[eventhouse]`

## 7. Credential Management

Spindle supports secure credential references so you never hardcode secrets:

```bash
# Environment variable
spindle publish retail --target sql-database \
  --connection-string "env://SPINDLE_SQL_CONNECTION"

# Azure Key Vault
spindle publish retail --target sql-database \
  --credential "kv://my-vault/spindle-sql-conn"

# File
spindle publish retail --target sql-database \
  --credential "file:///etc/secrets/spindle-conn.txt"
```

## 8. Permissions

### Minimum Permissions by Target

| Target | Required Role |
|--------|--------------|
| Lakehouse (Files) | Workspace **Contributor** or **Member** |
| Lakehouse (Tables) | Workspace **Contributor** or **Member** |
| SQL Database | `db_owner` on the database |
| Warehouse | `db_owner` on the warehouse |
| Eventhouse | **Database Admin** on the KQL database |
| Eventstream | **Contributor** on the Eventstream item |

### Service Principal Access

For CI/CD, register a service principal:

1. Create an App Registration in Azure Entra ID
2. Grant it **Fabric Workspace Contributor** role
3. Set environment variables:
   ```bash
   export AZURE_CLIENT_ID="your-app-id"
   export AZURE_CLIENT_SECRET="your-secret"
   export AZURE_TENANT_ID="your-tenant-id"
   ```
4. Use `--auth spn` with any `spindle publish` command

## 9. Running Acceptance Tests

Validate your setup with Spindle's built-in acceptance tests:

```bash
export SPINDLE_LAKEHOUSE_PATH="abfss://..."
export SPINDLE_SQL_CONNECTION="Server=..."
export SPINDLE_EVENTHOUSE_URI="https://..."
export SPINDLE_EVENTHOUSE_DB="mydb"

pytest tests/test_acceptance.py -v
```

Tests are automatically skipped for targets without credentials configured.

## 10. Troubleshooting

| Issue | Solution |
|-------|----------|
| `CredentialError: Environment variable not set` | Export the required env var |
| `Connection timeout` | Check Fabric capacity is running (paused capacities timeout) |
| `403 Forbidden` | Verify workspace role assignment |
| `ModuleNotFoundError: azure.kusto` | `pip install sqllocks-spindle[eventhouse]` |
| `ModuleNotFoundError: pyodbc` | `pip install sqllocks-spindle[fabric-sql]` |
| OneLake path errors | Verify `abfss://` format includes `.Lakehouse` suffix |

---

## See Also

- **Tutorial:** [10: Fabric Lakehouse](../tutorials/fabric/10-fabric-lakehouse.md) — step-by-step walkthrough
