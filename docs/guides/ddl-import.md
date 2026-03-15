# DDL Import

Import SQL DDL (CREATE TABLE statements) into a `.spindle.json` schema and generate synthetic data from any existing database design.

## Quick Start

```bash
spindle from-ddl my_tables.sql --output my_schema.spindle.json
spindle generate --schema my_schema.spindle.json --scale small --output ./data/
```

```python
from sqllocks_spindle.schema.ddl_parser import DdlParser
from sqllocks_spindle import Spindle

parser = DdlParser()
schema = parser.parse_file("my_tables.sql")

result = Spindle().generate(schema=schema, scale="small", seed=42)
print(result.summary())
```

## Supported SQL Dialects

| Dialect | Identity Syntax | Identifier Quoting | Notes |
| --- | --- | --- | --- |
| **SQL Server / Fabric** | `IDENTITY(1,1)` | `[brackets]` | T-SQL, schema-qualified names |
| **PostgreSQL** | `SERIAL`, `BIGSERIAL` | `"double quotes"` | Schema.table notation |
| **MySQL** | `AUTO_INCREMENT` | `` `backticks` `` | Standard MySQL syntax |
| **ANSI SQL** | — | `"double quotes"` | Generic fallback |

The parser auto-detects the dialect from syntax patterns. No configuration needed.

## What Gets Parsed

- **CREATE TABLE** statements (with optional `IF NOT EXISTS`)
- **Column definitions** with data types, precision/scale, and constraints
- **Primary keys** — inline and table-level `CONSTRAINT ... PRIMARY KEY`
- **Foreign keys** — inline `REFERENCES`, table-level `FOREIGN KEY`, and `ALTER TABLE ADD CONSTRAINT`
- **Identity/serial** columns → `sequence` strategy
- **NOT NULL / NULL** constraints → `null_rate: 0.0` or default

## Type-to-Strategy Mapping

The parser maps SQL types to appropriate generation strategies:

| SQL Type | Generator Strategy | Details |
| --- | --- | --- |
| `INT`, `BIGINT`, `SMALLINT` | `distribution: uniform` | Min/max based on type range |
| `DECIMAL(p,s)`, `NUMERIC` | `distribution: normal` | Mean/std derived from precision |
| `MONEY`, `SMALLMONEY` | `distribution: log_normal` | Realistic monetary distributions |
| `BIT`, `BOOLEAN` | `weighted_enum` | `{true: 0.85, false: 0.15}` |
| `DATETIME`, `DATE`, `TIMESTAMP` | `temporal: uniform` | Configurable date range |
| `UNIQUEIDENTIFIER`, `UUID` | `uuid` | UUID v4 |
| `VARCHAR`, `NVARCHAR`, `TEXT` | Heuristic (see below) | Based on column name |
| `VARBINARY`, `IMAGE` | Skipped | Binary columns excluded |

## Column Name Heuristics

String columns are matched against 24 exact names and 7 suffix patterns:

### Exact Matches

| Column Name | Faker Provider |
| --- | --- |
| `first_name` | `first_name` |
| `last_name` | `last_name` |
| `email` | `email` |
| `phone` | `phone_number` |
| `address` | `street_address` |
| `city` | `city` |
| `state` | `state_abbr` |
| `zip_code`, `postal_code` | `zipcode` |
| `country` | `country` |
| `company` | `company` |
| `username` | `user_name` |
| `ssn` | `ssn` |
| `ip_address` | `ipv4` |

### Suffix Patterns

| Suffix | Strategy |
| --- | --- |
| `*_name` | `faker: name` |
| `*_email` | `faker: email` |
| `*_phone` | `faker: phone_number` |
| `*_date` | `temporal` |
| `*_code` | `pattern` |
| `*_type`, `*_status` | `weighted_enum` |
| `*_id` | FK candidate (auto-linked) |

## Foreign Key Detection

FKs are resolved two ways:

1. **Explicit constraints** — parsed directly from `FOREIGN KEY` / `REFERENCES` clauses
2. **Naming convention** — columns ending in `_id` are matched to tables by name (e.g., `customer_id` → FK to `customer.customer_id`)

Both produce `foreign_key` strategy with Pareto distribution for realistic skew.

## Scale Auto-Generation

The parser generates three scale presets based on table topology:

| Scale | Root Tables (no FK deps) | Child Tables (with FK deps) |
| --- | --- | --- |
| `small` | 1,000 rows | 2,500 rows |
| `medium` | 10,000 rows | 25,000 rows |
| `large` | 100,000 rows | 250,000 rows |

## CLI Reference

```bash
spindle from-ddl INPUT_FILE [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `INPUT_FILE` | — | Path to SQL DDL file (required) |
| `--output, -o` | — | Output `.spindle.json` path |
| `--domain` | `custom` | Domain name for the generated schema |
| `--scale` | — | Scale overrides: `small:table1=1000,table2=5000` |

## Example: End-to-End

```sql
-- my_tables.sql
CREATE TABLE customer (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name  NVARCHAR(50) NOT NULL,
    last_name   NVARCHAR(50) NOT NULL,
    email       NVARCHAR(100),
    created_at  DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE [order] (
    order_id    INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customer(customer_id),
    order_date  DATE NOT NULL,
    total       DECIMAL(10,2),
    status      VARCHAR(20)
);
```

```bash
# Import DDL
spindle from-ddl my_tables.sql --output my_schema.spindle.json

# Generate data
spindle generate --schema my_schema.spindle.json --scale small --seed 42 --output ./data/
```

The parser will:

1. Detect SQL Server dialect (from `IDENTITY` and bracket quoting)
2. Map `customer_id` to `sequence` strategy (identity column)
3. Map `first_name` / `last_name` / `email` to appropriate Faker providers
4. Create FK relationship `order.customer_id → customer.customer_id`
5. Map `status` to `weighted_enum` (suffix heuristic)
6. Generate scale presets (customer=1K, order=2.5K for small)
