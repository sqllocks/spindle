# Custom Schemas

Spindle schemas are defined in `.spindle.json` files — JSON documents that describe tables, columns, generators, relationships, business rules, and scale presets.

## Three Input Methods

```python
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()

# 1. Built-in domain
result = spindle.generate(domain=RetailDomain(), scale="small")

# 2. JSON file
result = spindle.generate(schema="path/to/my_schema.spindle.json", scale="small")

# 3. Python dict
result = spindle.generate(schema={...}, scale="small")
```

## Schema Structure

A `.spindle.json` file has five top-level sections:

```json
{
  "model": { ... },
  "tables": { ... },
  "relationships": [ ... ],
  "business_rules": [ ... ],
  "generation": { ... }
}
```

### Model

Global settings for the schema.

```json
{
  "model": {
    "name": "my_schema",
    "description": "My custom schema",
    "domain": "custom",
    "schema_mode": "3nf",
    "locale": "en_US",
    "seed": 42,
    "date_range": {
      "start": "2022-01-01",
      "end": "2025-12-31"
    }
  }
}
```

### Tables

Each table has a primary key and columns with generator definitions.

```json
{
  "tables": {
    "customer": {
      "description": "Individual customers",
      "primary_key": ["customer_id"],
      "columns": {
        "customer_id": {
          "type": "integer",
          "generator": {"strategy": "sequence", "start": 1}
        },
        "first_name": {
          "type": "string",
          "max_length": 50,
          "generator": {"strategy": "faker", "provider": "first_name"}
        },
        "email": {
          "type": "string",
          "nullable": true,
          "null_rate": 0.05,
          "generator": {"strategy": "faker", "provider": "email"}
        },
        "loyalty_tier": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": {"Basic": 0.55, "Silver": 0.25, "Gold": 0.13, "Platinum": 0.07}
          }
        }
      }
    }
  }
}
```

**Column types:** `integer`, `string`, `decimal`, `timestamp`, `date`, `boolean`, `uuid`, `float`

**Column options:**

| Field | Type | Description |
| --- | --- | --- |
| `type` | str | Data type |
| `nullable` | bool | Allow nulls (default `false`) |
| `null_rate` | float | Fraction of rows that are null (0.0-1.0) |
| `max_length` | int | Max string length |
| `precision` | int | Decimal total digits |
| `scale` | int | Decimal fractional digits |
| `generator` | dict | Strategy configuration (see [Strategies](strategies.md)) |

### Relationships

Define foreign key relationships between tables.

```json
{
  "relationships": [
    {
      "name": "customer_orders",
      "parent": "customer",
      "child": "order",
      "parent_columns": ["customer_id"],
      "child_columns": ["customer_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "pareto",
        "min": 0,
        "max": 200,
        "alpha": 1.2
      }
    }
  ]
}
```

**Relationship types:** `one_to_many`, `self_referencing`

**Cardinality distributions:** `uniform`, `pareto`, `zipf`, `log_normal`, `bernoulli`

### Business Rules

Post-generation constraints that are validated and auto-fixed.

```json
{
  "business_rules": [
    {
      "name": "order_date_after_signup",
      "type": "cross_table",
      "rule": "order.order_date >= customer.signup_date",
      "via": "customer_id"
    },
    {
      "name": "cost_less_than_price",
      "type": "cross_column",
      "table": "product",
      "rule": "cost < unit_price"
    }
  ]
}
```

**Rule types:** `cross_table` (across FK), `cross_column` (within table), `constraint` (single column)

### Generation

Scale presets and derived row counts.

```json
{
  "generation": {
    "scales": {
      "small": {"customer": 1000, "product": 500, "order": 5000},
      "medium": {"customer": 50000, "product": 5000, "order": 500000}
    },
    "derived_counts": {
      "address": {"per_parent": "customer", "ratio": 1.5},
      "order_line": {"per_parent": "order", "distribution": "log_normal", "mean": 1.2, "sigma": 0.6}
    }
  }
}
```

## Validating a Schema

```bash
spindle validate my_schema.spindle.json
```

The validator checks: circular dependencies, FK targets exist, type consistency, and required fields.

## Inspecting a Schema

```bash
spindle describe retail
```

Shows tables, columns, generation order, relationships, and scale presets.
