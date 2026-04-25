# Spindle by SQLLocks — Phase 0 Specification

> "Synthea is to MITRE as Spindle is to SQLLocks"

## Overview

Spindle is a multi-domain, schema-aware, Fabric-native synthetic data generator. Phase 0 delivers the core engine, the schema definition format, and a Retail domain proof-of-concept that generates a proper normalized schema with realistic distributions.

**Package:** `sqllocks-spindle` (pip install sqllocks-spindle)
**Import:** `from sqllocks_spindle import RetailDomain`
**License:** MIT (open source)
**Runtime:** Python 3.10+ (Fabric Notebooks use 3.10/3.11)

---

## 1. Schema Definition Format (SDF)

The Schema Definition Format is the heart of Spindle. Every domain, every generation run, every output mode starts with an SDF document. It's a JSON file (`.spindle.json`) that describes:

- Tables and their columns
- Primary keys (simple, composite)
- Foreign keys and relationships (1:1, 1:N, M:N)
- Column-level generation rules
- Cross-column correlations and business rules
- Temporal and distribution profiles
- Scale parameters

### 1.1 Design Principles

1. **Human-readable** — a data engineer should understand it without docs
2. **Machine-executable** — the engine reads it and generates data with zero additional code
3. **Composable** — domains can reference each other (Retail + Supply Chain share Product)
4. **Schema-mode agnostic** — same format works for 3NF, star schema, CDM, or custom
5. **CDM-mappable** — columns can declare their CDM entity/attribute mapping

### 1.2 Top-Level Structure

```json
{
  "$schema": "https://sqllocks.com/spindle/schema/v1.json",
  "spindle_version": "0.1.0",
  "model": {
    "name": "retail_3nf",
    "description": "Retail domain — 3NF normalized schema",
    "domain": "retail",
    "schema_mode": "3nf",
    "locale": "en_US",
    "seed": 42,
    "date_range": {
      "start": "2022-01-01",
      "end": "2025-12-31"
    }
  },
  "tables": { },
  "relationships": [ ],
  "business_rules": [ ],
  "generation": { }
}
```

### 1.3 Table Definition

```json
{
  "tables": {
    "customer": {
      "description": "Individual customers",
      "primary_key": ["customer_id"],
      "cdm_mapping": "Contact",
      "columns": {
        "customer_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "first_name": {
          "type": "string",
          "max_length": 50,
          "generator": { "strategy": "faker", "provider": "first_name" }
        },
        "last_name": {
          "type": "string",
          "max_length": 50,
          "generator": { "strategy": "faker", "provider": "last_name" }
        },
        "email": {
          "type": "string",
          "max_length": 255,
          "nullable": true,
          "null_rate": 0.05,
          "generator": { "strategy": "faker", "provider": "email" }
        },
        "date_of_birth": {
          "type": "date",
          "generator": {
            "strategy": "distribution",
            "distribution": "normal",
            "params": { "mean_age": 42, "std_dev": 15, "min_age": 18, "max_age": 95 }
          }
        },
        "gender": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "M": 0.48, "F": 0.50, "NB": 0.02 }
          }
        },
        "loyalty_tier": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "Basic": 0.80, "Silver": 0.12, "Gold": 0.06, "Platinum": 0.02 }
          }
        },
        "signup_date": {
          "type": "date",
          "generator": {
            "strategy": "temporal",
            "pattern": "uniform",
            "range_ref": "model.date_range"
          }
        },
        "is_active": {
          "type": "boolean",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "true": 0.85, "false": 0.15 }
          }
        },
        "created_at": {
          "type": "timestamp",
          "generator": { "strategy": "derived", "source": "signup_date" }
        }
      }
    },

    "address": {
      "description": "Customer addresses — one customer can have multiple (billing, shipping)",
      "primary_key": ["address_id"],
      "columns": {
        "address_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "customer_id": {
          "type": "integer",
          "generator": { "strategy": "foreign_key", "ref": "customer.customer_id" }
        },
        "address_type": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "billing": 0.50, "shipping": 0.40, "both": 0.10 }
          }
        },
        "street": {
          "type": "string",
          "generator": { "strategy": "faker", "provider": "street_address" }
        },
        "city": {
          "type": "string",
          "generator": { "strategy": "faker", "provider": "city" }
        },
        "state": {
          "type": "string",
          "max_length": 2,
          "generator": {
            "strategy": "weighted_enum",
            "values_ref": "data://us_state_population_weights"
          }
        },
        "zip_code": {
          "type": "string",
          "generator": {
            "strategy": "correlated",
            "source_column": "state",
            "lookup": "data://us_zip_by_state"
          }
        },
        "is_primary": {
          "type": "boolean",
          "generator": { "strategy": "first_per_parent", "default": true }
        }
      }
    },

    "product_category": {
      "description": "Product category hierarchy — department > category > subcategory",
      "primary_key": ["category_id"],
      "columns": {
        "category_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "category_name": {
          "type": "string",
          "generator": { "strategy": "reference_data", "dataset": "retail_categories" }
        },
        "parent_category_id": {
          "type": "integer",
          "nullable": true,
          "generator": { "strategy": "self_referencing", "ref": "product_category.category_id" }
        },
        "level": {
          "type": "integer",
          "generator": {
            "strategy": "derived",
            "rule": "hierarchy_depth",
            "source": "parent_category_id"
          }
        }
      }
    },

    "product": {
      "description": "Individual products (SKUs)",
      "primary_key": ["product_id"],
      "cdm_mapping": "Product",
      "columns": {
        "product_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "sku": {
          "type": "string",
          "generator": { "strategy": "pattern", "format": "SKU-{category_code}-{seq:6}" }
        },
        "product_name": {
          "type": "string",
          "generator": { "strategy": "reference_data", "dataset": "retail_product_names" }
        },
        "category_id": {
          "type": "integer",
          "generator": { "strategy": "foreign_key", "ref": "product_category.category_id", "filter": "level = 3" }
        },
        "unit_price": {
          "type": "decimal",
          "precision": 10,
          "scale": 2,
          "generator": {
            "strategy": "distribution",
            "distribution": "log_normal",
            "params": { "mean": 3.5, "sigma": 1.2, "min": 0.99, "max": 2999.99 }
          }
        },
        "cost": {
          "type": "decimal",
          "precision": 10,
          "scale": 2,
          "generator": {
            "strategy": "correlated",
            "source_column": "unit_price",
            "rule": "multiply",
            "params": { "factor_range": [0.30, 0.70] }
          }
        },
        "weight_kg": {
          "type": "decimal",
          "nullable": true,
          "null_rate": 0.10,
          "generator": {
            "strategy": "distribution",
            "distribution": "log_normal",
            "params": { "mean": 0.5, "sigma": 1.0, "min": 0.01, "max": 50.0 }
          }
        },
        "is_active": {
          "type": "boolean",
          "generator": {
            "strategy": "lifecycle",
            "phases": {
              "introduced": 0.10,
              "active": 0.75,
              "discontinued": 0.15
            }
          }
        },
        "created_at": {
          "type": "timestamp",
          "generator": { "strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range" }
        }
      }
    },

    "store": {
      "description": "Physical and online store locations",
      "primary_key": ["store_id"],
      "columns": {
        "store_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "store_name": {
          "type": "string",
          "generator": { "strategy": "pattern", "format": "Store #{store_id:04d} - {city}" }
        },
        "store_type": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "physical": 0.85, "online": 0.10, "warehouse": 0.05 }
          }
        },
        "city": {
          "type": "string",
          "generator": { "strategy": "faker", "provider": "city" }
        },
        "state": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values_ref": "data://us_state_population_weights"
          }
        },
        "open_date": {
          "type": "date",
          "generator": {
            "strategy": "temporal",
            "pattern": "uniform",
            "range": { "start": "2010-01-01", "end": "2024-12-31" }
          }
        },
        "square_footage": {
          "type": "integer",
          "nullable": true,
          "generator": {
            "strategy": "distribution",
            "distribution": "normal",
            "params": { "mean": 45000, "std_dev": 15000, "min": 5000, "max": 150000 }
          }
        }
      }
    },

    "promotion": {
      "description": "Marketing promotions and discount campaigns",
      "primary_key": ["promotion_id"],
      "columns": {
        "promotion_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "promo_name": {
          "type": "string",
          "generator": { "strategy": "reference_data", "dataset": "retail_promo_names" }
        },
        "promo_type": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "percent_off": 0.40, "bogo": 0.20, "fixed_amount": 0.25, "bundle": 0.10, "clearance": 0.05 }
          }
        },
        "discount_value": {
          "type": "decimal",
          "generator": {
            "strategy": "correlated",
            "source_column": "promo_type",
            "rules": {
              "percent_off": { "distribution": "uniform", "params": { "min": 5, "max": 50 } },
              "bogo": { "fixed": 50.0 },
              "fixed_amount": { "distribution": "uniform", "params": { "min": 5, "max": 100 } },
              "bundle": { "distribution": "uniform", "params": { "min": 10, "max": 30 } },
              "clearance": { "distribution": "uniform", "params": { "min": 40, "max": 75 } }
            }
          }
        },
        "start_date": {
          "type": "date",
          "generator": { "strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range" }
        },
        "end_date": {
          "type": "date",
          "generator": {
            "strategy": "derived",
            "source": "start_date",
            "rule": "add_days",
            "params": { "distribution": "uniform", "min": 3, "max": 30 }
          }
        }
      }
    },

    "order": {
      "description": "Customer orders (header)",
      "primary_key": ["order_id"],
      "cdm_mapping": "SalesOrder",
      "columns": {
        "order_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "customer_id": {
          "type": "integer",
          "generator": { "strategy": "foreign_key", "ref": "customer.customer_id" }
        },
        "store_id": {
          "type": "integer",
          "generator": { "strategy": "foreign_key", "ref": "store.store_id" }
        },
        "shipping_address_id": {
          "type": "integer",
          "generator": {
            "strategy": "foreign_key",
            "ref": "address.address_id",
            "constrained_by": "customer_id"
          }
        },
        "promotion_id": {
          "type": "integer",
          "nullable": true,
          "null_rate": 0.70,
          "generator": {
            "strategy": "foreign_key",
            "ref": "promotion.promotion_id",
            "filter": "start_date <= order_date AND end_date >= order_date"
          }
        },
        "order_date": {
          "type": "timestamp",
          "generator": {
            "strategy": "temporal",
            "pattern": "seasonal",
            "profiles": {
              "day_of_week": { "Mon": 0.13, "Tue": 0.14, "Wed": 0.14, "Thu": 0.14, "Fri": 0.16, "Sat": 0.17, "Sun": 0.12 },
              "month": { "Jan": 0.06, "Feb": 0.06, "Mar": 0.07, "Apr": 0.08, "May": 0.08, "Jun": 0.08, "Jul": 0.08, "Aug": 0.09, "Sep": 0.08, "Oct": 0.08, "Nov": 0.11, "Dec": 0.13 },
              "hour_of_day": { "distribution": "bimodal", "peaks": [11, 20], "std_dev": 2 }
            }
          }
        },
        "status": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": { "completed": 0.82, "shipped": 0.08, "processing": 0.04, "cancelled": 0.05, "returned": 0.01 }
          }
        },
        "order_total": {
          "type": "decimal",
          "precision": 12,
          "scale": 2,
          "generator": {
            "strategy": "computed",
            "rule": "sum_children",
            "child_table": "order_line",
            "child_column": "line_total"
          }
        }
      }
    },

    "order_line": {
      "description": "Individual line items within an order",
      "primary_key": ["order_line_id"],
      "columns": {
        "order_line_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "order_id": {
          "type": "integer",
          "generator": { "strategy": "foreign_key", "ref": "order.order_id" }
        },
        "product_id": {
          "type": "integer",
          "generator": {
            "strategy": "foreign_key",
            "ref": "product.product_id",
            "distribution": "zipf",
            "params": { "alpha": 1.5 }
          }
        },
        "quantity": {
          "type": "integer",
          "generator": {
            "strategy": "distribution",
            "distribution": "geometric",
            "params": { "p": 0.6, "min": 1, "max": 20 }
          }
        },
        "unit_price": {
          "type": "decimal",
          "precision": 10,
          "scale": 2,
          "generator": {
            "strategy": "lookup",
            "source_table": "product",
            "source_column": "unit_price",
            "via": "product_id"
          }
        },
        "discount_percent": {
          "type": "decimal",
          "precision": 5,
          "scale": 2,
          "generator": {
            "strategy": "conditional",
            "condition": "order.promotion_id IS NOT NULL",
            "true_gen": { "strategy": "lookup", "source_table": "promotion", "source_column": "discount_value", "via": "order.promotion_id" },
            "false_gen": { "fixed": 0.00 }
          }
        },
        "line_total": {
          "type": "decimal",
          "precision": 12,
          "scale": 2,
          "generator": {
            "strategy": "formula",
            "expression": "quantity * unit_price * (1 - discount_percent / 100)"
          }
        }
      }
    },

    "return": {
      "description": "Return transactions",
      "primary_key": ["return_id"],
      "columns": {
        "return_id": {
          "type": "integer",
          "generator": { "strategy": "sequence", "start": 1 }
        },
        "order_id": {
          "type": "integer",
          "generator": {
            "strategy": "foreign_key",
            "ref": "order.order_id",
            "filter": "status = 'completed'",
            "sample_rate": 0.08
          }
        },
        "return_date": {
          "type": "timestamp",
          "generator": {
            "strategy": "derived",
            "source": "order.order_date",
            "via": "order_id",
            "rule": "add_days",
            "params": { "distribution": "log_normal", "mean": 2.0, "sigma": 0.8, "min": 1, "max": 90 }
          }
        },
        "reason": {
          "type": "string",
          "generator": {
            "strategy": "weighted_enum",
            "values": {
              "defective": 0.15,
              "wrong_size": 0.25,
              "not_as_described": 0.20,
              "changed_mind": 0.30,
              "arrived_late": 0.05,
              "other": 0.05
            }
          }
        },
        "refund_amount": {
          "type": "decimal",
          "precision": 12,
          "scale": 2,
          "generator": {
            "strategy": "lookup",
            "source_table": "order",
            "source_column": "order_total",
            "via": "order_id"
          }
        }
      }
    }
  }
}
```

### 1.4 Relationship Definitions

```json
{
  "relationships": [
    {
      "name": "customer_addresses",
      "parent": "customer",
      "child": "address",
      "parent_columns": ["customer_id"],
      "child_columns": ["customer_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "uniform",
        "min": 1,
        "max": 3,
        "mean": 1.5
      }
    },
    {
      "name": "category_hierarchy",
      "parent": "product_category",
      "child": "product_category",
      "parent_columns": ["category_id"],
      "child_columns": ["parent_category_id"],
      "type": "self_referencing",
      "levels": 3,
      "children_per_parent": { "min": 3, "max": 8 }
    },
    {
      "name": "product_in_category",
      "parent": "product_category",
      "child": "product",
      "parent_columns": ["category_id"],
      "child_columns": ["category_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "pareto",
        "min": 5,
        "max": 200,
        "alpha": 1.5
      }
    },
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
        "alpha": 1.2,
        "comment": "80/20 — most customers have few orders, some have many"
      }
    },
    {
      "name": "order_lines",
      "parent": "order",
      "child": "order_line",
      "parent_columns": ["order_id"],
      "child_columns": ["order_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "log_normal",
        "mean": 1.2,
        "sigma": 0.6,
        "min": 1,
        "max": 30,
        "comment": "Most orders 1-3 items, occasional large orders"
      }
    },
    {
      "name": "order_returns",
      "parent": "order",
      "child": "return",
      "parent_columns": ["order_id"],
      "child_columns": ["order_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "bernoulli",
        "probability": 0.08,
        "comment": "~8% of completed orders get returned"
      }
    },
    {
      "name": "order_store",
      "parent": "store",
      "child": "order",
      "parent_columns": ["store_id"],
      "child_columns": ["store_id"],
      "type": "one_to_many",
      "cardinality": {
        "distribution": "zipf",
        "alpha": 1.3,
        "comment": "Online store gets disproportionate share"
      }
    },
    {
      "name": "order_promotion",
      "parent": "promotion",
      "child": "order",
      "parent_columns": ["promotion_id"],
      "child_columns": ["promotion_id"],
      "type": "one_to_many",
      "optional": true
    }
  ]
}
```

### 1.5 Business Rules

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
      "name": "return_after_order",
      "type": "cross_table",
      "rule": "return.return_date > order.order_date",
      "via": "order_id"
    },
    {
      "name": "promotion_date_valid",
      "type": "cross_table",
      "rule": "order.order_date BETWEEN promotion.start_date AND promotion.end_date",
      "via": "promotion_id",
      "when": "order.promotion_id IS NOT NULL"
    },
    {
      "name": "shipped_has_address",
      "type": "constraint",
      "table": "order",
      "rule": "IF status IN ('shipped', 'completed') THEN shipping_address_id IS NOT NULL"
    },
    {
      "name": "line_total_positive",
      "type": "constraint",
      "table": "order_line",
      "rule": "line_total > 0"
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

### 1.6 Generation Configuration

```json
{
  "generation": {
    "scale": "medium",
    "scales": {
      "small":  { "customer": 1000,   "product": 500,   "order": 5000 },
      "medium": { "customer": 50000,  "product": 5000,  "order": 500000 },
      "large":  { "customer": 500000, "product": 25000, "order": 5000000 },
      "xlarge": { "customer": 5000000,"product": 100000,"order": 100000000 }
    },
    "derived_counts": {
      "address": { "per_parent": "customer", "ratio": 1.5 },
      "product_category": { "fixed": "from_reference_data" },
      "store": { "fixed": 150 },
      "promotion": { "per_year": 50 },
      "order_line": { "per_parent": "order", "distribution": "log_normal", "mean": 1.2, "sigma": 0.6 },
      "return": { "per_parent": "order", "distribution": "bernoulli", "probability": 0.08 }
    },
    "output": {
      "format": "delta",
      "path": "Tables/",
      "partition_by": {
        "order": ["order_date:year", "order_date:month"],
        "order_line": ["order_date:year"]
      }
    }
  }
}
```

---

## 2. Generator Strategies Reference

These are the column-level generation strategies the engine must support in Phase 0:

| Strategy | Description | Example |
|---|---|---|
| `sequence` | Auto-incrementing integer | PKs |
| `uuid` | UUID v4 generation | Alternative PKs |
| `faker` | Faker library provider | Names, emails, addresses |
| `weighted_enum` | Pick from weighted list | Status codes, categories |
| `distribution` | Statistical distribution | Prices (log-normal), ages (normal) |
| `temporal` | Time-aware generation | Seasonal order dates, business hours |
| `formula` | Computed from other columns | `quantity * unit_price` |
| `derived` | Derived from related column | `return_date = order_date + N days` |
| `correlated` | Value depends on another column | `zip_code` depends on `state` |
| `foreign_key` | References parent table PK | Relationship columns |
| `lookup` | Copy value from related table | `order_line.unit_price` from `product` |
| `reference_data` | Pick from built-in dataset | Category names, product names |
| `pattern` | Formatted string with tokens | SKU codes, order numbers |
| `conditional` | Different gen based on condition | Discount only if promotion exists |
| `computed` | Aggregated from child rows | `order_total = sum(line_totals)` |
| `lifecycle` | Phase-based status | Product active/discontinued |
| `self_referencing` | FK to same table | Category hierarchy |
| `first_per_parent` | Boolean, true for first child | Primary address flag |

### Distribution Types

| Distribution | Use Case | Parameters |
|---|---|---|
| `uniform` | Equal probability range | min, max |
| `normal` | Bell curve (ages, sizes) | mean, std_dev, min, max |
| `log_normal` | Right-skewed (prices, amounts) | mean, sigma, min, max |
| `pareto` | 80/20 distributions (order counts) | alpha, min, max |
| `zipf` | Power law (product popularity) | alpha |
| `geometric` | "How many tries until success" (quantities) | p, min, max |
| `bernoulli` | Yes/no probability (returns) | probability |
| `bimodal` | Two peaks (time of day) | peaks[], std_dev |
| `poisson` | Count events per interval (streaming) | lambda |

---

## 3. Engine Architecture

### 3.1 Core Components

```
sqllocks_spindle/
├── __init__.py              # Public API
├── schema/
│   ├── __init__.py
│   ├── parser.py            # Parse & validate .spindle.json
│   ├── validator.py         # Schema validation (circular refs, type checks)
│   ├── dependency.py        # Topological sort of table generation order
│   └── cdm_mapper.py        # CDM entity mapping (Phase 4+)
├── engine/
│   ├── __init__.py
│   ├── generator.py         # Main generation orchestrator
│   ├── table_generator.py   # Per-table row generation
│   ├── id_manager.py        # Track generated PKs for FK references
│   ├── strategies/
│   │   ├── __init__.py
│   │   ├── base.py          # Strategy interface
│   │   ├── sequence.py
│   │   ├── faker_strategy.py
│   │   ├── distribution.py
│   │   ├── temporal.py
│   │   ├── enum.py
│   │   ├── formula.py
│   │   ├── foreign_key.py
│   │   ├── correlated.py
│   │   ├── reference_data.py
│   │   ├── pattern.py
│   │   └── computed.py
│   └── rules/
│       ├── __init__.py
│       └── business_rules.py  # Post-generation constraint enforcement
├── domains/
│   ├── __init__.py
│   ├── base.py              # Domain base class
│   ├── retail/
│   │   ├── __init__.py
│   │   ├── retail_3nf.spindle.json
│   │   ├── retail_star.spindle.json
│   │   ├── reference_data/
│   │   │   ├── categories.json
│   │   │   ├── product_names.json
│   │   │   └── promo_names.json
│   │   └── retail.py        # RetailDomain class
│   └── ... (future domains)
├── output/
│   ├── __init__.py
│   ├── pandas_writer.py     # DataFrame output (local dev)
│   ├── delta_writer.py      # Delta Lake / Lakehouse output
│   ├── csv_writer.py        # CSV fallback
│   └── stream_writer.py     # Eventstream/Event Hub (Phase 2)
├── data/
│   ├── us_states.json
│   ├── us_zip_by_state.json
│   └── locale/
│       └── en_US/
│           └── ... (locale-specific reference data)
└── cli.py                   # Optional CLI: spindle generate retail --scale medium
```

### 3.2 Generation Pipeline

```
1. PARSE        .spindle.json → internal model
                 ↓
2. VALIDATE     Check types, refs, circular deps, FK targets exist
                 ↓
3. SORT         Topological sort: tables ordered by dependency
                 e.g., [product_category, customer, store, promotion, product, address, order, order_line, return]
                 ↓
4. GENERATE     For each table in dependency order:
                   a. Determine row count (fixed, ratio, or distribution)
                   b. For each row:
                      - Execute column strategies in dependency order
                      - Resolve FKs from id_manager's pool of generated parent PKs
                      - Apply cross-column correlations
                   c. Register generated PKs in id_manager
                   d. Apply business rules (validate & fix violations)
                 ↓
5. COMPUTE      Back-fill computed columns (order_total = sum of line_totals)
                 ↓
6. OUTPUT       Write to configured format (DataFrame, Delta, CSV)
```

### 3.3 ID Manager

The ID Manager is critical for relational integrity. It:

- Maintains a registry of all generated primary key values per table
- Provides FK resolution: "give me a random customer_id" respecting distribution
- Supports constrained FK: "give me an address_id that belongs to customer_id=42"
- Supports filtered FK: "give me an order_id where status='completed'"
- Handles M:N junction tables: allocate pairs from two PK pools
- Tracks used vs. unused parents (ensure no orphan dimensions)

### 3.4 Dependency Resolution

Tables form a DAG (directed acyclic graph) based on FK relationships:

```
product_category ─────→ product ──────────→ order_line
                                                ↑
customer ──→ address                            │
    │                                           │
    └──→ order ─────────────────────────────────┘
              │                         ↑
              └──→ return               │
                                        │
store ──────────────────────────────────┘
promotion ──────────────────────────────┘
```

Topological sort ensures parents are generated before children. Self-referencing tables (category hierarchy) are generated in level-order passes.

---

## 4. Python API (Phase 0 Target)

### 4.1 Quick Start — Pre-built Domain

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

# Generate medium-scale retail data as DataFrames
spindle = Spindle()
result = spindle.generate(
    domain=RetailDomain(schema_mode="3nf"),
    scale="medium",
    seed=42
)

# result is a dict of table_name → pandas DataFrame
print(result["customer"].head())
print(result["order"].shape)       # (500000, 8)
print(result["order_line"].shape)  # (~1.2M, 6)

# Verify referential integrity
assert result["order"]["customer_id"].isin(result["customer"]["customer_id"]).all()
```

### 4.2 Custom Schema

```python
from sqllocks_spindle import Spindle

spindle = Spindle()
result = spindle.generate(
    schema="path/to/my_custom.spindle.json",
    scale_overrides={"customer": 10000, "order": 100000},
    seed=42
)
```

### 4.3 Fabric Notebook — Lakehouse Output

```python
# In a Microsoft Fabric Notebook
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.output import DeltaWriter

spindle = Spindle()
result = spindle.generate(
    domain=RetailDomain(schema_mode="star"),
    scale="large",
    seed=42
)

# Write to Lakehouse
writer = DeltaWriter(lakehouse_path="/lakehouse/default")
writer.write_all(result, partition_config={
    "fact_sales": ["order_year", "order_month"]
})
```

### 4.4 Schema Inspection

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

spindle = Spindle()
schema = spindle.describe(RetailDomain(schema_mode="3nf"))

print(schema.tables)          # ['customer', 'address', 'product_category', ...]
print(schema.relationships)   # [('customer', '1:N', 'address'), ...]
print(schema.dependency_order) # ['product_category', 'customer', 'store', ...]
print(schema.estimated_rows("medium"))  # {'customer': 50000, 'order': 500000, ...}
```

---

## 5. Phase 0 Deliverables

### Must Have (MVP)
1. **Schema Definition Format** — `.spindle.json` spec, fully documented
2. **Schema parser & validator** — load, validate, dependency sort
3. **Core generation engine** — execute strategies in dependency order
4. **10 generator strategies** — sequence, faker, weighted_enum, distribution (uniform, normal, log_normal, pareto), temporal (basic), formula, foreign_key, lookup, reference_data
5. **ID Manager** — PK tracking, FK resolution with distribution support
6. **Business rules engine** — cross-column and cross-table constraint validation
7. **Retail domain (3NF)** — 9 tables, complete .spindle.json, reference data
8. **Pandas output** — generate to DataFrames (local testing)
9. **Basic CLI** — `spindle generate retail --scale small --seed 42`
10. **Referential integrity verification** — built-in validation that all FKs resolve

### Nice to Have (Phase 0+)
- Retail star schema variant
- CSV output writer
- Progress bar for large generation runs
- Schema diff tool (compare two .spindle.json files)
- Basic statistics report on generated data

### Explicitly Deferred
- Delta Lake / Lakehouse writer (Phase 1)
- PySpark parallelization (Phase 1)
- Streaming / Eventstream (Phase 2)
- All non-Retail domains (Phase 3)
- CDM mapping (Phase 4)
- MCP server companion (Phase 4)
- pip packaging and PyPI release (Phase 5)

---

## 6. Technical Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | Python 3.10+ | Fabric Notebooks, community familiarity |
| Core dependencies | `faker`, `numpy`, `pandas` | All available in Fabric Notebooks by default |
| Schema format | JSON (`.spindle.json`) | Machine-readable, ties into Data Modeler ecosystem |
| Random engine | `numpy.random.Generator` | Reproducible with seed, fast, full distribution support |
| FK strategy | ID Manager with pooled arrays | O(1) random FK lookup via numpy array indexing |
| Business rules | Post-generation fix-up pass | Generate fast, validate after, fix violations |
| Output | DataFrames first, Delta later | DataFrames are the universal interchange format in Python |
| CLI | Click library | Clean, composable, well-documented |
| Testing | pytest | Standard, with property-based tests for distribution accuracy |

---

## 7. Retail Domain — Reference Data Needed

Pre-built datasets to ship with the Retail domain:

| Dataset | Contents | Approximate Size |
|---|---|---|
| `categories.json` | 3-level hierarchy: 8 departments, ~40 categories, ~150 subcategories | ~200 entries |
| `product_names.json` | Realistic product names per subcategory | ~2000 names |
| `promo_names.json` | Promotion campaign names | ~100 names |
| `us_states.json` | State codes + population weights | 51 entries |
| `us_zip_by_state.json` | ZIP code ranges per state | ~500 entries |
| `merchant_categories.json` | MCC-like codes for retail | ~50 codes |

---

## 8. Success Criteria

Phase 0 is DONE when:

1. `spindle generate retail --scale small` produces 9 tables of data in < 30 seconds
2. 100% referential integrity — every FK resolves to an existing PK
3. Business rules pass — order_date > signup_date, return_date > order_date, etc.
4. Distributions are visually correct — plot order counts per customer, see Pareto shape
5. Temporal patterns visible — plot orders by month, see November/December spike
6. Reproducible — same seed produces identical output
7. Zero external dependencies beyond faker, numpy, pandas
8. A data engineer can read the .spindle.json and understand the schema without docs
