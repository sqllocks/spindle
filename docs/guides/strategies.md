# Generation Strategies

Spindle uses 21 column-level generation strategies. Each column in a `.spindle.json` schema specifies a `generator` with a `strategy` name and strategy-specific parameters.

## Quick Reference

| Strategy | Purpose | Example Use |
| --- | --- | --- |
| `sequence` | Auto-incrementing integers | Primary keys |
| `uuid` | UUID v4 strings | Alternative primary keys |
| `faker` | Realistic fake data via Faker | Names, emails, phone numbers |
| `weighted_enum` | Weighted random selection | Status codes, categories |
| `distribution` | Statistical distributions | Prices, ages, quantities |
| `temporal` | Time-aware dates/timestamps | Order dates with seasonality |
| `formula` | Computed from other columns | `quantity * unit_price` |
| `derived` | Transformed from another column | `return_date = order_date + N days` |
| `correlated` | Mathematically related | `cost = unit_price * 0.30-0.70` |
| `conditional` | Different logic per row | Discount only if promotion exists |
| `lifecycle` | Phase-based status values | active / introduced / discontinued |
| `foreign_key` | FK references with distribution | Pareto, Zipf, or uniform FK assignment |
| `lookup` | Copy value from parent table | Line item price from product |
| `reference_data` | Pick from bundled datasets | ZIP codes, ICD-10 codes |
| `pattern` | Formatted strings with tokens | `SKU-{seq:6}`, `Store #{seq:04d}` |
| `computed` | Aggregated from child rows | `order_total = sum(line_totals)` |
| `self_referencing` | FK to same table | Category hierarchy (parent_id) |
| `self_ref_field` | Read hierarchy metadata | Level number from self-referencing |
| `first_per_parent` | Boolean flag for first child | Primary address marker |
| `record_sample` | Sample complete records | Anchor for correlated reference data |
| `record_field` | Read field from sampled record | city/state/zip from sampled address |

---

## Primary Key Strategies

### `sequence`

Auto-incrementing integer sequences.

```json
{
  "strategy": "sequence",
  "start": 1,
  "step": 1
}
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `start` | int | `1` | Starting value |
| `step` | int | `1` | Increment per row |

### `uuid`

UUID v4 strings. No parameters.

```json
{
  "strategy": "uuid"
}
```

---

## Data Generation Strategies

### `faker`

Generate realistic fake data using the [Faker](https://faker.readthedocs.io/) library.

```json
{
  "strategy": "faker",
  "provider": "first_name"
}
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `provider` | str | — | Faker provider name (e.g., `first_name`, `email`, `city`, `phone_number`) |
| `args` | dict | `{}` | Arguments passed to the Faker provider |

The locale is inherited from `model.locale` in the schema (default `en_US`).

Common providers: `first_name`, `last_name`, `name`, `email`, `phone_number`, `street_address`, `city`, `state_abbr`, `zipcode`, `company`, `url`, `ssn`, `sentence`, `text`, `user_name`, `ipv4`.

### `weighted_enum`

Pick values from a weighted set.

```json
{
  "strategy": "weighted_enum",
  "values": {
    "completed": 0.77,
    "shipped": 0.08,
    "processing": 0.02,
    "cancelled": 0.04,
    "returned": 0.09
  }
}
```

| Param | Type | Description |
| --- | --- | --- |
| `values` | dict | `{value: weight}` — weights are normalized automatically |

!!! note
    If all keys are numeric strings (e.g., `"0.0"`, `"10.0"`), the strategy returns a `float64` array instead of strings.

### `distribution`

Statistical distributions powered by NumPy.

```json
{
  "strategy": "distribution",
  "distribution": "log_normal",
  "params": {"mean": 3.5, "sigma": 1.2, "min": 0.99, "max": 2999.99}
}
```

| Param | Type | Description |
| --- | --- | --- |
| `distribution` | str | Distribution name (see table below) |
| `params` | dict | Distribution-specific parameters |

**Available distributions:**

| Distribution | Params | Use Case |
| --- | --- | --- |
| `uniform` | `min`, `max` | Equal probability range |
| `normal` | `mean`, `std_dev`, `min`, `max` | Bell curve (ages, sizes) |
| `log_normal` | `mean`, `sigma`, `min`, `max` | Right-skewed (prices, amounts) |
| `pareto` | `alpha`, `min`, `max` | 80/20 distributions (order frequency) |
| `zipf` | `alpha` | Power law (product popularity) |
| `geometric` | `p`, `min`, `max` | "Tries until success" (quantities) |
| `poisson` | `lambda` | Count events per interval |
| `bernoulli` | `probability` | Yes/no (returns, churn) |

### `temporal`

Time-aware date and timestamp generation with optional seasonality.

=== "Uniform"

    ```json
    {
      "strategy": "temporal",
      "pattern": "uniform",
      "range_ref": "model.date_range"
    }
    ```

=== "Seasonal"

    ```json
    {
      "strategy": "temporal",
      "pattern": "seasonal",
      "range_ref": "model.date_range",
      "profiles": {
        "month": {"Jan": 0.06, "Feb": 0.06, "Nov": 0.11, "Dec": 0.13},
        "day_of_week": {"Mon": 0.13, "Fri": 0.16, "Sat": 0.17},
        "hour_of_day": {"distribution": "bimodal", "peaks": [11, 20], "std_dev": 2}
      }
    }
    ```

| Param | Type | Description |
| --- | --- | --- |
| `pattern` | str | `uniform` or `seasonal` |
| `range` | dict | `{start, end}` date strings |
| `range_ref` | str | Reference to model-level date range (e.g., `model.date_range`) |
| `profiles` | dict | Monthly, day-of-week, and hour-of-day weight profiles |

### `lifecycle`

Assign phase labels based on weighted probabilities.

```json
{
  "strategy": "lifecycle",
  "phases": {
    "introduced": 0.10,
    "active": 0.75,
    "discontinued": 0.15
  }
}
```

| Param | Type | Description |
| --- | --- | --- |
| `phases` | dict | `{phase_name: weight}` — same as weighted_enum but semantically for lifecycle states |

---

## Relationship Strategies

### `foreign_key`

Reference parent table primary keys with configurable distribution.

```json
{
  "strategy": "foreign_key",
  "ref": "customer.customer_id",
  "distribution": "pareto",
  "params": {"alpha": 1.16}
}
```

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `ref` | str | — | `table.column` reference to parent PK |
| `distribution` | str | `uniform` | `uniform`, `pareto`, or `zipf` |
| `params` | dict | `{}` | Distribution parameters (e.g., `alpha`) |
| `constrained_by` | str | — | Scope FK to match another FK (e.g., address must belong to same customer) |
| `sample_rate` | float | — | Sample only a fraction of parent PKs |
| `filter` | str | — | SQL-like filter on parent rows (e.g., `status = 'completed'`) |

!!! tip
    Use `distribution: "pareto"` with `alpha: 1.16` for the classic 80/20 rule — 20% of customers generate 80% of orders.

### `lookup`

Copy a value from a parent table via a foreign key join.

```json
{
  "strategy": "lookup",
  "source_table": "product",
  "source_column": "unit_price",
  "via": "product_id"
}
```

| Param | Type | Description |
| --- | --- | --- |
| `source_table` | str | Parent table name |
| `source_column` | str | Column to copy from parent |
| `via` | str | FK column in current table that links to parent |

### `self_referencing`

Create a hierarchy within a single table (e.g., category tree, org chart).

```json
{
  "strategy": "self_referencing",
  "pk_column": "category_id",
  "root_count": 8
}
```

| Param | Type | Description |
| --- | --- | --- |
| `pk_column` | str | Primary key column of the same table |
| `root_count` | int | Number of root-level rows (NULL parent) |
| `levels` | int | Number of hierarchy levels (from relationship def) |

### `self_ref_field`

Read metadata stashed by a `self_referencing` strategy (e.g., the hierarchy level).

```json
{
  "strategy": "self_ref_field",
  "field": "level"
}
```

### `first_per_parent`

Mark the first child row per parent group as `True`, rest as `False`.

```json
{
  "strategy": "first_per_parent",
  "parent_column": "customer_id",
  "default": true
}
```

---

## Computed & Derived Strategies

### `formula`

Compute a column value from other columns using a math expression.

```json
{
  "strategy": "formula",
  "expression": "quantity * unit_price * (1 - discount_percent / 100)"
}
```

| Param | Type | Description |
| --- | --- | --- |
| `expression` | str | Python math expression referencing other column names |

The expression is evaluated with safe builtins only (no arbitrary code execution).

### `derived`

Derive a value from another column with an optional transformation.

=== "Same table"

    ```json
    {
      "strategy": "derived",
      "source": "start_date",
      "rule": "add_days",
      "params": {"distribution": "uniform", "min": 1, "max": 30}
    }
    ```

=== "Cross-table"

    ```json
    {
      "strategy": "derived",
      "source": "order.order_date",
      "via": "order_id",
      "rule": "add_days",
      "params": {"distribution": "log_normal", "mean": 2.0, "sigma": 0.8, "min": 1, "max": 90}
    }
    ```

| Param | Type | Description |
| --- | --- | --- |
| `source` | str | Source column (or `table.column` for cross-table) |
| `via` | str | FK column for cross-table lookup |
| `rule` | str | Transformation: `copy`, `add_days` |
| `params` | dict | Rule-specific parameters |

### `correlated`

Generate a value mathematically related to another column.

```json
{
  "strategy": "correlated",
  "source_column": "unit_price",
  "rule": "multiply",
  "params": {"factor_min": 0.30, "factor_max": 0.70}
}
```

| Param | Type | Description |
| --- | --- | --- |
| `source_column` | str | Column to correlate with |
| `rule` | str | `multiply`, `add`, `subtract` |
| `params` | dict | `factor_min`/`factor_max` (multiply) or `offset_min`/`offset_max` (add/subtract) |

### `conditional`

Generate different values depending on a row-level condition.

```json
{
  "strategy": "conditional",
  "condition": "promo_id IS NOT NULL",
  "true_generator": {"strategy": "lookup", "source_table": "promotion", "source_column": "discount_value", "via": "order.promotion_id"},
  "false_generator": {"fixed": 0.00}
}
```

| Param | Type | Description |
| --- | --- | --- |
| `condition` | str | `IS NOT NULL`, `IS NULL`, `== value`, `!= value` |
| `true_generator` | dict | Generator config for rows matching condition |
| `false_generator` | dict | Generator config for rows not matching condition |

Inline generators can be a full strategy dict or `{"fixed": value}`.

### `computed`

Placeholder for post-generation aggregation from child tables. Backfilled after all tables are generated.

```json
{
  "strategy": "computed",
  "rule": "sum_children",
  "child_table": "order_line",
  "child_column": "line_total"
}
```

| Param | Type | Description |
| --- | --- | --- |
| `rule` | str | `sum_children`, `count_children`, `avg_children`, `min_children`, `max_children`, `lookup_parent` |
| `child_table` | str | Child table to aggregate from |
| `child_column` | str | Column to aggregate |
| `parent_table` | str | For `lookup_parent`: parent table |
| `via` | str | FK column linking child to parent |

---

## Reference Data Strategies

### `reference_data`

Pick values from bundled JSON datasets shipped with each domain.

```json
{
  "strategy": "reference_data",
  "dataset": "retail_categories"
}
```

| Param | Type | Description |
| --- | --- | --- |
| `dataset` | str | Name of the reference dataset (domain-specific) |

### `record_sample`

Sample complete records from a reference dataset. This is the **anchor** strategy — it picks one record per row and stashes all fields for use by `record_field`.

```json
{
  "strategy": "record_sample",
  "dataset": "us_zip_locations",
  "field": "city"
}
```

| Param | Type | Description |
| --- | --- | --- |
| `dataset` | str | Name of a JSON dataset containing arrays of objects |
| `field` | str | Which field to use as this column's value |

### `record_field`

Read a field from records already sampled by `record_sample`. Must appear after the anchor column in schema order.

```json
{
  "strategy": "record_field",
  "dataset": "us_zip_locations",
  "field": "state"
}
```

This is how Spindle generates correlated multi-column reference data (e.g., city + state + zip + lat + lng all from the same real US location).

---

## String Formatting

### `pattern`

Generate formatted strings with token substitution.

```json
{
  "strategy": "pattern",
  "format": "SKU-{category_code}-{seq:6}"
}
```

| Token | Description |
| --- | --- |
| `{seq:N}` | Zero-padded sequence number (N digits) |
| `{random:N}` | Random alphanumeric string (N chars) |
| `{column_name}` | Value from another column in the same row |

---

## See Also

- **Tutorial:** [01: Hello Spindle](../tutorials/beginner/01-hello-spindle.md) — step-by-step walkthrough
- **Tutorial:** [03: Custom Schemas](../tutorials/beginner/03-custom-schemas.md) — step-by-step walkthrough
- **Example script:** [`01_hello_world.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/01_hello_world.py)
- **Example script:** [`05_distribution_overrides.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/05_distribution_overrides.py)
- **Notebook:** [`T01_hello_spindle.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/quickstart/T01_hello_spindle.ipynb)
- **Notebook:** [`T05_distribution_overrides.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/quickstart/T05_distribution_overrides.ipynb)
