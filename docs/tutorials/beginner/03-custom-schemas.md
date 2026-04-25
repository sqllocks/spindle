# Tutorial 03: Custom Schemas

Build a complete schema from a Python dictionary with two tables, three generation strategies, and a foreign-key relationship.

---

## Prerequisites

- Completed [Tutorial 01: Hello Spindle](01-hello-spindle.md) and [Tutorial 02: Explore Domains](02-explore-domains.md)
- Comfortable with `Spindle.generate()` and the `GenerationResult` object

## What You'll Learn

- How to define a custom schema as a Python dict (the canonical input format)
- How to configure `weighted_enum`, `distribution`, and `formula` generation strategies
- How to wire tables together with foreign-key relationships using a Pareto distribution
- How to verify referential integrity on custom-generated data

---

## Step 1: Import Spindle

Every Spindle workflow starts with this single import. When you use a dict-based schema, no additional domain classes are needed -- the dict itself carries all configuration.

```python
from sqllocks_spindle import Spindle

print("Spindle imported successfully.")
```

## Step 2: Define the Schema Dict

We will build a tiny company model with two tables -- `department` and `employee` -- linked by a foreign key. The dict has five top-level keys:

| Key | Purpose |
|-----|---------|
| `model` | Metadata: name, domain, locale, seed, date range |
| `tables` | One entry per table with column definitions and generators |
| `relationships` | Foreign-key links between tables |
| `business_rules` | Cross-column constraints (empty for now) |
| `generation` | Row-count scales |

```python
schema = {
    "model": {
        "name": "my_company",
        "domain": "custom",
        "schema_mode": "3nf",
        "locale": "en_US",
        "seed": 42,
        "date_range": {"start": "2023-01-01", "end": "2025-12-31"},
    },
    "tables": {
        "department": {
            "columns": {
                "department_id": {"type": "integer", "generator": "sequence"},
                "name": {
                    "type": "string",
                    "generator": "enum",
                    "values": [
                        "Engineering", "Sales", "Marketing",
                        "Finance", "HR", "Legal",
                        "Operations", "Support", "Product", "Design",
                    ],
                },
            },
            "primary_key": "department_id",
        },
        "employee": {
            "columns": {
                "employee_id": {"type": "integer", "generator": "sequence"},
                "first_name": {"type": "string", "generator": "first_name"},
                "last_name": {"type": "string", "generator": "last_name"},
                "department_id": {
                    "type": "integer",
                    "generator": "foreign_key",
                    "references": "department.department_id",
                },
                "status": {
                    "type": "string",
                    "generator": "weighted_enum",
                    "values": ["active", "on_leave", "terminated"],
                    "weights": [0.80, 0.10, 0.10],
                },
                "monthly_salary": {
                    "type": "float",
                    "generator": "distribution",
                    "distribution": "log_normal",
                    "mean": 8.5,
                    "sigma": 0.4,
                },
                "hire_date": {"type": "date", "generator": "date_range"},
            },
            "primary_key": "employee_id",
        },
    },
    "relationships": [
        {
            "from": "employee.department_id",
            "to": "department.department_id",
            "type": "many_to_one",
            "distribution": "pareto",
        }
    ],
    "business_rules": [],
    "generation": {
        "scales": {
            "small": {"department": 10, "employee": 200},
        }
    },
}
```

Notice the three strategies at work in the `employee` table:

- **`weighted_enum`** on `status` -- draws from `["active", "on_leave", "terminated"]` with 80/10/10 probability.
- **`distribution`** on `monthly_salary` -- samples from a log-normal distribution (`mean=8.5`, `sigma=0.4`), producing a realistic right-skewed salary curve.
- **`foreign_key`** on `department_id` -- references the parent table. The relationship block adds `"distribution": "pareto"` so a few departments get most of the employees (the 80/20 rule).

## Step 3: Generate the Data

Pass the schema dict to `Spindle` and call `generate()`:

```python
spindle = Spindle(schema)
data = spindle.generate(scale="small")

print(f"Tables generated: {list(data.keys())}")
for name, df in data.items():
    print(f"  {name}: {len(df)} rows, {list(df.columns)}")
```

This produces 10 departments and 200 employees, all wired together by the foreign key.

## Step 4: Verify Foreign-Key Integrity

Confirm that every `department_id` in the `employee` table exists in the `department` table:

```python
dept_ids = set(data["department"]["department_id"])
emp_dept_ids = set(data["employee"]["department_id"])

print("Department IDs:", sorted(dept_ids))
print(f"Employee references {len(emp_dept_ids)} unique departments.")

orphans = emp_dept_ids - dept_ids
print(f"Orphan department_ids in employee table: "
      f"{orphans if orphans else 'None -- FK integrity confirmed!'}")
```

Zero orphans means Spindle honored the relationship contract.

## Step 5: Inspect the Weighted Enum Distribution

Check that the `status` column approximates the 80/10/10 split we configured:

```python
status_counts = data["employee"]["status"].value_counts()
status_pct = data["employee"]["status"].value_counts(normalize=True).mul(100).round(1)

print("Employee Status Distribution")
print("=" * 35)
for val in status_counts.index:
    print(f"  {val:<15} {status_counts[val]:>4} rows  ({status_pct[val]}%)")
print(f"\nTotal employees: {len(data['employee'])}")
```

With 200 rows and a seed of 42, you should see roughly 160 active, 20 on leave, and 20 terminated.

## Step 6: Inspect the Salary Distribution

The log-normal distribution produces a right-skewed salary curve. Since `exp(8.5)` is approximately 4,915, the median monthly salary should land around $4,900 (~$59k/year), with a long tail of higher earners:

```python
salary = data["employee"]["monthly_salary"]

print("Monthly Salary Distribution")
print("=" * 35)
print(f"  Mean:   ${salary.mean():>10,.2f}")
print(f"  Std:    ${salary.std():>10,.2f}")
print(f"  Min:    ${salary.min():>10,.2f}")
print(f"  Median: ${salary.median():>10,.2f}")
print(f"  Max:    ${salary.max():>10,.2f}")
print(f"\nSkewness: {salary.skew():.2f}  (positive = right-skewed, as expected)")
```

## Step 7: Observe the Pareto FK Distribution

Count employees per department. Because we set `"distribution": "pareto"` on the relationship, a few departments will have disproportionately many employees:

```python
import pandas as pd

dept_counts = (
    data["employee"]
    .merge(data["department"], on="department_id")
    .groupby("name")
    .size()
    .sort_values(ascending=False)
)

print("Employees per Department (Pareto FK distribution)")
print("=" * 50)
cumulative = 0
for dept, count in dept_counts.items():
    cumulative += count
    pct = count / len(data["employee"]) * 100
    cum_pct = cumulative / len(data["employee"]) * 100
    print(f"  {dept:<15} {count:>4} employees  ({pct:5.1f}%)  cumulative: {cum_pct:5.1f}%")
```

The top 2-3 departments should contain a large share of the 200 employees, mimicking real-world organizational distributions.

## Step 8: Add a Formula Column

The `formula` strategy lets you derive a column from other columns inside the schema itself -- no post-processing needed. Add `annual_salary` computed as `monthly_salary * 12`:

```python
schema["tables"]["employee"]["columns"]["annual_salary"] = {
    "type": "float",
    "generator": "formula",
    "expression": "monthly_salary * 12",
}

spindle = Spindle(schema)
data = spindle.generate(scale="small")

emp = data["employee"]
print("Sample: monthly vs. annual salary")
print(emp[["first_name", "last_name", "monthly_salary", "annual_salary"]].head(8).to_string(index=False))

match = (emp["annual_salary"].round(2) == (emp["monthly_salary"] * 12).round(2)).all()
print(f"\nFormula check (annual == monthly * 12): {match}")
```

The formula holds for every row, and the column is always present because it is declared in the schema.

---

> **Run It Yourself**
>
> - Notebook: [`T03_custom_schema.ipynb`](../../../examples/notebooks/quickstart/T03_custom_schema.ipynb)

---

## Related

- [Custom Schemas guide](../../guides/custom-schemas.md) -- full reference for the schema dict format and all available options

---

## Next Step

[Tutorial 04: Output Formats](04-output-formats.md) -- export your data to CSV, Parquet, Excel, SQL INSERT, and more.
