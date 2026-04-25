# Tutorial 02: Explore All Domains

Survey every built-in domain Spindle offers and compare their schemas side by side.

---

## Prerequisites

- Completed [Tutorial 01: Hello Spindle](01-hello-spindle.md) (or have `sqllocks-spindle` installed)
- Basic familiarity with relational schemas (tables, foreign keys)

## What You'll Learn

- How to import and list all 13 built-in domain classes
- How to compare table counts and relationship counts across domains
- How to deep-dive into a domain's schema structure and generation order
- How to generate two domains side by side and compare their output

---

## Step 1: Import All Domain Classes

Spindle ships with 13 pre-built domains. Each one is a self-contained schema definition — a blueprint for an entire relational database.

```python
from sqllocks_spindle import (
    RetailDomain,
    HealthcareDomain,
    FinancialDomain,
    SupplyChainDomain,
    IoTDomain,
    HrDomain,
    InsuranceDomain,
    MarketingDomain,
    EducationDomain,
    RealEstateDomain,
    ManufacturingDomain,
    TelecomDomain,
    CapitalMarketsDomain,
)

print("All 13 domain classes imported successfully!")
```

## Step 2: List Every Domain

Instantiate each domain, call `get_schema()`, and print a summary table. This gives you a bird's-eye view of everything Spindle offers without reading documentation page by page.

```python
domains = [
    RetailDomain(),
    HealthcareDomain(),
    FinancialDomain(),
    SupplyChainDomain(),
    IoTDomain(),
    HrDomain(),
    InsuranceDomain(),
    MarketingDomain(),
    EducationDomain(),
    RealEstateDomain(),
    ManufacturingDomain(),
    TelecomDomain(),
]

print(f"{'Domain':<22} {'Description':<45} {'Tables':>6} {'FKs':>6}")
print("-" * 82)

for domain in domains:
    schema = domain.get_schema()
    print(
        f"{schema.name:<22} "
        f"{schema.description[:43]:<45} "
        f"{len(schema.tables):>6} "
        f"{len(schema.relationships):>6}"
    )

print(f"\nTotal domains: {len(domains)}")
```

Each domain exposes a `name`, `description`, a `tables` dictionary, and a list of `relationships` (foreign keys). This loop prints them all in a compact comparison table.

## Step 3: Deep-Dive into the Retail Domain

Pick any domain and inspect its internal structure. Here we will look at the Retail domain's tables, column counts, and generation order.

```python
retail = RetailDomain()
schema = retail.get_schema()

print(f"Domain: {schema.name}")
print(f"Description: {schema.description}")
print(f"\n=== Tables ({len(schema.tables)}) ===")
for table_name, table_def in schema.tables.items():
    col_count = len(table_def.columns)
    print(f"  {table_name:<30} {col_count:>3} columns")

print(f"\n=== Generation Order ===")
for i, table_name in enumerate(schema.generation_order, 1):
    print(f"  {i}. {table_name}")
```

The generation order reveals the dependency graph. Parent tables like `customers` are generated first, then child tables like `orders` that reference them. This is how Spindle guarantees referential integrity without post-processing.

## Step 4: Generate and Compare Retail vs. Healthcare

Generate both domains at the `fabric_demo` scale and compare their summaries and timing.

```python
import time
from sqllocks_spindle import Spindle

# Generate Retail
t0 = time.perf_counter()
retail_result = Spindle.generate(
    domain=RetailDomain(), scale="fabric_demo", seed=42
)
retail_time = time.perf_counter() - t0

# Generate Healthcare
t0 = time.perf_counter()
healthcare_result = Spindle.generate(
    domain=HealthcareDomain(), scale="fabric_demo", seed=42
)
healthcare_time = time.perf_counter() - t0

print("=== Retail Domain ===")
print(retail_result.summary())

print("\n=== Healthcare Domain ===")
print(healthcare_result.summary())

print("\n=== Timing Comparison ===")
print(f"  Retail:     {retail_time:.2f}s")
print(f"  Healthcare: {healthcare_time:.2f}s")
```

Different domains have different complexities -- more tables, more relationships, more columns. This comparison helps you understand performance characteristics and choose appropriate scales for your use case.

---

> **Run It Yourself**
>
> - Notebook: [`T02_explore_all_domains.ipynb`](../../../examples/notebooks/quickstart/T02_explore_all_domains.ipynb)
> - Script: [`02_exploring_results.py`](../../../examples/scenarios/02_exploring_results.py)

---

## Related

- [Domain Catalog](../../domains/index.md) -- full reference for all 13 domains with table schemas and descriptions

---

## Next Step

[Tutorial 03: Custom Schemas](03-custom-schemas.md) -- define your own tables, columns, and relationships from scratch.
