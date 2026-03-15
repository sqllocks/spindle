# Tutorial 09: Composite Domains

Combine multiple Spindle domains into a single generation run with prefixed table names, shared seeds, and cross-domain foreign-key relationships.

## Prerequisites

- Completed [Tutorial 08: Validation Gates](08-validation-gates.md) (or equivalent experience)
- Familiarity with at least two Spindle domains (Retail, HR, Healthcare, Financial, etc.)
- Basic understanding of foreign-key relationships across systems

## What You'll Learn

- What `CompositeDomain` is and when to use it
- How table name prefixing prevents collisions across domains
- How `SharedEntityRegistry` and `SharedConcept` wire cross-domain FKs
- How to define explicit shared entities (PERSON, LOCATION, ORGANIZATION)
- How to use auto-registry for default concept mappings
- How to use named presets for common domain combinations

## Time Estimate

**~20 minutes**

---

## Why Composite Domains?

Real enterprises do not live in a single domain. A company has employees (HR), sells products (retail), and carries liability policies (insurance) -- all at the same time. Generating each domain separately means manual key alignment, potential table name collisions, and no cross-domain relationships.

| Without Composites | With Composites |
|---|---|
| Generate each domain separately | One call generates everything |
| Manual key alignment across datasets | Shared seed ensures consistency |
| Table name collisions (`customer` in retail vs. insurance) | Prefixed names (`retail_customer`, `insurance_policyholder`) |
| No cross-domain FKs | Optional cross-domain relationships |

## Step 1 -- Create a Composite Domain

Import two domain classes and wrap them in a `CompositeDomain`. This tells Spindle to generate both schemas in a single pass:

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.hr import HrDomain
from sqllocks_spindle.domains.composite import CompositeDomain

retail = RetailDomain()
hr = HrDomain()
composite = CompositeDomain(domains=[retail, hr])

print(f"Included domains: {[d.name for d in composite.domains]}")
print(f"Combined tables:  {len(composite.tables)}")
```

A `CompositeDomain` is not just a convenience wrapper -- it coordinates seed propagation, key ranges, and table prefixing so that the two domains coexist without collisions.

## Step 2 -- Generate the Composite Dataset

Pass the composite domain into `Spindle().generate()` just like any single domain. The API is identical:

```python
result = Spindle().generate(domain=composite, scale="small", seed=42)

print("Composite Dataset -- Retail + HR")
print("=" * 50)
for table_name, df in result.tables.items():
    print(f"  {table_name:<30} {len(df):>8,} rows")

print(f"\nTotal tables: {len(result.tables)}")
print(f"Total rows:   {sum(len(df) for df in result.tables.values()):,}")
```

The `scale` parameter applies proportionally to each domain, so `small` gives you a small retail dataset *and* a small HR dataset in one call.

## Step 3 -- Inspect Prefixed Table Names

Table names are automatically prefixed with the domain name to prevent collisions. Both retail and HR might have a `customer` or `address` table -- the prefix eliminates ambiguity:

```python
domain_groups = {}
for table_name in result.tables:
    prefix = table_name.split("_")[0]
    domain_groups.setdefault(prefix, []).append(table_name)

for domain_prefix, tables in domain_groups.items():
    total_rows = sum(len(result.tables[t]) for t in tables)
    print(f"\n=== {domain_prefix.upper()} Domain ({len(tables)} tables, {total_rows:,} rows) ===")
    for t in sorted(tables):
        df = result.tables[t]
        print(f"  {t:<30} {len(df):>6,} rows  |  cols: {list(df.columns)[:5]}...")
```

In a lakehouse or warehouse with dozens of tables, this prefixing makes it trivial to filter tables by domain in your catalog.

## Step 4 -- Define Shared Entities for Cross-Domain FKs

Cross-domain foreign keys express real-world relationships: the person who fulfills an order (retail) is the same person who has a salary record (HR). Use the `shared_entities` parameter to wire these connections:

```python
from sqllocks_spindle.domains.composite import CompositeDomain

composite_rh = CompositeDomain(
    domains=[RetailDomain(), HrDomain()],
    shared_entities={
        "person": {
            "primary": "hr.employee",                     # HR owns the canonical person record
            "links": {
                "retail": "customer.employee_id",         # retail customer FK -> hr employee
            },
        },
    },
)

result_rh = Spindle().generate(domain=composite_rh, scale="fabric_demo", seed=42)
print(f"Tables generated: {sorted(result_rh.tables.keys())}")
```

The `primary` field identifies which domain owns the canonical record for a concept. The `links` dictionary maps other domains to the FK column that references it.

## Step 5 -- Three-Domain Composite with Multiple Shared Concepts

Scale up to three domains with both PERSON and LOCATION shared concepts:

```python
from sqllocks_spindle.domains.financial.financial import FinancialDomain

composite_3 = CompositeDomain(
    domains=[RetailDomain(), HrDomain(), FinancialDomain()],
    shared_entities={
        "person": {
            "primary": "hr.employee",
            "links": {
                "retail":    "customer.employee_id",
                "financial": "customer.employee_id",
            },
        },
        "location": {
            "primary": "retail.store",
            "links": {
                "financial": "branch.store_id",
            },
        },
    },
)

result_3 = Spindle().generate(domain=composite_3, scale="fabric_demo", seed=42)
print(f"Tables generated: {len(result_3.tables)}")
print(f"Total rows: {sum(len(df) for df in result_3.tables.values()):,}")
```

## Step 6 -- Auto-Registry with SharedEntityRegistry

Instead of manually specifying shared entities, use `SharedEntityRegistry` with its built-in default concept mappings. The registry knows which tables in each domain represent PERSON, LOCATION, ORGANIZATION, and CALENDAR concepts:

```python
from sqllocks_spindle.domains.shared_registry import SharedConcept, SharedEntityRegistry

# List available shared concepts
for concept in SharedConcept:
    print(f"  {concept.value}")

# Use auto-registry -- no explicit shared_entities needed
registry = SharedEntityRegistry()
composite_auto = CompositeDomain(
    domains=[RetailDomain(), HealthcareDomain()],
    registry=registry,
)

result_auto = Spindle().generate(domain=composite_auto, scale="fabric_demo", seed=42)
print(f"Tables: {sorted(result_auto.tables.keys())}")
```

The auto-registry is convenient when you want default cross-domain wiring without specifying every link manually.

## Step 7 -- Verify Cross-Domain FK Integrity

After generating a composite dataset with shared entities, verify that the cross-domain foreign keys are valid:

```python
tables = result_rh.tables

if "employee" in tables and "customer" in tables:
    emp_ids = set(tables["employee"]["employee_id"].dropna().unique())
    cust_emp_fk = tables["customer"].get("employee_id")
    if cust_emp_fk is not None:
        linked = cust_emp_fk.dropna()
        orphans = linked[~linked.isin(emp_ids)]
        print(f"HR employees:             {len(emp_ids):,}")
        print(f"Retail->HR FK references: {len(linked):,}")
        print(f"Orphan references:        {len(orphans):,}  (should be 0)")
```

Zero orphans confirms that Spindle correctly wired the cross-domain relationship.

## Step 8 -- Use Named Presets

For common enterprise scenarios, Spindle ships named presets -- pre-configured composite domains with cross-domain relationships already wired:

```python
from sqllocks_spindle.presets import list_presets, get_preset

# List all available presets
print("=== Available Presets ===")
for p in list_presets():
    print(f"  {p.name:<20} {p.description}")

# Use a preset
preset = get_preset("enterprise_core")
print(f"\nPreset: {preset.name}")
print(f"Description: {preset.description}")
print(f"Domains: {[d.name for d in preset.domain.domains]}")

preset_result = Spindle().generate(domain=preset.domain, scale="small", seed=42)
print(f"Generated {len(preset_result.tables)} tables, "
      f"{sum(len(df) for df in preset_result.tables.values()):,} total rows")
```

Presets encode best-practice domain combinations so you do not have to know which domains pair well and how their keys should align.

## Step 9 -- Access Child Domains

The `child_domains` property lets you inspect which domains are inside a composite:

```python
for d in composite_3.child_domains:
    print(f"  {d.__class__.__name__}")
```

```
  RetailDomain
  HrDomain
  FinancialDomain
```

---

> **Run It Yourself**
>
> - Notebook: [`T16_composite_domains.ipynb`](../../../examples/notebooks/intermediate/T16_composite_domains.ipynb)
> - Script: [`17_composite_domain.py`](../../../examples/scenarios/17_composite_domain.py)

## Related

- [Composite Domains Guide](../../guides/composite-domains.md) -- full reference for shared entities, registry configuration, and advanced multi-domain patterns

## Next Step

Continue to [Tutorial 10: Fabric Lakehouse](../fabric/10-fabric-lakehouse.md) to learn how to write Delta tables directly to a Microsoft Fabric Lakehouse.
