# Composite Domains

Generate data across multiple domains with shared entities and cross-domain FK relationships. For example, generate Retail + HR + Financial data where employees are also customers and stores have locations linked to branches.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain, HrDomain, FinancialDomain
from sqllocks_spindle.domains.composite import CompositeDomain

composite = CompositeDomain(
    domains=[RetailDomain(), HrDomain(), FinancialDomain()],
    shared_entities={
        "person": {
            "primary": "hr.employee",
            "links": {
                "retail": "customer.employee_id",
                "financial": "account.holder_id",
            },
        },
    },
)

result = Spindle().generate(domain=composite, scale="small", seed=42)

# Tables are prefixed with domain name to avoid collisions
print(result.table_names)
# ['hr_employee', 'hr_department', ..., 'retail_customer', 'retail_order', ..., 'financial_account', ...]
```

## How It Works

1. **Schema merging** — all domain schemas are combined into a single `SpindleSchema`
2. **Table prefixing** — each table gets a `<domain>_` prefix to avoid name collisions
3. **Shared entity resolution** — the `SharedEntityRegistry` maps cross-domain concepts (PERSON, LOCATION, ORGANIZATION, CALENDAR) to specific tables
4. **Cross-domain FK rewiring** — FK relationships are created between shared entities across domains
5. **Dependency-ordered generation** — tables are generated in topological order respecting cross-domain FKs

## SharedEntityRegistry

The built-in registry has default mappings for all 12 domains:

```python
from sqllocks_spindle.domains.shared_registry import SharedEntityRegistry, SharedConcept

registry = SharedEntityRegistry()

# What domains have a PERSON concept?
domains = registry.get_domains_for_concept(SharedConcept.PERSON)
# ['retail', 'healthcare', 'financial', 'hr', 'insurance', ...]

# What table maps to PERSON in retail?
mapping = registry.get_mapping_for_domain(SharedConcept.PERSON, "retail")
# DomainEntityMapping(domain='retail', table='customer', pk_column='customer_id', ...)
```

### Shared Concepts

| Concept | Description | Example Tables |
| --- | --- | --- |
| `PERSON` | People/individuals | customer, patient, employee, policyholder |
| `LOCATION` | Physical locations | address, facility, store, warehouse |
| `ORGANIZATION` | Companies/departments | department, branch, supplier |
| `CALENDAR` | Time dimensions | (used for shared date ranges) |

## Custom Shared Entities

Override the default registry with custom mappings:

```python
composite = CompositeDomain(
    domains=[RetailDomain(), HrDomain()],
    shared_entities={
        "person": {
            "primary": "hr.employee",          # HR employees are the canonical people
            "links": {
                "retail": "customer.employee_id",  # retail customers link to employees
            },
        },
    },
)
```

The `primary` entity is generated first. Linked entities get FK references to the primary's PK.
