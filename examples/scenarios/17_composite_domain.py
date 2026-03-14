"""
Scenario 17 -- Composite Domain
=================================
Combine multiple domains into one generation run with cross-domain foreign
keys enforced. Use CompositeDomain when your solution spans more than one
vertical (e.g. an HR + Retail data platform).

SharedEntityRegistry defines which tables represent the same real-world
concept (PERSON, LOCATION, ORGANIZATION, CALENDAR) across domains, so
Spindle can wire the FK relationships automatically.

Run:
    python examples/scenarios/17_composite_domain.py
"""

from sqllocks_spindle.domains.composite import CompositeDomain
from sqllocks_spindle.domains.financial.financial import FinancialDomain
from sqllocks_spindle.domains.healthcare.healthcare import HealthcareDomain
from sqllocks_spindle.domains.hr.hr import HrDomain
from sqllocks_spindle.domains.retail.retail import RetailDomain
from sqllocks_spindle.domains.shared_registry import SharedConcept, SharedEntityRegistry
from sqllocks_spindle.engine.generator import Spindle

# ── 1. Two-domain composite: Retail + HR (shared PERSON) ─────────────────────
print("── 1. Retail + HR — shared PERSON ──")

composite_rh = CompositeDomain(
    domains=[RetailDomain(), HrDomain()],
    shared_entities={
        "person": {
            "primary": "hr.employee",     # HR owns the canonical person record
            "links": {
                "retail": "customer.employee_id",  # retail customer FK -> hr employee
            },
        },
    },
)

result_rh = Spindle().generate(domain=composite_rh, scale="fabric_demo", seed=42)
print(f"  Tables generated: {sorted(result_rh.tables.keys())}")
print(f"  Total rows:       {sum(len(df) for df in result_rh.tables.values()):,}")

# ── 2. Three-domain composite: Retail + HR + Financial ───────────────────────
print("\n── 2. Retail + HR + Financial — shared PERSON + LOCATION ──")

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
print(f"  Tables generated: {len(result_3.tables)}")
print(f"  Total rows:       {sum(len(df) for df in result_3.tables.values()):,}")

# ── 3. Auto-registry (no explicit shared_entities) ────────────────────────────
print("\n── 3. Auto-registry (SharedEntityRegistry default mappings) ──")

registry = SharedEntityRegistry()
composite_auto = CompositeDomain(
    domains=[RetailDomain(), HealthcareDomain()],
    registry=registry,  # uses built-in default concept mappings
)

result_auto = Spindle().generate(domain=composite_auto, scale="fabric_demo", seed=42)
print(f"  Tables: {sorted(result_auto.tables.keys())}")

# ── 4. Inspect shared concepts ────────────────────────────────────────────────
print("\n── 4. SharedConcept enum values ──")
for concept in SharedConcept:
    print(f"  {concept.value}")

# ── 5. Cross-domain FK verification ──────────────────────────────────────────
print("\n── 5. Cross-domain FK verification ──")

tables = result_rh.tables

# HR employees exist; retail customers should reference them (if linked)
if "employee" in tables and "customer" in tables:
    emp_ids = set(tables["employee"]["employee_id"].dropna().unique())
    cust_emp_fk = tables["customer"].get("employee_id")
    if cust_emp_fk is not None:
        linked = cust_emp_fk.dropna()
        orphans = linked[~linked.isin(emp_ids)]
        print(f"  HR employees:            {len(emp_ids):,}")
        print(f"  Retail->HR FK references: {len(linked):,}")
        print(f"  Orphan references:        {len(orphans):,}  (should be 0)")
    else:
        print("  (employee_id FK column not present in customer — depends on domain config)")
else:
    print(f"  Available tables: {sorted(tables.keys())}")

# ── 6. child_domains property ────────────────────────────────────────────────
print("\n── 6. CompositeDomain.child_domains ──")
for d in composite_3.child_domains:
    print(f"  {d.__class__.__name__}")

print("\nDone.")
