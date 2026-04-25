"""
Scenario 01 -- Hello World
=========================
The minimum viable Spindle example. Install, import, generate, inspect.

Run:
    pip install sqllocks-spindle
    python examples/scenarios/01_hello_world.py
"""

from sqllocks_spindle import Spindle, RetailDomain

# Generate the Retail domain at fabric_demo scale (~200 customers, ~1,000 orders)
# fabric_demo is the fastest preset -- ideal for notebooks and demos
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

# GenerationResult gives a human-readable summary
print(result)
# -> GenerationResult(9 tables, 3,xxx total rows, 0.xs)

# Access any table as a pandas DataFrame
customers = result["customer"]
orders    = result["order"]

print(f"\nCustomers: {len(customers):,} rows")
print(f"Orders:    {len(orders):,} rows")
print(f"\nCustomer columns: {list(customers.columns)}")

# Preview
print("\nFirst 3 customers:")
print(customers.head(3).to_string(index=False))

# All FK relationships are guaranteed to hold
errors = result.verify_integrity()
assert not errors, f"FK violations: {errors}"
print("\nReferential integrity: PASS")
