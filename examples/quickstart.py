"""Spindle quickstart — generate synthetic data in a few lines."""

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain

# --- Retail domain (default profile) ---
result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result)
# GenerationResult(9 tables, ~21K total rows, 0.3s)

# Access tables as pandas DataFrames
customers = result["customer"]
orders = result["order"]
print(f"Customers: {len(customers):,} rows, columns: {list(customers.columns)}")

# Verify referential integrity
errors = result.verify_integrity()
assert errors == [], f"FK errors: {errors}"
print("Referential integrity: PASS")

# --- Healthcare domain ---
hc = Spindle().generate(domain=HealthcareDomain(), scale="small", seed=42)
print(hc)
print(hc.summary())

# --- Override distributions at runtime ---
custom = RetailDomain(overrides={
    "customer.loyalty_tier": {"Basic": 0.40, "Silver": 0.30, "Gold": 0.20, "Platinum": 0.10},
})
result2 = Spindle().generate(domain=custom, scale="small", seed=42)
tiers = result2["customer"]["loyalty_tier"].value_counts(normalize=True)
print("\nCustom loyalty tier distribution:")
print(tiers)

# --- Export to CSV ---
from sqllocks_spindle.output import PandasWriter

writer = PandasWriter()
files = writer.to_csv(result.tables, "./output_csv")
print(f"\nWritten {len(files)} CSV files to ./output_csv/")
