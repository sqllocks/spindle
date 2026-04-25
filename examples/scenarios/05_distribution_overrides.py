"""
Scenario 05 -- Distribution Overrides
======================================
Spindle ships calibrated default distributions for every domain, sourced
from industry data (NRF, CMS, CDC, BLS, etc.). You can override any
distribution at runtime without editing any config files.

Use cases:
  - Simulate a specific business scenario (premium customers, high churn)
  - Reproduce a production incident (high fraud rate, high return rate)
  - Test edge cases (all-cancelled orders, telehealth-only encounters)
  - A/B test different demographic mixes

Run:
    python examples/scenarios/05_distribution_overrides.py
"""

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain, FinancialDomain

spindle = Spindle()

# ------------------------------------------------------------------
# 1. Default distributions -- see what Spindle uses out of the box
# ------------------------------------------------------------------
default = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

print("=== Default loyalty tier distribution ===")
print(
    default["customer"]["loyalty_tier"]
    .value_counts(normalize=True).mul(100).round(1).rename("pct %")
)

# ------------------------------------------------------------------
# 2. Override loyalty tier -- simulate a premium-skewed customer base
# ------------------------------------------------------------------
premium = spindle.generate(
    domain=RetailDomain(overrides={
        "customer.loyalty_tier": {
            "Basic":    0.10,
            "Silver":   0.20,
            "Gold":     0.35,
            "Platinum": 0.35,
        }
    }),
    scale="fabric_demo",
    seed=42,
)

print("\n=== Premium-skewed loyalty tiers ===")
print(
    premium["customer"]["loyalty_tier"]
    .value_counts(normalize=True).mul(100).round(1).rename("pct %")
)

# ------------------------------------------------------------------
# 3. Override order status -- simulate a high-return-rate scenario
# ------------------------------------------------------------------
high_returns = spindle.generate(
    domain=RetailDomain(overrides={
        "order.status": {
            "completed":  0.50,
            "returned":   0.25,
            "shipped":    0.10,
            "cancelled":  0.10,
            "processing": 0.05,
        }
    }),
    scale="fabric_demo",
    seed=42,
)

print("\n=== High-return order status distribution ===")
print(
    high_returns["order"]["status"]
    .value_counts(normalize=True).mul(100).round(1).rename("pct %")
)

# ------------------------------------------------------------------
# 4. Healthcare -- telehealth-heavy practice
# ------------------------------------------------------------------
telehealth = spindle.generate(
    domain=HealthcareDomain(overrides={
        "encounter.encounter_type": {
            "office_visit": 0.25,
            "telehealth":   0.50,
            "inpatient":    0.05,
            "emergency":    0.10,
            "outpatient":   0.10,
        }
    }),
    scale="fabric_demo",
    seed=42,
)

print("\n=== Telehealth-heavy encounter types ===")
print(
    telehealth["encounter"]["encounter_type"]
    .value_counts(normalize=True).mul(100).round(1).rename("pct %")
)

# ------------------------------------------------------------------
# 5. Financial -- skew transaction channel distribution
# ------------------------------------------------------------------
channel_skew = spindle.generate(
    domain=FinancialDomain(overrides={
        "transaction.channel": {"mobile": 0.70, "web": 0.20, "atm": 0.05, "branch": 0.05}
    }),
    scale="fabric_demo",
    seed=42,
)

print("\n=== Mobile-heavy transaction channels ===")
print(
    channel_skew["transaction"]["channel"]
    .value_counts(normalize=True).mul(100).round(1).rename("pct %")
)

# ------------------------------------------------------------------
# 6. Combine multiple overrides in one call
# ------------------------------------------------------------------
combined = spindle.generate(
    domain=RetailDomain(overrides={
        "customer.loyalty_tier":  {"Basic": 0.10, "Silver": 0.20, "Gold": 0.40, "Platinum": 0.30},
        "order.status":           {"completed": 0.70, "returned": 0.15, "cancelled": 0.10, "shipped": 0.05},
        "customer.account_status": {"active": 0.95, "suspended": 0.03, "closed": 0.02},
    }),
    scale="fabric_demo",
    seed=42,
)

print("\n=== Combined overrides ===")
print("loyalty_tier:")
print(combined["customer"]["loyalty_tier"].value_counts(normalize=True).mul(100).round(1))
print("\norder status:")
print(combined["order"]["status"].value_counts(normalize=True).mul(100).round(1))

# ------------------------------------------------------------------
# 7. Reproducibility -- same seed, same output every time
# ------------------------------------------------------------------
r1 = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=99)
r2 = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=99)
assert list(r1["customer"]["customer_id"]) == list(r2["customer"]["customer_id"])
print("\nSame seed -> identical output: PASS")
