# Insurance Domain

Insurance domain with policies, claims, underwriting, and premium management.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `agent` | 100 | Insurance agents with licensing info |
| `policyholder` | 1,000 | Insurance customers with demographics |
| `policy_type` | 30 | Policy categories and base premiums |
| `policy` | 1,800 | Insurance policies with coverage details |
| `coverage` | 4,500 | Coverage line items per policy |
| `claim` | 540 | Claims filed against policies |
| `claim_payment` | 810 | Payouts on claims |
| `premium_payment` | 10,800 | Premium payments by policyholders |
| `underwriting` | 1,800 | Risk assessment records per policy |

## Quick Start

```python
from sqllocks_spindle import Spindle, InsuranceDomain

result = Spindle().generate(domain=InsuranceDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Multi-line policy categories (Auto 25%, Home 20%, Life 18%, Health 15%, Commercial 15%)
- Normal-distributed credit scores (mean 700, sigma 80) for policyholders
- Deductible tiers ($500, $1000, $2000, $5000) with weighted selection
- Claim lifecycle with status tracking (Open, Under Review, Approved, Denied, Closed)
- Underwriting risk tiers with scored assessments
- Premium payment methods including Auto-Pay, Online, Mail, Agent

## Scale Presets

| Preset | `policyholder` |
| --- | --- |
| `fabric_demo` | 100 |
| `small` | 1,000 |
| `medium` | 10,000 |
| `large` | 100,000 |
| `warehouse` | 1,000,000 |
