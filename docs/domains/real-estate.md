# Real Estate Domain

Real estate domain with properties, listings, offers, transactions, and inspections.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `neighborhood` | 50 | Neighborhoods with demographics |
| `agent` | 100 | Real estate agents |
| `property` | 1,000 | Physical properties |
| `listing` | 1,500 | MLS property listings |
| `showing` | 7,500 | Property showings |
| `offer` | 2,250 | Purchase offers on listings |
| `transaction` | 600 | Closed real estate transactions |
| `inspection` | 600 | Property inspections for transactions |
| `appraisal` | 600 | Property appraisals for transactions |

## Quick Start

```python
from sqllocks_spindle import Spindle, RealEstateDomain

result = Spindle().generate(domain=RealEstateDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Seasonal listing patterns peaking in spring/summer (May 11%, June 10%)
- Log-normal property values and list prices ($50K-$5M range)
- Zipf-distributed agent activity (top agents get most listings)
- Offer status pipeline (Pending, Accepted, Rejected, Countered, Withdrawn)
- Neighborhood demographics with median income and walk scores
- Property details including bedrooms, bathrooms, sqft, lot size, year built

## Scale Presets

| Preset | `property` |
| --- | --- |
| `fabric_demo` | 100 |
| `small` | 1,000 |
| `medium` | 10,000 |
| `large` | 100,000 |
| `warehouse` | 1,000,000 |
