# Domain Catalog

Spindle ships 13 industry domains, each with realistic table schemas, weighted distributions, and configurable scale presets.

## Available Domains

| Domain | Module | Tables | Description |
| --- | --- | --- | --- |
| [Retail](retail.md) | `RetailDomain` | 9 | Customers, products, orders, and returns |
| [Healthcare](healthcare.md) | `HealthcareDomain` | 9 | Patients, encounters, diagnoses, procedures, and claims |
| [Financial](financial.md) | `FinancialDomain` | 10 | Accounts, transactions, loans, and fraud detection |
| [Supply Chain](supply-chain.md) | `SupplyChainDomain` | 10 | Warehouses, purchasing, inventory, and logistics |
| [IoT](iot.md) | `IoTDomain` | 8 | Devices, sensors, readings, alerts, and maintenance |
| [HR](hr.md) | `HrDomain` | 9 | Employees, departments, compensation, and performance |
| [Insurance](insurance.md) | `InsuranceDomain` | 9 | Policies, claims, underwriting, and premium management |
| [Marketing](marketing.md) | `MarketingDomain` | 10 | Campaigns, contacts, leads, opportunities, and conversions |
| [Education](education.md) | `EducationDomain` | 9 | Students, courses, enrollments, grades, and financial aid |
| [Real Estate](real-estate.md) | `RealEstateDomain` | 9 | Properties, listings, offers, transactions, and inspections |
| [Manufacturing](manufacturing.md) | `ManufacturingDomain` | 9 | Production lines, work orders, quality control, and equipment |
| [Telecom](telecom.md) | `TelecomDomain` | 9 | Subscribers, service lines, usage records, billing, and churn |
| [Capital Markets](capital-markets.md) | `CapitalMarketsDomain` | 10 | S&P 500 equities, daily OHLCV, dividends, earnings, insider transactions |

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result.summary())
```

Every domain supports the same scale presets (`fabric_demo`, `small`, `medium`, `large`, etc.) and can output to DataFrames, Parquet, or Delta Lake.
