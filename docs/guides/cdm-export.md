# CDM Export

Export Spindle-generated data to Microsoft Common Data Model (CDM) format, compatible with Dataverse, Power Apps, and Azure Synapse Analytics.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.transform.cdm_mapper import CdmMapper

domain = RetailDomain()
result = Spindle().generate(domain=domain, scale="small", seed=42)

mapper = CdmMapper(
    cdm_map=domain.cdm_map(),
    schema=domain.get_schema(),
)
mapper.export(result.tables, output_dir="./cdm_output/retail")
```

This produces:

```text
cdm_output/retail/
├── model.json
├── Contact/
│   └── Contact.csv
├── Product/
│   └── Product.csv
├── SalesOrder/
│   └── SalesOrder.csv
└── ...
```

## How It Works

Each Spindle domain provides a `cdm_map()` method that maps source tables to CDM standard entity names:

```python
domain = RetailDomain()
cdm_map = domain.cdm_map()

# Inspect the mapping
for source_table, cdm_entity in cdm_map.mappings.items():
    print(f"{source_table} → {cdm_entity}")
```

Output:

```text
customer → Contact
address → CustomerAddress
product_category → ProductCategory
product → Product
store → Store
promotion → Campaign
order → SalesOrder
order_line → SalesOrderProduct
return → ReturnOrder
```

## Available CDM Mappings

All 13 Spindle domains include CDM mappings:

| Domain | CDM Entities |
|--------|-------------|
| Retail | Contact, CustomerAddress, Product, Store, Campaign, SalesOrder |
| Healthcare | Patient, Practitioner, Account, Appointment, Condition, Invoice |
| Financial | Contact, Account, FinancialAccount, Transaction, Loan, Payment |
| HR | BusinessUnit, Position, Worker, Compensation, LeaveRequest |
| Education | BusinessUnit, Course, Contact, CourseEnrollment, Award |
| Insurance | Worker, Contact, Contract, Case, Payment, Assessment |
| IoT | Category, Location, Asset, Component, Observation, Alert |
| Manufacturing | BusinessUnit, Product, WorkOrder, QualityOrder, Asset |
| Marketing | Campaign, Contact, Lead, Opportunity, Activity, Goal |
| Real Estate | Location, Worker, Listing, Transaction, Appointment |
| Supply Chain | Warehouse, Vendor, Product, PurchaseOrder, Shipment |
| Telecom | Product, Contact, Subscription, Invoice, Payment |
| Capital Markets | Account, Organization, Category, Observation, Transaction |

## Output Formats

The CDM exporter supports multiple file formats within the CDM folder:

```python
mapper.export(
    result.tables,
    output_dir="./cdm_output/retail",
    format="parquet",  # csv (default) | parquet | jsonl
)
```

## model.json

The generated `model.json` manifest follows the CDM folder structure specification:

```json
{
  "name": "retail",
  "version": "1.0",
  "entities": [
    {
      "name": "Contact",
      "source": "customer",
      "attributes": [...]
    }
  ]
}
```

## CLI

```bash
spindle generate retail --scale small --format cdm --output ./cdm_output/retail
```

## Use Cases

- **Dataverse import** — Load CDM entities into Power Platform environments.
- **Synapse Analytics** — CDM folders are natively supported as linked datasets.
- **Cross-system testing** — Validate data pipelines that consume CDM format.
- **Power Apps prototyping** — Populate test data matching Dataverse entity schemas.

---

## See Also

- **Tutorial:** [05: Star Schema](../tutorials/intermediate/05-star-schema.md) — step-by-step walkthrough
- **Example script:** [`07_cdm_export.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/07_cdm_export.py)
- **Notebook:** [`T06_star_schema_export.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/quickstart/T06_star_schema_export.ipynb)
