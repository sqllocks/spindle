# Spindle by SQLLocks

> "Synthea is to MITRE as Spindle is to SQLLocks"

**Spindle** is a multi-domain, schema-aware synthetic data generator for Microsoft Fabric. It generates statistically realistic, relationally correct datasets — think normalized 3NF schemas with proper FK integrity, Pareto order distributions, seasonal temporal patterns, and real US addresses with lat/lng coordinates ready for Power BI maps.

```
pip install sqllocks-spindle
```

---

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()
result = spindle.generate(
    domain=RetailDomain(),
    scale="small",
    seed=42
)

print(result)
# GenerationResult(9 tables, 21,300 total rows, 0.3s)

# Access any table as a pandas DataFrame
customers = result["customer"]
orders    = result["order"]
addresses = result["address"]

# Check referential integrity
errors = result.verify_integrity()
assert errors == []

# Print a generation summary
print(result.summary())
```

---

## Domains

Spindle ships **12 production-ready domains** — each with calibrated distribution profiles, referential integrity enforcement, and 20+ passing tests:

| Domain | Tables | Description |
|--------|--------|-------------|
| **Retail** | 9 | Customers, products, orders, returns — 3NF normalized |
| **Healthcare** | 9 | Patients, encounters, diagnoses, claims — 3NF normalized |
| **Financial** | 10 | Branches, accounts, transactions, loans, fraud detection |
| **Supply Chain** | 10 | Warehouses, suppliers, POs, inventory, shipments |
| **IoT** | 8 | Devices, sensors, readings, alerts, maintenance |
| **HR** | 9 | Employees, departments, compensation, performance |
| **Insurance** | 9 | Agents, policies, claims, underwriting, payments |
| **Marketing** | 10 | Campaigns, contacts, leads, opportunities, conversions |
| **Education** | 9 | Students, courses, enrollments, grades, financial aid |
| **Real Estate** | 9 | Agents, listings, offers, transactions, inspections |
| **Manufacturing** | 9 | Production lines, work orders, quality control, equipment |
| **Telecom** | 9 | Subscribers, service lines, usage records, billing, churn |

Each domain ships with calibrated distribution profiles based on real-world data (see `METHODOLOGY.md`).

---

## Retail Domain

The built-in `RetailDomain` generates a fully normalized retail schema:

| Table | Small scale | Description |
|---|---|---|
| `customer` | 1,000 | Individual customers with loyalty tiers |
| `address` | 1,500 | Shipping/billing addresses with real US lat/lng |
| `product_category` | 50 | 3-level hierarchy (dept → category → subcategory) |
| `product` | 500 | SKUs with correlated cost/price |
| `store` | 150 | Physical and online stores |
| `promotion` | 200 | Discount campaigns |
| `order` | 5,000 | Order headers with Pareto customer distribution |
| `order_line` | ~12,500 | Line items with discount_percent |
| `return` | ~850 | Returns with dates derived from order dates |

Scale presets: `small`, `medium` (50K customers), `large` (500K), `xlarge` (5M)

---

## Healthcare Domain

The `HealthcareDomain` models clinical encounters, claims, and medications:

| Table | Small scale | Description |
|---|---|---|
| `provider` | 200 | Physicians, NPs, PAs with credentials |
| `facility` | 50 | Hospitals, clinics, urgent care centers |
| `patient` | 1,000 | Patient demographics and insurance |
| `encounter` | 5,000 | Office visits, ED, inpatient, telehealth |
| `diagnosis` | ~9,000 | ICD-10 codes linked to encounters |
| `procedure` | ~6,000 | CPT procedures with charges |
| `medication` | ~4,500 | Prescriptions with dosage and supply |
| `claim` | ~4,750 | Insurance claims with status |
| `claim_line` | ~11,875 | Claim line items with copays and adjustments |

All distributions calibrated from CMS, CDC, AAMC, KFF, and BLS data — see `METHODOLOGY.md`.

### What makes it realistic

- **Pareto orders** — 20% of customers place 80% of orders (`max_per_parent=50` hard cap)
- **Seasonal patterns** — November/December peaks, Friday/Saturday peaks, bimodal hour distribution
- **Real addresses** — 40,977 US ZIP codes from GeoNames (CC-BY-4.0): city, state, ZIP, lat, lng. Works directly in Power BI map visuals.
- **Correlated cost/price** — product cost is always 30–70% of unit price
- **Proper hierarchy** — product categories form a real 3-level tree
- **Business rules enforced** — return dates always after order dates, order dates after signup dates

### Address data for Power BI

```python
addr = result["address"]
print(addr[["city", "state", "zip_code", "lat", "lng"]].head())
#            city state zip_code        lat         lng
# 0        Reform    AL    35481  33.314928  -88.042923
# 1       Chinook    MT    59523  48.487741 -109.261678
```

Drop the lat/lng columns directly into a Power BI map visual — no geocoding required.

---

## Generation Strategies

Spindle supports 20 column-level strategies:

| Strategy | Description |
|---|---|
| `sequence` | Auto-incrementing integer PKs |
| `uuid` | UUID v4 alternative PKs |
| `faker` | Faker library providers (names, emails, etc.) |
| `weighted_enum` | Weighted random selection from a set of values |
| `distribution` | Statistical distributions: uniform, normal, log_normal, pareto, zipf, geometric, bernoulli, bimodal |
| `temporal` | Time-aware dates: uniform or seasonal with day/month/hour profiles |
| `formula` | Computed from other columns: `quantity * unit_price * (1 - discount_percent / 100)` |
| `derived` | Derived from another column with a transformation: `return_date = order_date + N days` |
| `correlated` | Mathematically related to another column: `cost = unit_price * 0.30–0.70` |
| `conditional` | Conditional on another column's value |
| `lifecycle` | Phase-based status values (introduced / active / discontinued) |
| `foreign_key` | FK references with uniform, Pareto, or Zipf distribution |
| `lookup` | Copy value from parent table via FK |
| `reference_data` | Pick from bundled JSON datasets |
| `pattern` | Formatted strings: `Store #{seq:4}` |
| `computed` | Aggregated from child table (e.g., order_total = sum of line_totals) |
| `self_referencing` | FK to same table for hierarchy columns |
| `self_ref_field` | Read level info stashed by self_referencing |
| `record_sample` | Sample complete records from a reference dataset (anchor) |
| `record_field` | Read a field from a previously sampled record (correlated derived columns) |

---

## Distribution Profiles

Every domain ships with a `default` profile calibrated from real-world data. You can override any distribution weight:

```python
# Override specific distributions
domain = RetailDomain(overrides={
    "customer.loyalty_tier": {"Basic": 0.40, "Silver": 0.30, "Gold": 0.20, "Platinum": 0.10},
    "order.status": {"completed": 0.85, "shipped": 0.05, "processing": 0.02, "cancelled": 0.03, "returned": 0.05},
})

# Use a named profile
domain = HealthcareDomain(profile="medicare")

# Check what's available
print(domain.available_profiles)   # ['default']
print(domain.profile_name)         # 'default'
```

Profile files live in `domains/<name>/profiles/` and follow the same JSON schema as `default.json`. See `METHODOLOGY.md` for the full list of distribution keys and their real-world sources.

---

## Custom Schemas

```python
from sqllocks_spindle import Spindle

spindle = Spindle()
result = spindle.generate(
    schema="path/to/my_schema.spindle.json",
    scale_overrides={"customer": 10000, "order": 100000},
    seed=42
)
```

Schemas are defined in `.spindle.json` files. See `PHASE-0-SPEC.md` for the full schema definition format.

---

## CLI

```bash
# Generate retail data at small scale
spindle generate retail --scale small --seed 42 --output ./output/

# Dry run — show what would be generated without generating
spindle generate retail --scale medium --dry-run

# Generate healthcare data as Parquet
spindle generate healthcare --scale small --format parquet --output ./data

# Stream retail orders to a file (fast mode)
spindle stream retail --table order --max-events 5000 --sink file --output events.jsonl

# Stream with real-time rate limiting and a burst window
spindle stream retail --table order --rate 100 --realtime --burst 30:60:10 --sink console

# Export to star schema (dim_* + fact_* tables as CSV)
spindle to-star retail --scale small --output ./star/

# Export to CDM folder (model.json + entity CSV files)
spindle to-cdm retail --scale small --output ./cdm/

# Describe a domain's schema and active profile
spindle describe retail

# List available domains and profiles
spindle list

# Validate a schema file
spindle validate my_schema.spindle.json
```

---

## Development

```bash
# Create virtual environment
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Regenerate bundled address data from GeoNames
python scripts/build_address_data.py
```

---

## Third-Party Data

Address data (city, state, ZIP, lat, lng) is sourced from GeoNames under Creative Commons Attribution 4.0 International (CC-BY-4.0). See `LICENSE-NOTICES.md`.

---

## License

MIT — see `LICENSE`

## Roadmap

- **Phase 0** ✅ Core engine, 21 strategies, Retail + Healthcare domains, calibrated profiles, 103 tests
- **Phase 1** ✅ Fabric Lakehouse writer, CSV/Parquet/Delta/JSONL/Excel output, CLI
- **Phase 2** ✅ Streaming engine — `SpindleStreamer`, Poisson inter-arrivals, token-bucket rate limiting, `AnomalyRegistry` (point/contextual/collective), Event Hub + Kafka sinks, `spindle stream` CLI
- **Phase 3** ✅ Domain expansion — 10 new domains (12 total), 409 tests, shared reference data
- **Phase 4** ✅ spindle-forge MCP server — TypeScript bridge with `spindle_list_domains`, `spindle_describe_domain`, `spindle_generate`
- **Phase 5** ✅ PyPI packaging, GitHub Actions CI/CD, sample notebooks
- **Phase 6** ✅ Star schema output, CDM folder export, `fabric_demo` + `warehouse` scale presets, `spindle to-star` / `spindle to-cdm` CLI
