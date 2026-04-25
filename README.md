<p align="center">
  <img src="Logo/spindle_logo_combined.png" alt="Spindle by SQLLocks" width="300">
</p>

<p align="center"><em>"Synthea is to MITRE as Spindle is to SQLLocks"</em></p>

**Spindle** is a multi-domain, schema-aware synthetic data generator for Microsoft Fabric. It generates statistically realistic, relationally correct datasets — think normalized 3NF schemas with proper FK integrity, Pareto order distributions, seasonal temporal patterns, and real US addresses with lat/lng coordinates ready for Power BI maps.

```
pip install sqllocks-spindle
```

---

## Why Spindle?

Every Fabric project starts with the same problem: **where's the test data?**

Random data generators give you `Customer_001` buying `Product_ABC` for `$10.00`. Dashboards look flat. Pipelines pass testing but fail on real cardinality. ML models train on data with no signal to find.

Spindle generates data that **looks and behaves like production data** — without any real data involved:

- **13 pre-built domains** with distributions sourced from published data (BLS, NAIC, NCES, NAR, FDIC, Federal Reserve, SEC, and 40+ more)
- **Schema-aware generation** — tables generated in dependency order, FK integrity guaranteed, composite keys handled
- **Chaos engine** — intentionally corrupt data (nulls, duplicates, schema drift) to stress-test your pipeline
- **Fabric-native** — write directly to Lakehouse, Warehouse, SQL Database, Eventhouse, and Semantic Models
- **Transparent** — every generation rule is a human-readable `.spindle.json` schema you can inspect and version control

Unlike ML-based generators (SDV, MOSTLY AI, Gretel), Spindle doesn't need training data. Unlike Faker, it produces relationally correct, statistically calibrated datasets at any scale.

---

## Documentation

| I want to... | Go to |
|-------------|-------|
| Get started in 5 minutes | [Quickstart (Python)](docs/getting-started/quickstart.md) |
| Use the CLI without writing Python | [Quickstart (CLI)](docs/getting-started/quickstart-cli.md) |
| Generate data in a Fabric notebook | [Quickstart (Fabric)](docs/getting-started/quickstart-fabric.md) |
| Follow step-by-step tutorials | [Tutorials](docs/tutorials/) (17 learning paths) |
| Understand a specific feature | [Guides](docs/guides/) (18 feature guides) |
| Run working example code | [Examples](examples/) (22 scripts + 35 notebooks) |
| Browse the API | [API Reference](https://sqllocks.github.io/spindle/reference/) |
| See all 13 domains | [Domain Catalog](docs/domains/) |
| Check calibration sources | [Methodology](docs/methodology/calibration.md) |

Full docs site: [sqllocks.github.io/spindle](https://sqllocks.github.io/spindle)

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

Spindle ships **13 production-ready domains** — each with calibrated distribution profiles, referential integrity enforcement, and 1,250+ passing tests:

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
| **Capital Markets** | 10 | S&P 500 companies, daily prices (GBM), dividends, earnings, trades |

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

Spindle supports 21 column-level strategies:

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
| `scd2` | SCD Type 2 versioning: effective_date, end_date, is_current, version |

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

Spindle provides 21 CLI commands covering generation, export, inference, incremental workflows, and Fabric publishing.

### Core Generation

```bash
spindle generate retail --scale small --seed 42 --output ./output/
spindle generate healthcare --scale small --format parquet --output ./data
spindle generate retail --scale medium --dry-run
```

### Export & Transform

```bash
spindle to-star retail --scale small --output ./star/
spindle to-cdm retail --scale small --output ./cdm/
spindle export-model retail --output retail.bim --source-type lakehouse
```

### Schema Import & Inference

```bash
spindle from-ddl my_tables.sql --output my_schema.spindle.json
spindle learn ./real_data/ --format csv --output inferred.spindle.json
spindle compare ./real/ ./synthetic/ --format csv --output report.md
spindle mask ./real_data/ --output ./masked/ --seed 42
```

### Incremental & Day 2

```bash
spindle continue retail --input ./day1/ --output ./deltas/ --inserts 100
spindle time-travel retail --months 12 --output ./snapshots/ --growth-rate 0.08
```

### Multi-Domain

```bash
spindle composite enterprise --scale small --output ./data/ --format parquet
spindle composite retail+hr+financial --scale medium --output ./enterprise/
spindle presets
```

### Streaming & Simulation

```bash
spindle stream retail --table order --max-events 5000 --sink file --output events.jsonl
spindle stream retail --table order --rate 100 --realtime --burst 30:60:10
```

### Fabric Publishing

```bash
spindle publish retail --target lakehouse --base-path "abfss://..." --scale small
spindle publish retail --target sql-database --connection-string "env://SPINDLE_SQL_CONNECTION"
spindle publish retail --target eventhouse --connection-string "https://..." --database mydb
```

**Performance** (v2.2.0): `FabricSqlDatabaseWriter` uses `fast_executemany` with vectorized coercion (~24s for 100K rows). `WarehouseBulkWriter` uses parallel multi-file COPY INTO staging per MS Learn guidelines — stage all Parquet chunks first, then one wildcard COPY INTO per table with concurrent table loading via ThreadPoolExecutor. Scale tiers up to `xxxl` (~1T rows).

### Discovery & Profiles

```bash
spindle list
spindle describe retail
spindle validate my_schema.spindle.json
spindle profile list retail
spindle profile export retail --output retail_profile.json
```

---

## Development

```bash
# Create virtual environment
python -m venv .venv
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

- **Phase 0** ✅ Core engine, 21 strategies, Retail + Healthcare domains, calibrated profiles
- **Phase 1** ✅ Fabric Lakehouse writer, CSV/Parquet/Delta/JSONL/Excel output, CLI
- **Phase 2** ✅ Streaming engine, AnomalyRegistry, Event Hub + Kafka sinks, `spindle stream` CLI
- **Phase 3** ✅ Domain expansion — 13 domains, shared reference data
- **Phase 4** ✅ MCP server bridge, PyPI packaging, GitHub Actions CI/CD
- **Phase 5** ✅ Star schema output, CDM export, `spindle to-star` / `spindle to-cdm` CLI
- **Tier 1** ✅ MkDocs site, 17 doc guides, 4 tutorial notebooks, GenerationResult convenience methods
- **Tier 2** ✅ SQL/DDL pipeline, FabricSqlDatabaseWriter, Capital Markets domain, star/CDM maps for all 13 domains, 12 notebooks
- **Tier 3** ✅ Inference engine (`spindle learn/compare/mask`), incremental engine (`spindle continue`), SCD2, time-travel snapshots, composite presets, 11 notebooks
- **Blueprint** ✅ Credential resolver, `spindle publish` CLI, Eventhouse writer, observability, 6 simulation pattern modules (clickstream, IoT telemetry, financial streams, operational logs, workflow state machines, SCD2 file drops), acceptance tests, provisioning guide
- **Launch** ✅ v2.2.0 — boolean fix, COPY INTO perf, xxl/xxxl scale tiers, 22/23 integration sweep PASS
