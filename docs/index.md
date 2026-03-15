# Spindle by SQLLocks

**Multi-domain, schema-aware synthetic data generator for Microsoft Fabric.**

> "Synthea is to MITRE as Spindle is to SQLLocks"

---

## What is Spindle?

Spindle generates statistically realistic, relationally correct datasets for Microsoft Fabric. Not random noise — structured data with proper FK integrity, Pareto order distributions, seasonal temporal patterns, and real US addresses with lat/lng coordinates ready for Power BI maps.

```bash
pip install sqllocks-spindle
```

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result.summary())
# GenerationResult(9 tables, 21,300 total rows, 0.3s)
```

---

## Key Features

<div class="grid cards" markdown>

-   :material-database-outline:{ .lg .middle } **13 Industry Domains**

    ---

    Retail, Healthcare, Financial, Supply Chain, IoT, HR, Insurance, Marketing, Education, Real Estate, Manufacturing, Telecom, Capital Markets — each with calibrated distribution profiles from real-world data.

    [:octicons-arrow-right-24: Domain Catalog](domains/index.md)

-   :material-cog-outline:{ .lg .middle } **21 Generation Strategies**

    ---

    Sequence, Faker, weighted enum, statistical distributions (Pareto, Zipf, log-normal), temporal seasonality, formulas, correlated columns, FK references, and more.

    [:octicons-arrow-right-24: Strategy Reference](guides/strategies.md)

-   :material-lightning-bolt:{ .lg .middle } **Chaos Engine**

    ---

    6 corruption categories (schema, value, file, referential, temporal, volume) with 4 intensity presets. Test your pipeline against realistic data quality issues.

    [:octicons-arrow-right-24: Chaos Guide](guides/chaos.md)

-   :material-microsoft:{ .lg .middle } **Fabric-Native**

    ---

    Write to Lakehouse, Warehouse, SQL Database, Eventhouse, and Semantic Models. Auto-detects Fabric runtime. Star schema and CDM folder export built in.

    [:octicons-arrow-right-24: Fabric Guides](guides/fabric-lakehouse.md)

-   :material-check-decagram:{ .lg .middle } **Validation Gates**

    ---

    8 built-in gates (referential integrity, schema conformance, null constraints, and more) with automatic quarantine for failed artifacts.

    [:octicons-arrow-right-24: Validation Guide](guides/validation.md)

-   :material-play-speed:{ .lg .middle } **Streaming + Simulation**

    ---

    Poisson inter-arrivals, token-bucket rate limiting, anomaly injection, file-drop simulation with late arrivals and schema drift, and hybrid batch+stream modes.

    [:octicons-arrow-right-24: Streaming Guide](guides/streaming.md)

</div>

---

## Where Do I Start?

| I am a... | Start here |
|-----------|-----------|
| :material-school: **Developer new to synthetic data** | [Before You Start](getting-started/before-you-start.md) then [Quickstart](getting-started/quickstart.md) |
| :material-pipe: **Data engineer building Fabric pipelines** | [Quickstart](getting-started/quickstart.md) then [Fabric Tutorials](tutorials/fabric/) |
| :material-database: **DBA who wants SQL test data** | [CLI Quickstart](getting-started/quickstart-cli.md) — no Python required |
| :material-chart-bell-curve: **Data scientist evaluating distributions** | [Methodology](methodology/calibration.md) then [Domain Catalog](domains/) |
| :material-sitemap: **Architect evaluating Spindle** | [Why Spindle?](#why-spindle) then [Domain Catalog](domains/) |
| :material-cog: **DevOps automating data generation** | [CLI Quickstart](getting-started/quickstart-cli.md) then [CI Integration](tutorials/advanced/17-ci-integration.md) |
| :material-presentation: **Presenter building a demo** | [60-Second Overview](getting-started/60-seconds.md) |
| :material-microsoft: **Already in a Fabric notebook** | [Fabric Quickstart](getting-started/quickstart-fabric.md) |

## Quick Links

| | |
|---|---|
| :material-rocket-launch: [Quickstart (Python)](getting-started/quickstart.md) | Generate your first dataset in 5 minutes |
| :material-console: [Quickstart (CLI)](getting-started/quickstart-cli.md) | Generate data from the command line |
| :material-microsoft: [Quickstart (Fabric)](getting-started/quickstart-fabric.md) | Generate data in a Fabric notebook |
| :material-school: [Tutorials](tutorials/) | 17 step-by-step learning paths |
| :material-download: [Installation](getting-started/installation.md) | `pip install sqllocks-spindle` and optional extras |
| :material-console: [CLI Cheatsheet](getting-started/cli-cheatsheet.md) | All CLI commands at a glance |
| :fontawesome-brands-github: [GitHub](https://github.com/sqllocks/spindle) | Source code, issues, contributing |
| :fontawesome-brands-python: [PyPI](https://pypi.org/project/sqllocks-spindle/) | `pip install sqllocks-spindle` |

---

## Why Spindle?

Every Fabric project starts with the same problem: **where's the test data?**

- Dashboards look flat because every metric has uniform variance
- Pipelines pass testing but fail on real cardinality
- ML models train on data that has no signal to find
- Stakeholders can't relate to `Customer_001` buying `Product_ABC` for `$10.00`

Spindle solves this with **rule-based, transparent** generation. Unlike ML generators that output black-box models, Spindle gives you a human-readable `.spindle.json` schema you can inspect, tweak, and version control. All 13 domains have distributions sourced from published data — BLS, NAIC, NCES, NAR, FDIC, Federal Reserve, SEC, and 40+ more. See the [Methodology](methodology/calibration.md) for per-parameter citations.

---

**MIT License** | Built by [Jonathan Stewart / SQLLocks](https://www.linkedin.com/in/sqllocks/)
