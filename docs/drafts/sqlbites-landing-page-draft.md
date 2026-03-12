# SQLBites Landing Page Draft: sqlbites.net/spindle
# ================================================
# This is a content draft for the WordPress page.
# Not HTML — just the copy, structure, and notes on layout.


## Hero Section

### Headline
**Spindle — Synthetic Data That Respects Your Schema**

### Subheadline
A free, open-source data generator for Microsoft Fabric that produces realistic, relationally-intact datasets. Not random noise — structured data that looks like it came from production.

### CTA Buttons
- **Get Started** → GitHub README / quick start section
- **View on GitHub** → github.com/sqllocks/spindle

### Hero Visual
[NOTE: Consider a before/after. Left side: a jumbled table of "John Doe, test@test.com, $0.00" garbage. Right side: the same schema with realistic Spindle output — diverse names, log-normal prices, proper FK relationships. The contrast is the whole pitch.]


---


## Problem Statement Section

### Heading
**Your demo data is lying to you.**

### Body
Every Fabric tutorial, every proof-of-concept, every demo environment starts the same way: someone generates fake data with random strings and uniform distributions. The result?

- Dashboards that look flat because every metric has the same variance
- Pipelines that pass testing but fail on real cardinality
- ML models trained on data that has no signal to find
- Stakeholders who can't relate to "Customer_001" buying "Product_ABC" for "$10.00"

You wouldn't test a bridge with imaginary physics. Why test your data platform with imaginary data?


---


## What Spindle Does — Feature Blocks

[NOTE: 3 or 4 feature blocks, each with an icon, short heading, and 2-3 sentences. Keep it scannable.]

### Schema-Aware Generation
Spindle reads your schema — tables, foreign keys, relationships — and generates data in dependency order. Every child row references a real parent. Every FK resolves. No orphans, no dangling references, no manual fixup.

### Realistic Distributions
Prices follow log-normal distributions. Customer order frequency follows Pareto (80/20). Seasonal patterns spike around holidays. Because your reporting, your aggregations, and your ML models all behave differently on realistic data than on uniform random noise.

### Pre-Built Industry Domains
Start with Retail (available now) — 8 tables, complete 3NF schema, curated reference data. Financial, Healthcare, Insurance, and 9 more domains coming. Each one profiled with real-world statistical distributions.

### Built for Fabric
Python 3.10+, zero exotic dependencies. Works in any Fabric Notebook out of the box. Outputs DataFrames, CSV, or Parquet today — native Delta Lake / Lakehouse writer coming in the next release.


---


## Quick Demo Section

### Heading
**From zero to 20,000 rows in 3 commands**

```bash
pip install sqllocks-spindle

spindle generate retail --scale small --seed 42

# Output:
#   8 tables, 19,815 rows, 0.2 seconds
#   Referential integrity: PASS
```

Or in Python:

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="medium", seed=42)
# 500K orders, 1.2M line items, 100% FK integrity
```

[NOTE: If we can get a screenshot of the CLI output or a Fabric notebook with the generated DataFrames, embed it here. Real output > code blocks for a landing page.]


---


## Domain Catalog Section

### Heading
**Industry Domains**

[NOTE: Grid layout — 3 or 4 columns. Each domain gets a card with name, icon, table count, and status badge (Available / Coming Soon). Only Retail is "Available" right now — that's fine, the roadmap shows ambition without overpromising.]

| Domain | Tables | Status |
|--------|--------|--------|
| Retail | 8 | Available |
| Financial | — | Coming Soon |
| Insurance | — | Coming Soon |
| Healthcare | — | Coming Soon |
| Supply Chain | — | Coming Soon |
| Telecom | — | Coming Soon |
| Education | — | Coming Soon |
| Energy | — | Coming Soon |
| HR | — | Coming Soon |
| Real Estate | — | Coming Soon |
| Marketing | — | Coming Soon |
| IoT | — | Coming Soon |

[NOTE: Don't show this as a sad list of 11 "coming soon" items. Either show 4–6 max with a "and more planned" note, or only show domains that are actively in development. Listing 11 empty domains screams vaporware. Be honest about what exists.]

**RECOMMENDED: Show Retail prominently as a deep-dive card, then a single line: "Financial, Healthcare, Insurance, and more domains in development." No grid of empties.**


---


## How It Works — Technical Summary

### Heading
**Under the hood**

[NOTE: This section is for the technical audience who clicked past the marketing. Keep it brief but credible.]

1. **Define** — Use a pre-built domain or write your own `.spindle.json` schema
2. **Generate** — Spindle resolves dependencies, generates tables in topological order, respects cardinality distributions
3. **Validate** — Built-in integrity checks verify every FK, enforce business rules (order dates after signup dates, returns after orders)
4. **Output** — DataFrames, CSV, Parquet. Delta Lake coming next.

The engine uses numpy's random generators for reproducibility and performance. 1.8M rows across 8 related tables in ~6 seconds on commodity hardware.


---


## Integration with SQLBites Content

### Heading
**Learn More on SQLBites**

[NOTE: This is where Spindle connects back to the broader SQLBites content ecosystem. Link to related blog posts, tutorials, and guides.]

- **Blog**: Deep-dives on why distributions matter, how the schema engine works, domain design decisions
- **Tutorials**: Step-by-step guides for using Spindle in Fabric Notebooks
- **Conference Talks**: "Stop Demoing With Garbage Data" — slides and resources

[NOTE: Only link to content that actually exists. Empty links are worse than no links.]


---


## CTA / Footer Section

### Heading
**Get Spindle**

- **GitHub**: Source code, issues, contributing guide
- **PyPI**: `pip install sqllocks-spindle`
- **SQLBites**: Tutorials, blog posts, domain guides

MIT License — free for any use, forever.

Built by Jonathan Stewart / SQLLocks.


---


# PAGE STRUCTURE NOTES

## Where this lives in SQLBites navigation
- Top nav: Home | Blog | Resources | **Spindle** | About
- Or under Resources dropdown: Tools → Spindle
- RECOMMENDED: Top-level nav item. Spindle is a product, not a blog post. It deserves its own nav slot. But only once it's actually released — until then, keep it as a child page under Resources or don't publish it at all.

## WordPress implementation
- Single page, not a post
- Custom slug: /spindle
- No sidebar — full-width layout for the landing page feel
- Code blocks need syntax highlighting (WP plugin or custom CSS)
- Consider a lightweight page builder block for the feature grid and domain cards

## What NOT to put on this page
- API reference (that goes in the GitHub README / future docs site)
- Changelog (GitHub releases)
- Issue tracker links (keep that developer-facing, not marketing-facing)
- The full SDF specification (way too technical for a landing page)

## Honest notes on timing
- Do NOT publish this page until Spindle is pip-installable from PyPI
- A landing page for software nobody can install is a credibility hit
- Draft it now, review it after Phase 1 (Fabric integration), publish alongside the PyPI release
- The GitHub README can go live whenever — that's for developers who find the repo
