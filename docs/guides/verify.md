# Verifying Synthetic Data Quality

`spindle verify` checks that your generated data is statistically sound and internally consistent — without requiring the original source data. It is designed for:

- **CI/CD pipelines**: exit code 0 = pass, 1 = fail, ready for `make check`
- **Reproducible analysis**: every report includes a methodology section and a reproduce command
- **Schema-free spot checks**: works with just a directory of CSVs, no schema required

---

## Quick Start

```bash
# Generate retail data
spindle generate retail --scale small --format csv --output ./retail/

# Verify it (schema-free: row counts only)
spindle verify ./retail/

# Verify against schema (adds conformance, null, PK, FK checks)
spindle verify ./retail/ --schema retail.spindle.json

# Full verification with distribution drift detection
spindle verify ./retail/ --schema retail.spindle.json --statistical

# Write a reproducible report
spindle verify ./retail/ --schema retail.spindle.json --statistical --output report.md
```

---

## What Gets Checked

| Gate | When active | What it checks |
|------|-------------|----------------|
| `schema_conformance` | `--schema` provided | All expected columns present; types compatible |
| `null_constraint` | `--schema` provided | Non-nullable columns contain no null values |
| `unique_constraint` | `--schema` provided | Primary key columns contain no duplicates |
| `referential_integrity` | `--schema` provided | FK values exist in parent PK columns |
| `distribution` | `--statistical` | KS test (numeric) and chi-squared (enum) against schema-fitted distributions |

---

## Output Formats

### Console (default)

```
Spindle v2.13.0 — Verify

Data path:   ./retail/
Schema:      retail.spindle.json
Statistical: yes

Gate                         Status   Errors Warnings
-------------------------------------------------------
schema_conformance           PASS          0        0
null_constraint              PASS          0        0
unique_constraint            PASS          0        0
referential_integrity        PASS          0        0
distribution                 PASS          0        3

Row counts:
  customers: 2,500
  order_items: 15,000
  orders: 5,000
  products: 200

  WARN  [distribution]: customers.age: KS p=0.03 < α=0.05 — distribution may have drifted

Result: PASS
```

### Markdown (`--output report.md`)

Produces a full reproducible report with Summary table, Row Counts, Gate Details, and a Methodology section. The Methodology section includes the exact command to reproduce the report.

### JSON (`--output report.json`)

Machine-readable output suitable for CI tooling:

```json
{
  "spindle_version": "2.13.0",
  "run_at": "2026-05-04T14:00:00Z",
  "passed": true,
  "row_counts": {"customers": 2500, "orders": 5000},
  "gates": [
    {"gate": "schema_conformance", "passed": true, "errors": [], "warnings": []}
  ]
}
```

---

## Distribution Checks (`--statistical`)

Distribution checking requires scipy:

```bash
pip install sqllocks-spindle[inference]
```

`spindle verify --statistical` reads the `generator` spec from each column in the schema:

- **`strategy: distribution`** columns (e.g., `norm`, `expon`, `uniform`): runs a Kolmogorov-Smirnov test comparing actual data to the fitted distribution. A p-value below α=0.05 produces a **warning** (not an error).
- **`strategy: enum`** columns: runs a chi-squared test comparing observed category frequencies to expected probabilities. Missing expected values also produce a warning.

Distribution warnings indicate drift but not failure. They are most meaningful when you generated the schema with `spindle learn` from real data — the schema captures the source distributions, and `verify` checks whether generation honored them.

---

## Using in CI

```yaml
# GitHub Actions example
- name: Generate and verify synthetic data
  run: |
    spindle generate retail --scale small --format csv --output ./retail/
    spindle verify ./retail/ --schema retail.spindle.json --output verify-report.json
    # Exit code is 0 on pass, 1 on fail — pipeline fails automatically on error

- name: Upload verify report
  uses: actions/upload-artifact@v3
  with:
    name: spindle-verify-report
    path: verify-report.json
```

Use `--strict` to also fail on warnings:

```bash
spindle verify ./data/ --schema schema.json --statistical --strict
```

---

## Relationship to `spindle compare`

| Command | Input | Use case |
|---------|-------|----------|
| `spindle compare real/ synth/` | Both real and synthetic data | How faithful is my synthetic data to the original? |
| `spindle verify synth/` | Only synthetic data (+ schema) | Is this data internally consistent and statistically sound? |

`compare` gives a 0-100 fidelity score against real data. `verify` is a pass/fail quality gate that works without the original data — ideal for pipelines and demos where the source data is not available.

---

## Programmatic API

```python
from sqllocks_spindle.verify import load_tables, VerifyRunner, VerifyReport
from sqllocks_spindle.schema.parser import SchemaParser

tables = load_tables("./retail/", "csv")
schema = SchemaParser().parse_file("retail.spindle.json")

runner = VerifyRunner(schema=schema, statistical=True, data_path="./retail/", schema_path="retail.spindle.json")
result = runner.run(tables)

print(f"Passed: {result.passed}")
print(f"Rows:   {result.row_counts}")

report = VerifyReport(result)
print(report.to_markdown())
```
