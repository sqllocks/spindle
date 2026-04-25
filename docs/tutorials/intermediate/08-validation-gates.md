# Tutorial 08: Validation Gates

Run automated quality checks against your data, catch violations before they reach downstream consumers, and quarantine bad records for investigation.

## Prerequisites

- Completed [Tutorial 07: Chaos Engineering](07-chaos-engineering.md) (or equivalent experience)
- Familiarity with `Spindle.generate()` and domain objects
- Understanding of data quality concerns (nulls, duplicates, orphaned FKs)

## What You'll Learn

- What validation gates are and how the gate framework works
- How to run individual gates: `ReferentialIntegrityGate`, `NullConstraintGate`, `UniqueConstraintGate`, `SchemaConformanceGate`
- How to orchestrate multiple gates with `GateRunner`
- How to interpret `GateResult` objects
- How to quarantine failed records with `QuarantineManager`
- How to combine chaos injection with validation for end-to-end testing

## Time Estimate

**~15 minutes**

---

## The Gate Framework

A validation gate is a single, focused check that answers one question about your data:

| Gate | Question It Answers |
|------|---------------------|
| `ReferentialIntegrityGate` | Do all foreign keys point to valid parent records? |
| `NullConstraintGate` | Are NOT NULL columns free of nulls? |
| `UniqueConstraintGate` | Are unique/primary key columns actually unique? |
| `SchemaConformanceGate` | Do the actual columns match the expected schema? |

Gates are the safety net between data generation (or ingestion) and consumption. They catch problems early -- before bad data poisons your dashboards, ML models, or reports.

## Step 1 -- Generate Clean Data and Create a Validation Context

Start by generating clean data and wrapping it in a `ValidationContext`. Clean data should pass all gates -- verifying this first confirms the gates do not produce false positives:

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.validation import (
    GateRunner,
    ValidationContext,
    ReferentialIntegrityGate,
    NullConstraintGate,
    UniqueConstraintGate,
)

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Create a validation context
ctx = ValidationContext(tables=result.tables, schema=result.schema)
print(f"Validation context created with {len(ctx.tables)} tables.")
```

The `ValidationContext` bundles your tables and schema metadata together so each gate has everything it needs.

## Step 2 -- Run the Referential Integrity Gate

This gate checks every foreign-key relationship defined in the schema and verifies that all child records reference valid parent records:

```python
ri_gate = ReferentialIntegrityGate()
ri_result = ri_gate.check(ctx)

print(f"Status: {'PASS' if ri_result.passed else 'FAIL'}")
print(f"Checks performed: {ri_result.checks_performed}")
print(f"Violations: {ri_result.violation_count}")
```

Orphaned records (child rows with no matching parent) cause silent data loss in JOINs. This gate catches them before they reach your queries.

## Step 3 -- Run the Null Constraint Gate

This gate checks that columns marked as NOT NULL in the schema contain no null values:

```python
null_gate = NullConstraintGate()
null_result = null_gate.check(ctx)

print(f"Status: {'PASS' if null_result.passed else 'FAIL'}")
print(f"Checks performed: {null_result.checks_performed}")
print(f"Violations: {null_result.violation_count}")
```

Unexpected nulls are the most common data quality issue. They cause calculation errors, broken aggregations, and misleading visualizations.

## Step 4 -- Run the Unique Constraint Gate

This gate verifies that primary key and unique columns contain no duplicate values:

```python
unique_gate = UniqueConstraintGate()
unique_result = unique_gate.check(ctx)

print(f"Status: {'PASS' if unique_result.passed else 'FAIL'}")
print(f"Checks performed: {unique_result.checks_performed}")
print(f"Violations: {unique_result.violation_count}")
```

Duplicate keys cause incorrect JOIN fan-outs, double-counting in aggregations, and subtle bugs that are hard to trace.

## Step 5 -- Run the Schema Conformance Gate

This gate checks whether the actual table columns match the expected schema -- catching schema drift before it causes downstream failures:

```python
from sqllocks_spindle.validation.gates import SchemaConformanceGate

sc_gate = SchemaConformanceGate()
sc_result = sc_gate.check(ctx)

print(f"Status: {'PASS' if sc_result.passed else 'FAIL'}")
```

## Step 6 -- Orchestrate with GateRunner

Instead of running gates individually, use the `GateRunner` to execute all gates in sequence and produce a unified report. This is the pattern you would use in a production pipeline:

```python
runner = GateRunner(
    gates=[ReferentialIntegrityGate(), NullConstraintGate(), UniqueConstraintGate()]
)

runner_result = runner.run(ctx)

print(f"Overall status: {'PASS' if runner_result.all_passed else 'FAIL'}")
print(f"Gates run: {runner_result.gates_run}")
print(f"Gates passed: {runner_result.gates_passed}")
print(f"Gates failed: {runner_result.gates_failed}")

for gate_name, gate_result in runner_result.results.items():
    status = "PASS" if gate_result.passed else "FAIL"
    print(f"  {gate_name}: {status} ({gate_result.violation_count} violations)")
```

The gate runner gives you a single pass/fail decision for your entire dataset. It also collects all violations across all gates, making it easy to generate a quality report or trigger alerts.

## Step 7 -- Inject Chaos and Watch Gates Catch It

Now for the payoff: intentionally corrupt the data, then run the gates again. This proves your quality checks work end-to-end:

```python
from sqllocks_spindle.chaos import ChaosConfig, ChaosEngine

# Apply chaos to create bad data
chaos_config = ChaosConfig(enabled=True, intensity="stormy", seed=99)
engine = ChaosEngine(chaos_config)

corrupted_tables = {}
for name, df in result.tables.items():
    corrupted_tables[name] = engine.corrupt_dataframe(df.copy(), day=5)

# Create a new context with corrupted data
corrupted_ctx = ValidationContext(tables=corrupted_tables, schema=result.schema)

# Run all gates
corrupted_result = runner.run(corrupted_ctx)

print(f"Overall status: {'PASS' if corrupted_result.all_passed else 'FAIL'}")
print(f"Gates passed: {corrupted_result.gates_passed}")
print(f"Gates failed: {corrupted_result.gates_failed}")

for gate_name, gate_result in corrupted_result.results.items():
    status = "PASS" if gate_result.passed else "FAIL"
    print(f"  {gate_name}: {status} ({gate_result.violation_count} violations)")
    if not gate_result.passed and gate_result.details:
        for detail in gate_result.details[:3]:
            print(f"    -> {detail}")
```

## Step 8 -- Quarantine Bad Records

Quarantining is better than rejecting an entire batch. Good records flow through to downstream consumers, while bad records go to a quarantine area for investigation.

### Manual Quarantine by Splitting DataFrames

You can split a DataFrame into clean and quarantined subsets based on gate results:

```python
customers_corrupted = corrupted_tables["customers"]
customers_original = result.tables["customers"]

# Identify NOT NULL columns from the original schema
not_null_cols = [col for col in customers_original.columns
                 if customers_original[col].isnull().sum() == 0]

# Split into clean and quarantine
has_violation = customers_corrupted[not_null_cols].isnull().any(axis=1)
clean_df = customers_corrupted[~has_violation].copy()
quarantine_df = customers_corrupted[has_violation].copy()

print(f"Total records: {len(customers_corrupted)}")
print(f"Clean records: {len(clean_df)}")
print(f"Quarantined records: {len(quarantine_df)}")
```

### QuarantineManager for Persistent Storage

For production workflows, use `QuarantineManager` to write quarantined records to a dedicated folder with metadata:

```python
from sqllocks_spindle.validation.quarantine import QuarantineManager
from pathlib import Path

QUARANTINE_DIR = Path("./demo_quarantine")
RUN_ID = "run_20250115_001"

qm = QuarantineManager(domain="retail")

# Quarantine the failed table
path = qm.quarantine_dataframe(
    df=corrupted_tables["order"],
    quarantine_root=QUARANTINE_DIR,
    run_id=RUN_ID,
    table_name="order",
    reason="Referential integrity violations detected",
    gate_name="referential_integrity",
    fmt="parquet",
)
print(f"Quarantined order table -> {path}")

# List all quarantined items
inventory = qm.list_quarantined(QUARANTINE_DIR)
print(f"Items in quarantine: {len(inventory)}")
for item in inventory:
    print(f"  - {item}")
```

## Step 9 -- Inspect GateResult Properties

Each `GateResult` provides structured information about what passed, what failed, and why:

```python
print(f"gate_name: {ri_bad.gate_name}")
print(f"passed:    {ri_bad.passed}")
print(f"errors:    {ri_bad.errors[:2]}")
print(f"warnings:  {ri_bad.warnings[:2]}")
print(f"details:   {ri_bad.details}")
```

Use these properties to build automated alerting, write quality reports, or drive remediation workflows.

---

> **Run It Yourself**
>
> - Notebook: [`T15_validation_gates.ipynb`](../../../examples/notebooks/intermediate/T15_validation_gates.ipynb)
> - Script: [`18_validation_gates.py`](../../../examples/scenarios/18_validation_gates.py)

## Related

- [Validation Guide](../../guides/validation.md) -- full reference for all gate types, custom gates, and quarantine configuration

## Next Step

Continue to [Tutorial 09: Composite Domains](09-composite-domains.md) to learn how to generate multi-domain datasets with cross-domain foreign-key relationships.
