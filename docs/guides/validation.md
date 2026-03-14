# Validation Gates

Spindle includes 8 built-in validation gates that check generated data for quality issues. Failed artifacts can be automatically moved to a quarantine directory.

## Quick Start

```python
from sqllocks_spindle.validation import GateRunner, ValidationContext

context = ValidationContext(
    tables=result.tables,
    schema=result.schema,
    file_paths=[],
    config={},
)

runner = GateRunner()  # runs all gates by default
results = runner.run_all(context)

for r in results:
    status = "PASS" if r.passed else "FAIL"
    print(f"  {r.gate_name}: {status}")
    for err in r.errors:
        print(f"    ERROR: {err}")
```

## Eight Built-In Gates

| Gate | Name | What It Checks |
| --- | --- | --- |
| `ReferentialIntegrityGate` | `referential_integrity` | Every FK value exists in the parent table's PK column |
| `SchemaConformanceGate` | `schema_conformance` | Column names and types match the expected schema |
| `NullConstraintGate` | `null_constraint` | Non-nullable columns contain no NULL values |
| `UniqueConstraintGate` | `unique_constraint` | Primary key columns have no duplicate values |
| `RangeConstraintGate` | `range_constraint` | Numeric values fall within defined min/max bounds |
| `TemporalConsistencyGate` | `temporal_consistency` | Dates follow logical sequences (e.g., order_date < ship_date) |
| `FileFormatGate` | `file_format` | Output files are readable and not truncated |
| `SchemaDriftGate` | `schema_drift` | Schema hasn't changed unexpectedly vs. a baseline |

## Running Specific Gates

```python
# Run only referential integrity and schema conformance
runner = GateRunner(gates=["referential_integrity", "schema_conformance"])
results = runner.run_all(context)

# Run a single gate
result = runner.run_gate("referential_integrity", context)
```

## GateResult

Each gate returns a `GateResult`:

```python
@dataclass
class GateResult:
    gate_name: str
    passed: bool
    errors: list[str]
    warnings: list[str]
    details: dict[str, Any]
```

## Aggregate Summary

```python
summary = GateRunner.summary(results)
# {
#   "total": 8,
#   "passed": 7,
#   "failed": 1,
#   "gate_results": {...}
# }
```

## Quarantine Manager

When validation fails, move failed artifacts to a quarantine directory:

```python
from sqllocks_spindle.validation import QuarantineManager

qm = QuarantineManager(domain="retail")

# Quarantine a file
qm.quarantine_file(
    source_path="./landing/orders_2025-01-15.parquet",
    quarantine_root="./quarantine/",
    run_id="run_001",
    reason="Orphaned FK values in customer_id",
    gate_name="referential_integrity",
)

# Quarantine a DataFrame (writes as Parquet or CSV)
qm.quarantine_dataframe(
    df=bad_dataframe,
    quarantine_root="./quarantine/",
    run_id="run_001",
    table_name="order",
    reason="NULL values in non-nullable columns",
    gate_name="null_constraint",
    fmt="parquet",  # or "csv"
)

# List all quarantined items
items = qm.list_quarantined("./quarantine/")

# Get a detailed report for a specific run
report = qm.get_quarantine_report("./quarantine/", run_id="run_001")
```

## CLI Usage

```bash
spindle validate-outputs ./output/ --gates all --quarantine ./quarantine/
```

## Custom Gates

Register your own validation gate:

```python
from sqllocks_spindle.validation import ValidationGate, GateResult, ValidationContext, GateRunner

class MyCustomGate(ValidationGate):
    name = "my_custom_check"

    def check(self, context: ValidationContext) -> GateResult:
        errors = []
        # your validation logic here
        return GateResult(
            gate_name=self.name,
            passed=len(errors) == 0,
            errors=errors,
            warnings=[],
            details={},
        )

GateRunner.register_gate("my_custom_check", MyCustomGate)
```
