# Quickstart

Generate your first synthetic dataset in under 5 minutes.

## Install

```bash
pip install sqllocks-spindle
```

## Generate Data (Python)

```python
from sqllocks_spindle import Spindle, RetailDomain

spindle = Spindle()
result = spindle.generate(
    domain=RetailDomain(),
    scale="small",
    seed=42
)

print(result.summary())
# GenerationResult(9 tables, 21,300 total rows, 0.3s)

# Access any table as a pandas DataFrame
customers = result["customer"]
orders = result["order"]
print(customers.head())

# Verify referential integrity
errors = result.verify_integrity()
assert errors == []
```

## Generate Data (CLI)

```bash
# Generate retail data as CSV
spindle generate retail --scale small --seed 42 --format csv --output ./output/

# See what would be generated without generating
spindle generate retail --scale medium --dry-run

# List all available domains
spindle list
```

## Scale Presets

| Preset | Customers | Orders | Approx Total Rows |
|--------|-----------|--------|-------------------|
| `fabric_demo` | 100 | 500 | ~3,500 |
| `small` | 1,000 | 5,000 | ~21,000 |
| `medium` | 50,000 | 500,000 | ~1.5M |
| `large` | 500,000 | 5,000,000 | ~15M |
| `warehouse` | 1,000,000 | 11,000,000 | ~30M |
| `xlarge` | 5,000,000 | 100,000,000 | ~280M |

## Output Formats

```bash
spindle generate retail --scale small --format csv --output ./data/
spindle generate retail --scale small --format parquet --output ./data/
spindle generate retail --scale small --format jsonl --output ./data/
spindle generate retail --scale small --format excel --output ./data/
spindle generate retail --scale small --format sql --output ./data/
spindle generate retail --scale small --format delta --output ./data/
```

Some formats require optional extras:

```bash
pip install sqllocks-spindle[parquet]   # Parquet support (pyarrow)
pip install sqllocks-spindle[excel]     # Excel support (openpyxl)
pip install sqllocks-spindle[fabric]    # Delta Lake support (deltalake)
```

## What's Next?

- [Installation](installation.md) — all extras and environment setup
- [CLI Cheatsheet](cli-cheatsheet.md) — all 12 CLI commands
- [Generation Strategies](../guides/strategies.md) — the 21 column-level strategies
- [Domain Catalog](../domains/index.md) — all 12 industry domains
