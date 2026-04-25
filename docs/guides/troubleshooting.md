# Troubleshooting

Common issues and how to fix them.

## Installation

### `ModuleNotFoundError: No module named 'sqllocks_spindle'`

You haven't installed the package, or you're in the wrong virtual environment.

```bash
pip install sqllocks-spindle
# Verify:
python -c "import sqllocks_spindle; print(sqllocks_spindle.__version__)"
```

### `ImportError: cannot import name 'EventHubSink'`

Streaming sinks require the `[streaming]` extra:

```bash
pip install sqllocks-spindle[streaming]
```

### `ModuleNotFoundError: No module named 'pyarrow'`

Parquet output requires the `[parquet]` extra:

```bash
pip install sqllocks-spindle[parquet]
```

### `ModuleNotFoundError: No module named 'openpyxl'`

Excel output requires the `[excel]` extra:

```bash
pip install sqllocks-spindle[excel]
```

## Generation

### `KeyError` when accessing a table from `GenerationResult`

Table names are case-sensitive and use snake_case. Use `result.table_names` to see available tables:

```python
result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result.table_names)
# ['customer', 'address', 'product_category', 'product', 'store', 'promotion', 'order', 'order_line', 'return']
```

### Generation is slow at large scales

Large and xlarge scales generate millions of rows. Tips:

- Use `--dry-run` first to see expected row counts
- Use Parquet output (`--format parquet`) instead of CSV for faster writes
- For `xlarge` scale, use Fabric Spark notebooks — pandas can't handle 100M+ rows in memory
- Close other memory-intensive applications

### `MemoryError` at xlarge scale

The `xlarge` preset generates 100M+ rows and requires 16GB+ RAM. For extreme scales:

1. Use Fabric notebooks with Spark (distributed memory)
2. Generate one domain at a time
3. Use the streaming engine to emit data incrementally instead of materializing everything in memory

### Integrity check returns errors

`result.verify_integrity()` checks FK relationships. If it returns errors:

- This is a bug — Spindle should always produce referentially intact data. Please [open an issue](https://github.com/sqllocks/spindle/issues) with your domain, scale, and seed.

## CLI

### `spindle: command not found`

The CLI is installed as a script entry point. Ensure your virtual environment is activated:

```bash
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows
spindle list
```

Or run as a module:

```bash
python -m sqllocks_spindle.cli list
```

### `spindle generate` produces empty output directory

Check that you specified `--output`:

```bash
spindle generate retail --scale small --format csv --output ./output/
```

Without `--output`, results are only printed to stdout.

## Fabric

### `LakehouseFilesWriter` raises authentication errors

Ensure you're running in a Fabric notebook or have `az login` configured:

```bash
az login --tenant <your-tenant-id>
```

The Fabric runtime auto-detects authentication. Outside Fabric, use `--auth cli`.

### Delta writes fail with schema mismatch

If writing to an existing Delta table, Spindle's schema must match. Use `overwriteSchema` option:

```python
spark.createDataFrame(df).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
```

### `OneLakePaths` returns wrong paths

Ensure you're running inside a Fabric notebook. `OneLakePaths` reads environment variables set by the Fabric runtime (`FABRIC_RUNTIME`, `TRIDENT_RUNTIME_VERSION`). Outside Fabric, construct paths manually.

## Chaos Engine

### Chaos mutations don't appear in output

Check your chaos intensity. The `calm` preset has low injection probability:

```python
from sqllocks_spindle.chaos import ChaosEngine, ChaosConfig

config = ChaosConfig(intensity="stormy")  # Higher injection rates
engine = ChaosEngine(config)
corrupted = engine.corrupt_dataframe(df, day=5)
```

### Chaos corrupts more data than expected

The `hurricane` preset (5x multiplier) is intentionally aggressive. Use `calm` (0.25x) or `moderate` (1x) for typical testing.

## Streaming

### Events arrive out of order

This is by design when `out_of_order=True` in `StreamConfig`. Spindle intentionally reorders events to test pipeline robustness. Set `out_of_order=False` for ordered delivery.

### Stream rate is lower than configured

In `realtime=True` mode, Spindle uses token-bucket rate limiting with Poisson inter-arrivals. Actual throughput will vary around the target rate. Set `realtime=False` for maximum throughput (no rate limiting).

## Still stuck?

[Open an issue on GitHub](https://github.com/sqllocks/spindle/issues) with:

1. Your Spindle version (`python -c "import sqllocks_spindle; print(sqllocks_spindle.__version__)"`)
2. Python version (`python --version`)
3. The command or code that failed
4. The full error traceback
