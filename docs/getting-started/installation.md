# Installation

## Requirements

- Python 3.10 or later (3.10, 3.11, 3.12, 3.13)
- Works in Microsoft Fabric Notebooks out of the box (Fabric uses Python 3.10/3.11)

## Core Install

```bash
pip install sqllocks-spindle
```

Core dependencies (installed automatically): `faker`, `numpy`, `pandas`, `click`.

## Optional Extras

Install additional capabilities as needed:

```bash
# Parquet output (pyarrow)
pip install sqllocks-spindle[parquet]

# Excel output (openpyxl)
pip install sqllocks-spindle[excel]

# Delta Lake / Fabric Lakehouse output (deltalake + pyarrow)
pip install sqllocks-spindle[fabric]

# Streaming sinks — Event Hub + Kafka (azure-eventhub, kafka-python)
pip install sqllocks-spindle[streaming]

# Everything
pip install sqllocks-spindle[all]

# Development (pytest)
pip install sqllocks-spindle[dev]
```

## Fabric Notebooks

In a Microsoft Fabric Notebook, install with:

```python
%pip install sqllocks-spindle
```

The core dependencies (`faker`, `numpy`, `pandas`) are pre-installed in the Fabric Spark runtime. For Delta output, the Fabric runtime includes PySpark Delta — use `sqllocks-spindle[fabric]` for the `deltalake` (delta-rs) writer if you need direct Delta table writes outside of Spark.

## Verify Installation

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
print(result.summary())
```

## Development Setup

```bash
git clone https://github.com/sqllocks/spindle.git
cd spindle
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
pip install -e ".[dev]"
pytest tests/ -v
```
