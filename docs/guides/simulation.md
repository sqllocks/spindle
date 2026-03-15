# Simulation

Spindle simulates realistic upstream data delivery patterns — file drops into landing zones, event streams, and hybrid batch+stream modes. Use these to test your ingestion pipelines end-to-end.

## Three Simulation Modes

| Mode | Class | What It Simulates |
| --- | --- | --- |
| **File Drop** | `FileDropSimulator` | Daily/hourly file drops into a landing zone with manifests, done flags, late arrivals, and duplicates |
| **Stream** | `StreamEmitter` | Event streams with jitter, out-of-order delivery, replay windows, and CloudEvents envelopes |
| **Hybrid** | `HybridSimulator` | Concurrent batch + stream with correlation ID linking |

## File Drop Simulator

Simulates files arriving in a partitioned landing zone — the pattern Fabric pipelines and notebooks consume.

```python
from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.simulation import FileDropSimulator, FileDropConfig

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

config = FileDropConfig(
    domain="retail",
    base_path="./landing",
    cadence="daily",                  # daily | hourly | every_15m
    date_range_start="2025-01-01",
    date_range_end="2025-01-31",
    formats=["parquet"],
    manifest_enabled=True,
    done_flag_enabled=True,
    lateness_enabled=True,            # some files arrive late
    lateness_probability=0.10,        # 10% chance per partition
    max_days_late=3,
    duplicates_enabled=True,
    duplicate_probability=0.02,
    seed=42,
)

sim = FileDropSimulator(tables=result.tables, config=config)
drop_result = sim.run()

print(f"Files written: {len(drop_result.files_written)}")
print(f"Manifests: {len(drop_result.manifest_paths)}")
```

### Landing Zone Layout

```text
landing/
  retail/
    customer/
      dt=2025-01-01/
        retail_customer_2025-01-01_001.parquet
      dt=2025-01-02/
        ...
    _control/
      manifest_2025-01-01.json
      done_2025-01-01.flag
```

### CLI

```bash
spindle simulate file-drop --domain retail --scale small \
  --start-date 2025-01-01 --end-date 2025-01-31 --output ./landing/
```

## Stream Emitter

Emits events with CloudEvents envelopes, jitter, out-of-order delivery, and replay windows.

```python
from sqllocks_spindle.simulation import StreamEmitter, StreamEmitConfig

config = StreamEmitConfig(
    rate_per_sec=100.0,
    jitter_ms=50.0,
    out_of_order_probability=0.05,
    replay_enabled=True,
    replay_window_minutes=5.0,
    replay_probability=0.05,
    sink_type="console",
    max_events=5000,
    seed=42,
)

emitter = StreamEmitter(tables=result.tables, config=config)
stream_result = emitter.emit()

print(f"Events sent: {stream_result.events_sent}")
print(f"Replay events: {stream_result.replay_events_sent}")
```

### CLI

```bash
spindle simulate stream --domain retail --scale small \
  --max-events 5000 --output ./events/
```

## Hybrid Simulator

Runs file drop + stream simultaneously with correlation IDs linking the two.

```python
from sqllocks_spindle.simulation import HybridSimulator, HybridConfig

config = HybridConfig(
    stream_config=stream_emit_config,
    file_drop_config=file_drop_config,
    link_strategy="correlation_id",   # correlation_id | natural_keys
    concurrent=False,                 # True for parallel execution
    seed=42,
)

sim = HybridSimulator(tables=result.tables, config=config)
hybrid_result = sim.run()

print(f"Correlation ID: {hybrid_result.correlation_id}")
print(f"Files: {len(hybrid_result.file_drop_result.files_written)}")
print(f"Events: {hybrid_result.stream_result.events_sent}")
```

### CLI

```bash
spindle simulate hybrid --domain retail --scale small --output ./output/
```

## Scenario Packs

Spindle ships 44 built-in scenario packs (11 domains x 4 types) that combine generation + simulation + validation into reusable YAML blueprints. See [GSL Specs](gsl-specs.md) for the orchestration layer.

```python
from sqllocks_spindle.packs import PackLoader, PackRunner

pack = PackLoader().load_builtin("retail", "fd_daily_batch")
result = PackRunner().run(pack, domain=RetailDomain(), scale="small", seed=42)
print(result.summary())
```

---

## See Also

- **Tutorial:** [14: Scenario Packs](../tutorials/advanced/14-scenario-packs.md) — step-by-step walkthrough
- **Example script:** [`14_file_drop_simulation.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/14_file_drop_simulation.py)
- **Example script:** [`15_stream_emitter.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/15_stream_emitter.py)
- **Example script:** [`16_hybrid_simulation.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/16_hybrid_simulation.py)
- **Example script:** [`19_scenario_packs.py`](https://github.com/sqllocks/spindle/blob/main/examples/scenarios/19_scenario_packs.py)
- **Notebook:** [`T13_file_drop_simulation.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/intermediate/T13_file_drop_simulation.ipynb)
- **Notebook:** [`08_scenario_packs.ipynb`](https://github.com/sqllocks/spindle/blob/main/examples/notebooks/showcase/08_scenario_packs.ipynb)
