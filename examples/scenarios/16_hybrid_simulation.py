"""
Scenario 16 -- Hybrid Simulation
==================================
Run file-drop (batch) and stream emission concurrently, linked by a shared
correlation ID. This mirrors Fabric architectures that ingest the same domain
data via two paths: micro-batches to Lakehouse Files and real-time events
to Eventhouse / KQL Database.

The HybridSimulator:
  - splits tables into batch_tables (-> FileDropSimulator) and stream_tables (-> StreamEmitter)
  - stamps both outputs with the same correlation_id so you can join them later
  - optionally runs both phases in parallel threads (concurrent=True)

Run:
    python examples/scenarios/16_hybrid_simulation.py
"""

from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.simulation.file_drop import FileDropConfig
from sqllocks_spindle.simulation.hybrid import HybridConfig, HybridSimulator
from sqllocks_spindle.simulation.stream_emit import StreamEmitConfig

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
print(f"Generated tables: {list(result.tables.keys())}")

# ── 1. Basic hybrid — sequential ─────────────────────────────────────────────
print("\n── 1. Sequential hybrid (batch + stream) ──")

cfg = HybridConfig(
    stream_to="eventhouse",
    micro_batch_to="lakehouse_files",
    stream_tables=["order"],                    # these go to the stream path
    batch_tables=["customer", "order_line"],    # these go to the file-drop path
    file_drop_config=FileDropConfig(
        domain="retail",
        base_path="./hybrid_output/files",
        cadence="daily",
        date_range_start="2025-01-01",
        date_range_end="2025-01-07",
        formats=["parquet"],
        manifest_enabled=True,
        done_flag_enabled=True,
    ),
    stream_config=StreamEmitConfig(
        rate_per_sec=5000,
        max_events=200,
        sink_type="console",
        topics=["retail.orders"],
        envelope_schema_version="1.0",
    ),
    link_strategy="correlation_id",
    concurrent=False,
    seed=42,
)

r = HybridSimulator(tables=result.tables, config=cfg).run()

batch_files = len(r.file_drop_result.files_written) if r.file_drop_result else 0
stream_evts  = r.stream_result.events_sent          if r.stream_result  else 0
print(f"  Batch files:    {batch_files}")
print(f"  Stream events:  {stream_evts:,}")
print(f"  Correlation ID: {r.correlation_id}")
print(f"  Link strategy:  {r.link_strategy}")

# ── 2. Concurrent hybrid ──────────────────────────────────────────────────────
print("\n── 2. Concurrent hybrid (both paths run in parallel) ──")

cfg_concurrent = HybridConfig(
    stream_to="eventhouse",
    micro_batch_to="lakehouse_files",
    stream_tables=["order"],
    batch_tables=["customer", "store", "order_line"],
    file_drop_config=FileDropConfig(
        domain="retail",
        base_path="./hybrid_output/concurrent",
        cadence="daily",
        date_range_start="2025-01-01",
        date_range_end="2025-01-03",
        formats=["parquet", "csv"],
    ),
    stream_config=StreamEmitConfig(
        rate_per_sec=5000,
        max_events=100,
        sink_type="file",
        sink_connection={"path": "./hybrid_output/orders_stream.jsonl"},
    ),
    link_strategy="correlation_id",
    concurrent=True,
    seed=42,
)

r2 = HybridSimulator(tables=result.tables, config=cfg_concurrent).run()
print(f"  Batch files:    {len(r2.file_drop_result.files_written) if r2.file_drop_result else 0}")
print(f"  Stream events:  {r2.stream_result.events_sent if r2.stream_result else 0:,}")
print(f"  Correlation ID: {r2.correlation_id}")

# ── 3. All tables — batch only ────────────────────────────────────────────────
print("\n── 3. Batch-only mode (stream_tables=[], all go to file-drop) ──")

cfg_batch_only = HybridConfig(
    stream_tables=[],   # empty = nothing goes to stream
    batch_tables=[],    # empty = all tables go to file-drop
    file_drop_config=FileDropConfig(
        domain="retail",
        base_path="./hybrid_output/batch_only",
        cadence="daily",
        date_range_start="2025-01-01",
        date_range_end="2025-01-02",
        formats=["parquet"],
    ),
    stream_config=StreamEmitConfig(max_events=0, sink_type="console"),
    seed=42,
)

r3 = HybridSimulator(tables=result.tables, config=cfg_batch_only).run()
print(f"  Batch files: {len(r3.file_drop_result.files_written) if r3.file_drop_result else 0}")
print(f"  Stream events: {r3.stream_result.events_sent if r3.stream_result else 0}")

# ── Summary ────────────────────────────────────────────────────────────────────
print("\n── Summary ──")
print(f"  {'Mode':<28} {'Files':>6} {'Events':>8} {'Correlation ID':>36}")
print(f"  {'-'*80}")
for label, res in [
    ("Sequential",  r),
    ("Concurrent",  r2),
    ("Batch-only",  r3),
]:
    files  = len(res.file_drop_result.files_written) if res.file_drop_result else 0
    events = res.stream_result.events_sent if res.stream_result else 0
    print(f"  {label:<28} {files:>6} {events:>8,} {res.correlation_id:>36}")

print("\nBoth outputs share the same correlation_id — use it to join batch")
print("manifests against stream event metadata in KQL / Spark.")
