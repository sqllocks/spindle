"""
Scenario 08 -- Streaming Basics
================================
SpindleStreamer emits generated rows one-by-one through a sink. This is
the foundation for feeding Fabric Eventstream, Event Hubs, Kafka, or any
other event pipeline.

This scenario covers the two built-in sinks that require no extra packages:
  ConsoleSink -- prints events to stdout (development / debugging)
  FileSink    -- writes events as JSONL (local files, ADLS paths)

Run:
    python examples/scenarios/08_streaming_basics.py
"""

import json
import tempfile
from pathlib import Path

from sqllocks_spindle import RetailDomain, HealthcareDomain
from sqllocks_spindle.streaming import (
    SpindleStreamer,
    StreamConfig,
    ConsoleSink,
    FileSink,
)

# ------------------------------------------------------------------
# 1. ConsoleSink -- print events to stdout
#    Good for quickly verifying the event shape before wiring to a real sink
# ------------------------------------------------------------------
print("=== ConsoleSink (first 10 events) ===")
result = SpindleStreamer(
    domain=RetailDomain(),
    sink=ConsoleSink(),
    config=StreamConfig(max_events=10, realtime=False),
    scale="fabric_demo",
    seed=42,
).stream("order")

print(f"\nStream result: {result}")
print(f"  Table:    {result.table}")
print(f"  Events:   {result.events_sent:,}")
print(f"  Elapsed:  {result.elapsed_seconds:.3f}s")
print(f"  Throughput: {result.events_per_second_actual:,.0f} events/s")

# ------------------------------------------------------------------
# 2. FileSink -- write events as JSONL
# ------------------------------------------------------------------
print("\n=== FileSink (JSONL) ===")
with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "orders.jsonl"

    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=50, realtime=False),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"Written: {outfile.name}  ({outfile.stat().st_size:,} bytes)")
    print(f"Events:  {result.events_sent}")

    # Each line is a JSON object -- preview the first three
    lines = outfile.read_text(encoding="utf-8").splitlines()
    print("\nFirst 3 events:")
    for line in lines[:3]:
        event = json.loads(line)
        # _spindle_table and _spindle_seq are added automatically
        print(f"  seq={event['_spindle_seq']}  "
              f"order_id={event.get('order_id')}  "
              f"total={event.get('order_total')}  "
              f"status={event.get('status')}")

# ------------------------------------------------------------------
# 3. Stream a different table
# ------------------------------------------------------------------
print("\n=== Streaming order_line table ===")
with tempfile.TemporaryDirectory() as tmp:
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(Path(tmp) / "order_lines.jsonl"), mode="w"),
        config=StreamConfig(max_events=100, realtime=False),
        scale="fabric_demo",
        seed=42,
    ).stream("order_line")

    print(f"order_line: {result.events_sent} events  {result.elapsed_seconds:.3f}s")

# ------------------------------------------------------------------
# 4. Stream from pre-generated tables
#    Generate once -> stream multiple tables without re-generating
# ------------------------------------------------------------------
print("\n=== Pre-generated tables -- stream without re-generating ===")
from sqllocks_spindle import Spindle
batch = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

with tempfile.TemporaryDirectory() as tmp:
    for table_name in ["order", "order_line", "return"]:
        outfile = Path(tmp) / f"{table_name}.jsonl"
        result = SpindleStreamer(
            tables=batch.tables,           # reuse pre-generated data
            sink=FileSink(str(outfile), mode="w"),
            config=StreamConfig(max_events=30, realtime=False),
        ).stream(table_name)
        print(f"  {table_name:<12} {result.events_sent:>3} events  "
              f"{outfile.stat().st_size:>6,} bytes")

# ------------------------------------------------------------------
# 5. Healthcare domain streaming
# ------------------------------------------------------------------
print("\n=== Healthcare -- stream encounters ===")
result = SpindleStreamer(
    domain=HealthcareDomain(),
    sink=ConsoleSink(),
    config=StreamConfig(max_events=5, realtime=False),
    scale="fabric_demo",
    seed=42,
).stream("encounter")

print(f"\n{result}")
