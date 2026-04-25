"""
Scenario 10 -- Anomaly Injection
=================================
Spindle can inject three types of anomalies into any stream. Every
anomalous event is tagged with:
  _spindle_is_anomaly  = True
  _spindle_anomaly_type = "point:<name>" | "contextual:<name>" | "collective:<name>"

Anomaly types:
  PointAnomaly      -- individual extreme values (fraud, sensor spikes, data errors)
  ContextualAnomaly -- values anomalous only in a specific context (wrong product in wrong season)
  CollectiveAnomaly -- clustered events compressed into a short time window (velocity fraud)

Use cases:
  - Build and test anomaly detection models with labeled ground truth
  - Stress-test alerting pipelines
  - Validate ML features under distribution shift

Run:
    python examples/scenarios/10_streaming_anomalies.py
"""

import json
import tempfile
from pathlib import Path

from sqllocks_spindle import RetailDomain
from sqllocks_spindle.streaming import (
    SpindleStreamer,
    StreamConfig,
    FileSink,
    AnomalyRegistry,
    PointAnomaly,
    ContextualAnomaly,
    CollectiveAnomaly,
)

# ------------------------------------------------------------------
# 1. PointAnomaly -- extreme individual values
#    Good for: fraud detection, sensor outliers, data quality errors
#    multiplier_range controls how extreme: (min, max) * column mean
# ------------------------------------------------------------------
print("=== PointAnomaly -- extreme order totals ===")
registry = AnomalyRegistry([
    PointAnomaly(
        name="extreme_total",
        column="order_total",
        multiplier_range=(10.0, 50.0),  # anomalous values are 10-50x the column mean
        fraction=0.05,                  # 5% of events are anomalous
    )
])

with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "point_anomalies.jsonl"
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=200, realtime=False),
        anomaly_registry=registry,
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"Events:    {result.events_sent}")
    print(f"Anomalies: {result.anomaly_count}  ({result.anomaly_count/result.events_sent:.1%})")

    # Read back and check the anomalous events
    events = [json.loads(line) for line in outfile.read_text().splitlines()]
    anomalous = [e for e in events if e.get("_spindle_is_anomaly")]
    normal    = [e for e in events if not e.get("_spindle_is_anomaly")]

    if anomalous and normal:
        avg_normal    = sum(e["order_total"] for e in normal) / len(normal)
        avg_anomalous = sum(e["order_total"] for e in anomalous) / len(anomalous)
        print(f"Avg normal total:    ${avg_normal:.2f}")
        print(f"Avg anomalous total: ${avg_anomalous:.2f}  ({avg_anomalous/avg_normal:.1f}x normal)")

# ------------------------------------------------------------------
# 2. ContextualAnomaly -- anomalous given a condition
#    Good for: business logic violations, impossible state combinations
#    normal_values = rows eligible to corrupt
#    anomalous_values = replacement values to inject
# ------------------------------------------------------------------
print("\n=== ContextualAnomaly -- cancelled orders get 'delivered' status ===")
registry = AnomalyRegistry([
    ContextualAnomaly(
        name="cancelled_delivered",
        column="status",
        condition_column="status",
        normal_values=["cancelled"],         # only corrupt cancelled orders
        anomalous_values=["delivered", "completed"],  # replace with contradictory status
        fraction=0.40,                       # 40% of cancelled orders get corrupted
    )
])

with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "contextual.jsonl"
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=300, realtime=False),
        anomaly_registry=registry,
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    events = [json.loads(line) for line in outfile.read_text().splitlines()]
    anomalous = [e for e in events if e.get("_spindle_is_anomaly")]
    print(f"Total events:         {result.events_sent}")
    print(f"Contextual anomalies: {len(anomalous)}")
    for e in anomalous[:3]:
        print(f"  status={e['status']}  type={e['_spindle_anomaly_type']}")

# ------------------------------------------------------------------
# 3. CollectiveAnomaly -- burst of events in the same group
#    Good for: account takeover, coordinated fraud, bot traffic
#    All rows in affected groups have their timestamp compressed
#    into a short window (window_seconds)
# ------------------------------------------------------------------
print("\n=== CollectiveAnomaly -- burst of orders from same customer ===")
registry = AnomalyRegistry([
    CollectiveAnomaly(
        name="velocity_fraud",
        group_column="customer_id",
        timestamp_column="order_date",
        window_seconds=300,     # compress affected group into 5-minute window
        fraction=0.05,          # 5% of customers get a burst
    )
])

with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "collective.jsonl"
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=300, realtime=False),
        anomaly_registry=registry,
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"Events:    {result.events_sent}")
    print(f"Anomalies: {result.anomaly_count}  ({result.anomaly_count/result.events_sent:.1%})")

# ------------------------------------------------------------------
# 4. Combined registry -- multiple anomaly types at once
# ------------------------------------------------------------------
print("\n=== Combined anomaly registry ===")
combined_registry = AnomalyRegistry([
    PointAnomaly(
        name="extreme_total",
        column="order_total",
        multiplier_range=(10.0, 50.0),
        fraction=0.03,
    ),
    ContextualAnomaly(
        name="returned_delivered",
        column="status",
        condition_column="status",
        normal_values=["returned"],
        anomalous_values=["delivered", "completed"],
        fraction=0.25,
    ),
])

with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "combined.jsonl"
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=300, realtime=False),
        anomaly_registry=combined_registry,
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    events = [json.loads(l) for l in outfile.read_text().splitlines()]
    point_count       = sum(1 for e in events if str(e.get("_spindle_anomaly_type", "")).startswith("point:"))
    contextual_count  = sum(1 for e in events if str(e.get("_spindle_anomaly_type", "")).startswith("contextual:"))

    print(f"Total events:         {result.events_sent}")
    print(f"Point anomalies:      {point_count}")
    print(f"Contextual anomalies: {contextual_count}")
    print(f"Total anomaly rate:   {result.anomaly_count/result.events_sent:.1%}")

# ------------------------------------------------------------------
# 5. Disable anomaly labels (use for production-like streams)
# ------------------------------------------------------------------
print("\n=== Anomalies without labels (label_anomalies=False) ===")
with tempfile.TemporaryDirectory() as tmp:
    outfile = Path(tmp) / "unlabeled.jsonl"
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(outfile), mode="w"),
        config=StreamConfig(max_events=50, realtime=False, label_anomalies=False),
        anomaly_registry=AnomalyRegistry([
            PointAnomaly("spike", column="order_total", fraction=0.10),
        ]),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    events = [json.loads(l) for l in outfile.read_text().splitlines()]
    has_label = any("_spindle_is_anomaly" in e for e in events)
    print(f"Events: {len(events)}  has _spindle_is_anomaly column: {has_label}")
