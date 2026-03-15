# Tutorial 12: Fabric Streaming

Stream synthetic events to Fabric Eventstream with configurable throughput, burst windows, out-of-order delivery, and anomaly injection.

---

## Prerequisites

- Python 3.10 or later
- `pip install sqllocks-spindle[streaming]` (includes `azure-eventhub`)
- Completed [Tutorial 11: Fabric Warehouse](11-fabric-warehouse.md)
- For Eventstream integration: a Fabric workspace with an Eventstream custom endpoint

## What You'll Learn

- How to stream generated data to console and file sinks using `SpindleStreamer`
- How to configure burst windows that simulate traffic spikes
- How to enable out-of-order delivery for realistic network conditions
- How to inject labeled anomalies (point, contextual, collective) into the event stream
- How to use `FabricStreamWriter` to send events to Fabric Eventstream
- How to connect to Event Hubs and Kafka endpoints

---

## Step 1: Stream Events to Console

`SpindleStreamer` generates data on-demand and converts rows to event dictionaries. Each event gets metadata fields (`_spindle_table`, `_spindle_seq`, `_spindle_event_time`) that downstream consumers can use for ordering and deduplication.

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.financial import FinancialDomain
from sqllocks_spindle.streaming import SpindleStreamer, StreamConfig, ConsoleSink

# Generate financial data
spindle = Spindle()
result = spindle.generate(domain=FinancialDomain(), scale="small", seed=42)

print(f"Generated tables: {result.table_names}")
print(f"Total rows: {sum(result.row_counts.values()):,}")

# Stream the first table to console (limited to 10 events)
streamer = SpindleStreamer(
    tables=result.tables,
    sink=ConsoleSink(),
    config=StreamConfig(max_events=10),
    seed=42,
)

first_table = result.table_names[0]
stream_result = streamer.stream(first_table)
print(f"\n{stream_result}")
```

Each event is a JSON object containing all table columns plus the `_spindle_*` metadata fields.

## Step 2: Stream to a File

Use `FileSink` to capture events as JSONL (one JSON object per line). This is how you would test your processing pipeline locally before connecting to Eventstream.

```python
from sqllocks_spindle.streaming import FileSink
import json

file_sink = FileSink("streaming_events.jsonl", mode="w")
streamer = SpindleStreamer(
    tables=result.tables,
    sink=file_sink,
    config=StreamConfig(max_events=200),
    seed=42,
)

stream_result = streamer.stream(first_table)
print(f"Streamed {stream_result.events_sent} events to streaming_events.jsonl")
print(f"Elapsed: {stream_result.elapsed_seconds:.3f}s")
print(f"Throughput: {stream_result.events_per_second_actual:,.0f} events/sec")

# Inspect the first event
with open("streaming_events.jsonl") as f:
    first_event = json.loads(f.readline())
print(f"\nSample event keys: {list(first_event.keys())}")
```

## Step 3: Burst Windows and Out-of-Order Delivery

Real streaming systems experience traffic bursts (flash sales, market opens) and network-induced reordering. Configure a `BurstWindow` that creates a 10x traffic spike, and enable out-of-order delivery for 10% of events.

```python
from sqllocks_spindle.streaming import BurstWindow, TimePattern

config = StreamConfig(
    max_events=500,
    out_of_order_fraction=0.10,          # 10% of events arrive out of order
    out_of_order_max_delay_slots=20,     # Late events shift up to 20 positions
    burst_windows=[
        BurstWindow(
            start_offset_seconds=5,      # Burst starts at t=5s
            duration_seconds=3,          # Lasts 3 seconds
            multiplier=10.0,             # 10x normal rate during burst
        ),
    ],
    time_pattern=TimePattern.business_hours(),  # Higher traffic during work hours
    realtime=False,  # Fast mode (no rate limiting) for this demo
)

streamer = SpindleStreamer(
    tables=result.tables,
    sink=FileSink("burst_events.jsonl", mode="w"),
    config=config,
    seed=42,
)

stream_result = streamer.stream(first_table)
print(f"Events sent:     {stream_result.events_sent:,}")
print(f"Out-of-order:    {stream_result.out_of_order_count}")
print(f"Elapsed:         {stream_result.elapsed_seconds:.3f}s")
```

Your KQL queries and Eventstream processing rules need to handle both burst traffic and late-arriving events. This configuration lets you validate that behavior.

## Step 4: Anomaly Injection

If you are building anomaly detection in KQL or Spark, you need known anomalies in your test data. Spindle supports three anomaly types, and labels each injected anomaly so you can verify your detection rules catch them.

```python
from sqllocks_spindle.streaming import (
    AnomalyRegistry, PointAnomaly, ContextualAnomaly, CollectiveAnomaly
)

registry = AnomalyRegistry()

# 5% of events get outlier values in a numeric column
registry.add(PointAnomaly(
    name="extreme_value",
    column="branch_id",
    fraction=0.05,
))

# 3% get contextually wrong values
registry.add(ContextualAnomaly(
    name="wrong_context",
    column="branch_name",
    condition_column="city",
    normal_values=["Macon", "Washington", "Toms River"],
    anomalous_values=["ANOMALY_VALUE_A", "ANOMALY_VALUE_B"],
    fraction=0.03,
))

# 2% form suspicious clusters
registry.add(CollectiveAnomaly(
    name="velocity_burst",
    group_column="branch_id",
    timestamp_column="opened_date",
    fraction=0.02,
))

# Stream with anomaly injection and labeling
config_anomaly = StreamConfig(
    max_events=500,
    label_anomalies=True,  # Keep _spindle_is_anomaly column in output
)

streamer = SpindleStreamer(
    tables=result.tables,
    sink=FileSink("anomaly_events.jsonl", mode="w"),
    config=config_anomaly,
    anomaly_registry=registry,
    seed=42,
)

stream_result = streamer.stream(first_table)
print(f"Events sent:     {stream_result.events_sent:,}")
print(f"Anomalies:       {stream_result.anomaly_count}")
print(f"Anomaly rate:    {stream_result.anomaly_count / max(stream_result.events_sent, 1) * 100:.1f}%")
```

The `label_anomalies=True` flag keeps a `_spindle_is_anomaly` column on each event so you can verify your detection logic against ground truth.

## Step 5: FabricStreamWriter for Eventstream

`FabricStreamWriter` is the high-level convenience API that wraps domain generation, streaming engine setup, and Event Hub protocol into a single call. You need the Event Hub-compatible connection string from your Eventstream custom endpoint.

```python
from sqllocks_spindle.fabric import FabricStreamWriter

writer = FabricStreamWriter(
    connection_string="Endpoint=sb://YOUR_EVENTSTREAM.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
    domain="financial",
    table="branch",
)

result = writer.stream(max_events=1000, rate=100.0)
print(f"Streamed {result.events_sent:,} events in {result.elapsed_seconds:.1f}s")
```

To set up the Eventstream endpoint in Fabric:

1. Create an Eventstream in your Fabric workspace
2. Add a source of type **Custom App**
3. Copy the connection string (`Endpoint=sb://...`)
4. Paste it as the `connection_string` parameter above

## Step 6: Event Hub and Kafka Sinks

For production streaming, Spindle provides dedicated sinks for Azure Event Hubs and Apache Kafka.

**Event Hub sink:**

```python
from sqllocks_spindle.streaming import EventHubSink

result = SpindleStreamer(
    domain=RetailDomain(),
    sink=EventHubSink(
        connection_string=EVENT_HUB_CONNECTION_STRING,
        eventhub_name="spindle-retail",
    ),
    config=StreamConfig(events_per_second=100.0, max_events=500, realtime=True),
    anomaly_registry=AnomalyRegistry([
        PointAnomaly(column="order_total", fraction=0.03, scale_factor=10.0),
    ]),
    scale="fabric_demo",
    seed=42,
).stream("order")
```

**Kafka sink:**

```python
from sqllocks_spindle.streaming import KafkaSink

result = SpindleStreamer(
    domain=RetailDomain(),
    sink=KafkaSink(
        bootstrap_servers="your-broker:9092",
        topic="spindle-retail-orders",
    ),
    config=StreamConfig(events_per_second=200.0, max_events=1000, realtime=True),
    scale="small",
    seed=42,
).stream("order")
```

**Multi-table streaming** sends each table to its own topic:

```python
tables_to_stream = {
    "order":      "spindle-orders",
    "order_line": "spindle-order-lines",
    "return":     "spindle-returns",
}

batch = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

for table_name, hub_name in tables_to_stream.items():
    result = SpindleStreamer(
        tables=batch.tables,
        sink=EventHubSink(EVENT_HUB_CONNECTION_STRING, eventhub_name=hub_name),
        config=StreamConfig(events_per_second=50.0, realtime=True),
    ).stream(table_name)
    print(f"  {table_name} -> {hub_name}: {result.events_sent} events")
```

---

> **Run It Yourself**
>
> - Notebook: [`F04_realtime_streaming.ipynb`](../../../examples/notebooks/fabric-scenarios/F04_realtime_streaming.ipynb)
> - Script: [`11_streaming_eventhub_kafka.py`](../../../examples/scenarios/11_streaming_eventhub_kafka.py)

---

## Related

- [Streaming guide](../../guides/streaming.md) -- the condensed reference for streaming configuration and sinks

---

## Next Step

[Tutorial 13: Medallion Architecture](13-medallion.md) -- build a complete Bronze/Silver/Gold pipeline with chaos injection, validation gates, and star schema transformation.
