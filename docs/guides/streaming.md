# Streaming

Spindle's streaming engine generates and emits events row-by-row with realistic timing — Poisson inter-arrivals, token-bucket rate limiting, burst windows, out-of-order delivery, and anomaly injection.

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain, SpindleStreamer, StreamConfig, ConsoleSink

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

streamer = SpindleStreamer(
    tables=result.tables,
    sink=ConsoleSink(),
    config=StreamConfig(max_events=100),
)
stream_result = streamer.stream("order")
print(f"Sent {stream_result.events_sent} events in {stream_result.elapsed_seconds:.1f}s")
```

## StreamConfig

```python
from sqllocks_spindle import StreamConfig, BurstWindow

config = StreamConfig(
    events_per_second=100.0,         # target rate (realtime mode)
    max_events=5000,                 # stop after N events
    duration_seconds=60.0,           # stop after N seconds (realtime)
    out_of_order_fraction=0.05,      # 5% of events reordered
    out_of_order_max_delay_slots=10, # max positions to shift
    burst_windows=[                  # traffic spikes
        BurstWindow(start_offset_seconds=30, duration_seconds=60, multiplier=10),
    ],
    batch_size=100,                  # events per batch
    realtime=True,                   # enable rate limiting
    label_anomalies=True,            # add anomaly marker columns
)
```

## Sinks

| Sink | Deps | Description |
| --- | --- | --- |
| `ConsoleSink` | None | Print events to stdout |
| `FileSink` | None | Write events as JSON Lines |
| `EventHubSink` | `[streaming]` | Send to Azure Event Hub / Fabric Eventstream |
| `KafkaSink` | `[streaming]` | Send to Apache Kafka |

```python
from sqllocks_spindle import ConsoleSink, FileSink
from sqllocks_spindle.streaming.sinks import EventHubSink, KafkaSink

# Console
sink = ConsoleSink(indent=2, prefix="EVENT: ")

# File (JSON Lines)
sink = FileSink(path="events.jsonl")

# Event Hub / Fabric Eventstream
sink = EventHubSink(connection_string="Endpoint=sb://...")

# Kafka
sink = KafkaSink(bootstrap_servers="localhost:9092", topic="orders")
```

## Anomaly Injection

Inject labeled anomalies for ML training and detection testing.

```python
from sqllocks_spindle import AnomalyRegistry, PointAnomaly, ContextualAnomaly, CollectiveAnomaly

registry = AnomalyRegistry([
    PointAnomaly("extreme_amount", column="order_total", multiplier_range=(10, 100), fraction=0.01),
    ContextualAnomaly("wrong_status", column="status", condition_column="status",
                      normal_values=["completed"], anomalous_values=["cancelled"], fraction=0.01),
    CollectiveAnomaly("velocity_burst", group_column="customer_id",
                      timestamp_column="order_date", window_seconds=600, fraction=0.005),
])

streamer = SpindleStreamer(
    tables=result.tables,
    sink=FileSink("events.jsonl"),
    config=StreamConfig(max_events=5000),
    anomaly_registry=registry,
)
stream_result = streamer.stream("order")
print(f"Anomalies injected: {stream_result.anomaly_count}")
```

Every anomaly-injected row gets `_spindle_is_anomaly=True` and `_spindle_anomaly_type` columns for ground-truth labeling.

### Three Anomaly Types

| Type | What It Does | Example |
| --- | --- | --- |
| `PointAnomaly` | Extreme value in a single column | $99,999 order on a $50 average ticket |
| `ContextualAnomaly` | Normal value in wrong context | Eligible rows get an anomalous replacement value |
| `CollectiveAnomaly` | Group of rows compressed into short window | 47 orders from one customer in 10 minutes |

## Burst Windows

Simulate traffic spikes:

```python
config = StreamConfig(
    events_per_second=50,
    realtime=True,
    burst_windows=[
        BurstWindow(start_offset_seconds=30, duration_seconds=60, multiplier=10),
        # At 30s into the stream, rate jumps to 500/s for 60s, then back to 50/s
    ],
)
```

## CLI

```bash
# Basic streaming to console
spindle stream retail --table order --max-events 1000

# Realtime with rate limiting and burst
spindle stream retail --table order --rate 100 --realtime \
  --burst 30:60:10 --max-events 5000

# To file with anomalies and out-of-order
spindle stream retail --table order --sink file --output events.jsonl \
  --anomaly-fraction 0.05 --out-of-order 0.03
```

## Event Metadata

Every emitted event includes metadata columns:

| Column | Description |
| --- | --- |
| `_spindle_table` | Source table name |
| `_spindle_seq` | Sequence number within the stream |
| `_spindle_event_time` | Auto-detected timestamp from the first datetime column |
| `_spindle_is_anomaly` | `True` if this row was injected as an anomaly |
| `_spindle_anomaly_type` | Anomaly type label (e.g., `point:extreme_amount`) |
