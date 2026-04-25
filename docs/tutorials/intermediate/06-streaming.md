# Tutorial 06: Streaming

Emit realistic event streams with configurable throughput, out-of-order delivery, burst windows, and anomaly injection.

## Prerequisites

- Completed [Tutorial 05: Star Schema](05-star-schema.md) (or equivalent experience)
- Familiarity with `Spindle.generate()` and domain objects
- Basic understanding of event-driven architectures

## What You'll Learn

- How to configure `SpindleStreamer` with `StreamConfig`
- How to write events to files with `FileSink` and to the console with `ConsoleSink`
- How to control throughput with rate limiting and `realtime` mode
- How to simulate traffic spikes with `BurstWindow`
- How to inject out-of-order events for late-arrival testing
- How to inject labeled anomalies with `AnomalyRegistry`

## Time Estimate

**~20 minutes**

---

## Step 1 -- Stream Events to a File

The `SpindleStreamer` emits generated rows one by one through a sink. The two built-in sinks require no extra packages:

| Sink | Purpose |
|------|---------|
| `ConsoleSink` | Print events to stdout (development/debugging) |
| `FileSink` | Write events as newline-delimited JSON (JSONL) |

Start by streaming retail order events to a JSONL file:

```python
from sqllocks_spindle import SpindleStreamer, StreamConfig, FileSink, RetailDomain

config = StreamConfig(
    events_per_second=100,
    max_events=500,
    out_of_order_fraction=0.05,
)

sink = FileSink("./stream_demo/events.jsonl", mode="w")
streamer = SpindleStreamer(
    domain=RetailDomain(),
    sink=sink,
    config=config,
    scale="small",
    seed=42,
)

result = streamer.stream("order")
sink.close()

print(f"Events sent: {result.events_sent}")
print(f"Out-of-order events: {result.out_of_order_count}")
print(f"Duration: {result.duration_seconds:.2f}s")
```

Each line in the JSONL file is a self-contained JSON object representing one event. Every event includes `_spindle_table` and `_spindle_seq` metadata fields automatically.

## Step 2 -- Inspect the Output

Before wiring a stream into a production pipeline, inspect the event shape:

```python
import json

with open("./stream_demo/events.jsonl") as f:
    for i, line in enumerate(f):
        if i >= 3:
            break
        event = json.loads(line)
        print(json.dumps(event, indent=2, default=str))
```

Each event contains the full row payload -- order ID, customer references, totals, status, and timestamps -- plus Spindle metadata.

## Step 3 -- Detect Out-of-Order Events

Real event streams are never perfectly ordered. Network delays, retries, and partition rebalancing all cause events to arrive out of sequence. The `out_of_order_fraction` parameter controls what percentage of events arrive with timestamps earlier than their predecessor:

```python
events = []
with open("./stream_demo/events.jsonl") as f:
    for line in f:
        events.append(json.loads(line))

ooo_count = 0
for i in range(1, len(events)):
    current_ts = events[i].get("event_time") or events[i].get("timestamp", "")
    previous_ts = events[i - 1].get("event_time") or events[i - 1].get("timestamp", "")
    if current_ts < previous_ts:
        ooo_count += 1

print(f"Total events: {len(events)}")
print(f"Out-of-order events: {ooo_count}")
print(f"OOO rate: {ooo_count / len(events) * 100:.1f}%")
```

You can also control how far back events can be reordered:

```python
config = StreamConfig(
    max_events=50,
    realtime=False,
    out_of_order_fraction=0.20,           # 20% of events arrive out of order
    out_of_order_max_delay_slots=5,       # reordered by up to 5 positions
)
```

## Step 4 -- Rate Limiting and Realtime Mode

By default Spindle emits events as fast as possible (`realtime=False`). Set `realtime=True` to enable a token-bucket rate limiter with Poisson inter-arrival times:

```python
from sqllocks_spindle.streaming import SpindleStreamer, StreamConfig, FileSink

result = SpindleStreamer(
    domain=RetailDomain(),
    sink=FileSink("./stream_demo/rate_limited.jsonl", mode="w"),
    config=StreamConfig(
        events_per_second=20.0,
        max_events=40,
        realtime=True,              # enable rate limiting + Poisson spacing
    ),
    scale="fabric_demo",
    seed=42,
).stream("order")

print(f"Events: {result.events_sent}")
print(f"Elapsed: {result.elapsed_seconds:.2f}s")
print(f"Actual rate: {result.events_per_second_actual:.1f} events/s  (target: 20)")
```

With `realtime=True`, 40 events at 20/sec takes approximately 2 seconds of wall-clock time.

## Step 5 -- Burst Windows

Production systems experience traffic bursts that can overwhelm downstream consumers. A `BurstWindow` defines a time period where the event rate spikes dramatically:

```python
from sqllocks_spindle import ConsoleSink, BurstWindow

burst_config = StreamConfig(
    events_per_second=50,
    max_events=100,
    out_of_order_fraction=0.0,
    burst_windows=[BurstWindow(start_event=30, end_event=60, multiplier=5.0)],
)

console_sink = ConsoleSink(verbose=False)
burst_streamer = SpindleStreamer(
    domain=RetailDomain(),
    sink=console_sink,
    config=burst_config,
    scale="small",
    seed=99,
)

burst_result = burst_streamer.stream("order")
print(f"Events sent: {burst_result.events_sent}")
print(f"Effective rate: {burst_result.events_sent / burst_result.duration_seconds:.0f} events/sec")
```

In realtime mode, burst windows use time offsets instead of event indices:

```python
burst_config = StreamConfig(
    events_per_second=10.0,
    max_events=80,
    realtime=True,
    burst_windows=[
        BurstWindow(
            start_offset_seconds=1.0,
            duration_seconds=2.0,
            multiplier=5.0,     # 50 events/sec during the burst
        )
    ],
)
```

You can stack multiple burst windows to simulate scenarios like Black Friday with rolling traffic spikes:

```python
config = StreamConfig(
    events_per_second=5.0,
    max_events=60,
    realtime=True,
    burst_windows=[
        BurstWindow(start_offset_seconds=0.5, duration_seconds=1.0, multiplier=4.0),
        BurstWindow(start_offset_seconds=3.0, duration_seconds=1.5, multiplier=6.0),
    ],
)
```

## Step 6 -- Anomaly Injection

Spindle can inject three types of labeled anomalies into any stream. Every anomalous event is tagged with `_spindle_is_anomaly=True` and `_spindle_anomaly_type` for ground-truth labeling.

### Point Anomalies -- Extreme Individual Values

Use for fraud detection, sensor outliers, or data quality errors:

```python
from sqllocks_spindle.streaming import (
    AnomalyRegistry,
    PointAnomaly,
    ContextualAnomaly,
    CollectiveAnomaly,
)

registry = AnomalyRegistry([
    PointAnomaly(
        name="extreme_total",
        column="order_total",
        multiplier_range=(10.0, 50.0),  # 10-50x the column mean
        fraction=0.05,                  # 5% of events
    )
])
```

### Contextual Anomalies -- Values Anomalous in Context

Use for business logic violations or impossible state combinations:

```python
registry = AnomalyRegistry([
    ContextualAnomaly(
        name="cancelled_delivered",
        column="status",
        condition_column="status",
        normal_values=["cancelled"],
        anomalous_values=["delivered", "completed"],
        fraction=0.40,
    )
])
```

### Collective Anomalies -- Clustered Event Bursts

Use for account takeover, coordinated fraud, or bot traffic:

```python
registry = AnomalyRegistry([
    CollectiveAnomaly(
        name="velocity_fraud",
        group_column="customer_id",
        timestamp_column="order_date",
        window_seconds=300,     # compress group into 5-minute window
        fraction=0.05,
    )
])
```

### Combined Registry

Mix anomaly types in a single registry for realistic multi-fault scenarios:

```python
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

result = SpindleStreamer(
    domain=RetailDomain(),
    sink=FileSink("./stream_demo/combined.jsonl", mode="w"),
    config=StreamConfig(max_events=300, realtime=False),
    anomaly_registry=combined_registry,
    scale="fabric_demo",
    seed=42,
).stream("order")
```

### Disable Labels for Production-Like Streams

For testing anomaly detection models where you want unlabeled data:

```python
config = StreamConfig(max_events=50, realtime=False, label_anomalies=False)
```

With `label_anomalies=False`, anomalies are still injected but the `_spindle_is_anomaly` and `_spindle_anomaly_type` fields are omitted from the output.

## Step 7 -- Stream from Pre-Generated Tables

If you have already generated data in batch mode, you can stream from those tables without re-generating:

```python
from sqllocks_spindle import Spindle

batch = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

for table_name in ["order", "order_line", "return"]:
    result = SpindleStreamer(
        tables=batch.tables,           # reuse pre-generated data
        sink=FileSink(f"./stream_demo/{table_name}.jsonl", mode="w"),
        config=StreamConfig(max_events=30, realtime=False),
    ).stream(table_name)
    print(f"  {table_name:<12} {result.events_sent:>3} events")
```

---

> **Run It Yourself**
>
> - Notebook: [`T12_streaming_events.ipynb`](../../../examples/notebooks/intermediate/T12_streaming_events.ipynb)
> - Scripts:
>   - [`08_streaming_basics.py`](../../../examples/scenarios/08_streaming_basics.py) -- ConsoleSink, FileSink, pre-generated tables
>   - [`09_streaming_realtime.py`](../../../examples/scenarios/09_streaming_realtime.py) -- rate limiting, burst windows, time patterns, OOO events
>   - [`10_streaming_anomalies.py`](../../../examples/scenarios/10_streaming_anomalies.py) -- point, contextual, and collective anomalies

## Related

- [Streaming Guide](../../guides/streaming.md) -- full reference for sinks, configs, time patterns, and anomaly types

## Next Step

Continue to [Tutorial 07: Chaos Engineering](07-chaos-engineering.md) to learn how to inject schema drift, value corruption, and referential breakage into your data.
