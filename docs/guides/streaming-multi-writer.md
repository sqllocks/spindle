# StreamingMultiWriter

`StreamingMultiWriter` fans out a `generate_stream()` iterator to multiple `StreamWriter` sinks in parallel using `ThreadPoolExecutor`.

## Quick Start

```python
from sqllocks_spindle import Spindle, StreamingMultiWriter
from sqllocks_spindle.streaming import ConsoleSink, FileSink

spindle = Spindle()

smw = StreamingMultiWriter(
    console=ConsoleSink(),
    file=FileSink("events.jsonl"),
)

result = smw.stream(
    spindle.generate_stream(domain=RetailDomain(), scale="small", seed=42)
)
print(result.summary())
```

## Using Real Sinks

### EventHub + Kafka + Console + File (all 4)

```python
from sqllocks_spindle.streaming import ConsoleSink, FileSink, EventHubSink, KafkaSink

smw = StreamingMultiWriter(
    console=ConsoleSink(),
    file=FileSink("events.jsonl"),
    eventhub=EventHubSink(connection_str="Endpoint=sb://..."),
    kafka=KafkaSink(bootstrap_servers="localhost:9092", topic="spindle"),
    batch_size=100,
    max_workers=4,
)
result = smw.stream(spindle.generate_stream(domain=domain, scale="large"))
print(result.summary())
```

## Error Handling

By default, a failing sink does **not** abort streaming. Its errors are captured in `SinkResult.errors` and the stream continues to all other sinks.

```python
smw = StreamingMultiWriter(
    good=ConsoleSink(),
    bad=some_broken_sink,
    stop_on_sink_error=False,  # default
)
result = smw.stream(generator)
print(result.partial_failure)  # True if some sinks failed
```

## Dynamic Sink Management

```python
smw = StreamingMultiWriter(primary=FileSink("out.jsonl"))
smw.add_sink("secondary", ConsoleSink())
smw.remove_sink("primary")
```

## Stream a Single Table

```python
success_map = smw.stream_table("orders", orders_df)
# Returns: {"sink_name": True/False, ...}
```

## Result Fields

| Field | Type | Description |
|-------|------|-------------|
| `success` | bool | All sinks succeeded |
| `partial_failure` | bool | Some sinks succeeded, some failed |
| `total_tables` | int | Number of table batches streamed |
| `elapsed_seconds` | float | Wall-clock time |
| `sinks` | list[SinkResult] | Per-sink stats |
