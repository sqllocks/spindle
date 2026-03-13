"""
Scenario 11 -- Event Hub & Kafka Sinks
=======================================
For production streaming to Azure Event Hubs (Fabric Eventstream) or Kafka,
install the streaming extras:

    pip install sqllocks-spindle[streaming]
    # installs: azure-eventhub>=5.11, kafka-python>=2.0

Fabric Eventstream setup:
  1. Create an Eventstream in your Fabric workspace
  2. Add a source -> Custom App
  3. Copy the connection string (Endpoint=sb://...)
  4. Paste it as EVENT_HUB_CONNECTION_STRING below

Kafka setup:
  1. Provide your broker address and topic name below
  2. Works with Azure Event Hubs Kafka endpoint, MSK, Confluent, etc.

Run (after pip install sqllocks-spindle[streaming]):
    python examples/scenarios/11_streaming_eventhub_kafka.py
"""

import os

# ------------------------------------------------------------------
# Configuration -- set these before running
# ------------------------------------------------------------------
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "SPINDLE_EVENTHUB_CONN_STR",
    "Endpoint=sb://YOUR_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=...",
)
EVENT_HUB_NAME = os.getenv("SPINDLE_EVENTHUB_NAME", "spindle-retail")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("SPINDLE_KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("SPINDLE_KAFKA_TOPIC", "spindle-retail-orders")

# ------------------------------------------------------------------
# Shared imports
# ------------------------------------------------------------------
from sqllocks_spindle import RetailDomain
from sqllocks_spindle.streaming import (
    SpindleStreamer,
    StreamConfig,
    BurstWindow,
    AnomalyRegistry,
    PointAnomaly,
)

def make_config(max_events: int = 500, rate: float = 100.0) -> StreamConfig:
    return StreamConfig(
        events_per_second=rate,
        max_events=max_events,
        realtime=True,
        burst_windows=[
            # Simulate a flash-sale burst at t=5s
            BurstWindow(start_offset_seconds=5.0, duration_seconds=3.0, multiplier=5.0),
        ],
        out_of_order_fraction=0.02,     # 2% late-arriving events
    )

# ------------------------------------------------------------------
# 1. EventHubSink -- Azure Event Hubs / Fabric Eventstream
# ------------------------------------------------------------------
def stream_to_eventhub():
    """Stream retail orders to Azure Event Hubs."""
    try:
        from sqllocks_spindle.streaming import EventHubSink
    except ImportError:
        print("EventHubSink requires: pip install sqllocks-spindle[streaming]")
        return

    if "YOUR_NAMESPACE" in EVENT_HUB_CONNECTION_STRING:
        print("EventHubSink: set EVENT_HUB_CONNECTION_STRING to run")
        return

    print(f"=== Streaming to Event Hub: {EVENT_HUB_NAME} ===")

    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=EventHubSink(
            connection_string=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME,
        ),
        config=make_config(max_events=500, rate=100.0),
        anomaly_registry=AnomalyRegistry([
            PointAnomaly(column="order_total", fraction=0.03, scale_factor=10.0),
        ]),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"Sent:      {result.events_sent:,} events")
    print(f"Anomalies: {result.anomaly_count}")
    print(f"OOO:       {result.out_of_order_count}")
    print(f"Elapsed:   {result.elapsed_seconds:.1f}s")
    print(f"Throughput:{result.events_per_second_actual:.0f} events/s")


# ------------------------------------------------------------------
# 2. KafkaSink -- Apache Kafka / Azure Event Hubs Kafka endpoint
# ------------------------------------------------------------------
def stream_to_kafka():
    """Stream retail orders to a Kafka topic."""
    try:
        from sqllocks_spindle.streaming import KafkaSink
    except ImportError:
        print("KafkaSink requires: pip install sqllocks-spindle[streaming]")
        return

    if KAFKA_BOOTSTRAP_SERVERS == "localhost:9092":
        print("KafkaSink: set SPINDLE_KAFKA_BROKERS and SPINDLE_KAFKA_TOPIC to run")
        return

    print(f"=== Streaming to Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> {KAFKA_TOPIC} ===")

    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=KafkaSink(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
        ),
        config=make_config(max_events=1000, rate=200.0),
        scale="small",
        seed=42,
    ).stream("order")

    print(f"Sent:      {result.events_sent:,} events")
    print(f"Elapsed:   {result.elapsed_seconds:.1f}s")
    print(f"Throughput:{result.events_per_second_actual:.0f} events/s")


# ------------------------------------------------------------------
# 3. Multi-table streaming -- run one streamer per table
#    In production, run each in a separate thread or process
# ------------------------------------------------------------------
def stream_multi_table_eventhub():
    """Stream multiple tables to separate Event Hub topics."""
    try:
        from sqllocks_spindle.streaming import EventHubSink
    except ImportError:
        print("EventHubSink requires: pip install sqllocks-spindle[streaming]")
        return

    if "YOUR_NAMESPACE" in EVENT_HUB_CONNECTION_STRING:
        print("Multi-table EventHub: set EVENT_HUB_CONNECTION_STRING to run")
        return

    from sqllocks_spindle import Spindle

    # Generate once, stream each table to its own Event Hub topic
    batch = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

    tables_to_stream = {
        "order":       "spindle-orders",
        "order_line":  "spindle-order-lines",
        "return":      "spindle-returns",
    }

    for table_name, hub_name in tables_to_stream.items():
        result = SpindleStreamer(
            tables=batch.tables,
            sink=EventHubSink(EVENT_HUB_CONNECTION_STRING, eventhub_name=hub_name),
            config=StreamConfig(events_per_second=50.0, realtime=True),
        ).stream(table_name)
        print(f"  {table_name:<12} -> {hub_name:<28} {result.events_sent:>4} events")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    stream_to_eventhub()
    stream_to_kafka()
    # stream_multi_table_eventhub()  # uncomment for multi-table example
