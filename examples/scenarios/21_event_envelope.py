"""
Scenario 21 -- EventEnvelope & EnvelopeFactory
===============================================
Wrap individual rows in CloudEvents-style envelopes for streaming.
The envelope adds schema versioning, event type routing, timestamps,
correlation IDs, and tenant context — all the metadata a downstream
consumer needs to deserialise, deduplicate, and route events.

Run:
    python examples/scenarios/21_event_envelope.py
"""

import json

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.streaming.envelope import EnvelopeFactory, EventEnvelope

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
orders  = result["order"]
returns = result.tables.get("return", orders.head(5))  # fallback if no return table

# ── 1. Create a single envelope ───────────────────────────────────────────────
print("── 1. Single envelope ──")

factory = EnvelopeFactory(default_tenant_id="acme-corp")
row = orders.iloc[0].to_dict()

envelope = factory.create_envelope(
    row_dict=row,
    table_name="order",
    event_type="retail.order.created",
    schema_version="1.0",
)

print(f"  schema_version: {envelope.schema_version}")
print(f"  event_type:     {envelope.event_type}")
print(f"  event_time:     {envelope.event_time}")
print(f"  correlation_id: {envelope.correlation_id}")
print(f"  tenant_id:      {envelope.tenant_id}")
print(f"  payload keys:   {list(envelope.payload.keys())[:5]}...")
print(f"  metadata:       {envelope.metadata}")

# ── 2. Serialise to JSON ──────────────────────────────────────────────────────
print("\n── 2. Serialise to JSON ──")

envelope_json = EnvelopeFactory.to_json(envelope)
parsed = json.loads(envelope_json)
print(f"  JSON keys: {list(parsed.keys())}")
print(f"  Payload sample: order_id={parsed['payload'].get('order_id')}")

# ── 3. Serialise to dict ─────────────────────────────────────────────────────
print("\n── 3. Serialise to dict ──")

envelope_dict = EnvelopeFactory.to_dict(envelope)
print(f"  Type: {type(envelope_dict)}")
print(f"  Keys: {list(envelope_dict.keys())}")

# ── 4. Per-row batch wrapping ─────────────────────────────────────────────────
print("\n── 4. Wrap first 5 orders as envelopes ──")

sample = orders.head(5)
envelopes = [
    factory.create_envelope(
        row_dict=row.to_dict(),
        table_name="order",
        event_type="retail.order.created",
        schema_version="1.0",
    )
    for _, row in sample.iterrows()
]

print(f"  Envelopes created: {len(envelopes)}")
# Verify all correlation IDs are unique
corr_ids = [e.correlation_id for e in envelopes]
print(f"  All correlation_ids unique: {len(set(corr_ids)) == len(corr_ids)}")

# ── 5. Custom event types per table ───────────────────────────────────────────
print("\n── 5. Custom event type per table ──")

EVENT_TYPE_MAP = {
    "order":  "retail.order.created",
    "return": "retail.return.initiated",
    "customer": "retail.customer.registered",
}

tables_to_wrap = {
    "order":    orders.head(3),
    "customer": result["customer"].head(3),
}

for table_name, df in tables_to_wrap.items():
    event_type = EVENT_TYPE_MAP.get(table_name, f"retail.{table_name}.event")
    wrapped = [
        factory.create_envelope(
            row_dict=row.to_dict(),
            table_name=table_name,
            event_type=event_type,
        )
        for _, row in df.iterrows()
    ]
    print(f"  {table_name:<12} -> {event_type:<35} ({len(wrapped)} envelopes)")

# ── 6. Custom metadata fields ────────────────────────────────────────────────
print("\n── 6. Envelope with extra metadata ──")

env_meta = factory.create_envelope(
    row_dict=orders.iloc[0].to_dict(),
    table_name="order",
    event_type="retail.order.created",
    schema_version="2.0",
    metadata={
        "source_system": "spindle_demo",
        "pipeline_run_id": "abc123",
        "environment": "dev",
    },
)

print(f"  schema_version: {env_meta.schema_version}")
print(f"  metadata:       {env_meta.metadata}")

# ── 7. Tenant override per envelope ──────────────────────────────────────────
print("\n── 7. Per-envelope tenant override ──")

env_t1 = factory.create_envelope(
    row_dict=orders.iloc[0].to_dict(),
    table_name="order",
    event_type="retail.order.created",
    tenant_id="tenant-north",
)
env_t2 = factory.create_envelope(
    row_dict=orders.iloc[1].to_dict(),
    table_name="order",
    event_type="retail.order.created",
    tenant_id="tenant-south",
)

print(f"  Event 1 tenant: {env_t1.tenant_id}")
print(f"  Event 2 tenant: {env_t2.tenant_id}")
print("\nDone.")
