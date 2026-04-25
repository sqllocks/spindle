"""
Scenario 22 -- Fabric Integration
===================================
Use OneLakePaths, LakehouseFilesWriter, and EventstreamClient to write
generated data directly to Microsoft Fabric infrastructure.

OneLakePaths:       Resolves canonical Lakehouse paths (auto-detects Fabric vs. local)
LakehouseFilesWriter: Writes partitioned files, manifests, and done flags
EventstreamClient:  Sends events to a Fabric Eventstream (requires [streaming] extra)

NOTE: The EventstreamClient section at the end requires a real Azure Event Hub
connection string. It is shown as a blueprint — replace the connection string
with your Eventstream custom endpoint before running in Fabric.

Run (locally, dry layout demo):
    python examples/scenarios/22_fabric_integration.py
"""

import json
from pathlib import Path

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter
from sqllocks_spindle.fabric.onelake_paths import OneLakePaths

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
orders    = result["order"]
customers = result["customer"]
print(f"Generated: {len(orders):,} orders, {len(customers):,} customers")

# ── 1. OneLakePaths — path resolution ─────────────────────────────────────────
print("\n── 1. OneLakePaths ──")

# Local mode: auto-detects that /lakehouse/default doesn't exist,
# falls back to ./lakehouse_files as the base
paths = OneLakePaths(base_path="./demo_lakehouse")

# Landing zone partition (daily)
lz = paths.landing_zone_path("retail", "order", "2025-01-15")
print(f"  Landing zone:  {lz}")

# With hour partition
lz_hourly = paths.landing_zone_path("retail", "order", "2025-01-15", hour=10)
print(f"  Landing (hourly): {lz_hourly}")

# Manifest and done flag
manifest = paths.manifest_path("retail", "order", "2025-01-15")
done_flag = paths.done_flag_path("retail", "order", "2025-01-15")
print(f"  Manifest:      {manifest}")
print(f"  Done flag:     {done_flag}")

# Tables path (Delta table location)
tables_path = paths.tables_path("retail_order")
print(f"  Tables path:   {tables_path}")

# Quarantine path
quarantine = paths.quarantine_path("retail", run_id="run_abc123")
print(f"  Quarantine:    {quarantine}")

# Control path
control = paths.control_path("retail", "order")
print(f"  Control:       {control}")

# ── 2. LakehouseFilesWriter — write partitions ────────────────────────────────
print("\n── 2. LakehouseFilesWriter — write partitions ──")

writer = LakehouseFilesWriter(
    base_path="./demo_lakehouse",
    default_format="parquet",
)

# Write orders partition
partition_dir = writer.paths.landing_zone_path("retail", "order", "2025-01-15")
written = writer.write_partition(
    df=orders,
    path=partition_dir,
    format="parquet",
)
print(f"  Parquet written:  {written}")

# Write same partition as CSV
written_csv = writer.write_partition(
    df=orders,
    path=partition_dir,
    format="csv",
)
print(f"  CSV written:      {written_csv}")

# Write customer partition
cust_dir = writer.paths.landing_zone_path("retail", "customer", "2025-01-15")
writer.write_partition(df=customers, path=cust_dir, format="parquet")
print(f"  Customer written: {cust_dir}")

# ── 3. Write manifest + done flag ─────────────────────────────────────────────
print("\n── 3. Write manifest + done flag ──")

manifest_dict = {
    "entity":     "order",
    "domain":     "retail",
    "date":       "2025-01-15",
    "file_count": 1,
    "row_count":  len(orders),
    "formats":    ["parquet", "csv"],
    "run_id":     "run_20250115_001",
}

manifest_path = writer.write_manifest(
    manifest_dict,
    writer.paths.manifest_path("retail", "order", "2025-01-15"),
)
print(f"  Manifest: {manifest_path}")

done_flag_path = writer.write_done_flag(
    writer.paths.done_flag_path("retail", "order", "2025-01-15"),
)
print(f"  Done flag: {done_flag_path}")

# Verify manifest contents
with open(manifest_path) as f:
    saved = json.load(f)
print(f"  Manifest row_count: {saved['row_count']:,}")

# ── 4. Multi-day write loop ───────────────────────────────────────────────────
print("\n── 4. Multi-day write loop (Jan 1–3) ──")

for dt in ["2025-01-01", "2025-01-02", "2025-01-03"]:
    # Slice a subset of orders for each "day" (simulated)
    day_orders = orders.head(max(1, len(orders) // 3))

    p = writer.paths.landing_zone_path("retail", "order", dt)
    writer.write_partition(df=day_orders, path=p, format="parquet")

    writer.write_manifest(
        {"entity": "order", "date": dt, "row_count": len(day_orders)},
        writer.paths.manifest_path("retail", "order", dt),
    )
    writer.write_done_flag(writer.paths.done_flag_path("retail", "order", dt))
    print(f"  {dt}: {len(day_orders):,} rows written")

# ── 5. EventstreamClient blueprint ────────────────────────────────────────────
print("\n── 5. EventstreamClient (Fabric Eventstream) ──")
print("   Requires: pip install sqllocks-spindle[streaming]")
print("   Replace the connection_string below with your Eventstream custom endpoint.\n")

BLUEPRINT = '''
from sqllocks_spindle.fabric.eventstream_client import EventstreamClient
from sqllocks_spindle.simulation.stream_emit import StreamEmitter, StreamEmitConfig
from sqllocks_spindle import Spindle, RetailDomain

gen_result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)

# Fabric Eventstream uses Event Hub-compatible connection strings.
# Find yours in: Fabric workspace -> Eventstream -> Custom endpoint -> Keys
client = EventstreamClient(
    connection_string="Endpoint=sb://eh-<name>.servicebus.windows.net/;"
                      "SharedAccessKeyName=RootManageSharedAccessKey;"
                      "SharedAccessKey=<your-key>",
    eventhub_name="retail-orders",
    partition_key_column="customer_id",  # partition by customer for ordering
    max_batch_size=500,
    max_retries=3,
)

result = StreamEmitter(
    tables=gen_result.tables,
    config=StreamEmitConfig(
        rate_per_sec=100,
        max_events=5000,
        sink_type="eventstream",
        topics=["retail.orders"],
        envelope_schema_version="1.0",
    ),
    sink=client,
).emit()

print(f"Sent {result.events_sent:,} events to Fabric Eventstream")
client.close()
'''

print(BLUEPRINT)
print(f"\nOutput written to: {Path('./demo_lakehouse').resolve()}")
print("Run `find ./demo_lakehouse -type f` to see the full partition layout.")
