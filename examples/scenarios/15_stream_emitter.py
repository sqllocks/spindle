"""
Scenario 15 -- Stream Emitter
==============================
Emit rows as CloudEvents-style envelopes. Covers rate limiting, jitter,
out-of-order events, replay windows (re-delivery simulation), topic mapping,
console sink, and file sink.

The StreamEmitter wraps every row in an EventEnvelope (schema_version,
event_type, event_time, correlation_id, payload, metadata) before sending
to the configured sink.

Run:
    python examples/scenarios/15_stream_emitter.py
"""

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.simulation.stream_emit import StreamEmitConfig, StreamEmitter

# ── Base data ────────────────────────────────────────────────────────────────
result = Spindle().generate(domain=RetailDomain(), scale="fabric_demo", seed=42)

# ── 1. Console sink — quick preview ──────────────────────────────────────────
print("── 1. Console sink (first 10 events) ──")

cfg_console = StreamEmitConfig(
    rate_per_sec=1000,   # max speed (realtime=False means no actual sleeping)
    max_events=10,
    sink_type="console",
    topics=["retail.orders"],
    envelope_schema_version="1.0",
    envelope_source="spindle_demo",
    seed=42,
)

r = StreamEmitter(tables={"order": result["order"]}, config=cfg_console).emit()
print(f"\n  Events sent: {r.events_sent}")
print(f"  Topics:      {r.topics_used}")
print(f"  Elapsed:     {r.elapsed_seconds:.3f}s")

# ── 2. File sink — JSONL output ────────────────────────────────────────────────
print("\n── 2. File sink → events.jsonl ──")

cfg_file = StreamEmitConfig(
    rate_per_sec=5000,
    max_events=500,
    sink_type="file",
    sink_connection={"path": "./events.jsonl"},
    topics=["retail.orders"],
    seed=42,
)

r2 = StreamEmitter(tables={"order": result["order"]}, config=cfg_file).emit()
print(f"  Events written: {r2.events_sent:,}")
print(f"  Output:         ./events.jsonl")

# ── 3. Jitter + out-of-order ─────────────────────────────────────────────────
print("\n── 3. Jitter + out-of-order events ──")

cfg_ooo = StreamEmitConfig(
    rate_per_sec=5000,
    jitter_ms=25,                   # ±25ms timing jitter per event
    out_of_order_probability=0.10,  # 10% of events arrive before their neighbours
    max_events=200,
    sink_type="console",
    seed=42,
)

r3 = StreamEmitter(tables={"order": result["order"]}, config=cfg_ooo).emit()
print(f"  Events: {r3.events_sent}")
print(f"  (10% were shuffled out of order in the emit buffer)")

# ── 4. Replay window — simulating re-delivery ─────────────────────────────────
print("\n── 4. Replay window (simulates at-least-once re-delivery) ──")

cfg_replay = StreamEmitConfig(
    rate_per_sec=5000,
    replay_enabled=True,
    replay_window_minutes=5,   # keep a 5-minute sliding re-delivery buffer
    replay_probability=0.15,   # 15% chance to re-emit a cached event
    replay_burst_size=5,       # re-emit up to 5 events per burst
    max_events=300,
    sink_type="console",
    seed=99,
)

r4 = StreamEmitter(tables={"order": result["order"]}, config=cfg_replay).emit()
print(f"  Primary events:  {r4.events_sent:,}")
print(f"  Replayed events: {r4.replay_events_sent:,}")
print(f"  Total emitted:   {r4.total_events:,}")

# ── 5. Multi-table with explicit topic mapping ────────────────────────────────
print("\n── 5. Multi-table emit with custom topic names ──")

cfg_multi = StreamEmitConfig(
    rate_per_sec=5000,
    max_events=400,
    topics=["retail.orders", "retail.returns"],  # one topic per table
    sink_type="console",
    seed=42,
)

r5 = StreamEmitter(
    tables={"order": result["order"], "return": result.tables.get("return", result["order"].head(20))},
    config=cfg_multi,
).emit()
print(f"  Topics used:  {r5.topics_used}")
print(f"  Total events: {r5.events_sent:,}")

# ── 6. Summary ────────────────────────────────────────────────────────────────
print("\n── Summary ──")
print(f"  {'Scenario':<35} {'Events':>8}")
print(f"  {'-'*45}")
for label, res in [
    ("Console (10 events)", r),
    ("File sink (500 events)", r2),
    ("Jitter + OOO (200 events)", r3),
    ("Replay window (300 primary)", r4),
    ("Multi-table (400 events)", r5),
]:
    print(f"  {label:<35} {res.events_sent:>8,}")
