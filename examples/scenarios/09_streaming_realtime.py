"""
Scenario 09 -- Realtime Streaming (Rate Limiting, Bursts, Time Patterns)
=========================================================================
By default Spindle emits events as fast as possible (realtime=False).
Set realtime=True to enable:
  - Token-bucket rate limiter (smooth throughput at target events/sec)
  - Poisson inter-arrival times (statistically realistic spacing)
  - Burst windows (simulate flash sales, end-of-month spikes)
  - Time-of-day patterns (business hours, retail evenings)
  - Out-of-order events (simulate late-arriving data)

Run:
    python examples/scenarios/09_streaming_realtime.py

Note: realtime=True will run for a few seconds. Adjust max_events to
      control runtime (max_events / events_per_second ≈ runtime in seconds).
"""

import tempfile
from pathlib import Path

from sqllocks_spindle import RetailDomain
from sqllocks_spindle.streaming import (
    SpindleStreamer,
    StreamConfig,
    BurstWindow,
    TimePattern,
    FileSink,
    ConsoleSink,
)

# ------------------------------------------------------------------
# 1. Basic rate limiting -- emit at a controlled events/sec
# ------------------------------------------------------------------
print("=== Rate-limited stream (20 events/sec, 40 events total) ===")
with tempfile.TemporaryDirectory() as tmp:
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(Path(tmp) / "orders.jsonl"), mode="w"),
        config=StreamConfig(
            events_per_second=20.0,
            max_events=40,
            realtime=True,              # enable rate limiting + Poisson inter-arrivals
        ),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"  Events:    {result.events_sent}")
    print(f"  Elapsed:   {result.elapsed_seconds:.2f}s")
    print(f"  Actual:    {result.events_per_second_actual:.1f} events/s  (target: 20)")

# ------------------------------------------------------------------
# 2. Burst windows -- simulate a traffic spike
#    E.g., a flash sale starts at t=1s and runs for 2 seconds at 5x rate
# ------------------------------------------------------------------
print("\n=== Burst window (5x spike at t=1s for 2s) ===")
with tempfile.TemporaryDirectory() as tmp:
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(Path(tmp) / "burst.jsonl"), mode="w"),
        config=StreamConfig(
            events_per_second=10.0,
            max_events=80,
            realtime=True,
            burst_windows=[
                BurstWindow(
                    start_offset_seconds=1.0,
                    duration_seconds=2.0,
                    multiplier=5.0,     # 50 events/sec during burst
                )
            ],
        ),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"  Events:  {result.events_sent}")
    print(f"  Elapsed: {result.elapsed_seconds:.2f}s")
    print(f"  Actual:  {result.events_per_second_actual:.1f} events/s  "
          f"(burst inflated total)")

# ------------------------------------------------------------------
# 3. Multiple burst windows -- simulate Black Friday with multiple spikes
# ------------------------------------------------------------------
print("\n=== Multiple burst windows ===")
config = StreamConfig(
    events_per_second=5.0,
    max_events=60,
    realtime=True,
    burst_windows=[
        BurstWindow(start_offset_seconds=0.5, duration_seconds=1.0, multiplier=4.0),
        BurstWindow(start_offset_seconds=3.0, duration_seconds=1.5, multiplier=6.0),
    ],
)
print(f"  Burst 1: t=0.5s-1.5s at 4x ({5.0 * 4:.0f} events/sec)")
print(f"  Burst 2: t=3.0s-4.5s at 6x ({5.0 * 6:.0f} events/sec)")

with tempfile.TemporaryDirectory() as tmp:
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(Path(tmp) / "multi_burst.jsonl"), mode="w"),
        config=config,
        scale="fabric_demo",
        seed=42,
    ).stream("order")
    print(f"  Result: {result}")

# ------------------------------------------------------------------
# 4. Time-of-day patterns
#    business_hours() -- high 8am-6pm weekdays, low overnight + weekends
#    retail_peak()    -- high 7pm-10pm, elevated weekends
# ------------------------------------------------------------------
print("\n=== Time patterns (fast mode -- realtime=False, patterns shown for reference) ===")

biz_pattern = TimePattern.business_hours()
retail_pattern = TimePattern.retail_peak()

# Show sample multipliers
print(f"  business_hours @ 9am Monday:   {biz_pattern.get_multiplier(9, 0):.1f}x")
print(f"  business_hours @ 2am Monday:   {biz_pattern.get_multiplier(2, 0):.1f}x")
print(f"  business_hours @ 9am Saturday: {biz_pattern.get_multiplier(9, 5):.2f}x")

print(f"  retail_peak @ 8pm Saturday:    {retail_pattern.get_multiplier(20, 5):.1f}x")
print(f"  retail_peak @ 3am Sunday:      {retail_pattern.get_multiplier(3, 6):.1f}x")

# ------------------------------------------------------------------
# 5. Out-of-order events -- simulate late-arriving data
# ------------------------------------------------------------------
print("\n=== Out-of-order events (20% OOO fraction) ===")
with tempfile.TemporaryDirectory() as tmp:
    result = SpindleStreamer(
        domain=RetailDomain(),
        sink=FileSink(str(Path(tmp) / "ooo.jsonl"), mode="w"),
        config=StreamConfig(
            max_events=50,
            realtime=False,
            out_of_order_fraction=0.20,         # 20% of events arrive out of order
            out_of_order_max_delay_slots=5,     # reordered by up to 5 positions
        ),
        scale="fabric_demo",
        seed=42,
    ).stream("order")

    print(f"  Events:        {result.events_sent}")
    print(f"  OOO events:    {result.out_of_order_count}")
    print(f"  OOO fraction:  {result.out_of_order_count / result.events_sent:.1%}")
