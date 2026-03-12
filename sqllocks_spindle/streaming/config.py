"""Streaming configuration dataclasses for Spindle Phase 2."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BurstWindow:
    """A time window where event rate is multiplied by a factor.

    Args:
        start_offset_seconds: Seconds from stream start when the burst begins.
        duration_seconds: How long the burst lasts.
        multiplier: Rate multiplier during the burst (e.g. 10.0 = 10x normal rate).

    Example::

        # 10x rate burst starting at t=30s, lasting 60 seconds
        BurstWindow(start_offset_seconds=30, duration_seconds=60, multiplier=10.0)
    """

    start_offset_seconds: float
    duration_seconds: float
    multiplier: float

    def is_active(self, elapsed: float) -> bool:
        """Return True if the burst window is active at the given elapsed time."""
        end = self.start_offset_seconds + self.duration_seconds
        return self.start_offset_seconds <= elapsed < end


@dataclass
class TimePattern:
    """Time-of-day and day-of-week rate multipliers.

    Applied on top of the base rate when simulating real-world temporal patterns.
    E.g., e-commerce has high traffic 7pm–10pm and lower traffic at 3am.

    Args:
        hour_multipliers: Mapping of hour (0–23) to multiplier.
        dow_multipliers: Mapping of day-of-week (0=Monday … 6=Sunday) to multiplier.
    """

    hour_multipliers: dict[int, float] = field(default_factory=dict)
    dow_multipliers: dict[int, float] = field(default_factory=dict)

    def get_multiplier(self, hour: int, dow: int) -> float:
        """Return the composite multiplier for a given hour and day-of-week."""
        h = self.hour_multipliers.get(hour, 1.0)
        d = self.dow_multipliers.get(dow, 1.0)
        return h * d

    @classmethod
    def business_hours(cls) -> "TimePattern":
        """Higher traffic 8am–6pm weekdays, very low on weekends."""
        hour_mults = {h: 0.3 for h in range(24)}
        for h in range(8, 18):
            hour_mults[h] = 1.5
        for h in range(18, 21):
            hour_mults[h] = 1.0
        return cls(
            hour_multipliers=hour_mults,
            dow_multipliers={5: 0.4, 6: 0.2},
        )

    @classmethod
    def retail_peak(cls) -> "TimePattern":
        """E-commerce pattern: evenings peak, weekends elevated."""
        hour_mults = {h: 0.4 for h in range(24)}
        for h in range(19, 23):
            hour_mults[h] = 2.0
        for h in range(12, 15):
            hour_mults[h] = 1.2
        return cls(
            hour_multipliers=hour_mults,
            dow_multipliers={5: 1.5, 6: 1.8},
        )


@dataclass
class StreamConfig:
    """Configuration for a Spindle streaming run.

    Args:
        events_per_second: Base target throughput.
        duration_seconds: Stop after this many wall-clock seconds (``None`` = no limit).
        max_events: Stop after this many events (``None`` = no limit).
        out_of_order_fraction: Fraction of events to reorder (0.0–1.0).
        out_of_order_max_delay_slots: Maximum slot positions an OOO event is delayed.
        burst_windows: List of :class:`BurstWindow` definitions.
        time_pattern: Optional :class:`TimePattern` for time-of-day multipliers.
        label_anomalies: Keep ``_spindle_is_anomaly`` / ``_spindle_anomaly_type``
            columns in emitted events (default ``True``).
        batch_size: Events per :meth:`StreamWriter.send_batch` call.
        realtime: If ``True``, rate-limit emissions using a token bucket + Poisson
            inter-arrival times.  If ``False`` (default), emit as fast as possible
            (useful for bulk loading and unit tests).
    """

    events_per_second: float = 10.0
    duration_seconds: float | None = None
    max_events: int | None = None
    out_of_order_fraction: float = 0.0
    out_of_order_max_delay_slots: int = 10
    burst_windows: list[BurstWindow] = field(default_factory=list)
    time_pattern: TimePattern | None = None
    label_anomalies: bool = True
    batch_size: int = 100
    realtime: bool = False

    def get_rate_multiplier(
        self,
        elapsed: float,
        wall_hour: int = 0,
        wall_dow: int = 0,
    ) -> float:
        """Return the composite rate multiplier at a given moment.

        Args:
            elapsed: Seconds since stream start.
            wall_hour: Current wall-clock hour (0–23).
            wall_dow: Current wall-clock day-of-week (0=Monday).
        """
        mult = 1.0

        for bw in self.burst_windows:
            if bw.is_active(elapsed):
                mult *= bw.multiplier

        if self.time_pattern:
            mult *= self.time_pattern.get_multiplier(wall_hour, wall_dow)

        return max(mult, 0.001)
