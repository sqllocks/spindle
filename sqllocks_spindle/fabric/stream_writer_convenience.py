"""High-level convenience wrapper for streaming Spindle data to Fabric Eventstream.

Wraps :class:`SpindleStreamer` + :class:`EventstreamClient` into a single
class with sensible defaults, designed for use in Fabric Notebooks.

Requires the ``streaming`` extra::

    pip install sqllocks-spindle[streaming]

Usage::

    from sqllocks_spindle.fabric import FabricStreamWriter

    writer = FabricStreamWriter(
        connection_string="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...",
        domain="retail",
        table="order",
    )
    result = writer.stream(max_events=1000, rate=100.0)
    print(f"Streamed {result.events_sent:,} events in {result.elapsed_seconds:.1f}s")
"""

from __future__ import annotations

from typing import Any


class FabricStreamWriter:
    """Stream Spindle-generated events to Fabric Eventstream or Event Hub.

    Combines domain generation, streaming engine, anomaly injection, and
    the EventstreamClient sink into a single high-level API call.

    Args:
        connection_string: Event Hub-compatible connection string for the
            Eventstream custom endpoint.
        domain: Domain name (e.g. ``"retail"``, ``"healthcare"``).
        table: Table/entity name to stream (e.g. ``"order"``).
        scale: Scale preset for data generation (default ``"small"``).
        eventhub_name: Optional Event Hub name (if not in connection string).
        partition_key_column: Column to partition events by for ordered delivery.

    Example (Fabric Notebook)::

        writer = FabricStreamWriter(
            connection_string="Endpoint=sb://...",
            domain="retail",
            table="order",
            scale="small",
        )

        # Stream 5,000 events at 200 eps with 5% anomalies
        result = writer.stream(max_events=5000, rate=200.0, anomaly_rate=0.05)
        print(result)
    """

    def __init__(
        self,
        connection_string: str,
        domain: str = "retail",
        table: str = "order",
        scale: str = "small",
        eventhub_name: str | None = None,
        partition_key_column: str | None = None,
    ) -> None:
        self._connection_string = connection_string
        self._domain_name = domain
        self._table = table
        self._scale = scale
        self._eventhub_name = eventhub_name
        self._partition_key_column = partition_key_column

    def stream(
        self,
        max_events: int = 1000,
        rate: float = 100.0,
        anomaly_rate: float = 0.05,
        burst: str | None = None,
        out_of_order: float = 0.0,
        seed: int = 42,
        realtime: bool = True,
    ) -> Any:
        """Stream events to the configured Eventstream endpoint.

        Args:
            max_events: Total events to stream.
            rate: Baseline rate in events per second.
            anomaly_rate: Fraction of events to flag as anomalous (0.0-1.0).
            burst: Optional burst spec as ``"start_sec:duration_sec:multiplier"``
                (e.g. ``"30:60:10"`` = at 30s, burst for 60s at 10x rate).
            out_of_order: Fraction of events to deliberately reorder (0.0-1.0).
            seed: Random seed for reproducibility.
            realtime: Rate-limit output to the target rate (default True).

        Returns:
            :class:`~sqllocks_spindle.streaming.streamer.StreamResult` with
            event counts, anomaly stats, and timing.
        """
        from sqllocks_spindle.fabric.eventstream_client import EventstreamClient
        from sqllocks_spindle.streaming import (
            AnomalyRegistry,
            BurstWindow,
            PointAnomaly,
            SpindleStreamer,
            StreamConfig,
        )

        # Resolve the domain
        domain = self._resolve_domain(self._domain_name)

        # Build sink
        sink = EventstreamClient(
            connection_string=self._connection_string,
            eventhub_name=self._eventhub_name,
            partition_key_column=self._partition_key_column,
        )

        # Build burst windows
        burst_windows = []
        if burst:
            parts = burst.split(":")
            if len(parts) == 3:
                burst_windows.append(BurstWindow(
                    start_offset_seconds=float(parts[0]),
                    duration_seconds=float(parts[1]),
                    multiplier=float(parts[2]),
                ))

        # Build config
        config = StreamConfig(
            events_per_second=rate,
            max_events=max_events,
            out_of_order_fraction=out_of_order,
            burst_windows=burst_windows,
            realtime=realtime,
        )

        # Build anomaly registry
        anomaly_registry = AnomalyRegistry()
        if anomaly_rate > 0:
            anomaly_registry.add(
                PointAnomaly("auto", column="_auto_", fraction=anomaly_rate)
            )

        # Stream
        streamer = SpindleStreamer(
            domain=domain,
            sink=sink,
            config=config,
            anomaly_registry=anomaly_registry,
            scale=self._scale,
            seed=seed,
        )

        try:
            result = streamer.stream(self._table)
        finally:
            sink.close()

        return result

    @staticmethod
    def _resolve_domain(domain_name: str):
        """Resolve a domain name to a Domain instance."""
        import importlib
        import pkgutil

        import sqllocks_spindle.domains as _pkg
        from sqllocks_spindle.domains.base import Domain

        for _, mod_name, is_pkg in pkgutil.iter_modules(_pkg.__path__, _pkg.__name__ + "."):
            if not is_pkg:
                continue
            try:
                module = importlib.import_module(mod_name)
            except Exception:
                continue
            for attr in getattr(module, "__all__", dir(module)):
                cls = getattr(module, attr, None)
                if (
                    isinstance(cls, type)
                    and issubclass(cls, Domain)
                    and cls is not Domain
                ):
                    instance = cls.__new__(cls)
                    name = cls.name.fget(instance)  # type: ignore[attr-defined]
                    if name == domain_name:
                        return cls(schema_mode="3nf")

        raise ValueError(f"Unknown domain: '{domain_name}'")
