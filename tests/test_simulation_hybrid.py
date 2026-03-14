"""Tests for HybridSimulator."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.file_drop import FileDropConfig
from sqllocks_spindle.simulation.hybrid import HybridConfig, HybridResult, HybridSimulator
from sqllocks_spindle.simulation.stream_emit import StreamEmitConfig, StreamEmitResult
from sqllocks_spindle.streaming.stream_writer import StreamWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_tables():
    n = 30
    rng = np.random.default_rng(1)
    return {
        "order": pd.DataFrame({
            "order_id": range(1, n + 1),
            "customer_id": rng.integers(1, 10, size=n),
            "total_amount": rng.uniform(10.0, 200.0, size=n).round(2),
        }),
        "customer": pd.DataFrame({
            "customer_id": range(1, 11),
            "name": [f"Customer {i}" for i in range(1, 11)],
        }),
    }


class CaptureSink(StreamWriter):
    def __init__(self):
        self.sent: list[dict[str, Any]] = []

    def send(self, event: dict[str, Any]) -> None:
        self.sent.append(event)

    def send_batch(self, events: list[dict[str, Any]]) -> None:
        self.sent.extend(events)


@pytest.fixture
def file_drop_cfg(tmp_path):
    return FileDropConfig(
        domain="retail",
        base_path=str(tmp_path / "landing"),
        cadence="daily",
        date_range_start="2024-01-01",
        date_range_end="2024-01-03",
        formats=["parquet"],
        manifest_enabled=False,
        done_flag_enabled=False,
        lateness_enabled=False,
        seed=42,
    )


# ---------------------------------------------------------------------------
# HybridConfig defaults
# ---------------------------------------------------------------------------

class TestHybridConfig:
    def test_defaults(self):
        cfg = HybridConfig()
        assert cfg.concurrent is False
        assert cfg.link_strategy == "correlation_id"
        assert cfg.stream_to == "eventhouse"
        assert cfg.micro_batch_to == "lakehouse_files"


# ---------------------------------------------------------------------------
# HybridSimulator — sequential run
# ---------------------------------------------------------------------------

class TestHybridSimulatorSequential:
    def test_run_returns_hybrid_result(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(
            file_drop_config=file_drop_cfg,
            stream_config=StreamEmitConfig(),
            concurrent=False,
        )
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert isinstance(result, HybridResult)

    def test_correlation_id_set(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(file_drop_config=file_drop_cfg, stream_config=StreamEmitConfig())
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.correlation_id != ""

    def test_correlation_id_stamped_in_tables(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(
            file_drop_config=file_drop_cfg,
            stream_config=StreamEmitConfig(),
            link_strategy="correlation_id",
        )
        sim = HybridSimulator(tables=simple_tables, config=cfg, sink=sink)
        result = sim.run()
        # Check that events contain the correlation id column in data
        if sink.sent:
            event = sink.sent[0]
            data = event.get("data", {})
            assert "_correlation_id" in data

    def test_link_strategy_in_result(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(file_drop_config=file_drop_cfg, stream_config=StreamEmitConfig())
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.link_strategy == "correlation_id"

    def test_file_drop_result_present(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(file_drop_config=file_drop_cfg, stream_config=StreamEmitConfig())
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.file_drop_result is not None
        assert len(result.file_drop_result.files_written) > 0

    def test_stream_result_events_emitted(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(file_drop_config=file_drop_cfg, stream_config=StreamEmitConfig())
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.stream_result is not None
        assert result.stream_result.events_sent > 0

    def test_repr_no_raise(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(file_drop_config=file_drop_cfg, stream_config=StreamEmitConfig())
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert isinstance(repr(result), str)


# ---------------------------------------------------------------------------
# HybridSimulator — concurrent run
# ---------------------------------------------------------------------------

class TestHybridSimulatorConcurrent:
    def test_concurrent_run_returns_result(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(
            file_drop_config=file_drop_cfg,
            stream_config=StreamEmitConfig(),
            concurrent=True,
        )
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert isinstance(result, HybridResult)
        assert result.file_drop_result is not None
        assert result.stream_result is not None


# ---------------------------------------------------------------------------
# HybridSimulator — empty table routing
# ---------------------------------------------------------------------------

class TestHybridSimulatorTableRouting:
    def test_empty_batch_tables_makes_none(self, simple_tables, file_drop_cfg):
        """When batch_tables is set to a nonexistent table, batch result is None."""
        sink = CaptureSink()
        cfg = HybridConfig(
            batch_tables=["nonexistent_table"],
            file_drop_config=file_drop_cfg,
            stream_config=StreamEmitConfig(),
        )
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.file_drop_result is None

    def test_empty_stream_tables_makes_none(self, simple_tables, file_drop_cfg):
        sink = CaptureSink()
        cfg = HybridConfig(
            stream_tables=["nonexistent_table"],
            file_drop_config=file_drop_cfg,
            stream_config=StreamEmitConfig(),
        )
        result = HybridSimulator(tables=simple_tables, config=cfg, sink=sink).run()
        assert result.stream_result is None
