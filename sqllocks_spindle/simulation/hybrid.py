"""Hybrid simulator — combines file-drop and stream emission concurrently."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from sqllocks_spindle.simulation.file_drop import (
    FileDropConfig,
    FileDropResult,
    FileDropSimulator,
)
from sqllocks_spindle.simulation.stream_emit import (
    StreamEmitConfig,
    StreamEmitResult,
    StreamEmitter,
)
from sqllocks_spindle.streaming.stream_writer import StreamWriter


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class HybridConfig:
    """Configuration for :class:`HybridSimulator`.

    Specifies which tables (or all) go to the streaming path, which go to the
    micro-batch file-drop path, and how they are linked.

    Args:
        stream_to: Streaming target — ``"eventhouse"``, ``"lakehouse"``, or
            ``"both"``.  Informational; the actual sink is determined by the
            embedded :attr:`stream_config`.
        micro_batch_to: Batch target — ``"lakehouse_files"`` (default).
        stream_tables: Tables routed to the stream path.  Empty = all.
        batch_tables: Tables routed to the file-drop path.  Empty = all.
        stream_config: Full :class:`StreamEmitConfig` for the streaming side.
        file_drop_config: Full :class:`FileDropConfig` for the batch side.
        link_strategy: How stream events and batch files are correlated.
            ``"correlation_id"`` (default) stamps every event and manifest with
            the same run-level id.  ``"natural_keys"`` relies on matching
            business keys across both paths.
        concurrent: Run stream and batch phases in parallel threads.
        seed: Random seed (propagated to child configs if they use defaults).
    """

    stream_to: str = "eventhouse"
    micro_batch_to: str = "lakehouse_files"
    stream_tables: list[str] = field(default_factory=list)
    batch_tables: list[str] = field(default_factory=list)
    stream_config: StreamEmitConfig = field(default_factory=StreamEmitConfig)
    file_drop_config: FileDropConfig = field(default_factory=FileDropConfig)
    link_strategy: str = "correlation_id"
    concurrent: bool = False
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class HybridResult:
    """Result of a :meth:`HybridSimulator.run` invocation.

    Attributes:
        file_drop_result: Result from the batch (file-drop) phase.
        stream_result: Result from the streaming phase.
        correlation_id: Run-level correlation id linking both outputs.
        link_strategy: The linking strategy that was applied.
    """

    file_drop_result: FileDropResult | None
    stream_result: StreamEmitResult | None
    correlation_id: str
    link_strategy: str

    def __repr__(self) -> str:
        batch = self.file_drop_result
        stream = self.stream_result
        batch_files = len(batch.files_written) if batch else 0
        stream_events = stream.total_events if stream else 0
        return (
            f"HybridResult(batch_files={batch_files}, stream_events={stream_events}, "
            f"link={self.link_strategy}, corr_id={self.correlation_id[:8]}...)"
        )


# ---------------------------------------------------------------------------
# HybridSimulator
# ---------------------------------------------------------------------------

class HybridSimulator:
    """Run file-drop and stream-emit simulations together, linked by correlation.

    The simulator:

    1. Splits the input tables into *batch tables* and *stream tables* based on
       :attr:`HybridConfig.batch_tables` / :attr:`HybridConfig.stream_tables`.
       By default all tables go to both paths.
    2. Stamps a shared ``correlation_id`` into every stream envelope and every
       batch manifest so downstream pipelines can join the two.
    3. Optionally runs both phases concurrently in threads.

    Args:
        tables: Pre-generated ``dict[table_name, DataFrame]``.
        config: :class:`HybridConfig`.
        sink: Optional explicit :class:`StreamWriter` for the streaming side.

    Example::

        from sqllocks_spindle.simulation import (
            HybridSimulator, HybridConfig, FileDropConfig, StreamEmitConfig,
        )

        cfg = HybridConfig(
            file_drop_config=FileDropConfig(
                domain="retail",
                date_range_start="2024-01-01",
                date_range_end="2024-01-07",
            ),
            stream_config=StreamEmitConfig(rate_per_sec=20),
            concurrent=True,
        )
        result = HybridSimulator(tables=gen_result.tables, config=cfg).run()
    """

    def __init__(
        self,
        tables: dict[str, pd.DataFrame],
        config: HybridConfig | None = None,
        sink: StreamWriter | None = None,
    ) -> None:
        self._tables = tables
        self._config = config or HybridConfig()
        self._sink = sink
        self._correlation_id = str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> HybridResult:
        """Execute both simulation phases and return combined results."""
        cfg = self._config
        batch_tables = self._select_tables(cfg.batch_tables)
        stream_tables = self._select_tables(cfg.stream_tables)

        # Inject correlation_id into DataFrames if using that strategy
        if cfg.link_strategy == "correlation_id":
            batch_tables = self._stamp_correlation(batch_tables)
            stream_tables = self._stamp_correlation(stream_tables)

        if cfg.concurrent:
            return self._run_concurrent(batch_tables, stream_tables)
        return self._run_sequential(batch_tables, stream_tables)

    # ------------------------------------------------------------------
    # Internal — table selection
    # ------------------------------------------------------------------

    def _select_tables(
        self,
        names: list[str],
    ) -> dict[str, pd.DataFrame]:
        """Return a subset of tables.  Empty *names* means all tables."""
        if not names:
            return {k: v.copy() for k, v in self._tables.items()}
        return {k: self._tables[k].copy() for k in names if k in self._tables}

    def _stamp_correlation(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """Add a ``_correlation_id`` column to every DataFrame."""
        for name, df in tables.items():
            df["_correlation_id"] = self._correlation_id
            tables[name] = df
        return tables

    # ------------------------------------------------------------------
    # Internal — execution modes
    # ------------------------------------------------------------------

    def _run_sequential(
        self,
        batch_tables: dict[str, pd.DataFrame],
        stream_tables: dict[str, pd.DataFrame],
    ) -> HybridResult:
        """Run batch first, then stream."""
        fd_result = self._run_file_drop(batch_tables)
        se_result = self._run_stream_emit(stream_tables)
        return HybridResult(
            file_drop_result=fd_result,
            stream_result=se_result,
            correlation_id=self._correlation_id,
            link_strategy=self._config.link_strategy,
        )

    def _run_concurrent(
        self,
        batch_tables: dict[str, pd.DataFrame],
        stream_tables: dict[str, pd.DataFrame],
    ) -> HybridResult:
        """Run batch and stream in parallel threads."""
        with ThreadPoolExecutor(max_workers=2) as pool:
            fd_future: Future[FileDropResult | None] = pool.submit(
                self._run_file_drop, batch_tables,
            )
            se_future: Future[StreamEmitResult | None] = pool.submit(
                self._run_stream_emit, stream_tables,
            )
            fd_result = fd_future.result()
            se_result = se_future.result()

        return HybridResult(
            file_drop_result=fd_result,
            stream_result=se_result,
            correlation_id=self._correlation_id,
            link_strategy=self._config.link_strategy,
        )

    # ------------------------------------------------------------------
    # Internal — phase runners
    # ------------------------------------------------------------------

    def _run_file_drop(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> FileDropResult | None:
        """Run the file-drop simulator if there are tables to process."""
        if not tables:
            return None
        sim = FileDropSimulator(tables=tables, config=self._config.file_drop_config)
        return sim.run()

    def _run_stream_emit(
        self,
        tables: dict[str, pd.DataFrame],
    ) -> StreamEmitResult | None:
        """Run the stream emitter if there are tables to process."""
        if not tables:
            return None
        emitter = StreamEmitter(
            tables=tables,
            config=self._config.stream_config,
            sink=self._sink,
        )
        return emitter.emit()
