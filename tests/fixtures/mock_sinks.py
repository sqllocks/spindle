"""Mock sink implementations for validation matrix tests."""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class MockSink:
    """Records write calls without performing real IO."""

    sink_type: str
    write_count: int = 0
    total_rows: int = 0
    tables_written: list[str] = field(default_factory=list)
    _chunks: list[tuple[str, pd.DataFrame]] = field(default_factory=list)

    def write(self, result) -> None:
        """Write a full GenerationResult (seeding/inference mode)."""
        for table_name, df in result.tables.items():
            self._chunks.append((table_name, df))
            self.tables_written.append(table_name)
            self.total_rows += len(df)
        self.write_count += 1

    def write_stream(self, table_name: str, df: pd.DataFrame) -> None:
        """Write a single table chunk (streaming mode)."""
        self._chunks.append((table_name, df))
        self.tables_written.append(table_name)
        self.total_rows += len(df)

    def assert_written(self, min_rows: int = 1) -> None:
        assert self.total_rows >= min_rows, (
            f"Expected >= {min_rows} rows, got {self.total_rows}"
        )


def make_mock_sink(sink_type: str) -> MockSink:
    """Return a MockSink for the given sink type."""
    valid = {"lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"}
    if sink_type not in valid:
        raise ValueError(f"Unknown sink type: {sink_type!r}. Valid: {valid}")
    return MockSink(sink_type=sink_type)
