"""Tests for fabric_utils — partition parsing and environment detection."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from sqllocks_spindle.output.fabric_utils import (
    FabricEnvironment,
    detect_fabric_environment,
    parse_partition_spec,
)


# ---------------------------------------------------------------------------
# detect_fabric_environment
# ---------------------------------------------------------------------------


class TestDetectFabricEnvironment:
    @patch("sqllocks_spindle.output.fabric_utils.Path")
    def test_not_in_fabric(self, mock_path_cls):
        # Ensure /lakehouse/default is not detected even if it exists locally
        mock_path_cls.return_value.exists.return_value = False
        mock_path_cls.return_value.is_dir.return_value = False
        env = detect_fabric_environment()
        assert isinstance(env, FabricEnvironment)
        assert env.is_fabric is False
        assert env.lakehouse_path is None
        assert env.default_tables_path is None


# ---------------------------------------------------------------------------
# parse_partition_spec
# ---------------------------------------------------------------------------


@pytest.fixture()
def order_df() -> pd.DataFrame:
    """Small order DataFrame with datetime column."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "order_date": pd.to_datetime(
                ["2025-01-15", "2025-03-22", "2025-07-04", "2025-12-31"]
            ),
            "total": [100.0, 250.0, 75.0, 300.0],
            "region": ["East", "West", "East", "South"],
        }
    )


class TestParsePartitionSpec:
    def test_empty_specs(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec([], order_df)
        assert cols == []
        assert list(result_df.columns) == list(order_df.columns)

    def test_year_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["order_date:year"], order_df)
        assert cols == ["order_date_year"]
        assert "order_date_year" in result_df.columns
        assert list(result_df["order_date_year"]) == [2025, 2025, 2025, 2025]

    def test_month_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["order_date:month"], order_df)
        assert cols == ["order_date_month"]
        assert list(result_df["order_date_month"]) == [1, 3, 7, 12]

    def test_day_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["order_date:day"], order_df)
        assert cols == ["order_date_day"]
        assert list(result_df["order_date_day"]) == [15, 22, 4, 31]

    def test_quarter_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["order_date:quarter"], order_df)
        assert cols == ["order_date_quarter"]
        assert list(result_df["order_date_quarter"]) == [1, 1, 3, 4]

    def test_week_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["order_date:week"], order_df)
        assert cols == ["order_date_week"]
        assert all(isinstance(v, (int,)) for v in result_df["order_date_week"])

    def test_multiple_specs(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(
            ["order_date:year", "order_date:month"], order_df
        )
        assert cols == ["order_date_year", "order_date_month"]
        assert "order_date_year" in result_df.columns
        assert "order_date_month" in result_df.columns

    def test_plain_column_spec(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(["region"], order_df)
        assert cols == ["region"]
        # Original column untouched, no new columns added
        assert list(result_df.columns) == list(order_df.columns)

    def test_mixed_plain_and_extraction(self, order_df: pd.DataFrame):
        result_df, cols = parse_partition_spec(
            ["region", "order_date:year"], order_df
        )
        assert cols == ["region", "order_date_year"]

    def test_does_not_mutate_source(self, order_df: pd.DataFrame):
        original_cols = list(order_df.columns)
        parse_partition_spec(["order_date:year"], order_df)
        assert list(order_df.columns) == original_cols

    def test_missing_column_raises(self, order_df: pd.DataFrame):
        with pytest.raises(ValueError, match="not found in DataFrame"):
            parse_partition_spec(["nonexistent:year"], order_df)

    def test_missing_plain_column_raises(self, order_df: pd.DataFrame):
        with pytest.raises(ValueError, match="not found in DataFrame"):
            parse_partition_spec(["nonexistent"], order_df)

    def test_unknown_extraction_raises(self, order_df: pd.DataFrame):
        with pytest.raises(ValueError, match="Unknown partition extraction"):
            parse_partition_spec(["order_date:foobar"], order_df)
