"""Tests for DeltaWriter — Delta Lake output."""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import patch

try:
    import deltalake  # noqa: F401

    HAS_DELTALAKE = True
except ImportError:
    HAS_DELTALAKE = False

pytestmark = pytest.mark.skipif(
    not HAS_DELTALAKE, reason="deltalake not installed (pip install sqllocks-spindle[fabric])"
)


@pytest.fixture()
def sample_tables() -> dict[str, pd.DataFrame]:
    """Small multi-table dataset for testing."""
    return {
        "customer": pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "signup_date": pd.to_datetime(
                    ["2025-01-10", "2025-06-15", "2025-11-20"]
                ),
            }
        ),
        "order": pd.DataFrame(
            {
                "order_id": [10, 20, 30, 40],
                "customer_id": [1, 2, 1, 3],
                "order_date": pd.to_datetime(
                    ["2025-02-01", "2025-07-01", "2025-08-15", "2025-12-01"]
                ),
                "total": [100.0, 250.0, 75.0, 300.0],
            }
        ),
    }


class TestDeltaWriterWriteSingle:
    def test_write_single_table(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir=tmp_path)
        path = writer.write("customer", sample_tables["customer"])

        assert path.exists()
        assert (path / "_delta_log").exists()

        from deltalake import DeltaTable

        dt = DeltaTable(str(path))
        result = dt.to_pandas()
        assert len(result) == 3
        assert set(result.columns) == {"customer_id", "name", "signup_date"}

    def test_write_empty_dataframe(self, tmp_path):
        from sqllocks_spindle.output import DeltaWriter

        df = pd.DataFrame({"id": pd.Series([], dtype="int64"), "val": pd.Series([], dtype="str")})
        writer = DeltaWriter(output_dir=tmp_path)
        path = writer.write("empty_table", df)

        assert path.exists()
        from deltalake import DeltaTable

        dt = DeltaTable(str(path))
        assert len(dt.to_pandas()) == 0


class TestDeltaWriterWriteAll:
    def test_write_all_tables(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir=tmp_path)
        paths = writer.write_all(sample_tables)

        assert len(paths) == 2
        for p in paths:
            assert p.exists()
            assert (p / "_delta_log").exists()

    def test_returns_correct_paths(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir=tmp_path)
        paths = writer.write_all(sample_tables)

        path_names = {p.name for p in paths}
        assert path_names == {"customer", "order"}


class TestDeltaWriterPartitioning:
    def test_partition_by_year_month(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(
            output_dir=tmp_path,
            partition_by={"order": ["order_date:year", "order_date:month"]},
        )
        paths = writer.write_all(sample_tables)

        order_path = tmp_path / "order"
        assert order_path.exists()

        from deltalake import DeltaTable

        dt = DeltaTable(str(order_path))
        result = dt.to_pandas()
        assert len(result) == 4
        assert "order_date_year" in result.columns
        assert "order_date_month" in result.columns

    def test_no_partition_for_unspecified_table(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(
            output_dir=tmp_path,
            partition_by={"order": ["order_date:year"]},
        )
        writer.write_all(sample_tables)

        # Customer table should have no partition columns
        from deltalake import DeltaTable

        dt = DeltaTable(str(tmp_path / "customer"))
        result = dt.to_pandas()
        assert set(result.columns) == {"customer_id", "name", "signup_date"}


class TestDeltaWriterModes:
    def test_overwrite_mode(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir=tmp_path, mode="overwrite")
        writer.write("customer", sample_tables["customer"])
        writer.write("customer", sample_tables["customer"])

        from deltalake import DeltaTable

        dt = DeltaTable(str(tmp_path / "customer"))
        assert len(dt.to_pandas()) == 3  # Not 6

    def test_append_mode(self, tmp_path, sample_tables):
        from sqllocks_spindle.output import DeltaWriter

        writer = DeltaWriter(output_dir=tmp_path, mode="append")
        writer.write("customer", sample_tables["customer"])
        writer.write("customer", sample_tables["customer"])

        from deltalake import DeltaTable

        dt = DeltaTable(str(tmp_path / "customer"))
        assert len(dt.to_pandas()) == 6


class TestDeltaWriterErrors:
    def test_no_output_dir_no_fabric_raises(self):
        from sqllocks_spindle.output import DeltaWriter
        from sqllocks_spindle.output.fabric_utils import FabricEnvironment

        with patch(
            "sqllocks_spindle.output.delta_writer.detect_fabric_environment",
            return_value=FabricEnvironment(is_fabric=False),
        ):
            with pytest.raises(ValueError, match="No output_dir specified"):
                DeltaWriter()
