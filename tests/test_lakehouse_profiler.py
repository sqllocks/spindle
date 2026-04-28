"""Tests for LakehouseProfiler (unit tests using mocks — no live Fabric connection)."""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


class TestLakehouseProfilerUnit:
    def test_import_succeeds(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        assert LakehouseProfiler is not None

    def test_constructor_stores_ids(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws-123", lakehouse_id="lh-456")
        assert lp.workspace_id == "ws-123"
        assert lp.lakehouse_id == "lh-456"

    def test_constructor_default_sample_rows(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        assert lp.default_sample_rows == 100_000

    def test_profile_table_with_mock_df(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        from sqllocks_spindle.inference.profiler import TableProfile

        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        mock_df = pd.DataFrame({"id": range(10), "name": [f"u{i}" for i in range(10)]})

        with patch.object(lp, "_read_table", return_value=mock_df):
            profile = lp.profile_table("users")

        assert isinstance(profile, TableProfile)
        assert profile.name == "users"
        assert "id" in profile.columns

    def test_profile_all_returns_dict(self):
        from sqllocks_spindle.inference import LakehouseProfiler

        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")
        mock_df = pd.DataFrame({"x": [1, 2, 3]})

        with patch.object(lp, "_list_tables", return_value=["t1", "t2"]), \
             patch.object(lp, "_read_table", return_value=mock_df):
            profiles = lp.profile_all()

        assert set(profiles.keys()) == {"t1", "t2"}

    def test_read_table_raises_helpful_error_without_deltalake(self):
        from sqllocks_spindle.inference import LakehouseProfiler
        import sqllocks_spindle.inference.lakehouse_profiler as _lp_module
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")

        with patch.object(_lp_module, "HAS_DELTALAKE", False):
            with pytest.raises((ImportError, RuntimeError)):
                lp._read_table("nonexistent_table")
