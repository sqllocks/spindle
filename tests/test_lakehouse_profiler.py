"""Tests for LakehouseProfiler.

Unit tests (mock-based, always run) and live integration tests that require
a Delta table in Fabric_Lakehouse_Demo and Sound BI credentials.

To run the live tests:
    1. Ensure a Delta table exists in Fabric_Lakehouse_Demo (write via seed script
       or: spindle demo run retail --mode seeding)
    2. pytest tests/test_lakehouse_profiler.py -m live -v
       (browser prompt fires once for Sound BI auth)
"""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Live-test config
# ---------------------------------------------------------------------------
_WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
_LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"
_LIVE_TABLE = "spindle_customer"  # written by Spindle Demo Engine retail scenario


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

    def test_abfss_path_construction(self):
        """ABFSS path must match the OneLake DFS format exactly."""
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws-abc", lakehouse_id="lh-def")
        root = lp._abfss_tables_root()
        assert root == "abfss://ws-abc@onelake.dfs.fabric.microsoft.com/lh-def/Tables"

    def test_storage_options_includes_bearer_token(self):
        """_storage_options must include bearer_token when token_provider returns a token."""
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(
            workspace_id="ws", lakehouse_id="lh",
            token_provider=lambda: "my-test-token",
        )
        opts = lp._storage_options()
        assert opts["bearer_token"] == "my-test-token"
        assert opts["use_emulator"] == "false"

    def test_storage_options_empty_when_no_token(self):
        """_storage_options returns empty dict when no token available and azure-identity absent."""
        from sqllocks_spindle.inference import LakehouseProfiler
        import sqllocks_spindle.inference.lakehouse_profiler as _lp_module
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh", token_provider=None)
        orig = _lp_module.HAS_AZURE_IDENTITY
        _lp_module.HAS_AZURE_IDENTITY = False
        try:
            opts = lp._storage_options()
            assert opts == {}
        finally:
            _lp_module.HAS_AZURE_IDENTITY = orig


# ---------------------------------------------------------------------------
# Live integration tests — skipped unless explicitly marked
# ---------------------------------------------------------------------------

_SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"

# Module-level credential — cached across tests so browser prompt fires once.
_browser_cred: object | None = None


def _get_storage_token() -> str | None:
    """Acquire an Azure storage token via InteractiveBrowserCredential."""
    global _browser_cred
    try:
        from azure.identity import InteractiveBrowserCredential
        if _browser_cred is None:
            _browser_cred = InteractiveBrowserCredential(tenant_id=_SOUND_BI_TENANT)
        token = _browser_cred.get_token("https://storage.azure.com/.default")
        return token.token if token else None
    except Exception:
        return None


@pytest.mark.live
class TestLakehouseProfilerLive:
    """Live integration tests for LakehouseProfiler against Fabric_Lakehouse_Demo.

    Requires a Delta table in Fabric_Lakehouse_Demo.  Auth via InteractiveBrowserCredential
    — browser prompt fires once per session, token is cached for all tests.
    """

    def setup_method(self):
        """Acquire storage token once per test (cached after first browser login)."""
        self.token = _get_storage_token()
        assert self.token, "Could not acquire storage token via InteractiveBrowserCredential"

        from sqllocks_spindle.inference.lakehouse_profiler import LakehouseProfiler
        self.profiler = LakehouseProfiler(
            workspace_id=_WORKSPACE_ID,
            lakehouse_id=_LAKEHOUSE_ID,
            token_provider=lambda: self.token,
        )

    def test_list_tables_returns_list(self):
        """_list_tables should return a list (possibly empty if no tables exist)."""
        tables = self.profiler._list_tables()
        assert isinstance(tables, list)
        print(f"\nTables found: {tables}")

    def test_profile_table_returns_table_profile(self):
        """profile_table should return a TableProfile with row_count and columns."""
        from sqllocks_spindle.inference.profiler import TableProfile

        profile = self.profiler.profile_table(_LIVE_TABLE)
        assert isinstance(profile, TableProfile)
        assert profile.name == _LIVE_TABLE
        assert profile.row_count > 0
        assert len(profile.columns) > 0
        print(f"\nTable: {_LIVE_TABLE}")
        print(f"Rows: {profile.row_count}")
        print(f"Columns: {list(profile.columns.keys())}")

    def test_profile_table_column_stats(self):
        """profile_table returns column profiles with expected dtypes for known columns."""
        profile = self.profiler.profile_table(_LIVE_TABLE, sample_rows=500)
        assert profile.row_count > 0

        # Known columns from the seed script
        assert "customer_id" in profile.columns
        assert "segment" in profile.columns
        assert "annual_revenue" in profile.columns

        id_col = profile.columns["customer_id"]
        rev_col = profile.columns["annual_revenue"]
        assert id_col.dtype in ("integer", "int64", "int32")
        assert rev_col.dtype in ("float", "float64")
        print(f"\ncustomer_id dtype: {id_col.dtype}")
        print(f"annual_revenue dtype: {rev_col.dtype}")
