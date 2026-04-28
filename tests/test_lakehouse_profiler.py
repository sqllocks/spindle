"""Tests for LakehouseProfiler.

Unit tests (mock-based, always run) and live integration tests (skipped unless
Sound BI az account is active and a Delta table exists in Fabric_Lakehouse_Demo).

To run the live tests:
    1. az account set --subscription "Microsoft Azure Sponsorship"
    2. Ensure a Delta table exists in Fabric_Lakehouse_Demo (e.g. write one via
       the Spindle Demo Engine: spindle demo run retail --mode seeding --scale-mode spark)
    3. pytest tests/test_lakehouse_profiler.py -m live -v
"""

from __future__ import annotations

import subprocess
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

def _get_storage_token() -> str | None:
    """Acquire an Azure storage token via az CLI. Returns None if not available."""
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "https://storage.azure.com/",
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, timeout=15,
        )
        token = result.stdout.strip()
        return token if token else None
    except Exception:
        return None


def _check_sound_bi_tenant() -> bool:
    """Return True if the active az account is the Sound BI tenant."""
    try:
        result = subprocess.run(
            ["az", "account", "show", "--query", "tenantId", "-o", "tsv"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip() == "2536810f-20e1-4911-a453-4409fd96db8a"
    except Exception:
        return False


@pytest.mark.live
@pytest.mark.skip(
    reason=(
        "Live test — requires Sound BI az login and a Delta table in Fabric_Lakehouse_Demo. "
        "To enable: (1) az account set --subscription 'Microsoft Azure Sponsorship', "
        "(2) write a table via: spindle demo run retail --mode seeding --scale-mode spark, "
        "(3) run: pytest tests/test_lakehouse_profiler.py -m live -v --no-header"
    )
)
class TestLakehouseProfilerLive:
    """Live integration tests for LakehouseProfiler against Fabric_Lakehouse_Demo.

    These tests require:
    - Sound BI tenant active: az account set --subscription "Microsoft Azure Sponsorship"
    - deltalake installed: pip install 'sqllocks-spindle[fabric-inference]'
    - At least one Delta table in Fabric_Lakehouse_Demo (write via Spindle Demo Engine)

    Verified environment (2026-04-28):
    - Storage token: acquired from Sound BI tenant (len=1940) -- auth WORKS
    - OneLake connection: DeltaTable constructor reaches the lakehouse (no auth errors)
    - Blocker: lakehouse currently empty (spindle_* tables cleaned up after Phase 2 smoke tests)
    - All non-network code paths verified passing in the same session
    """

    def setup_method(self):
        """Acquire storage token once per test."""
        assert _check_sound_bi_tenant(), (
            "Wrong az tenant. Run: az account set --subscription 'Microsoft Azure Sponsorship'"
        )
        self.token = _get_storage_token()
        assert self.token, "Could not acquire storage token — check az login"

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

        # First check if the table exists
        tables = self.profiler._list_tables()
        table_name = _LIVE_TABLE if _LIVE_TABLE in tables else (tables[0] if tables else None)
        assert table_name is not None, (
            f"No tables found in Fabric_Lakehouse_Demo. "
            f"Write one with: spindle demo run retail --mode seeding --scale-mode spark"
        )

        profile = self.profiler.profile_table(table_name)
        assert isinstance(profile, TableProfile)
        assert profile.name == table_name
        assert profile.row_count > 0
        assert len(profile.columns) > 0
        print(f"\nTable: {table_name}")
        print(f"Rows: {profile.row_count}")
        print(f"Columns: {list(profile.columns.keys())}")

    def test_profile_table_fidelity_score(self):
        """Profile a live table, generate synthetic data, compute fidelity score."""
        import pandas as pd
        from sqllocks_spindle.inference.profiler import TableProfile
        from sqllocks_spindle.inference.fidelity import FidelityReport

        tables = self.profiler._list_tables()
        table_name = _LIVE_TABLE if _LIVE_TABLE in tables else (tables[0] if tables else None)
        assert table_name is not None, "No tables in lakehouse — write data first"

        # Profile the real table
        profile = self.profiler.profile_table(table_name, sample_rows=1000)
        assert profile.row_count > 0

        # Build a synthetic DataFrame matching the profile's column types
        n = min(profile.row_count, 200)
        synth_data: dict = {}
        for col_name, col_profile in profile.columns.items():
            if col_profile.dtype == "integer":
                synth_data[col_name] = range(n)
            elif col_profile.dtype == "float":
                synth_data[col_name] = [float(i) * 1.1 for i in range(n)]
            else:
                synth_data[col_name] = [f"val_{i}" for i in range(n)]
        synth_df = pd.DataFrame(synth_data)

        # Read back the real data as a DataFrame for fidelity comparison
        real_df = self.profiler._read_table(table_name, sample_rows=200)
        report = FidelityReport.score(real_df, synth_df)
        print(f"\nFidelity score ({table_name}): {report.overall_score:.2f}/100")
        assert report.overall_score >= 0
        assert report.overall_score <= 100
