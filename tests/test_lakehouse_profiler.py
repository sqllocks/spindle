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
# Sampled cross-table FK detection (advisory). STORY-016 / ADR-009
# ---------------------------------------------------------------------------

def _two_table_lakehouse():
    """2-table fixture: orders.customer_id is a FK into customer.customer_id."""
    customer = pd.DataFrame({
        "customer_id": list(range(1, 21)),
        "segment": (["enterprise", "smb"] * 10),
    })
    orders = pd.DataFrame({
        "order_id": list(range(100, 130)),
        # every customer_id is a valid parent key, overlap 1.0
        "customer_id": [(i % 20) + 1 for i in range(30)],
        "amount": [float(i) for i in range(30)],
    })
    return {"customer": customer, "orders": orders}


class TestLakehouseProfilerFKDetection:
    def _profiler_with_frames(self, frames):
        from sqllocks_spindle.inference import LakehouseProfiler
        lp = LakehouseProfiler(workspace_id="ws", lakehouse_id="lh")

        def fake_read(table_name, sample_rows=None):
            df = frames[table_name]
            if sample_rows is not None:
                df = df.head(sample_rows)
            return df

        return lp, fake_read

    def test_advisory_fk_detected_with_overlap(self):
        """AC1+AC2: sampled pass detects orders.customer_id to customer, reports overlap."""
        frames = _two_table_lakehouse()
        lp, fake_read = self._profiler_with_frames(frames)

        with patch.object(lp, "_list_tables", return_value=list(frames.keys())), \
             patch.object(lp, "_read_table", side_effect=fake_read):
            fks = lp.detect_foreign_keys()

        assert "orders" in fks
        assert "customer_id" in fks["orders"]
        entry = fks["orders"]["customer_id"]
        assert entry["parent_table"] == "customer"
        assert entry["advisory"] is True
        assert entry["full_scan"] is False
        # all child keys present in parent, overlap == 1.0
        assert entry["overlap"] == pytest.approx(1.0)

    def test_overlap_reported_below_one(self):
        """Overlap is the measured ratio, not just a boolean pass."""
        frames = _two_table_lakehouse()
        # Make 5 of 30 order rows reference a non-existent customer (id 999).
        orders = frames["orders"].copy()
        orders.loc[:4, "customer_id"] = 999
        frames["orders"] = orders
        lp, fake_read = self._profiler_with_frames(frames)

        with patch.object(lp, "_list_tables", return_value=list(frames.keys())), \
             patch.object(lp, "_read_table", side_effect=fake_read):
            fks = lp.detect_foreign_keys(overlap_threshold=0.8)

        entry = fks["orders"]["customer_id"]
        # distinct child values: {999} + 25 valid ids = 26 distinct; 25 overlap -> 25/26
        assert 0.9 < entry["overlap"] < 1.0

    def test_threshold_configurable_suppresses_low_overlap(self):
        """AC3: raising the threshold above the measured overlap drops the FK."""
        frames = _two_table_lakehouse()
        orders = frames["orders"].copy()
        orders.loc[:14, "customer_id"] = 999  # ~half reference missing parent
        frames["orders"] = orders
        lp, fake_read = self._profiler_with_frames(frames)

        with patch.object(lp, "_list_tables", return_value=list(frames.keys())), \
             patch.object(lp, "_read_table", side_effect=fake_read):
            fks = lp.detect_foreign_keys(overlap_threshold=0.99)

        assert fks.get("orders", {}).get("customer_id") is None

    def test_sample_rows_forwarded_to_read(self):
        """AC3: sample_rows is passed through to _read_table."""
        frames = _two_table_lakehouse()
        lp, fake_read = self._profiler_with_frames(frames)
        seen = {}

        def tracking_read(table_name, sample_rows=None):
            seen[table_name] = sample_rows
            return fake_read(table_name, sample_rows=sample_rows)

        with patch.object(lp, "_list_tables", return_value=list(frames.keys())), \
             patch.object(lp, "_read_table", side_effect=tracking_read):
            lp.detect_foreign_keys(sample_rows=5)

        assert seen["customer"] == 5
        assert seen["orders"] == 5

    def test_full_scan_ignores_sampling(self):
        """AC3: full_scan=True reads entire tables (sample_rows=None) for confirmation."""
        frames = _two_table_lakehouse()
        lp, fake_read = self._profiler_with_frames(frames)
        seen = {}

        def tracking_read(table_name, sample_rows=None):
            seen[table_name] = sample_rows
            return fake_read(table_name, sample_rows=sample_rows)

        with patch.object(lp, "_list_tables", return_value=list(frames.keys())), \
             patch.object(lp, "_read_table", side_effect=tracking_read):
            fks = lp.detect_foreign_keys(sample_rows=5, full_scan=True)

        assert seen["customer"] is None
        assert seen["orders"] is None
        assert fks["orders"]["customer_id"]["full_scan"] is True


class TestSharedFKCoreBackwardCompat:
    """The in-memory _detect_foreign_keys must keep returning col->parent (no overlap)."""

    def test_in_memory_wrapper_unchanged_shape(self):
        from sqllocks_spindle.inference.profiler import DataProfiler
        frames = _two_table_lakehouse()
        prof = DataProfiler()
        ds = prof.profile_dataset(frames)
        # detected_fks on the orders table is still col_name -> parent_table (str)
        orders_fks = ds.tables["orders"].detected_fks
        assert orders_fks.get("customer_id") == "customer"
        assert isinstance(orders_fks["customer_id"], str)

    def test_advisory_core_returns_overlap_tuple(self):
        from sqllocks_spindle.inference.profiler import DataProfiler
        frames = _two_table_lakehouse()
        prof = DataProfiler()
        advisory = prof._detect_foreign_keys_advisory("orders", frames["orders"], frames)
        parent, overlap = advisory["customer_id"]
        assert parent == "customer"
        assert overlap == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Live integration tests — skipped unless explicitly marked
# ---------------------------------------------------------------------------

_SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"

# Module-level credential, cached across tests.
_storage_cred: object | None = None


def _get_storage_token() -> str | None:
    """Acquire an Azure storage token via AzureCliCredential.

    Reuses the caller's `az login` session (the ~90-day refresh token in their
    AZURE_CONFIG_DIR): runs headless or at a keyboard, never launches a browser, and
    works for any signed-in user/CI. Raises (caught) if not signed in. We avoid
    DefaultAzureCredential: its chain probes MSI / shared-cache / VS Code before the
    CLI and stalls on dev/CI hosts.
    """
    global _storage_cred
    try:
        from azure.identity import AzureCliCredential
        if _storage_cred is None:
            # tenant_id PINNED: fail-closed for client isolation. `az ... --tenant`
            # refuses to mint a token if the active account is a different client's
            # tenant, instead of silently using the ambient (possibly wrong) account.
            _storage_cred = AzureCliCredential(tenant_id=_SOUND_BI_TENANT)
        token = _storage_cred.get_token("https://storage.azure.com/.default")
        return token.token if token else None
    except Exception:
        return None


@pytest.mark.live
class TestLakehouseProfilerLive:
    """Live integration tests for LakehouseProfiler against Fabric_Lakehouse_Demo.

    Requires a Delta table in Fabric_Lakehouse_Demo. Auth via DefaultAzureCredential
    (env-resolved: `az login` / MSI / SPN env vars), cached for all tests.
    """

    def setup_method(self):
        """Acquire storage token once per test (cached on the module credential)."""
        self.token = _get_storage_token()
        assert self.token, (
            "Could not acquire a storage token via DefaultAzureCredential. "
            "Run `az login` (or set MSI / SPN env vars) for the target tenant."
        )

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
