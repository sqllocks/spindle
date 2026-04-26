"""Tests for SeedingDemoMode v2 (Phase 2 wiring)."""
from unittest.mock import MagicMock
import pytest

from sqllocks_spindle.demo.modes.seeding import _resolve_scale_mode


class _Conn:
    def __init__(self, lakehouse_id="lh-1", workspace_id="ws-1"):
        self.lakehouse_id = lakehouse_id
        self.workspace_id = workspace_id
        self.warehouse_conn_str = ""
        self.warehouse_staging_path = ""
        self.sql_db_conn_str = ""
        self.eventhouse_uri = ""
        self.eventhouse_database = ""


def test_scale_mode_auto_picks_local_under_threshold():
    assert _resolve_scale_mode("auto", _Conn(), rows=100_000) == "local"


def test_scale_mode_auto_picks_spark_with_connection_and_large_rows():
    assert _resolve_scale_mode("auto", _Conn(), rows=500_000) == "spark"
    assert _resolve_scale_mode("auto", _Conn(), rows=1_000_000) == "spark"


def test_scale_mode_auto_picks_local_when_no_connection():
    assert _resolve_scale_mode("auto", None, rows=10_000_000) == "local"


def test_scale_mode_auto_picks_local_when_no_lakehouse():
    conn = _Conn(lakehouse_id="")
    assert _resolve_scale_mode("auto", conn, rows=10_000_000) == "local"


def test_scale_mode_explicit_local_always_returns_local():
    assert _resolve_scale_mode("local", _Conn(), rows=10_000_000) == "local"
    assert _resolve_scale_mode("local", None, rows=10_000_000) == "local"


def test_scale_mode_explicit_spark_without_connection_raises():
    with pytest.raises(ValueError, match="Spark mode requires a connection profile"):
        _resolve_scale_mode("spark", None, rows=100)


def test_scale_mode_explicit_spark_without_lakehouse_raises():
    conn = _Conn(lakehouse_id="")
    with pytest.raises(ValueError, match="lakehouse_id"):
        _resolve_scale_mode("spark", conn, rows=100)


def test_scale_mode_explicit_spark_with_full_connection_returns_spark():
    assert _resolve_scale_mode("spark", _Conn(), rows=100) == "spark"


from sqllocks_spindle.demo.modes.seeding import _build_sinks


def test_build_sinks_empty_when_no_targets():
    conn = _Conn()
    conn.lakehouse_id = ""
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    assert sinks == []
    assert sinks_list == []
    assert sink_config["workspace_id"] == "ws-1"
    assert sink_config["token"] == "t"


def test_build_sinks_lakehouse_only():
    conn = _Conn()
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    assert len(sinks) == 1
    assert sinks_list == [{"type": "lakehouse",
                           "config": {"workspace_id": "ws-1",
                                      "lakehouse_id": "lh-1"}}]
    assert sink_config["lakehouse_id"] == "lh-1"


def test_build_sinks_all_targets_with_extra_fields():
    """Warehouse and KQL only build when extra fields are present."""
    conn = _Conn()
    conn.warehouse_conn_str = "Driver=...;Server=wh"
    conn.warehouse_staging_path = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/staging"
    conn.sql_db_conn_str = "Driver=...;Server=sql"
    conn.eventhouse_uri = "https://eh.kusto"
    conn.eventhouse_database = "demo_db"
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    types = [s["type"] for s in sinks_list]
    assert types == ["lakehouse", "warehouse", "sql_db", "kql"]
    assert len(sinks) == 4


def test_build_sinks_skips_warehouse_without_staging_path():
    """Warehouse sink is skipped when warehouse_staging_path is empty."""
    conn = _Conn()
    conn.warehouse_conn_str = "Driver=...;Server=wh"
    # warehouse_staging_path is "" by default — should be skipped
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    types = [s["type"] for s in sinks_list]
    assert "warehouse" not in types
    assert "lakehouse" in types  # still built


def test_build_sinks_skips_kql_without_database():
    """KQL sink is skipped when eventhouse_database is empty."""
    conn = _Conn()
    conn.eventhouse_uri = "https://eh.kusto"
    # eventhouse_database is "" by default — should be skipped
    sinks, sinks_list, sink_config = _build_sinks(conn, token="t")
    types = [s["type"] for s in sinks_list]
    assert "kql" not in types


from sqllocks_spindle.demo.modes.seeding import _acquire_token


def test_acquire_token_uses_azure_cli_credential(monkeypatch):
    fake_token = MagicMock()
    fake_token.token = "ey-fake-token"
    fake_credential = MagicMock()
    fake_credential.get_token.return_value = fake_token

    monkeypatch.setattr(
        "azure.identity.AzureCliCredential",
        MagicMock(return_value=fake_credential),
    )
    token = _acquire_token()
    assert token == "ey-fake-token"
    fake_credential.get_token.assert_called_once_with(
        "https://api.fabric.microsoft.com/.default"
    )
