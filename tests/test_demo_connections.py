"""Tests for ConnectionRegistry."""
import pytest
from pathlib import Path
from sqllocks_spindle.demo.connections import ConnectionRegistry, ConnectionProfile


@pytest.fixture
def tmp_registry(tmp_path):
    return ConnectionRegistry(path=tmp_path / "connections.json")


def test_save_and_load(tmp_registry):
    profile = ConnectionProfile(
        name="test_conn", workspace_id="ws-123",
        warehouse_conn_str="Driver={ODBC Driver 18 for SQL Server};Server=test",
        auth_method="cli",
    )
    tmp_registry.save(profile)
    loaded = tmp_registry.load("test_conn")
    assert loaded.name == "test_conn"
    assert loaded.workspace_id == "ws-123"
    assert loaded.auth_method == "cli"


def test_load_missing_raises(tmp_registry):
    with pytest.raises(KeyError, match="No connection profile"):
        tmp_registry.load("nonexistent")


def test_list(tmp_registry):
    assert tmp_registry.list() == []
    tmp_registry.save(ConnectionProfile(name="a"))
    tmp_registry.save(ConnectionProfile(name="b"))
    assert set(tmp_registry.list()) == {"a", "b"}


def test_delete(tmp_registry):
    tmp_registry.save(ConnectionProfile(name="to_delete"))
    tmp_registry.delete("to_delete")
    assert "to_delete" not in tmp_registry.list()


def test_exists(tmp_registry):
    assert not tmp_registry.exists("x")
    tmp_registry.save(ConnectionProfile(name="x"))
    assert tmp_registry.exists("x")
