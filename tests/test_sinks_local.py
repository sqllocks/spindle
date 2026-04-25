from __future__ import annotations
import pytest


def test_sink_protocol_importable():
    from sqllocks_spindle.engine.sinks.base import Sink, FabricConnectionProfile
    assert Sink is not None
    assert FabricConnectionProfile is not None


def test_fabric_connection_profile_fields():
    from sqllocks_spindle.engine.sinks.base import FabricConnectionProfile
    p = FabricConnectionProfile(token="tok", endpoint="https://example.com")
    assert p.token == "tok"
    assert p.endpoint == "https://example.com"


def test_memory_sink_accumulates_chunks():
    import numpy as np
    import pandas as pd
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    sink = MemorySink()
    sink.open(schema=None)

    chunk1 = {"id": np.array([1, 2, 3]), "name": np.array(["a", "b", "c"], dtype=object)}
    chunk2 = {"id": np.array([4, 5]), "name": np.array(["d", "e"], dtype=object)}
    sink.write_chunk("customers", chunk1)
    sink.write_chunk("customers", chunk2)
    sink.close()

    result = sink.result()
    assert "customers" in result
    assert len(result["customers"]) == 5
    assert list(result["customers"]["id"]) == [1, 2, 3, 4, 5]


def test_memory_sink_warns_at_4gb(monkeypatch):
    import numpy as np
    import warnings
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    sink = MemorySink(max_memory_gb=None)
    sink.open(schema=None)

    # Simulate _cumulative_bytes already above 4GB threshold (4GB = 4 * 1024^3 bytes)
    monkeypatch.setattr(sink, "_cumulative_bytes", 4 * 1024 ** 3 + 1)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        sink.write_chunk("t", {"col": np.array([1])})
        assert any(issubclass(x.category, ResourceWarning) for x in w), "Expected ResourceWarning"
    sink.close()


def test_memory_sink_raises_when_max_exceeded():
    import numpy as np
    import pytest
    from sqllocks_spindle.engine.sinks.memory import MemorySink

    sink = MemorySink(max_memory_gb=0.0)  # immediate trigger
    sink.open(schema=None)
    with pytest.raises(MemoryError):
        sink.write_chunk("t", {"col": np.ones(1_000_000, dtype=np.float64)})


def test_parquet_sink_writes_partitioned_files(tmp_path):
    import numpy as np
    from sqllocks_spindle.engine.sinks.parquet import ParquetSink

    sink = ParquetSink(output_dir=str(tmp_path))
    sink.open(schema=None)

    arrays = {"id": np.array([1, 2, 3]), "val": np.array([10.0, 20.0, 30.0])}
    sink.write_chunk("orders", arrays)
    sink.write_chunk("orders", {"id": np.array([4]), "val": np.array([40.0])})
    sink.close()

    parts = sorted((tmp_path / "orders").glob("part-*.parquet"))
    assert len(parts) == 2, f"Expected 2 partition files, got {len(parts)}"


def test_parquet_sink_creates_output_dir(tmp_path):
    import numpy as np
    from sqllocks_spindle.engine.sinks.parquet import ParquetSink

    out = tmp_path / "nonexistent" / "nested"
    sink = ParquetSink(output_dir=str(out))
    sink.open(schema=None)
    sink.write_chunk("t", {"x": np.array([1, 2])})
    sink.close()

    assert (out / "t").exists()
    assert len(list((out / "t").glob("*.parquet"))) == 1


def test_parquet_sink_multiple_tables(tmp_path):
    import numpy as np
    from sqllocks_spindle.engine.sinks.parquet import ParquetSink

    sink = ParquetSink(output_dir=str(tmp_path))
    sink.open(schema=None)
    sink.write_chunk("customers", {"id": np.array([1, 2])})
    sink.write_chunk("orders", {"id": np.array([10, 20, 30])})
    sink.close()

    assert (tmp_path / "customers").exists()
    assert (tmp_path / "orders").exists()


def test_lakehouse_sink_delegates_to_files_writer(tmp_path):
    """LakehouseSink with a local base_path should write files via LakehouseFilesWriter."""
    import numpy as np
    from sqllocks_spindle.engine.sinks.lakehouse import LakehouseSink

    # Use local file path mode — no Fabric token needed
    sink = LakehouseSink(base_path=str(tmp_path))
    sink.open(schema=None)
    sink.write_chunk("customers", {"id": np.array([1, 2, 3]), "name": np.array(["a", "b", "c"], dtype=object)})
    sink.close()

    # LakehouseFilesWriter writes parquet into base_path/customers/
    files = list(tmp_path.glob("**/*.parquet"))
    assert len(files) >= 1


def test_warehouse_sink_accumulates_and_flushes():
    """WarehouseSink accumulates chunks and calls write_tables on close."""
    import numpy as np
    from unittest.mock import MagicMock, patch
    from sqllocks_spindle.engine.sinks.warehouse import WarehouseSink

    mock_result = MagicMock()
    mock_result.errors = []
    mock_writer = MagicMock()
    mock_writer.write_tables.return_value = mock_result

    with patch("sqllocks_spindle.fabric.warehouse_bulk_writer.WarehouseBulkWriter", return_value=mock_writer):
        sink = WarehouseSink(connection_string="Server=fake", staging_lakehouse_path="abfss://fake")
        sink.open(schema=None)
        sink.write_chunk("orders", {"id": np.array([1, 2]), "amount": np.array([10.0, 20.0])})
        sink.write_chunk("orders", {"id": np.array([3]), "amount": np.array([30.0])})
        sink.close()

    mock_writer.write_tables.assert_called_once()
    _, kwargs = mock_writer.write_tables.call_args
    assert "orders" in kwargs["tables"]
    assert len(kwargs["tables"]["orders"]) == 3


def test_kql_sink_accumulates_and_flushes():
    """KQLSink accumulates chunks and calls write on close."""
    import numpy as np
    from unittest.mock import MagicMock, patch
    from sqllocks_spindle.engine.sinks.kql import KQLSink

    mock_result = MagicMock()
    mock_result.errors = []
    mock_writer = MagicMock()
    mock_writer.write.return_value = mock_result

    with patch("sqllocks_spindle.fabric.eventhouse_writer.EventhouseWriter", return_value=mock_writer):
        sink = KQLSink(cluster_uri="https://fake.kusto.fabric.microsoft.com", database="mydb")
        sink.open(schema=None)
        sink.write_chunk("events", {"ts": np.array([1, 2, 3]), "val": np.array([0.1, 0.2, 0.3])})
        sink.close()

    mock_writer.write.assert_called_once()
    _, kwargs = mock_writer.write.call_args
    assert "events" in kwargs["result"]
    assert len(kwargs["result"]["events"]) == 3


def test_sql_database_sink_accumulates_and_flushes():
    """SQLDatabaseSink accumulates chunks and calls write on close."""
    import numpy as np
    from unittest.mock import MagicMock, patch
    from sqllocks_spindle.engine.sinks.sql_database import SQLDatabaseSink

    mock_result = MagicMock()
    mock_result.errors = []
    mock_writer = MagicMock()
    mock_writer.write.return_value = mock_result

    with patch("sqllocks_spindle.fabric.sql_database_writer.FabricSqlDatabaseWriter", return_value=mock_writer):
        sink = SQLDatabaseSink(connection_string="Server=fake;Database=mydb")
        sink.open(schema=None)
        sink.write_chunk("users", {"id": np.array([10, 20]), "name": np.array(["a", "b"], dtype=object)})
        sink.close()

    mock_writer.write.assert_called_once()
    _, kwargs = mock_writer.write.call_args
    assert "users" in kwargs["result"]
    assert len(kwargs["result"]["users"]) == 2
