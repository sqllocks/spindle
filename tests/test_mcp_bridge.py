"""Tests for the MCP bridge command handlers."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def reset_stream_manager():
    """Reset StreamManager singleton between tests to prevent state bleed."""
    from sqllocks_spindle.engine.stream_manager import StreamManager
    yield
    with StreamManager._class_lock:
        if StreamManager._instance is not None:
            with StreamManager._instance._lock:
                for state in StreamManager._instance._streams.values():
                    state.stop_event.set()
            StreamManager._instance = None


from sqllocks_spindle.mcp_bridge import (
    cmd_list,
    cmd_describe,
    cmd_generate,
    cmd_dry_run,
    cmd_validate,
    cmd_preview,
    cmd_profile_info,
    cmd_scale_generate,
    cmd_stream,
    cmd_stream_status,
    cmd_stream_stop,
)


class TestMcpBridgeList:
    def test_list_returns_domains(self):
        result = cmd_list({})
        assert "domains" in result
        assert "count" in result

    def test_list_contains_domains(self):
        result = cmd_list({})
        domains = result["domains"]
        assert len(domains) >= 12
        names = [d["name"] for d in domains]
        assert "retail" in names
        assert "healthcare" in names


class TestMcpBridgeDescribe:
    def test_describe_retail(self):
        result = cmd_describe({"domain": "retail"})
        assert "tables" in result
        assert result["table_count"] > 0

    def test_describe_healthcare(self):
        result = cmd_describe({"domain": "healthcare"})
        assert "tables" in result

    def test_describe_unknown_domain_errors(self):
        with pytest.raises(ValueError, match="Unknown domain"):
            cmd_describe({"domain": "nonexistent_domain_xyz"})


class TestMcpBridgeDryRun:
    def test_dry_run_returns_counts(self):
        result = cmd_dry_run({"domain": "retail", "scale": "small"})
        assert "planned_rows" in result
        assert result["total_rows"] > 0


class TestMcpBridgeGenerate:
    def test_generate_retail_small(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert result["total_rows"] > 0
        assert result["integrity_pass"] is True

    def test_generate_returns_summary(self):
        result = cmd_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "format": "summary",
        })
        assert "tables" in result


class TestMcpBridgePreview:
    def test_preview_returns_rows(self):
        result = cmd_preview({
            "domain": "retail",
            "rows": 5,
            "seed": 42,
        })
        assert "tables" in result
        # Each table should have preview data
        for tname, tdata in result["tables"].items():
            assert "data" in tdata
            assert tdata["preview_rows"] <= 5


class TestMcpBridgeProfileInfo:
    def test_profile_info_retail(self):
        result = cmd_profile_info({"domain": "retail"})
        assert result["domain"] == "retail"
        assert "available_profiles" in result


class TestMcpBridgeValidate:
    def test_validate_missing_file_errors(self):
        with pytest.raises(FileNotFoundError):
            cmd_validate({"schema_path": "/nonexistent/path.spindle.json"})


class TestMcpBridgeScaleGenerate:
    def test_scale_generate_local_single_mode(self):
        """scale_mode=local_single with memory sink returns expected shape."""
        result = cmd_scale_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "scale_mode": "local_single",
            "sinks": ["memory"],
            "sink_config": {},
        })
        assert "rows_generated" in result
        assert result["rows_generated"] > 0
        assert "sinks_written" in result
        assert result["sinks_written"].get("memory") == "ok"

    def test_scale_generate_local_mp_memory_sink(self):
        """scale_mode=local_mp with memory sink — full multi-process path."""
        result = cmd_scale_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 42,
            "scale_mode": "local_mp",
            "sinks": ["memory"],
            "sink_config": {},
            "chunk_size": 100,
        })
        assert result["rows_generated"] > 0
        assert result.get("throughput_rows_per_sec", 0) > 0

    def test_scale_generate_parquet_sink(self, tmp_path):
        result = cmd_scale_generate({
            "domain": "retail",
            "scale": "small",
            "seed": 1,
            "scale_mode": "local_mp",
            "sinks": ["parquet"],
            "sink_config": {"parquet": {"output_dir": str(tmp_path)}},
            "chunk_size": 200,
        })
        assert result["sinks_written"].get("parquet") == "ok"
        assert len(list(tmp_path.iterdir())) > 0

    def test_scale_generate_fabric_spark_not_implemented(self):
        result = cmd_scale_generate({
            "domain": "retail",
            "scale": "small",
            "scale_mode": "fabric_spark",
            "sinks": ["memory"],
            "sink_config": {},
        })
        assert result.get("error") == "not_implemented"


class TestMcpBridgeStreaming:
    def test_stream_returns_stream_id(self):
        result = cmd_stream({
            "domain": "retail",
            "scale": "small",
            "sinks": ["memory"],
            "sink_config": {},
            "interval_seconds": 0,
            "chunk_size": 10,
            "max_chunks": 1,
        })
        assert "stream_id" in result
        assert isinstance(result["stream_id"], str)
        assert result.get("status") == "started"

    def test_stream_status_returns_progress(self):
        import time
        start_result = cmd_stream({
            "domain": "retail",
            "scale": "small",
            "sinks": ["memory"],
            "sink_config": {},
            "interval_seconds": 0,
            "chunk_size": 5,
            "max_chunks": 1,
        })
        stream_id = start_result["stream_id"]
        # Poll until the single chunk finishes (up to 10 s)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            status = cmd_stream_status({"stream_id": stream_id})
            if not status.get("running"):
                break
            time.sleep(0.05)
        assert status["chunks_written"] == 1
        assert status["rows_written"] == 5
        assert status["running"] is False

    def test_stream_stop(self):
        import time
        start_result = cmd_stream({
            "domain": "retail",
            "scale": "small",
            "sinks": ["memory"],
            "sink_config": {},
            "interval_seconds": 10,
            "chunk_size": 5,
            "max_chunks": None,
        })
        stream_id = start_result["stream_id"]
        time.sleep(0.1)
        stop_result = cmd_stream_stop({"stream_id": stream_id})
        assert stop_result.get("status") in ("stopped", "stop_timeout")
        assert stop_result.get("stream_id") == stream_id

    def test_stream_stop_unknown_id(self):
        stop_result = cmd_stream_stop({"stream_id": "nonexistent-uuid"})
        assert "error" in stop_result

    def test_stream_status_unknown_id(self):
        status = cmd_stream_status({"stream_id": "nonexistent-uuid"})
        assert "error" in status
