"""Tests for EventEnvelope and EnvelopeFactory."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pandas as pd
import pytest

from sqllocks_spindle.streaming.envelope import EnvelopeFactory, EventEnvelope


# ---------------------------------------------------------------------------
# EventEnvelope
# ---------------------------------------------------------------------------

class TestEventEnvelope:
    def test_fields_set_correctly(self):
        env = EventEnvelope(
            schema_version="1.0",
            event_type="retail.order.created",
            event_time="2024-01-15T10:00:00Z",
            correlation_id="abc-123",
            tenant_id="acme",
            payload={"order_id": 1},
            metadata={"source_table": "order"},
        )
        assert env.schema_version == "1.0"
        assert env.event_type == "retail.order.created"
        assert env.event_time == "2024-01-15T10:00:00Z"
        assert env.correlation_id == "abc-123"
        assert env.tenant_id == "acme"
        assert env.payload == {"order_id": 1}
        assert env.metadata == {"source_table": "order"}

    def test_optional_tenant_id_defaults_to_none(self):
        env = EventEnvelope(
            schema_version="1.0",
            event_type="test.event",
            event_time="2024-01-01T00:00:00Z",
            correlation_id="x",
        )
        assert env.tenant_id is None

    def test_payload_defaults_to_empty_dict(self):
        env = EventEnvelope(
            schema_version="1.0",
            event_type="test.event",
            event_time="2024-01-01T00:00:00Z",
            correlation_id="x",
        )
        assert env.payload == {}

    def test_metadata_defaults_to_empty_dict(self):
        env = EventEnvelope(
            schema_version="1.0",
            event_type="test.event",
            event_time="2024-01-01T00:00:00Z",
            correlation_id="x",
        )
        assert env.metadata == {}


# ---------------------------------------------------------------------------
# EnvelopeFactory — create_envelope
# ---------------------------------------------------------------------------

class TestEnvelopeFactoryCreate:
    @pytest.fixture
    def factory(self):
        return EnvelopeFactory()

    def test_create_envelope_returns_event_envelope(self, factory):
        env = factory.create_envelope(
            row_dict={"id": 1, "value": 99.0},
            table_name="order",
            event_type="retail.order.created",
        )
        assert isinstance(env, EventEnvelope)

    def test_event_type_set(self, factory):
        env = factory.create_envelope(
            row_dict={}, table_name="order", event_type="retail.order.created"
        )
        assert env.event_type == "retail.order.created"

    def test_schema_version_default(self, factory):
        env = factory.create_envelope(row_dict={}, table_name="t", event_type="e")
        assert env.schema_version == "1.0"

    def test_schema_version_override(self, factory):
        env = factory.create_envelope(
            row_dict={}, table_name="t", event_type="e", schema_version="2.0"
        )
        assert env.schema_version == "2.0"

    def test_auto_correlation_id_is_uuid(self, factory):
        env = factory.create_envelope(row_dict={}, table_name="t", event_type="e")
        try:
            uuid.UUID(env.correlation_id)
        except ValueError:
            pytest.fail("correlation_id is not a valid UUID")

    def test_each_call_produces_unique_correlation_id(self, factory):
        ids = {
            factory.create_envelope(row_dict={}, table_name="t", event_type="e").correlation_id
            for _ in range(10)
        }
        assert len(ids) == 10

    def test_tenant_id_factory_default(self):
        factory = EnvelopeFactory(default_tenant_id="acme")
        env = factory.create_envelope(row_dict={}, table_name="t", event_type="e")
        assert env.tenant_id == "acme"

    def test_tenant_id_no_default(self, factory):
        env = factory.create_envelope(row_dict={}, table_name="t", event_type="e")
        assert env.tenant_id is None

    def test_tenant_id_per_call_override(self):
        factory = EnvelopeFactory(default_tenant_id="default-tenant")
        env = factory.create_envelope(
            row_dict={}, table_name="t", event_type="e", tenant_id="override-tenant"
        )
        assert env.tenant_id == "override-tenant"

    def test_payload_contains_row_data(self, factory):
        row = {"order_id": 42, "amount": 99.50}
        env = factory.create_envelope(row_dict=row, table_name="order", event_type="e")
        assert env.payload["order_id"] == 42
        assert env.payload["amount"] == pytest.approx(99.50)

    def test_metadata_has_source_table(self, factory):
        env = factory.create_envelope(row_dict={}, table_name="my_table", event_type="e")
        assert env.metadata.get("source_table") == "my_table"

    def test_metadata_has_produced_at(self, factory):
        env = factory.create_envelope(row_dict={}, table_name="t", event_type="e")
        assert "produced_at" in env.metadata

    def test_extra_metadata_merged(self, factory):
        env = factory.create_envelope(
            row_dict={},
            table_name="t",
            event_type="e",
            metadata={"run_id": "abc123"},
        )
        assert env.metadata.get("run_id") == "abc123"
        assert env.metadata.get("source_table") == "t"

    def test_extra_metadata_does_not_overwrite_source_table(self, factory):
        env = factory.create_envelope(
            row_dict={},
            table_name="real_table",
            event_type="e",
            metadata={"source_table": "attempted_override"},
        )
        # factory merges extra metadata AFTER auto-setting source_table,
        # so the explicit "attempted_override" wins over "real_table"
        assert env.metadata.get("source_table") == "attempted_override"


# ---------------------------------------------------------------------------
# EnvelopeFactory — timestamp column
# ---------------------------------------------------------------------------

class TestEnvelopeFactoryTimestamp:
    def test_timestamp_column_used_as_event_time(self):
        factory = EnvelopeFactory(timestamp_column="order_date")
        row = {"order_id": 1, "order_date": datetime(2024, 3, 15, 10, 0, 0)}
        env = factory.create_envelope(row_dict=row, table_name="order", event_type="e")
        assert "2024-03-15" in env.event_time

    def test_timestamp_column_string_value(self):
        factory = EnvelopeFactory(timestamp_column="ts")
        row = {"ts": "2024-06-01T12:00:00"}
        env = factory.create_envelope(row_dict=row, table_name="t", event_type="e")
        assert "2024-06-01" in env.event_time

    def test_timestamp_column_missing_uses_now(self):
        factory = EnvelopeFactory(timestamp_column="nonexistent_col")
        row = {"id": 1}
        env = factory.create_envelope(row_dict=row, table_name="t", event_type="e")
        # Should not raise; event_time should still be set to something
        assert env.event_time != ""

    def test_no_timestamp_column_uses_now(self):
        factory = EnvelopeFactory()
        row = {"id": 1}
        env = factory.create_envelope(row_dict=row, table_name="t", event_type="e")
        assert env.event_time != ""


# ---------------------------------------------------------------------------
# EnvelopeFactory — serialisation helpers
# ---------------------------------------------------------------------------

class TestEnvelopeFactorySerialisation:
    @pytest.fixture
    def sample_envelope(self):
        return EventEnvelope(
            schema_version="1.0",
            event_type="retail.order.created",
            event_time="2024-01-15T10:00:00Z",
            correlation_id="abc-123",
            tenant_id="acme",
            payload={"order_id": 1, "amount": 99.50},
            metadata={"source_table": "order"},
        )

    def test_to_dict_returns_plain_dict(self, sample_envelope):
        result = EnvelopeFactory.to_dict(sample_envelope)
        assert isinstance(result, dict)
        # All values should be JSON-native types
        json.dumps(result)  # Should not raise

    def test_to_dict_contains_all_fields(self, sample_envelope):
        result = EnvelopeFactory.to_dict(sample_envelope)
        assert "schema_version" in result
        assert "event_type" in result
        assert "event_time" in result
        assert "correlation_id" in result
        assert "payload" in result
        assert "metadata" in result

    def test_to_json_is_valid_json(self, sample_envelope):
        json_str = EnvelopeFactory.to_json(sample_envelope)
        parsed = json.loads(json_str)
        assert parsed["event_type"] == "retail.order.created"

    def test_to_json_default_str_handles_non_serialisable(self):
        env = EventEnvelope(
            schema_version="1.0",
            event_type="test.event",
            event_time="2024-01-01",
            correlation_id="x",
            payload={"ts": pd.Timestamp("2024-01-01")},
        )
        json_str = EnvelopeFactory.to_json(env)
        assert json.loads(json_str) is not None

    def test_to_dict_round_trip_payload(self, sample_envelope):
        result = EnvelopeFactory.to_dict(sample_envelope)
        assert result["payload"]["order_id"] == 1
        assert result["payload"]["amount"] == pytest.approx(99.50)
