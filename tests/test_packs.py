"""Tests for PackLoader, PackValidator, and ScenarioPack."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from sqllocks_spindle.packs.loader import (
    PackLoader,
    ScenarioPack,
    StreamTopicSpec,
    FileDropSpec,
    StreamSpec,
)
from sqllocks_spindle.packs.validator import PackValidationResult, PackValidator
from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    SpindleSchema,
    TableDef,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(path: Path, data: dict) -> Path:
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


def _minimal_pack_dict(**overrides) -> dict:
    base = {
        "pack_version": 1,
        "id": "test_pack",
        "kind": "file_drop",
        "domain": "retail",
        "description": "A test pack",
        "fabric_targets": {"lakehouse": "my_lakehouse"},
        "file_drop": {
            "cadence": "daily",
            "formats": ["parquet"],
            "entities": ["order"],
        },
    }
    base.update(overrides)
    return base


class MockDomain:
    """Minimal domain mock for PackValidator tests."""

    name = "retail"

    def get_schema(self):
        col = ColumnDef(name="order_id", type="integer", generator={"strategy": "sequence"})
        table = TableDef(name="order", columns={"order_id": col}, primary_key=["order_id"])
        return SpindleSchema(
            model=ModelDef(name="retail"),
            tables={"order": table},
            relationships=[],
            business_rules=[],
            generation=GenerationConfig(),
        )


# ---------------------------------------------------------------------------
# ScenarioPack properties
# ---------------------------------------------------------------------------

class TestScenarioPackProperties:
    def test_entities_from_file_drop(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(
                tmp_path / "test_pack_entities.yaml",
                _minimal_pack_dict(),
            )
        )
        assert "order" in pack.entities

    def test_entities_empty_when_no_file_drop(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(
                tmp_path / "test_pack_no_entities.yaml",
                {
                    "pack_version": 1, "id": "x", "kind": "stream", "domain": "retail",
                    "description": "", "fabric_targets": {},
                    "streaming": {"topics": [{"name": "t1", "event_type": "e1"}]},
                },
            )
        )
        assert pack.entities == []

    def test_topics_from_streaming(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(
                tmp_path / "test_pack_topics.yaml",
                {
                    "pack_version": 1, "id": "x", "kind": "stream", "domain": "retail",
                    "description": "", "fabric_targets": {},
                    "streaming": {
                        "topics": [
                            {"name": "orders_topic", "event_type": "retail.order.created"},
                        ]
                    },
                },
            )
        )
        assert len(pack.topics) == 1
        assert pack.topics[0].name == "orders_topic"

    def test_topics_empty_when_no_streaming(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "p.yaml", _minimal_pack_dict())
        )
        assert pack.topics == []


# ---------------------------------------------------------------------------
# PackLoader.load
# ---------------------------------------------------------------------------

class TestPackLoader:
    def test_load_returns_scenario_pack(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )
        assert isinstance(pack, ScenarioPack)

    def test_load_pack_version(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )
        assert pack.pack_version == 1

    def test_load_kind(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )
        assert pack.kind == "file_drop"

    def test_load_domain(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )
        assert pack.domain == "retail"

    def test_load_file_drop_section(self, tmp_path):
        pack = PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )
        assert pack.file_drop is not None
        assert isinstance(pack.file_drop, FileDropSpec)
        assert pack.file_drop.cadence == "daily"

    def test_load_streaming_section(self, tmp_path):
        data = {
            "pack_version": 1, "id": "x", "kind": "stream", "domain": "retail",
            "description": "", "fabric_targets": {},
            "streaming": {
                "cadence": {"rate_per_sec": 20.0},
                "topics": [{"name": "t", "event_type": "e"}],
            },
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        assert pack.streaming is not None
        assert isinstance(pack.streaming, StreamSpec)

    def test_load_hybrid_section(self, tmp_path):
        data = {
            "pack_version": 1, "id": "x", "kind": "hybrid", "domain": "retail",
            "description": "", "fabric_targets": {},
            "hybrid": {
                "stream_to": "eventhouse",
                "micro_batch_to": "lakehouse_files",
                "micro_batch": {"cadence": "every_15m", "formats": ["jsonl"]},
            },
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        assert pack.hybrid is not None
        assert pack.hybrid.micro_batch is not None

    def test_load_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PackLoader().load("/totally/nonexistent/pack.yaml")

    def test_load_builtin_missing_domain_raises(self, tmp_path):
        loader = PackLoader(builtin_root=tmp_path)
        with pytest.raises(FileNotFoundError, match="nonexistent_domain"):
            loader.load_builtin("nonexistent_domain", "some_pack")

    def test_load_builtin_missing_pack_raises(self, tmp_path):
        (tmp_path / "retail").mkdir()
        loader = PackLoader(builtin_root=tmp_path)
        with pytest.raises(FileNotFoundError):
            loader.load_builtin("retail", "nonexistent_pack")

    def test_load_failure_injection(self, tmp_path):
        data = _minimal_pack_dict()
        data["failure_injection"] = {
            "enabled": True,
            "corrupt_file_probability": 0.1,
            "schema_drift": {"enabled": True, "mode": "additive", "breaking_change_day": 10},
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        assert pack.failure_injection is not None
        assert pack.failure_injection.enabled is True
        assert pack.failure_injection.schema_drift is not None

    def test_load_validation_section(self, tmp_path):
        data = _minimal_pack_dict()
        data["validation"] = {
            "required_gates": ["schema_conformance", "null_check"],
            "quarantine_folder": "quarantine/",
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        assert pack.validation is not None
        assert "schema_conformance" in pack.validation.required_gates

    def test_list_builtin_returns_list(self, tmp_path):
        domain_dir = tmp_path / "retail"
        domain_dir.mkdir()
        _write_yaml(domain_dir / "test_pack.yaml", _minimal_pack_dict())
        loader = PackLoader(builtin_root=tmp_path)
        results = loader.list_builtin()
        assert len(results) == 1
        assert results[0]["domain"] == "retail"
        assert results[0]["pack_id"] == "test_pack"


# ---------------------------------------------------------------------------
# PackValidator
# ---------------------------------------------------------------------------

class TestPackValidator:
    @pytest.fixture
    def validator(self):
        return PackValidator()

    @pytest.fixture
    def domain(self):
        return MockDomain()

    @pytest.fixture
    def valid_pack(self, tmp_path):
        return PackLoader().load(
            _write_yaml(tmp_path / "pack.yaml", _minimal_pack_dict())
        )

    def test_valid_pack_passes(self, validator, valid_pack, domain):
        result = validator.validate(valid_pack, domain)
        assert result.is_valid

    def test_unknown_entity_produces_error(self, validator, domain, tmp_path):
        data = _minimal_pack_dict()
        data["file_drop"]["entities"] = ["nonexistent_entity"]
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert not result.is_valid
        assert any("nonexistent_entity" in e for e in result.errors)

    def test_bad_kind_produces_error(self, validator, domain, tmp_path):
        data = _minimal_pack_dict(kind="invalid_kind")
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert not result.is_valid
        assert any("kind" in e.lower() or "invalid_kind" in e for e in result.errors)

    def test_missing_file_drop_section_error(self, validator, domain, tmp_path):
        data = {
            "pack_version": 1, "id": "x", "kind": "file_drop", "domain": "retail",
            "description": "", "fabric_targets": {"lh": "x"},
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert not result.is_valid
        assert any("file_drop" in e for e in result.errors)

    def test_missing_streaming_section_error(self, validator, domain, tmp_path):
        data = {
            "pack_version": 1, "id": "x", "kind": "stream", "domain": "retail",
            "description": "", "fabric_targets": {"lh": "x"},
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert not result.is_valid
        assert any("stream" in e.lower() for e in result.errors)

    def test_missing_hybrid_section_error(self, validator, domain, tmp_path):
        data = {
            "pack_version": 1, "id": "x", "kind": "hybrid", "domain": "retail",
            "description": "", "fabric_targets": {"lh": "x"},
        }
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert not result.is_valid

    def test_no_fabric_targets_warning(self, validator, domain, tmp_path):
        data = _minimal_pack_dict()
        data["fabric_targets"] = {}
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert any("fabric_targets" in w.lower() or "fabric" in w.lower() for w in result.warnings)

    def test_domain_mismatch_is_warning_not_error(self, validator, domain, tmp_path):
        data = _minimal_pack_dict(domain="financial")
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        # Mismatch triggers warning, not error
        assert any("domain" in w.lower() for w in result.warnings)

    def test_unknown_gate_is_warning(self, validator, domain, tmp_path):
        data = _minimal_pack_dict()
        data["validation"] = {"required_gates": ["my_custom_gate"]}
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert any("my_custom_gate" in w for w in result.warnings)

    def test_is_valid_with_only_warnings(self, validator, domain, tmp_path):
        data = _minimal_pack_dict()
        data["fabric_targets"] = {}
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        assert result.is_valid

    def test_summary_pass(self, validator, valid_pack, domain):
        result = validator.validate(valid_pack, domain)
        summary = result.summary()
        assert "PASS" in summary

    def test_summary_fail(self, validator, domain, tmp_path):
        data = _minimal_pack_dict(kind="bad_kind")
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        summary = result.summary()
        assert "FAIL" in summary

    def test_summary_shows_error_count(self, validator, domain, tmp_path):
        data = _minimal_pack_dict(kind="bad_kind")
        pack = PackLoader().load(_write_yaml(tmp_path / "pack.yaml", data))
        result = validator.validate(pack, domain)
        summary = result.summary()
        assert "ERROR" in summary
