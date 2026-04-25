"""Tests for GSLParser and GenerationSpec."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from sqllocks_spindle.specs.gsl_parser import (
    ChaosSpec,
    DateRangeSpec,
    GenerationSpec,
    GSLParser,
    LakehouseOutputSpec,
    OutputsSpec,
    SchemaRef,
    ScenarioRef,
    ValidationGateSpec,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(path: Path, data: dict) -> Path:
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


def _minimal_spec() -> dict:
    return {
        "version": 1,
        "name": "test_spec",
        "schema": {"type": "domain", "domain": "retail"},
        "scenario": {"pack": "fd_daily_batch", "scale": "small", "seed": 42},
    }


# ---------------------------------------------------------------------------
# GSLParser.parse_dict — minimal
# ---------------------------------------------------------------------------

class TestGSLParserMinimal:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_parse_dict_returns_generation_spec(self, parser):
        spec = parser.parse_dict(_minimal_spec())
        assert isinstance(spec, GenerationSpec)

    def test_version_parsed(self, parser):
        spec = parser.parse_dict(_minimal_spec())
        assert spec.version == 1

    def test_name_parsed(self, parser):
        spec = parser.parse_dict(_minimal_spec())
        assert spec.name == "test_spec"

    def test_defaults_when_missing(self, parser):
        spec = parser.parse_dict({})
        assert spec.version == 1
        assert spec.name == ""

    def test_none_sections_when_absent(self, parser):
        spec = parser.parse_dict({})
        assert spec.schema is None
        assert spec.scenario is None
        assert spec.chaos is None
        assert spec.outputs is None
        assert spec.validation is None


# ---------------------------------------------------------------------------
# schema section
# ---------------------------------------------------------------------------

class TestGSLParserSchemaRef:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_domain_type(self, parser):
        spec = parser.parse_dict({"schema": {"type": "domain", "domain": "retail"}})
        assert spec.schema is not None
        assert spec.schema.type == "domain"
        assert spec.schema.domain == "retail"

    def test_path_type(self, parser):
        spec = parser.parse_dict({"schema": {"type": "spindle_json", "path": "my_schema.json"}})
        assert spec.schema.type == "spindle_json"
        assert spec.schema.path == "my_schema.json"

    def test_defaults(self, parser):
        spec = parser.parse_dict({"schema": {}})
        assert spec.schema.type == "domain"


# ---------------------------------------------------------------------------
# scenario section
# ---------------------------------------------------------------------------

class TestGSLParserScenarioRef:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_pack_and_scale(self, parser):
        spec = parser.parse_dict({"scenario": {"pack": "my_pack", "scale": "medium"}})
        assert spec.scenario.pack == "my_pack"
        assert spec.scenario.scale == "medium"

    def test_seed_parsed(self, parser):
        spec = parser.parse_dict({"scenario": {"seed": 99}})
        assert spec.scenario.seed == 99

    def test_date_range_parsed(self, parser):
        spec = parser.parse_dict({
            "scenario": {
                "pack": "x",
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            }
        })
        assert spec.scenario.date_range is not None
        assert spec.scenario.date_range.start == "2024-01-01"
        assert spec.scenario.date_range.end == "2024-12-31"

    def test_no_date_range_is_none(self, parser):
        spec = parser.parse_dict({"scenario": {"pack": "x"}})
        assert spec.scenario.date_range is None

    def test_defaults(self, parser):
        spec = parser.parse_dict({"scenario": {}})
        assert spec.scenario.scale == "small"
        assert spec.scenario.seed == 42


# ---------------------------------------------------------------------------
# chaos section
# ---------------------------------------------------------------------------

class TestGSLParserChaos:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_chaos_enabled(self, parser):
        spec = parser.parse_dict({"chaos": {"enabled": True, "intensity": "high"}})
        assert spec.chaos.enabled is True
        assert spec.chaos.intensity == "high"

    def test_chaos_disabled(self, parser):
        spec = parser.parse_dict({"chaos": {"enabled": False}})
        assert spec.chaos.enabled is False

    def test_extra_keys_go_to_config(self, parser):
        spec = parser.parse_dict({"chaos": {"enabled": True, "seed": 77, "warmup_days": 5}})
        assert spec.chaos.config.get("seed") == 77
        assert spec.chaos.config.get("warmup_days") == 5

    def test_known_keys_not_in_config(self, parser):
        spec = parser.parse_dict({"chaos": {"enabled": True, "intensity": "moderate"}})
        assert "enabled" not in spec.chaos.config
        assert "intensity" not in spec.chaos.config

    def test_defaults(self, parser):
        spec = parser.parse_dict({"chaos": {}})
        assert spec.chaos.enabled is False
        assert spec.chaos.intensity == "moderate"


# ---------------------------------------------------------------------------
# outputs section
# ---------------------------------------------------------------------------

class TestGSLParserOutputs:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_lakehouse_output(self, parser):
        spec = parser.parse_dict({
            "outputs": {
                "lakehouse": {
                    "mode": "tables_only",
                    "tables": ["customer", "order"],
                }
            }
        })
        assert spec.outputs.lakehouse is not None
        assert spec.outputs.lakehouse.mode == "tables_only"
        assert "customer" in spec.outputs.lakehouse.tables

    def test_lakehouse_landing_zone(self, parser):
        spec = parser.parse_dict({
            "outputs": {
                "lakehouse": {
                    "landing_zone": {"root": "Files/landing"},
                }
            }
        })
        assert spec.outputs.lakehouse.landing_zone is not None
        assert spec.outputs.lakehouse.landing_zone.root == "Files/landing"

    def test_eventstream_output(self, parser):
        spec = parser.parse_dict({
            "outputs": {
                "eventstream": {
                    "enabled": True,
                    "endpoint_secret_ref": "my_secret",
                    "topics": [{"name": "orders", "event_type": "retail.order.created"}],
                }
            }
        })
        assert spec.outputs.eventstream is not None
        assert spec.outputs.eventstream.enabled is True
        assert len(spec.outputs.eventstream.topics) == 1
        assert spec.outputs.eventstream.topics[0].name == "orders"

    def test_no_lakehouse_is_none(self, parser):
        spec = parser.parse_dict({"outputs": {"eventstream": {"enabled": False}}})
        assert spec.outputs.lakehouse is None


# ---------------------------------------------------------------------------
# validation section
# ---------------------------------------------------------------------------

class TestGSLParserValidation:
    @pytest.fixture
    def parser(self):
        return GSLParser()

    def test_gates_list(self, parser):
        spec = parser.parse_dict({
            "validation": {"gates": ["schema_conformance", "null_check"]}
        })
        assert "schema_conformance" in spec.validation.gates
        assert "null_check" in spec.validation.gates

    def test_drift_policy(self, parser):
        spec = parser.parse_dict({
            "validation": {"drift_policy": "fail_on_breaking_change"}
        })
        assert spec.validation.drift_policy == "fail_on_breaking_change"

    def test_defaults(self, parser):
        spec = parser.parse_dict({"validation": {}})
        assert spec.validation.gates == []
        assert "quarantine" in spec.validation.drift_policy


# ---------------------------------------------------------------------------
# GSLParser.parse (file)
# ---------------------------------------------------------------------------

class TestGSLParserFile:
    def test_parse_file_returns_generation_spec(self, tmp_path):
        path = _write_yaml(tmp_path / "spec.yaml", _minimal_spec())
        spec = GSLParser().parse(path)
        assert isinstance(spec, GenerationSpec)

    def test_parse_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            GSLParser().parse("/nonexistent/spec.gsl.yaml")

    def test_base_dir_set_to_spec_parent(self, tmp_path):
        path = _write_yaml(tmp_path / "spec.yaml", _minimal_spec())
        spec = GSLParser().parse(path)
        assert spec._base_dir == tmp_path


# ---------------------------------------------------------------------------
# GenerationSpec.resolve_path
# ---------------------------------------------------------------------------

class TestGenerationSpecResolvePath:
    def test_absolute_path_returned_unchanged(self, tmp_path):
        abs_path = str(tmp_path / "file.json")
        spec = GenerationSpec(_base_dir=tmp_path)
        resolved = spec.resolve_path(abs_path)
        assert resolved == Path(abs_path)

    def test_relative_path_resolved_against_base(self, tmp_path):
        spec = GenerationSpec(_base_dir=tmp_path)
        resolved = spec.resolve_path("subdir/schema.json")
        assert resolved == tmp_path / "subdir" / "schema.json"


# ---------------------------------------------------------------------------
# GSLParser — edge cases
# ---------------------------------------------------------------------------

class TestGSLParserEdgeCases:
    def test_parse_malformed_yaml_raises(self, tmp_path):
        bad_path = tmp_path / "bad.yaml"
        bad_path.write_text("invalid: yaml: :\n  - bad\n bad_indent", encoding="utf-8")
        with pytest.raises(Exception):  # yaml.YAMLError or subclass
            GSLParser().parse(bad_path)

    def test_parse_dict_empty_returns_defaults(self):
        spec = GSLParser().parse_dict({})
        assert isinstance(spec, GenerationSpec)
        assert spec.version == 1
        assert spec.name == ""
        assert spec.schema is None
        assert spec.scenario is None
