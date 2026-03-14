"""Parse Generation Spec Language (GSL) YAML files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml


# ---------------------------------------------------------------------------
# Nested spec dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SchemaRef:
    """Reference to the data schema (spindle JSON or domain)."""
    type: str = "domain"  # spindle_json | domain
    path: Optional[str] = None
    domain: Optional[str] = None


@dataclass
class DateRangeSpec:
    """Date range for scenario simulation."""
    start: str = ""
    end: str = ""


@dataclass
class ScenarioRef:
    """Reference to a scenario pack and its runtime parameters."""
    pack: str = ""  # path or builtin ID
    scale: str = "small"
    seed: int = 42
    date_range: Optional[DateRangeSpec] = None


@dataclass
class ChaosSpec:
    """Chaos engineering configuration."""
    enabled: bool = False
    intensity: str = "moderate"  # low | moderate | high | extreme
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class LandingZoneSpec:
    """Lakehouse landing zone configuration."""
    root: str = ""


@dataclass
class LakehouseOutputSpec:
    """Lakehouse output configuration."""
    mode: str = "tables_and_files"  # tables_and_files | tables_only | files_only
    tables: list[str] = field(default_factory=list)
    landing_zone: Optional[LandingZoneSpec] = None


@dataclass
class EventstreamTopicSpec:
    """Single eventstream topic definition."""
    name: str = ""
    event_type: str = ""


@dataclass
class EventstreamOutputSpec:
    """Eventstream output configuration."""
    enabled: bool = False
    endpoint_secret_ref: str = ""
    topics: list[EventstreamTopicSpec] = field(default_factory=list)


@dataclass
class OutputsSpec:
    """Combined output specification."""
    lakehouse: Optional[LakehouseOutputSpec] = None
    eventstream: Optional[EventstreamOutputSpec] = None


@dataclass
class ValidationGateSpec:
    """Validation configuration for the generation run."""
    gates: list[str] = field(default_factory=list)
    drift_policy: str = "quarantine_on_breaking_change"


# ---------------------------------------------------------------------------
# Top-level GenerationSpec
# ---------------------------------------------------------------------------

@dataclass
class GenerationSpec:
    """Complete Generation Spec Language document.

    Represents a fully resolved GSL YAML file that ties together a schema,
    a scenario pack, chaos configuration, output targets, and validation gates.
    """
    version: int = 1
    name: str = ""
    schema: Optional[SchemaRef] = None
    scenario: Optional[ScenarioRef] = None
    chaos: Optional[ChaosSpec] = None
    outputs: Optional[OutputsSpec] = None
    validation: Optional[ValidationGateSpec] = None

    # Internal: base directory for resolving relative paths
    _base_dir: Path = field(default_factory=lambda: Path("."), repr=False)

    def resolve_path(self, relative: str) -> Path:
        """Resolve a relative path against the spec file's directory."""
        p = Path(relative)
        if p.is_absolute():
            return p
        return (self._base_dir / p).resolve()


# ---------------------------------------------------------------------------
# GSLParser
# ---------------------------------------------------------------------------

class GSLParser:
    """Parse Generation Spec Language (GSL) YAML files.

    Example::

        parser = GSLParser()
        spec = parser.parse("my_estate.gsl.yaml")
        print(spec.name, spec.scenario.scale)
    """

    def parse(self, path: str | Path) -> GenerationSpec:
        """Parse a GSL YAML file and return a GenerationSpec.

        Relative paths in the spec are resolved relative to the spec file's
        parent directory.
        """
        path = Path(path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"GSL spec not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        base_dir = path.parent
        return self._parse(raw, base_dir)

    def parse_dict(self, raw: dict[str, Any], base_dir: str | Path = ".") -> GenerationSpec:
        """Parse a GSL spec from a raw dict (useful for tests)."""
        return self._parse(raw, Path(base_dir))

    # ------------------------------------------------------------------
    # Internal parsing
    # ------------------------------------------------------------------

    def _parse(self, raw: dict[str, Any], base_dir: Path) -> GenerationSpec:
        return GenerationSpec(
            version=raw.get("version", 1),
            name=raw.get("name", ""),
            schema=self._parse_schema_ref(raw.get("schema")),
            scenario=self._parse_scenario_ref(raw.get("scenario")),
            chaos=self._parse_chaos(raw.get("chaos")),
            outputs=self._parse_outputs(raw.get("outputs")),
            validation=self._parse_validation(raw.get("validation")),
            _base_dir=base_dir,
        )

    def _parse_schema_ref(self, raw: dict | None) -> SchemaRef | None:
        if raw is None:
            return None
        return SchemaRef(
            type=raw.get("type", "domain"),
            path=raw.get("path"),
            domain=raw.get("domain"),
        )

    def _parse_scenario_ref(self, raw: dict | None) -> ScenarioRef | None:
        if raw is None:
            return None
        date_range = None
        dr_raw = raw.get("date_range")
        if dr_raw:
            date_range = DateRangeSpec(
                start=str(dr_raw.get("start", "")),
                end=str(dr_raw.get("end", "")),
            )
        return ScenarioRef(
            pack=raw.get("pack", ""),
            scale=raw.get("scale", "small"),
            seed=raw.get("seed", 42),
            date_range=date_range,
        )

    def _parse_chaos(self, raw: dict | None) -> ChaosSpec | None:
        if raw is None:
            return None
        # Pull out known keys; everything else goes to config
        enabled = raw.get("enabled", False)
        intensity = raw.get("intensity", "moderate")
        config = {k: v for k, v in raw.items() if k not in ("enabled", "intensity")}
        return ChaosSpec(
            enabled=enabled,
            intensity=intensity,
            config=config,
        )

    def _parse_outputs(self, raw: dict | None) -> OutputsSpec | None:
        if raw is None:
            return None
        return OutputsSpec(
            lakehouse=self._parse_lakehouse_output(raw.get("lakehouse")),
            eventstream=self._parse_eventstream_output(raw.get("eventstream")),
        )

    def _parse_lakehouse_output(self, raw: dict | None) -> LakehouseOutputSpec | None:
        if raw is None:
            return None
        lz_raw = raw.get("landing_zone")
        landing_zone = None
        if lz_raw:
            landing_zone = LandingZoneSpec(root=lz_raw.get("root", ""))
        return LakehouseOutputSpec(
            mode=raw.get("mode", "tables_and_files"),
            tables=raw.get("tables", []),
            landing_zone=landing_zone,
        )

    def _parse_eventstream_output(self, raw: dict | None) -> EventstreamOutputSpec | None:
        if raw is None:
            return None
        topics = []
        for t_raw in raw.get("topics", []):
            topics.append(EventstreamTopicSpec(
                name=t_raw.get("name", ""),
                event_type=t_raw.get("event_type", ""),
            ))
        return EventstreamOutputSpec(
            enabled=raw.get("enabled", False),
            endpoint_secret_ref=raw.get("endpoint_secret_ref", ""),
            topics=topics,
        )

    def _parse_validation(self, raw: dict | None) -> ValidationGateSpec | None:
        if raw is None:
            return None
        return ValidationGateSpec(
            gates=raw.get("gates", []),
            drift_policy=raw.get("drift_policy", "quarantine_on_breaking_change"),
        )
