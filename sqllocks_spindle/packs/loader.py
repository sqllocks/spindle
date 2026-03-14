"""Load scenario pack YAML files into ScenarioPack dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml


# ---------------------------------------------------------------------------
# Nested spec dataclasses — mirror the YAML structure
# ---------------------------------------------------------------------------

@dataclass
class ManifestSpec:
    """File-drop manifest configuration."""
    enabled: bool = True
    name: str = "manifest_{dt}.json"


@dataclass
class DoneFlagSpec:
    """Done-flag marker configuration."""
    enabled: bool = True
    name: str = "done_{dt}.flag"


@dataclass
class LatenessSpec:
    """Late-arriving data configuration."""
    enabled: bool = False
    probability: float = 0.0
    max_days_late: int = 0


@dataclass
class DuplicateSpec:
    """Duplicate record injection configuration."""
    enabled: bool = False
    probability: float = 0.0


@dataclass
class BackfillSpec:
    """Historical backfill configuration."""
    enabled: bool = False
    max_days_back: int = 0


@dataclass
class FileDropSpec:
    """File-drop landing zone specification."""
    cadence: str = "daily"
    partitioning: str = "dt=YYYY-MM-DD"
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    file_naming: str = "{domain}_{entity}_{dt}_{seq}.parquet"
    entities: list[str] = field(default_factory=list)
    manifest: Optional[ManifestSpec] = None
    done_flag: Optional[DoneFlagSpec] = None
    lateness: Optional[LatenessSpec] = None
    duplicates: Optional[DuplicateSpec] = None
    backfill: Optional[BackfillSpec] = None


@dataclass
class StreamEnvelopeSpec:
    """Envelope schema for stream events."""
    schemaVersion: str = "1.0"
    fields: list[str] = field(default_factory=list)


@dataclass
class StreamCadenceSpec:
    """Rate and timing configuration for streams."""
    rate_per_sec: float = 10.0
    realtime: bool = True
    jitter_ms: int = 0
    burst: Optional[dict[str, Any]] = None


@dataclass
class StreamOrderingSpec:
    """Out-of-order event configuration."""
    out_of_order_probability: float = 0.0
    max_delay_seconds: int = 0


@dataclass
class StreamReplaySpec:
    """Event replay window configuration."""
    enabled: bool = False
    window_minutes: int = 15


@dataclass
class StreamTopicSpec:
    """Definition of a single stream topic."""
    name: str = ""
    event_type: str = ""
    payload_fields: list[str] = field(default_factory=list)


@dataclass
class StreamAnomalySpec:
    """Anomaly injection configuration for streams."""
    enabled: bool = False
    types: list[str] = field(default_factory=list)


@dataclass
class StreamSpec:
    """Streaming specification."""
    envelope: Optional[StreamEnvelopeSpec] = None
    cadence: Optional[StreamCadenceSpec] = None
    ordering: Optional[StreamOrderingSpec] = None
    replay: Optional[StreamReplaySpec] = None
    topics: list[StreamTopicSpec] = field(default_factory=list)
    anomalies: Optional[StreamAnomalySpec] = None


@dataclass
class HybridMicroBatchSpec:
    """Micro-batch portion of hybrid mode."""
    cadence: str = "every_15m"
    formats: list[str] = field(default_factory=lambda: ["jsonl"])
    partitioning: str = "dt=YYYY-MM-DD/hour=HH"
    entities: list[str] = field(default_factory=list)


@dataclass
class HybridStreamSpec:
    """Stream portion of hybrid mode."""
    rate_per_sec: float = 10.0
    topics: list[StreamTopicSpec] = field(default_factory=list)


@dataclass
class HybridLinkStrategySpec:
    """How stream and batch data link together."""
    correlation_id: bool = True
    natural_keys: bool = True


@dataclass
class HybridSpec:
    """Hybrid (stream + micro-batch) specification."""
    stream_to: str = "eventhouse"
    micro_batch_to: str = "lakehouse_files"
    micro_batch: Optional[HybridMicroBatchSpec] = None
    stream: Optional[HybridStreamSpec] = None
    link_strategy: Optional[HybridLinkStrategySpec] = None


@dataclass
class SchemaDriftSpec:
    """Schema drift injection configuration."""
    enabled: bool = False
    mode: str = "additive"
    breaking_change_day: int = 0


@dataclass
class FailureInjectionSpec:
    """Failure injection specification."""
    enabled: bool = False
    corrupt_file_probability: float = 0.0
    partial_write_probability: float = 0.0
    schema_drift: Optional[SchemaDriftSpec] = None


@dataclass
class ValidationSpec:
    """Validation gate configuration."""
    required_gates: list[str] = field(default_factory=list)
    quarantine_folder: Optional[str] = None


# ---------------------------------------------------------------------------
# Top-level ScenarioPack
# ---------------------------------------------------------------------------

@dataclass
class ScenarioPack:
    """Complete scenario pack definition loaded from YAML."""
    pack_version: int
    id: str
    kind: str  # file_drop | stream | hybrid
    domain: str
    description: str
    fabric_targets: dict[str, Any]
    file_drop: Optional[FileDropSpec] = None
    streaming: Optional[StreamSpec] = None
    hybrid: Optional[HybridSpec] = None
    failure_injection: Optional[FailureInjectionSpec] = None
    validation: Optional[ValidationSpec] = None
    chaos: Optional[dict[str, Any]] = None

    @property
    def entities(self) -> list[str]:
        """Return the list of entities referenced in this pack."""
        if self.file_drop and self.file_drop.entities:
            return self.file_drop.entities
        if self.hybrid and self.hybrid.micro_batch and self.hybrid.micro_batch.entities:
            return self.hybrid.micro_batch.entities
        return []

    @property
    def topics(self) -> list[StreamTopicSpec]:
        """Return the list of stream topics referenced in this pack."""
        if self.streaming and self.streaming.topics:
            return self.streaming.topics
        if self.hybrid and self.hybrid.stream and self.hybrid.stream.topics:
            return self.hybrid.stream.topics
        return []


# ---------------------------------------------------------------------------
# PackLoader
# ---------------------------------------------------------------------------

# Default location of bundled packs (relative to project root)
_BUILTIN_PACKS_ROOT = Path(__file__).resolve().parent.parent.parent / "scenario_packs_extracted" / "packs"


class PackLoader:
    """Load scenario pack YAML files into ScenarioPack instances."""

    def __init__(self, builtin_root: str | Path | None = None):
        self._builtin_root = Path(builtin_root) if builtin_root else _BUILTIN_PACKS_ROOT

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, path: str | Path) -> ScenarioPack:
        """Load a scenario pack from an arbitrary YAML file path."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Scenario pack not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        return self._parse(raw)

    def load_builtin(self, domain: str, pack_id: str) -> ScenarioPack:
        """Load a builtin scenario pack by domain and pack ID.

        Example::

            loader = PackLoader()
            pack = loader.load_builtin("retail", "fd_daily_batch")
        """
        pack_dir = self._builtin_root / domain
        if not pack_dir.exists():
            raise FileNotFoundError(
                f"No builtin packs for domain '{domain}'. "
                f"Available: {', '.join(self._list_domains())}"
            )
        pack_file = pack_dir / f"{pack_id}.yaml"
        if not pack_file.exists():
            available = [f.stem for f in pack_dir.glob("*.yaml")]
            raise FileNotFoundError(
                f"Pack '{pack_id}' not found in domain '{domain}'. "
                f"Available: {', '.join(available)}"
            )
        return self.load(pack_file)

    def list_builtin(self) -> list[dict[str, str]]:
        """Return metadata for all available builtin packs.

        Returns a list of dicts with keys: domain, pack_id, path.
        """
        results: list[dict[str, str]] = []
        if not self._builtin_root.exists():
            return results
        for domain_dir in sorted(self._builtin_root.iterdir()):
            if not domain_dir.is_dir():
                continue
            for yaml_file in sorted(domain_dir.glob("*.yaml")):
                results.append({
                    "domain": domain_dir.name,
                    "pack_id": yaml_file.stem,
                    "path": str(yaml_file),
                })
        return results

    # ------------------------------------------------------------------
    # Internal parsing
    # ------------------------------------------------------------------

    def _list_domains(self) -> list[str]:
        if not self._builtin_root.exists():
            return []
        return sorted(d.name for d in self._builtin_root.iterdir() if d.is_dir())

    def _parse(self, raw: dict[str, Any]) -> ScenarioPack:
        return ScenarioPack(
            pack_version=raw.get("pack_version", 1),
            id=raw.get("id", "unknown"),
            kind=raw.get("kind", "file_drop"),
            domain=raw.get("domain", ""),
            description=raw.get("description", ""),
            fabric_targets=raw.get("fabric_targets", {}),
            file_drop=self._parse_file_drop(raw.get("file_drop")),
            streaming=self._parse_streaming(raw.get("streaming")),
            hybrid=self._parse_hybrid(raw.get("hybrid")),
            failure_injection=self._parse_failure_injection(raw.get("failure_injection")),
            validation=self._parse_validation(raw.get("validation")),
            chaos=raw.get("chaos"),
        )

    def _parse_file_drop(self, raw: dict | None) -> FileDropSpec | None:
        if raw is None:
            return None
        return FileDropSpec(
            cadence=raw.get("cadence", "daily"),
            partitioning=raw.get("partitioning", "dt=YYYY-MM-DD"),
            formats=raw.get("formats", ["parquet"]),
            file_naming=raw.get("file_naming", "{domain}_{entity}_{dt}_{seq}.parquet"),
            entities=raw.get("entities", []),
            manifest=self._parse_manifest(raw.get("manifest")),
            done_flag=self._parse_done_flag(raw.get("done_flag")),
            lateness=self._parse_lateness(raw.get("lateness")),
            duplicates=self._parse_duplicates(raw.get("duplicates")),
            backfill=self._parse_backfill(raw.get("backfill")),
        )

    def _parse_manifest(self, raw: dict | None) -> ManifestSpec | None:
        if raw is None:
            return None
        return ManifestSpec(
            enabled=raw.get("enabled", True),
            name=raw.get("name", "manifest_{dt}.json"),
        )

    def _parse_done_flag(self, raw: dict | None) -> DoneFlagSpec | None:
        if raw is None:
            return None
        return DoneFlagSpec(
            enabled=raw.get("enabled", True),
            name=raw.get("name", "done_{dt}.flag"),
        )

    def _parse_lateness(self, raw: dict | None) -> LatenessSpec | None:
        if raw is None:
            return None
        return LatenessSpec(
            enabled=raw.get("enabled", False),
            probability=raw.get("probability", 0.0),
            max_days_late=raw.get("max_days_late", 0),
        )

    def _parse_duplicates(self, raw: dict | None) -> DuplicateSpec | None:
        if raw is None:
            return None
        return DuplicateSpec(
            enabled=raw.get("enabled", False),
            probability=raw.get("probability", 0.0),
        )

    def _parse_backfill(self, raw: dict | None) -> BackfillSpec | None:
        if raw is None:
            return None
        return BackfillSpec(
            enabled=raw.get("enabled", False),
            max_days_back=raw.get("max_days_back", 0),
        )

    def _parse_streaming(self, raw: dict | None) -> StreamSpec | None:
        if raw is None:
            return None
        return StreamSpec(
            envelope=self._parse_stream_envelope(raw.get("envelope")),
            cadence=self._parse_stream_cadence(raw.get("cadence")),
            ordering=self._parse_stream_ordering(raw.get("ordering")),
            replay=self._parse_stream_replay(raw.get("replay")),
            topics=[self._parse_stream_topic(t) for t in raw.get("topics", [])],
            anomalies=self._parse_stream_anomalies(raw.get("anomalies")),
        )

    def _parse_stream_envelope(self, raw: dict | None) -> StreamEnvelopeSpec | None:
        if raw is None:
            return None
        return StreamEnvelopeSpec(
            schemaVersion=raw.get("schemaVersion", "1.0"),
            fields=raw.get("fields", []),
        )

    def _parse_stream_cadence(self, raw: dict | None) -> StreamCadenceSpec | None:
        if raw is None:
            return None
        return StreamCadenceSpec(
            rate_per_sec=raw.get("rate_per_sec", 10.0),
            realtime=raw.get("realtime", True),
            jitter_ms=raw.get("jitter_ms", 0),
            burst=raw.get("burst"),
        )

    def _parse_stream_ordering(self, raw: dict | None) -> StreamOrderingSpec | None:
        if raw is None:
            return None
        return StreamOrderingSpec(
            out_of_order_probability=raw.get("out_of_order_probability", 0.0),
            max_delay_seconds=raw.get("max_delay_seconds", 0),
        )

    def _parse_stream_replay(self, raw: dict | None) -> StreamReplaySpec | None:
        if raw is None:
            return None
        return StreamReplaySpec(
            enabled=raw.get("enabled", False),
            window_minutes=raw.get("window_minutes", 15),
        )

    def _parse_stream_topic(self, raw: dict) -> StreamTopicSpec:
        return StreamTopicSpec(
            name=raw.get("name", ""),
            event_type=raw.get("event_type", ""),
            payload_fields=raw.get("payload_fields", []),
        )

    def _parse_stream_anomalies(self, raw: dict | None) -> StreamAnomalySpec | None:
        if raw is None:
            return None
        return StreamAnomalySpec(
            enabled=raw.get("enabled", False),
            types=raw.get("types", []),
        )

    def _parse_hybrid(self, raw: dict | None) -> HybridSpec | None:
        if raw is None:
            return None
        # Parse micro_batch
        mb_raw = raw.get("micro_batch")
        micro_batch = None
        if mb_raw:
            micro_batch = HybridMicroBatchSpec(
                cadence=mb_raw.get("cadence", "every_15m"),
                formats=mb_raw.get("formats", ["jsonl"]),
                partitioning=mb_raw.get("partitioning", "dt=YYYY-MM-DD/hour=HH"),
                entities=mb_raw.get("entities", []),
            )
        # Parse stream portion
        st_raw = raw.get("stream")
        stream = None
        if st_raw:
            stream = HybridStreamSpec(
                rate_per_sec=st_raw.get("rate_per_sec", 10.0),
                topics=[self._parse_stream_topic(t) for t in st_raw.get("topics", [])],
            )
        # Parse link strategy
        ls_raw = raw.get("link_strategy")
        link_strategy = None
        if ls_raw:
            link_strategy = HybridLinkStrategySpec(
                correlation_id=ls_raw.get("correlation_id", True),
                natural_keys=ls_raw.get("natural_keys", True),
            )
        return HybridSpec(
            stream_to=raw.get("stream_to", "eventhouse"),
            micro_batch_to=raw.get("micro_batch_to", "lakehouse_files"),
            micro_batch=micro_batch,
            stream=stream,
            link_strategy=link_strategy,
        )

    def _parse_failure_injection(self, raw: dict | None) -> FailureInjectionSpec | None:
        if raw is None:
            return None
        sd_raw = raw.get("schema_drift")
        schema_drift = None
        if sd_raw:
            schema_drift = SchemaDriftSpec(
                enabled=sd_raw.get("enabled", False),
                mode=sd_raw.get("mode", "additive"),
                breaking_change_day=sd_raw.get("breaking_change_day", 0),
            )
        return FailureInjectionSpec(
            enabled=raw.get("enabled", False),
            corrupt_file_probability=raw.get("corrupt_file_probability", 0.0),
            partial_write_probability=raw.get("partial_write_probability", 0.0),
            schema_drift=schema_drift,
        )

    def _parse_validation(self, raw: dict | None) -> ValidationSpec | None:
        if raw is None:
            return None
        return ValidationSpec(
            required_gates=raw.get("required_gates", []),
            quarantine_folder=raw.get("quarantine_folder"),
        )
