"""Validate a ScenarioPack against a domain's schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqllocks_spindle.packs.loader import ScenarioPack


@dataclass
class PackValidationResult:
    """Result of validating a scenario pack against a domain."""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = []
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  ERROR: {e}")
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  WARN:  {w}")
        if self.is_valid and not self.warnings:
            lines.append("Pack validation: PASS")
        elif self.is_valid:
            lines.append(f"Pack validation: PASS ({len(self.warnings)} warnings)")
        else:
            lines.append(f"Pack validation: FAIL ({len(self.errors)} errors)")
        return "\n".join(lines)


class PackValidator:
    """Validate a ScenarioPack against a domain's schema.

    Checks that entities referenced in the pack exist in the domain,
    event types are plausible, and paths are well-formed.
    """

    def validate(self, pack: ScenarioPack, domain: Any) -> PackValidationResult:
        """Validate a pack against the given domain instance.

        Args:
            pack: The scenario pack to validate.
            domain: A Domain instance (e.g., RetailDomain()) whose schema
                provides the ground truth for entity/table names.

        Returns:
            PackValidationResult with errors and warnings.
        """
        result = PackValidationResult()

        # Resolve the domain's schema to get available table names
        try:
            schema = domain.get_schema()
            domain_tables = set(schema.table_names)
        except Exception as exc:
            result.errors.append(f"Failed to load domain schema: {exc}")
            return result

        # Check domain name alignment
        domain_name = getattr(domain, "name", None)
        if domain_name and pack.domain and pack.domain != domain_name:
            result.warnings.append(
                f"Pack domain '{pack.domain}' does not match domain '{domain_name}'"
            )

        # Check pack version
        if pack.pack_version < 1:
            result.errors.append(f"Invalid pack_version: {pack.pack_version}")

        # Check kind
        valid_kinds = {"file_drop", "stream", "hybrid"}
        if pack.kind not in valid_kinds:
            result.errors.append(
                f"Invalid kind '{pack.kind}'. Must be one of: {', '.join(sorted(valid_kinds))}"
            )

        # Check entities exist in domain
        self._validate_entities(pack, domain_tables, result)

        # Check topics reference plausible event types
        self._validate_topics(pack, domain_tables, result)

        # Check file_drop specifics
        if pack.kind == "file_drop":
            self._validate_file_drop(pack, result)

        # Check streaming specifics
        if pack.kind == "stream":
            self._validate_streaming(pack, result)

        # Check hybrid specifics
        if pack.kind == "hybrid":
            self._validate_hybrid(pack, result)

        # Check fabric_targets
        self._validate_fabric_targets(pack, result)

        # Check validation gates
        self._validate_gates(pack, result)

        return result

    def _validate_entities(
        self,
        pack: ScenarioPack,
        domain_tables: set[str],
        result: PackValidationResult,
    ) -> None:
        """Check that referenced entities exist as tables in the domain."""
        for entity in pack.entities:
            if entity not in domain_tables:
                result.errors.append(
                    f"Entity '{entity}' referenced in pack but not found in domain schema. "
                    f"Available tables: {', '.join(sorted(domain_tables))}"
                )

    def _validate_topics(
        self,
        pack: ScenarioPack,
        domain_tables: set[str],
        result: PackValidationResult,
    ) -> None:
        """Check that topic names and payload fields are plausible."""
        for topic in pack.topics:
            if not topic.name:
                result.errors.append("Stream topic has empty name")
            if not topic.event_type:
                result.warnings.append(f"Topic '{topic.name}' has no event_type defined")
            if not topic.payload_fields:
                result.warnings.append(f"Topic '{topic.name}' has no payload_fields defined")

    def _validate_file_drop(self, pack: ScenarioPack, result: PackValidationResult) -> None:
        """Validate file_drop-specific configuration."""
        if pack.file_drop is None:
            result.errors.append("Pack kind is 'file_drop' but no file_drop section defined")
            return
        valid_cadences = {"daily", "hourly", "every_15m", "every_5m", "weekly"}
        if pack.file_drop.cadence not in valid_cadences:
            result.warnings.append(
                f"Unusual cadence '{pack.file_drop.cadence}'. "
                f"Common values: {', '.join(sorted(valid_cadences))}"
            )
        if not pack.file_drop.entities:
            result.warnings.append("file_drop section has no entities listed")
        if not pack.file_drop.formats:
            result.errors.append("file_drop section has no formats defined")

    def _validate_streaming(self, pack: ScenarioPack, result: PackValidationResult) -> None:
        """Validate stream-specific configuration."""
        if pack.streaming is None:
            result.errors.append("Pack kind is 'stream' but no streaming section defined")
            return
        if not pack.streaming.topics:
            result.warnings.append("streaming section has no topics defined")
        if pack.streaming.cadence and pack.streaming.cadence.rate_per_sec <= 0:
            result.errors.append("Streaming rate_per_sec must be positive")

    def _validate_hybrid(self, pack: ScenarioPack, result: PackValidationResult) -> None:
        """Validate hybrid-specific configuration."""
        if pack.hybrid is None:
            result.errors.append("Pack kind is 'hybrid' but no hybrid section defined")
            return
        if pack.hybrid.micro_batch is None and pack.hybrid.stream is None:
            result.errors.append(
                "Hybrid pack must define at least micro_batch or stream"
            )

    def _validate_fabric_targets(self, pack: ScenarioPack, result: PackValidationResult) -> None:
        """Check fabric_targets are present and well-formed."""
        if not pack.fabric_targets:
            result.warnings.append("No fabric_targets defined — pack cannot target Fabric resources")

    def _validate_gates(self, pack: ScenarioPack, result: PackValidationResult) -> None:
        """Validate the validation gates section."""
        if pack.validation is None:
            return
        known_gates = {
            "schema_conformance",
            "referential_integrity",
            "row_count",
            "null_check",
            "uniqueness",
        }
        for gate in pack.validation.required_gates:
            if gate not in known_gates:
                result.warnings.append(
                    f"Unknown validation gate '{gate}'. "
                    f"Known gates: {', '.join(sorted(known_gates))}"
                )
