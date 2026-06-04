"""SafeProfile — the versioned, persisted, safe-by-construction profile model.

This is the canonical on-disk transport model (ADR-001 / ADR-007). It is
*decoupled* from the rich in-memory ``DatasetProfile`` / ``ColumnProfile``
(``profiler.py``): the rich profile is the *source*, ``SafeProfile`` is the
*transport*.

It carries ONLY the safe-and-sufficient statistic set. By construction it has
NO raw-bearing fields — there is no ``min_value``, ``max_value``,
``enum_values`` or ``value_counts_ext`` anywhere on this model. Raw extremes
are replaced by winsorized ``bounds`` (populated by STORY-006); rare categories
are suppressed into ``categorical_weights`` (populated by STORY-007).

STORY-001 scope: the dataclasses + ``schema_version`` + byte-stable
``to_safe_dict`` / ``from_safe_dict`` round-trip. The *mapping* from a rich
profile (STORY-002), the ``ProfileStore`` save/load path (STORY-003), the
serialization guard on the rich dataclasses (STORY-004), and population of the
``redaction_manifest`` (STORY-009) are out of scope for this story.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Current persisted schema version. Every persisted-statistic addition bumps
# this (ARCHITECTURE.md Invariants). Legacy files load read-only as version 0.
SCHEMA_VERSION = 1

# Raw-bearing field names that must NEVER appear on the safe model (ADR-007).
# Used by the introspection conformance test (and a cheap self-check below).
FORBIDDEN_RAW_FIELDS = frozenset(
    {"min_value", "max_value", "enum_values", "value_counts_ext"}
)


# ---------------------------------------------------------------------------
# SafeColumnProfile
# ---------------------------------------------------------------------------


@dataclass
class SafeColumnProfile:
    """Safe, persisted statistic set for a single column.

    Carries ONLY non-raw-bearing statistics. Notably absent (by construction):
    ``min_value``, ``max_value``, ``enum_values``, ``value_counts_ext``.

    Numeric extremes live in ``bounds`` (winsorized quantile bounds, ADR-002,
    populated by STORY-006). Categorical mass lives in ``categorical_weights``
    (post-k-anon suppression, ADR-003, populated by STORY-007).
    """

    name: str
    dtype: str  # spindle type: integer, float, string, date, datetime, boolean
    null_rate: float
    cardinality: int

    # Numeric summary statistics.
    mean: float | None = None
    std: float | None = None
    # Quantile fingerprint p1..p99 (e.g. {"p1": .., "p50": .., "p99": ..}).
    quantiles: dict[str, float] | None = None
    # Best-fit distribution name + named params.
    distribution: str | None = None
    distribution_params: dict[str, float] | None = None
    # Winsorized bounds replacing raw min/max: {"lo": .., "hi": ..} (ADR-002).
    bounds: dict[str, float] | None = None

    # Categorical: post-suppression value -> weight (ADR-003). NOT raw enum
    # values — sub-k categories are folded into "__OTHER__" before they land
    # here (STORY-007).
    categorical_weights: dict[str, float] | None = None

    # String/pattern statistics.
    pattern: str | None = None
    # Length distribution histogram (normalized bins), distinct from the
    # min/mean/max/p95 summary carried in ``string_length``.
    length_dist: dict[str, float] | None = None
    string_length: dict[str, float] | None = None

    # Temporal histograms (per-column, where applicable — date/datetime cols).
    hour_histogram: list[float] | None = None
    dow_histogram: list[float] | None = None

    # ----- serialization -----

    def to_safe_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict. Deterministic key order for byte-stability."""
        return {
            "name": self.name,
            "dtype": self.dtype,
            "null_rate": self.null_rate,
            "cardinality": self.cardinality,
            "mean": self.mean,
            "std": self.std,
            "quantiles": self.quantiles,
            "distribution": self.distribution,
            "distribution_params": self.distribution_params,
            "bounds": self.bounds,
            "categorical_weights": self.categorical_weights,
            "pattern": self.pattern,
            "length_dist": self.length_dist,
            "string_length": self.string_length,
            "hour_histogram": self.hour_histogram,
            "dow_histogram": self.dow_histogram,
        }

    @classmethod
    def from_safe_dict(cls, data: dict[str, Any]) -> "SafeColumnProfile":
        return cls(
            name=data["name"],
            dtype=data["dtype"],
            null_rate=data["null_rate"],
            cardinality=data["cardinality"],
            mean=data.get("mean"),
            std=data.get("std"),
            quantiles=data.get("quantiles"),
            distribution=data.get("distribution"),
            distribution_params=data.get("distribution_params"),
            bounds=data.get("bounds"),
            categorical_weights=data.get("categorical_weights"),
            pattern=data.get("pattern"),
            length_dist=data.get("length_dist"),
            string_length=data.get("string_length"),
            hour_histogram=data.get("hour_histogram"),
            dow_histogram=data.get("dow_histogram"),
        )


# ---------------------------------------------------------------------------
# SafeTableProfile
# ---------------------------------------------------------------------------


@dataclass
class SafeTableProfile:
    """Safe, persisted profile for a single table."""

    name: str
    row_count: int
    columns: dict[str, SafeColumnProfile] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    # Detected FK col_name -> parent_table (advisory, ADR-009). Names/overlap
    # only — no raw values.
    detected_fks: dict[str, str] = field(default_factory=dict)
    # Inter-column Pearson correlation (per-table, where applicable).
    correlation_matrix: dict[str, dict[str, float]] | None = None

    # ----- serialization -----

    def to_safe_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict. Columns serialized in declared order."""
        return {
            "name": self.name,
            "row_count": self.row_count,
            "columns": {
                col_name: col.to_safe_dict()
                for col_name, col in self.columns.items()
            },
            "primary_key": list(self.primary_key),
            "detected_fks": dict(self.detected_fks),
            "correlation_matrix": self.correlation_matrix,
        }

    @classmethod
    def from_safe_dict(cls, data: dict[str, Any]) -> "SafeTableProfile":
        return cls(
            name=data["name"],
            row_count=data["row_count"],
            columns={
                col_name: SafeColumnProfile.from_safe_dict(col_data)
                for col_name, col_data in data.get("columns", {}).items()
            },
            primary_key=list(data.get("primary_key", [])),
            detected_fks=dict(data.get("detected_fks", {})),
            correlation_matrix=data.get("correlation_matrix"),
        )


# ---------------------------------------------------------------------------
# SafeProfile
# ---------------------------------------------------------------------------


@dataclass
class SafeProfile:
    """The canonical, versioned, on-disk safe profile (ADR-001).

    Top-level transport object. Carries ``schema_version`` and an embedded
    ``redaction_manifest`` (populated by STORY-009 — present but empty here).
    """

    tables: dict[str, SafeTableProfile] = field(default_factory=dict)
    relationships: list[dict] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION
    # Self-describing redaction manifest (ADR-005). Populated by STORY-009;
    # present-but-empty in STORY-001.
    redaction_manifest: dict = field(default_factory=dict)

    # ----- serialization -----

    def to_safe_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict with deterministic key order."""
        return {
            "schema_version": self.schema_version,
            "tables": {
                tname: tprofile.to_safe_dict()
                for tname, tprofile in self.tables.items()
            },
            "relationships": self.relationships,
            "redaction_manifest": self.redaction_manifest,
        }

    @classmethod
    def from_safe_dict(cls, data: dict[str, Any]) -> "SafeProfile":
        return cls(
            tables={
                tname: SafeTableProfile.from_safe_dict(tdata)
                for tname, tdata in data.get("tables", {}).items()
            },
            relationships=list(data.get("relationships", [])),
            schema_version=data.get("schema_version", SCHEMA_VERSION),
            redaction_manifest=dict(data.get("redaction_manifest", {})),
        )


# ---------------------------------------------------------------------------
# Conformance self-check (ADR-007 invariant, cheap, import-time)
# ---------------------------------------------------------------------------


def _assert_no_raw_fields() -> None:
    """No safe dataclass may declare a raw-bearing field name (ADR-007)."""
    import dataclasses as _dc

    for klass in (SafeColumnProfile, SafeTableProfile, SafeProfile):
        declared = {f.name for f in _dc.fields(klass)}
        leaked = declared & FORBIDDEN_RAW_FIELDS
        if leaked:
            raise AssertionError(
                f"{klass.__name__} declares forbidden raw-bearing field(s): "
                f"{sorted(leaked)}"
            )


_assert_no_raw_fields()
