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
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids any import cost
    from sqllocks_spindle.inference.profiler import (
        ColumnProfile,
        DatasetProfile,
        TableProfile,
    )

# Current persisted schema version. Every persisted-statistic addition bumps
# this (ARCHITECTURE.md Invariants). Legacy files load read-only as version 0.
#   v1: STORY-001 baseline safe statistic set.
#   v2: STORY-006 adds p0_5/p99_5 to the quantile fingerprint (winsorized-bounds
#       widening fallback endpoints; ADR-002).
SCHEMA_VERSION = 2

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

    # ----- mapping (rich -> safe) -----

    @classmethod
    def from_column_profile(
        cls,
        col: "ColumnProfile",
        config: dict[str, Any] | None = None,
    ) -> "SafeColumnProfile":
        """Map a rich ``ColumnProfile`` to a ``SafeColumnProfile`` (STORY-002).

        Selects ONLY the safe-and-sufficient statistic set. Reads the REAL
        attribute names on ``ColumnProfile`` (``min_value``/``max_value`` are
        *never* read — ADR-002; bounds derive from ``quantiles``). This fixes
        the B2 attribute-mismatch bug class where the legacy registry read
        non-existent ``.min``/``.max``/``.top_values``.

        Disclosure-control transforms are applied via hooks that are STUBS in
        this story and become real in their owning stories:

        * ``bounds``  — winsorized quantile bounds (STORY-006 / ADR-002). Stub
          here: ``{"lo": p1, "hi": p99}`` taken from ``quantiles`` if present.
        * ``categorical_weights`` — k-anon suppression (STORY-007 / ADR-003).
          Stub here: passthrough of the seeded weights (no suppression yet).
        * pattern-only PII gate (STORY-008 / ADR-004). Stub here: passthrough.
        """
        config = config or {}

        # Numeric summary statistics — carried verbatim (safe aggregates).
        mean = getattr(col, "mean", None)
        std = getattr(col, "std", None)
        quantiles = getattr(col, "quantiles", None)
        distribution = getattr(col, "distribution", None)
        distribution_params = getattr(col, "distribution_params", None)

        # Winsorized bounds STUB (STORY-006 owns the real winsorization).
        # ADR-002: derive from quantiles p1/p99 — NEVER from min_value/max_value.
        bounds = cls._winsorized_bounds_hook(quantiles, config)

        # Categorical weights: seed from enum_values, fall back to
        # value_counts_ext. Both are REAL attribute names on ColumnProfile.
        categorical_weights = None
        if getattr(col, "is_enum", False):
            seed = getattr(col, "enum_values", None) or getattr(
                col, "value_counts_ext", None
            )
            if seed:
                categorical_weights = cls._suppress_categories_hook(seed, config)

        # String/pattern statistics. ``string_length`` is a safe summary
        # (min/mean/max/p95) carried verbatim; ``length_dist`` (a normalized
        # histogram) has no source on the rich profile yet — left None for the
        # STORY-008 pattern-only path to populate.
        pattern = getattr(col, "pattern", None)
        string_length = getattr(col, "string_length", None)
        length_dist = None

        return cls(
            name=col.name,
            dtype=col.dtype,
            null_rate=getattr(col, "null_rate", 0.0),
            cardinality=getattr(col, "cardinality", 0),
            mean=mean,
            std=std,
            quantiles=quantiles,
            distribution=distribution,
            distribution_params=distribution_params,
            bounds=bounds,
            categorical_weights=categorical_weights,
            pattern=pattern,
            length_dist=length_dist,
            string_length=string_length,
            hour_histogram=getattr(col, "hour_histogram", None),
            dow_histogram=getattr(col, "dow_histogram", None),
        )

    # ----- disclosure-control hooks (STUBS — replaced by later stories) -----

    # Widening fallback percentile keys (ADR-002): when p1/p99 winsorization
    # drops a heavy-tailed column's fidelity below tolerance, widen to
    # p0.5/p99.5 (still aggregate quantiles — non-literal). The decision to
    # widen is owned by the fidelity gate (ADR-012 / STORY-011); STORY-006
    # provides the mechanism + the quantile endpoints to widen to.
    WIDEN_BOUNDS_CONFIG: ClassVar[dict[str, str]] = {
        "bounds_lo_quantile": "p0_5",
        "bounds_hi_quantile": "p99_5",
    }

    @staticmethod
    def _winsorized_bounds_hook(
        quantiles: dict[str, float] | None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, float] | None:
        """Winsorized bounds from quantile percentiles (ADR-002 / STORY-006).

        Returns ``{"lo": <lo-quantile>, "hi": <hi-quantile>}`` derived ONLY
        from the already-computed aggregate ``quantiles`` fingerprint — raw
        ``min_value``/``max_value`` are NEVER read.

        Percentiles are configurable per call:

        * default: p1 / p99 (``bounds_lo_quantile`` / ``bounds_hi_quantile``).
        * widening fallback: p0.5 / p99.5 (keys ``p0_5`` / ``p99_5``), selected
          by passing :pyattr:`WIDEN_BOUNDS_CONFIG` (or an equivalent config).
          Recovers tail mass for heavy-tailed columns where p1/p99 clips too
          aggressively, while staying non-literal.

        If the requested percentile keys are not present in ``quantiles`` (e.g.
        a widened request against a legacy v1 fingerprint that lacks p0_5/p99_5)
        the hook falls back to the default p1/p99 keys rather than returning
        ``None``, so bounds are still produced.
        """
        if not quantiles:
            return None
        config = config or {}
        lo_key = config.get("bounds_lo_quantile", "p1")
        hi_key = config.get("bounds_hi_quantile", "p99")
        lo = quantiles.get(lo_key)
        hi = quantiles.get(hi_key)
        # Graceful degradation: a widened request against a fingerprint that
        # lacks the widened endpoints falls back to the default p1/p99.
        if lo is None:
            lo = quantiles.get("p1")
        if hi is None:
            hi = quantiles.get("p99")
        if lo is None or hi is None:
            return None
        return {"lo": float(lo), "hi": float(hi)}

    @staticmethod
    def _suppress_categories_hook(
        weights: dict[str, float],
        config: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """STORY-007 hook (stub). k-anon ``__OTHER__`` suppression (ADR-003).

        Passthrough in STORY-002 — the real implementation folds any value
        with count < k into a single ``__OTHER__`` bucket. Returns a copy so
        callers never mutate the rich profile's dict.
        """
        return dict(weights)

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

    # ----- mapping (rich -> safe) -----

    @classmethod
    def from_table_profile(
        cls,
        table: "TableProfile",
        config: dict[str, Any] | None = None,
    ) -> "SafeTableProfile":
        """Map a rich ``TableProfile`` to a ``SafeTableProfile`` (STORY-002).

        One ``SafeColumnProfile`` per column. Carries the table-level
        ``correlation_matrix``, ``primary_key`` and advisory ``detected_fks``
        (names/overlap only — no raw values). Column order is preserved.
        """
        return cls(
            name=table.name,
            row_count=getattr(table, "row_count", 0),
            columns={
                col_name: SafeColumnProfile.from_column_profile(col, config)
                for col_name, col in table.columns.items()
            },
            primary_key=list(getattr(table, "primary_key", []) or []),
            detected_fks=dict(getattr(table, "detected_fks", {}) or {}),
            correlation_matrix=getattr(table, "correlation_matrix", None),
        )

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

    # ----- mapping (rich -> safe) -----

    @classmethod
    def from_dataset_profile(
        cls,
        dataset_profile: "DatasetProfile",
        config: dict[str, Any] | None = None,
    ) -> "SafeProfile":
        """Map a rich ``DatasetProfile`` to a ``SafeProfile`` (STORY-002 / ADR-001).

        Builds one ``SafeTableProfile`` per table and one ``SafeColumnProfile``
        per column, selecting ONLY the safe-and-sufficient statistic set. The
        rich profile is the *source*; the returned ``SafeProfile`` is the safe
        *transport*.

        The mapper reads only REAL attribute names on the rich dataclasses
        (``min_value``/``max_value`` are never read — bounds derive from
        ``quantiles`` per ADR-002), fixing the B2 attribute-mismatch bug class.

        ``config`` is an optional per-profile/per-column settings dict threaded
        to the disclosure-control hooks (winsorization percentiles, k-anon k,
        PII gate) which are stubs in this story.

        ``redaction_manifest`` is left empty here — STORY-009 populates it.
        """
        return cls(
            tables={
                tname: SafeTableProfile.from_table_profile(tprofile, config)
                for tname, tprofile in dataset_profile.tables.items()
            },
            relationships=list(getattr(dataset_profile, "relationships", []) or []),
            schema_version=SCHEMA_VERSION,
            redaction_manifest={},
        )

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
