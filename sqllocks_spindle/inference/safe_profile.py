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
serialization guard on the rich dataclasses (STORY-004).

STORY-009 (ADR-005) adds safe-by-default behaviour: the scrub (winsorize +
k-anon + PII gate) runs on the mapping path by DEFAULT; an opt-out
``unsafe_full_fidelity`` flag disables the disclosure-control transforms,
persists full-fidelity statistics, and stamps ``unsafe=true`` on the artifact.
Every artifact embeds a self-describing ``redaction_manifest`` reporting, per
column, what was actually suppressed (rare categories dropped, bounds
winsorized, pattern-only columns, the k used, the sensitive flag).
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
#   v3: STORY-007 adds per-column ``suppressed_category_count`` (k-anon
#       __OTHER__ suppression bookkeeping for the redaction manifest; ADR-003).
SCHEMA_VERSION = 3

# k-anonymity defaults (ADR-003). A categorical value whose count is below the
# applicable k is folded into a single ``__OTHER__`` bucket carrying aggregate
# weight. ``k`` is configurable per-profile and per-column; a ``sensitive`` /
# health flag raises it to ``K_SENSITIVE``.
K_DEFAULT = 5
K_SENSITIVE = 11
# The residual bucket every suppressed sub-k value is folded into.
OTHER_BUCKET = "__OTHER__"

# Raw-bearing field names that must NEVER appear on the safe model (ADR-007).
# Used by the introspection conformance test (and a cheap self-check below).
FORBIDDEN_RAW_FIELDS = frozenset(
    {"min_value", "max_value", "enum_values", "value_counts_ext"}
)

# Value-pattern PII gate (ADR-004 / STORY-008). The profiler's value-based
# pattern detector (``ColumnProfile.pattern``) emits these names for the PII
# classes ADR-004 enumerates (email / ssn / cc / phone / ip / iban / postal).
# A column whose detected ``pattern`` is in this set — REGARDLESS of its column
# NAME — persists ``pattern`` + ``length_dist`` only, never values. This is the
# name-independent catch the name-based masker/profiler heuristics miss
# (the B6 hole: PII in ``notes`` / ``c_47``).
#
# Detection is value-based, so it is DEFENSE-IN-DEPTH, NOT a completeness
# guarantee (ADR-004 / ADR-011): a novel/encoded PII format the regexes do not
# recognise can pass the pattern check — the cardinality≈row_count backstop
# below is the generic suppressor for high-card free-text that the regexes miss.
PII_PATTERNS = frozenset(
    {
        "email",
        "ssn",
        "credit_card",  # ADR-004 "cc"
        "phone",
        "ip_address",  # ADR-004 "ip"
        "iban",
        "postal_code",  # ADR-004 "postal"
    }
)

# cardinality≈row_count backstop (ADR-004). A column whose distinct-value count
# is within this fraction of the row count is treated as effectively unique
# free-text (names, addresses, notes, obfuscated/concatenated PII the regexes
# miss) and is gated to pattern + length only. Default 0.95; configurable via
# ``config["pii_cardinality_ratio"]``.
PII_CARDINALITY_RATIO = 0.95


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
    # Number of distinct categories that were suppressed (folded into
    # ``__OTHER__``) by k-anon (ADR-003 / STORY-007). Recorded so the redaction
    # manifest (STORY-009) can report per-column what was dropped. None for
    # non-categorical columns; 0 when a categorical had nothing to suppress.
    suppressed_category_count: int | None = None

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
        row_count: int | None = None,
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
        * ``categorical_weights`` — k-anon suppression (STORY-007 / ADR-003):
          any value with count < k folded into a single ``__OTHER__`` bucket.
          ``count`` is derived from the seeded proportion x ``row_count`` (the
          rich ``enum_values`` / ``value_counts_ext`` carry value->proportion,
          not raw counts). ``row_count`` is threaded in by the table mapper.
        * value-pattern PII gate (STORY-008 / ADR-004). When a column's detected
          ``pattern`` is a PII class (:pydata:`PII_PATTERNS`) OR its cardinality
          is approximately the row count (high-card free-text backstop), the
          column persists ``pattern`` + ``length_dist`` ONLY — ``categorical_
          weights`` are dropped and no values are carried. Detection is
          name-independent (catches PII in ``notes`` / ``c_47``). This is
          DEFENSE-IN-DEPTH, NOT a completeness guarantee (ADR-004 / ADR-011).
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
        # k-anon suppression folds sub-k values into __OTHER__ (ADR-003).
        categorical_weights = None
        suppressed_category_count = None
        if getattr(col, "is_enum", False):
            seed = getattr(col, "enum_values", None) or getattr(
                col, "value_counts_ext", None
            )
            if seed:
                categorical_weights, suppressed_category_count = (
                    cls._suppress_categories_hook(
                        seed,
                        config,
                        row_count=row_count,
                        column_name=col.name,
                    )
                )

        # String/pattern statistics. ``string_length`` is a safe summary
        # (min/mean/max/p95) carried verbatim.
        pattern = getattr(col, "pattern", None)
        string_length = getattr(col, "string_length", None)
        length_dist = None

        # Value-pattern PII gate (STORY-008 / ADR-004). Name-independent: keyed
        # off the value-detected ``pattern`` and the cardinality≈row_count
        # backstop, never the column name. When it fires the column persists
        # ``pattern`` + ``length_dist`` ONLY — categorical weights are dropped so
        # no values reach disk.
        if cls._pii_gate_hook(
            pattern=pattern,
            cardinality=getattr(col, "cardinality", 0),
            row_count=row_count,
            config=config,
        ):
            length_dist = cls._length_dist_from_summary(string_length)
            categorical_weights = None
            suppressed_category_count = None

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
            suppressed_category_count=suppressed_category_count,
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
    def _resolve_k(
        config: dict[str, Any] | None,
        column_name: str | None,
    ) -> int:
        """Resolve the effective k for a column (ADR-003).

        Precedence (most specific wins):

        1. Per-column override: ``config["columns"][<column_name>]["k"]``.
        2. Per-column sensitive flag: ``config["columns"][<column_name>]
           ["sensitive"]`` -> ``K_SENSITIVE``.
        3. Profile-level ``config["k"]``.
        4. Profile-level ``config["sensitive"]`` -> ``K_SENSITIVE``.
        5. ``K_DEFAULT`` (5).

        A column override of ``k`` always beats a profile-level ``sensitive``.
        """
        config = config or {}
        col_cfg: dict[str, Any] = {}
        if column_name is not None:
            cols = config.get("columns")
            if isinstance(cols, dict):
                maybe = cols.get(column_name)
                if isinstance(maybe, dict):
                    col_cfg = maybe

        # 1 / 2 — per-column.
        if "k" in col_cfg and col_cfg["k"] is not None:
            return int(col_cfg["k"])
        if col_cfg.get("sensitive"):
            return K_SENSITIVE

        # 3 / 4 — profile-level.
        if "k" in config and config["k"] is not None:
            return int(config["k"])
        if config.get("sensitive"):
            return K_SENSITIVE

        return K_DEFAULT

    @classmethod
    def _suppress_categories_hook(
        cls,
        weights: dict[str, float],
        config: dict[str, Any] | None = None,
        row_count: int | None = None,
        column_name: str | None = None,
    ) -> tuple[dict[str, float], int]:
        """k-anon ``__OTHER__`` suppression (ADR-003 / STORY-007).

        Folds any category whose **count** is below the effective ``k`` into a
        single ``__OTHER__`` bucket carrying the aggregate weight. Surviving
        categories keep their original weights; only post-suppression weights
        are returned, so sub-k values never reach disk (the leak ADR-003 closes).

        ``weights`` is the rich seed ``value -> proportion`` (normalized; sums
        to ~1.0), NOT raw counts. The count for each value is reconstructed as
        ``round(proportion * row_count)``. When ``row_count`` is unknown
        (``None`` or <= 0) suppression cannot be applied count-wise; the weights
        pass through unchanged with a suppressed count of 0 (fail-open on the
        bookkeeping, never fabricating a count we can't derive).

        Returns ``(post_suppression_weights, suppressed_category_count)``.
        ``suppressed_category_count`` is the number of distinct categories that
        were folded into ``__OTHER__`` (recorded for the STORY-009 manifest).

        A returned dict never mutates the caller's input.
        """
        if not weights:
            return {}, 0

        k = cls._resolve_k(config, column_name)

        # Without a row count we cannot turn proportions into counts; pass the
        # weights through untouched rather than guess. (k<=1 disables
        # suppression — every count is >= 1.)
        if not row_count or row_count <= 0 or k <= 1:
            return dict(weights), 0

        surviving: dict[str, float] = {}
        other_weight = 0.0
        suppressed = 0
        for value, weight in weights.items():
            # __OTHER__ already present in the seed (shouldn't happen pre-
            # suppression) is treated as residual mass, never as a real value.
            if value == OTHER_BUCKET:
                other_weight += float(weight)
                continue
            count = round(float(weight) * row_count)
            if count < k:
                other_weight += float(weight)
                suppressed += 1
            else:
                surviving[value] = float(weight)

        if suppressed > 0 or other_weight > 0.0:
            # Accumulate (don't overwrite) in case a residual bucket pre-existed.
            surviving[OTHER_BUCKET] = (
                surviving.get(OTHER_BUCKET, 0.0) + other_weight
            )

        return surviving, suppressed

    @staticmethod
    def _pii_gate_hook(
        pattern: str | None,
        cardinality: int,
        row_count: int | None,
        config: dict[str, Any] | None = None,
    ) -> bool:
        """Value-pattern PII gate decision (ADR-004 / STORY-008).

        Returns ``True`` if the column must be reduced to ``pattern`` +
        ``length_dist`` only (no values, no categorical weights). The decision
        is NAME-INDEPENDENT — it consults only the value-detected ``pattern``
        and the cardinality≈row_count backstop, never the column name. This
        catches PII in oddly-named columns (``notes`` / ``c_47``) that the
        name-based masker/profiler heuristics miss (the B6 hole).

        Two independent triggers (either fires the gate):

        1. **PII pattern match** — ``pattern`` is one of the profiler-detected
           PII classes (:pydata:`PII_PATTERNS`: email/ssn/cc/phone/ip/iban/
           postal). Value-based, so it is DEFENSE-IN-DEPTH, not a completeness
           guarantee (ADR-004 / ADR-011): a novel/encoded format the regexes
           miss can pass it — trigger 2 is the backstop.

        2. **cardinality≈row_count backstop** — the column's distinct-value
           count is within :pydata:`PII_CARDINALITY_RATIO` of the row count
           (configurable via ``config["pii_cardinality_ratio"]``). Catches
           high-card free-text (names, addresses, obfuscated/concatenated PII)
           generically, independent of any regex.

        The gate can be disabled per-call with ``config["pii_gate"] = False``
        (e.g. behind ``--unsafe-full-fidelity``, STORY-009).

        This gate is defense-in-depth and is NOT a completeness guarantee
        (ADR-004 / ADR-011): value-based regex detection can miss novel or
        encoded PII formats. The threat model (ADR-011) names the residual.
        """
        config = config or {}
        if config.get("pii_gate") is False:
            return False

        # Trigger 1: value-pattern PII match (name-independent).
        if pattern is not None and pattern in PII_PATTERNS:
            return True

        # Trigger 2: cardinality≈row_count backstop for high-card free-text.
        if row_count and row_count > 0:
            ratio = config.get("pii_cardinality_ratio", PII_CARDINALITY_RATIO)
            if cardinality / row_count >= ratio:
                return True

        return False

    @staticmethod
    def _length_dist_from_summary(
        string_length: dict[str, float] | None,
    ) -> dict[str, float] | None:
        """Derive a safe ``length_dist`` descriptor from the length summary.

        The rich profiler computes a ``string_length`` summary
        (``min``/``mean``/``max``/``p95`` of ``len(value)``) but no length
        histogram. For a PII-gated column the safe transport carries
        ``length_dist`` as that aggregate length descriptor — it is an aggregate
        over lengths only (never a value), so it is safe by construction.

        Returns ``None`` when no length summary is available (e.g. a gated
        non-string column with no ``string_length``).
        """
        if not string_length:
            return None
        # Copy (never alias the rich profile's dict) and keep only numeric
        # aggregates — these are lengths, not values.
        return {
            key: float(val)
            for key, val in string_length.items()
            if isinstance(val, (int, float))
        }

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
            "suppressed_category_count": self.suppressed_category_count,
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
            suppressed_category_count=data.get("suppressed_category_count"),
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
        row_count = getattr(table, "row_count", 0)
        return cls(
            name=table.name,
            row_count=row_count,
            columns={
                col_name: SafeColumnProfile.from_column_profile(
                    col, config, row_count=row_count
                )
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
    # Self-describing redaction manifest (ADR-005 / STORY-009). Reports, per
    # column, what the scrub actually suppressed. Empty {} only on an artifact
    # built without a rich source (e.g. a hand-built test fixture).
    redaction_manifest: dict = field(default_factory=dict)
    # Safe-by-default stamp (ADR-005 / STORY-009). ``False`` on a scrubbed
    # (safe) artifact; ``True`` only when built with ``unsafe_full_fidelity``
    # — i.e. disclosure controls were disabled and full-fidelity statistics
    # persisted. ``validate --safe`` (STORY-010) rejects ``unsafe=true``.
    unsafe: bool = False

    # ----- mapping (rich -> safe) -----

    @classmethod
    def from_dataset_profile(
        cls,
        dataset_profile: "DatasetProfile",
        config: dict[str, Any] | None = None,
        unsafe_full_fidelity: bool = False,
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
        PII gate).

        Safe-by-default (ADR-005 / STORY-009)
        -------------------------------------
        The scrub — winsorized bounds (ADR-002), k-anon ``__OTHER__``
        suppression (ADR-003), and the value-pattern PII gate (ADR-004) — runs
        by DEFAULT. The safe path is the path of least resistance.

        ``unsafe_full_fidelity=True`` is the explicit, single opt-out. It
        disables the disclosure-control transforms (k-anon suppression and the
        PII gate are turned off so full-fidelity categorical weights / values
        survive) and stamps ``unsafe=True`` on the returned profile. Such an
        artifact is rejected by ``validate --safe`` (STORY-010). It is the ONLY
        way to persist un-scrubbed statistics.

        Every returned profile carries an accurate ``redaction_manifest`` (built
        from the rich source vs. the scrubbed safe columns — see
        :func:`build_redaction_manifest`).
        """
        # Merge the opt-out into an effective config that disables the
        # disclosure-control transforms. We do not mutate the caller's config.
        effective_config = dict(config or {})
        if unsafe_full_fidelity:
            # k<=1 disables k-anon suppression (every count is >= 1); the PII
            # gate is turned off so values are not reduced to pattern-only.
            # Bounds stay winsorized-from-quantiles by construction (the safe
            # model has no raw min/max field — ADR-007), but the manifest
            # reflects that no suppression occurred.
            effective_config["k"] = 1
            effective_config["pii_gate"] = False

        profile = cls(
            tables={
                tname: SafeTableProfile.from_table_profile(tprofile, effective_config)
                for tname, tprofile in dataset_profile.tables.items()
            },
            relationships=list(getattr(dataset_profile, "relationships", []) or []),
            schema_version=SCHEMA_VERSION,
            redaction_manifest={},
            unsafe=bool(unsafe_full_fidelity),
        )
        profile.redaction_manifest = build_redaction_manifest(
            dataset_profile,
            profile,
            config=effective_config,
            unsafe=bool(unsafe_full_fidelity),
        )
        return profile

    # ----- serialization -----

    def to_safe_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict with deterministic key order."""
        return {
            "schema_version": self.schema_version,
            "unsafe": self.unsafe,
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
            unsafe=bool(data.get("unsafe", False)),
        )


# ---------------------------------------------------------------------------
# Redaction manifest (ADR-005 / STORY-009)
# ---------------------------------------------------------------------------


def build_redaction_manifest(
    dataset_profile: "DatasetProfile",
    safe_profile: "SafeProfile",
    config: dict[str, Any] | None = None,
    unsafe: bool = False,
) -> dict[str, Any]:
    """Build the self-describing redaction manifest (ADR-005 / STORY-009).

    The manifest is computed from the rich *source* profile and the scrubbed
    *safe* profile together, so it reports what was ACTUALLY suppressed — not
    what was intended. Accuracy is the AC: every figure is read off the real
    mapping outcome.

    Shape::

        {
          "unsafe": <bool>,            # mirrors SafeProfile.unsafe
          "k_default": <int>,          # profile-level k that applied by default
          "tables": {
            <table>: {
              <column>: {
                "categories_dropped": <int>,       # k-anon __OTHER__ folds (rare)
                "bounds_winsorized": <bool>,       # winsorized quantile bounds set
                "pattern_only": <bool>,            # PII-gated to pattern+length only
                "k": <int>,                        # effective k for this column
                "sensitive": <bool>,               # sensitive flag raised k
              }, ...
            }, ...
          }
        }

    ``rare_categories_dropped`` reads the per-column
    ``suppressed_category_count`` the k-anon hook actually recorded (STORY-007).
    ``pattern_only`` re-evaluates the exact PII-gate decision the mapper used
    (STORY-008). In ``unsafe`` mode the effective config disabled both controls,
    so these report 0 / False — accurately.
    """
    config = config or {}
    profile_k = SafeColumnProfile._resolve_k(config, None)

    tables_manifest: dict[str, Any] = {}
    for tname, tprofile in dataset_profile.tables.items():
        safe_table = safe_profile.tables.get(tname)
        if safe_table is None:
            continue
        row_count = getattr(tprofile, "row_count", 0)
        cols_manifest: dict[str, Any] = {}
        for cname, rich_col in tprofile.columns.items():
            safe_col = safe_table.columns.get(cname)
            if safe_col is None:
                continue

            effective_k = SafeColumnProfile._resolve_k(config, cname)
            # ``sensitive`` is true when the resolved k came from a sensitive
            # flag (profile- or column-level) rather than an explicit k.
            sensitive = _k_from_sensitive(config, cname)

            # pattern_only: the exact PII-gate decision the mapper used.
            pattern_only = SafeColumnProfile._pii_gate_hook(
                pattern=getattr(rich_col, "pattern", None),
                cardinality=getattr(rich_col, "cardinality", 0),
                row_count=row_count,
                config=config,
            )

            cols_manifest[cname] = {
                "categories_dropped": int(
                    safe_col.suppressed_category_count or 0
                ),
                "bounds_winsorized": safe_col.bounds is not None,
                "pattern_only": bool(pattern_only),
                "k": int(effective_k),
                "sensitive": bool(sensitive),
            }
        tables_manifest[tname] = cols_manifest

    return {
        "unsafe": bool(unsafe),
        "k_default": int(profile_k),
        "tables": tables_manifest,
    }


def _k_from_sensitive(
    config: dict[str, Any] | None,
    column_name: str | None,
) -> bool:
    """True when the effective k for ``column_name`` is set by a sensitive flag.

    Mirrors :meth:`SafeColumnProfile._resolve_k` precedence: an explicit ``k``
    (per-column or profile) takes precedence over a sensitive flag, so it is
    NOT considered sensitive-driven even if a sensitive flag is also present.
    """
    config = config or {}
    col_cfg: dict[str, Any] = {}
    if column_name is not None:
        cols = config.get("columns")
        if isinstance(cols, dict):
            maybe = cols.get(column_name)
            if isinstance(maybe, dict):
                col_cfg = maybe

    if "k" in col_cfg and col_cfg["k"] is not None:
        return False
    if col_cfg.get("sensitive"):
        return True
    if "k" in config and config["k"] is not None:
        return False
    if config.get("sensitive"):
        return True
    return False


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
