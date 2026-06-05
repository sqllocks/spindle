"""SafeProfileAdapter — bridge a loaded SafeProfile into the generator (STORY-005).

A persisted :class:`~sqllocks_spindle.inference.safe_profile.SafeProfile`
(written and re-read via
:class:`~sqllocks_spindle.inference.profile_store.ProfileStore`) carries ONLY the
safe-and-sufficient statistic set (ADR-001 / ADR-007): no raw ``min_value`` /
``max_value`` / ``enum_values`` / ``value_counts_ext``. Numeric extremes live in
winsorized ``bounds`` (ADR-002); categorical mass lives in
``categorical_weights`` with sub-k values folded into ``__OTHER__`` (ADR-003).

The generation engine (``engine/generator.py``) consumes a
:class:`~sqllocks_spindle.schema.parser.SpindleSchema` whose per-column
``generator`` dicts name a registered strategy. This module is the **adapter**
that maps a loaded ``SafeProfile`` to such a schema, selecting generator
strategies that consume the safe statistics:

* numeric with a fitted ``distribution`` + ``distribution_params``  →
  ``distribution`` strategy, with ``bounds`` threaded in as ``min``/``max`` so
  the engine clips regenerated values to the winsorized bounds (ADR-002).
* numeric with ``quantiles`` but no usable distribution  → ``empirical``
  strategy (quantile interpolation), still clipped to ``bounds``.
* categorical with ``categorical_weights``  → ``weighted_enum`` strategy
  (samples the post-suppression weights, ``__OTHER__`` included).
* date/datetime  → ``temporal`` strategy.
* string  → ``faker`` strategy (name-heuristic provider; pattern-only PII
  columns are refined in STORY-008).

The loaded ``SafeProfile`` ALSO satisfies the structural contract the
generator's ``fidelity_profile=`` path reads (``.tables[t].row_count``,
``.tables[t].columns[c].null_rate`` / ``.cardinality``), so the SAME loaded
object can be passed straight back in as ``fidelity_profile`` to obtain a
``FidelityReport`` — no live in-memory ``DatasetProfile`` required.

Scope (STORY-005): make ``profile → save → load → generate`` run end-to-end and
shape-correct. Real winsorization (STORY-006), real k-anon suppression
(STORY-007), the pattern-only PII gate (STORY-008) and the >=90% fidelity
assertion (STORY-011) are owned by their stories; the STORY-002 stubs supply
``bounds`` / ``categorical_weights`` here.
"""

from __future__ import annotations

import math
from typing import Any

from sqllocks_spindle.inference.safe_profile import (
    SafeColumnProfile,
    SafeProfile,
    SafeTableProfile,
)
from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)

__all__ = ["SafeProfileAdapter", "safe_profile_to_schema"]


# Map the SAFE column dtype -> SpindleSchema column ``type`` string.
_DTYPE_TO_COLUMN_TYPE: dict[str, str] = {
    "integer": "integer",
    "float": "decimal",
    "string": "string",
    "date": "date",
    "datetime": "datetime",
    "boolean": "boolean",
}

# Distribution names the engine's DistributionStrategy actually implements
# (engine/strategies/distribution.py: ``_dist_<name>``). The profiler fits a
# DIFFERENT vocabulary (normal/uniform/exponential/lognormal with scipy-style
# loc/scale params), so the adapter translates names + param keys below.
_ENGINE_DISTRIBUTIONS = frozenset(
    {
        "uniform",
        "normal",
        "log_normal",
        "pareto",
        "zipf",
        "geometric",
        "poisson",
        "bernoulli",
    }
)


class SafeProfileAdapter:
    """Adapt a loaded :class:`SafeProfile` to a generatable :class:`SpindleSchema`.

    Stateless; instantiate and call :meth:`to_schema`, or use the module-level
    :func:`safe_profile_to_schema` convenience wrapper.
    """

    def to_schema(
        self,
        profile: SafeProfile,
        domain_name: str = "safe_inferred",
    ) -> SpindleSchema:
        """Build a :class:`SpindleSchema` from a loaded :class:`SafeProfile`.

        The returned schema is ready to pass to
        ``Spindle().generate(schema=..., fidelity_profile=profile)``.

        Raw fields are never consulted (there are none on the safe model);
        numeric clipping is driven by the winsorized ``bounds`` (ADR-002).
        """
        # Parent PK lookup for FK references (the safe model carries
        # primary_key + advisory detected_fks; names only).
        parent_pk_map: dict[str, str] = {}
        for tname, tprofile in profile.tables.items():
            if tprofile.primary_key:
                parent_pk_map[tname] = tprofile.primary_key[0]

        tables: dict[str, TableDef] = {}
        for tname, tprofile in profile.tables.items():
            tables[tname] = self._build_table(tname, tprofile, parent_pk_map)

        relationships = self._build_relationships(profile)
        generation = self._build_generation_config(profile)

        model = ModelDef(
            name=f"{domain_name}_safe",
            description=(
                f"Schema adapted from a loaded SafeProfile "
                f"({len(tables)} tables)"
            ),
            domain=domain_name,
            schema_mode="3nf",
        )

        return SpindleSchema(
            model=model,
            tables=tables,
            relationships=relationships,
            business_rules=[],
            generation=generation,
        )

    # ------------------------------------------------------------------
    # Table / column mapping
    # ------------------------------------------------------------------

    def _build_table(
        self,
        tname: str,
        tprofile: SafeTableProfile,
        parent_pk_map: dict[str, str],
    ) -> TableDef:
        columns: dict[str, ColumnDef] = {}
        pk_set = set(tprofile.primary_key or [])
        fk_map = dict(tprofile.detected_fks or {})

        for cname, cprofile in tprofile.columns.items():
            gen = self._column_to_generator(
                cprofile,
                is_pk=cname in pk_set,
                fk_parent=fk_map.get(cname),
                parent_pk_map=parent_pk_map,
            )
            columns[cname] = ColumnDef(
                name=cname,
                type=_DTYPE_TO_COLUMN_TYPE.get(cprofile.dtype, "string"),
                generator=gen,
                nullable=cprofile.null_rate > 0,
                null_rate=cprofile.null_rate,
            )

        pk = list(tprofile.primary_key or [])
        if not pk:
            # No PK on the safe profile — inject a synthetic surrogate key so
            # the schema validates and generation can proceed (mirrors
            # SchemaBuilder's _row_id behaviour).
            columns["_row_id"] = ColumnDef(
                name="_row_id",
                type="integer",
                generator={"strategy": "sequence", "start": 1},
                nullable=False,
                null_rate=0.0,
            )
            pk = ["_row_id"]

        return TableDef(
            name=tname,
            columns=columns,
            primary_key=pk,
            description=f"Adapted from SafeProfile ({tprofile.row_count} rows)",
        )

    def _column_to_generator(
        self,
        col: SafeColumnProfile,
        *,
        is_pk: bool,
        fk_parent: str | None,
        parent_pk_map: dict[str, str],
    ) -> dict[str, Any]:
        """Map one SafeColumnProfile to a generator dict (safe fields only)."""

        # 1. Primary key — sequence (no raw min on the safe model, so start=1).
        if is_pk:
            if col.dtype == "string" or col.pattern == "uuid":
                return {"strategy": "uuid"}
            return {"strategy": "sequence", "start": 1}

        # 2. Foreign key — advisory detected FK (declaration overrides upstream).
        if fk_parent:
            parent_pk = parent_pk_map.get(fk_parent, f"{fk_parent}_id")
            return {"strategy": "foreign_key", "ref": f"{fk_parent}.{parent_pk}"}

        # 3. Pattern columns -> faker provider (PII-gate refinement in STORY-008).
        pattern_gen = self._pattern_generator(col)
        if pattern_gen is not None:
            return pattern_gen

        # 4. Date / datetime -> temporal strategy.
        if col.dtype in ("date", "datetime"):
            return self._temporal_generator(col)

        # 5. Categorical -> weighted_enum from post-suppression weights
        #    (includes __OTHER__). ADR-003.
        if col.categorical_weights:
            return {
                "strategy": "weighted_enum",
                "values": dict(col.categorical_weights),
            }

        # 6. Boolean fallback.
        if col.dtype == "boolean":
            return {
                "strategy": "weighted_enum",
                "values": {"true": 0.5, "false": 0.5},
            }

        # 7. Numeric -> distribution (clipped to bounds) or empirical.
        if col.dtype in ("integer", "float"):
            return self._numeric_generator(col)

        # 8. String fallback -> faker (name-heuristic provider via engine).
        return {"strategy": "faker", "provider": _guess_provider(col.name)}

    # ------------------------------------------------------------------
    # Generator builders
    # ------------------------------------------------------------------

    def _pattern_generator(self, col: SafeColumnProfile) -> dict[str, Any] | None:
        """Pattern -> faker provider (only on non-numeric columns)."""
        if not col.pattern or col.dtype in ("integer", "float"):
            return None
        provider = _PATTERN_TO_FAKER.get(col.pattern)
        if provider is None:
            return None
        return {"strategy": "faker", "provider": provider}

    def _temporal_generator(self, col: SafeColumnProfile) -> dict[str, Any]:
        gen: dict[str, Any] = {"strategy": "temporal", "type": col.dtype}
        th = col.temporal_histogram or {}
        # Coarse SAFE year range (winsorized p1/p99 years; aggregate, no raw
        # date) so regenerated dates land in the right PERIOD. Closes the
        # datetime fidelity gap (was: engine default 2022-2025 = wrong era).
        if th.get("lo_year") is not None and th.get("hi_year") is not None:
            gen["date_range"] = {
                "start": f"{int(th['lo_year'])}-01-01",
                "end": f"{int(th['hi_year'])}-12-31",
            }
        if col.hour_histogram or col.dow_histogram or th.get("month_weights"):
            gen["pattern"] = "seasonal"
            profiles: dict[str, dict[str, float]] = {}
            if th.get("month_weights"):
                profiles["month"] = {
                    str(i + 1): float(w) for i, w in enumerate(th["month_weights"])
                }
            if col.hour_histogram:
                profiles["hour_of_day"] = {
                    str(h): float(w) for h, w in enumerate(col.hour_histogram)
                }
            if col.dow_histogram:
                dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                profiles["day_of_week"] = {
                    dow_names[i]: float(w)
                    for i, w in enumerate(col.dow_histogram)
                    if i < len(dow_names)
                }
            gen["profiles"] = profiles
        return gen

    def _numeric_generator(self, col: SafeColumnProfile) -> dict[str, Any]:
        """Numeric generator: distribution+params clipped to bounds (ADR-002).

        Preference order:
          1. A fitted distribution the engine implements, translated to engine
             param keys, with bounds injected as min/max for clipping.
          2. Otherwise quantiles -> empirical (still clipped to bounds).
          3. Otherwise a normal from mean/std (clipped to bounds).
          4. Otherwise a uniform across bounds, or a degenerate constant.
        """
        bounds = col.bounds or {}
        lo = bounds.get("lo")
        hi = bounds.get("hi")

        # 1. Fitted distribution the engine can generate.
        engine_dist = _translate_distribution(
            col.distribution, col.distribution_params
        )
        if engine_dist is not None:
            name, params = engine_dist
            params = dict(params)
            _inject_bounds(params, lo, hi)
            return {"strategy": "distribution", "distribution": name, "params": params}

        # 2. Empirical from the quantile fingerprint (needs the full p1..p99 set).
        #    Thread the winsorized bounds through so the empirical strategy clips
        #    regenerated values to [lo, hi] (ADR-002 / STORY-006). The empirical
        #    quantile interpolation naturally clamps to [p1, p99]; explicit
        #    bounds enforce the configured window (e.g. widened p0.5/p99.5).
        if col.quantiles and _has_full_quantiles(col.quantiles):
            gen: dict[str, Any] = {
                "strategy": "empirical",
                "quantiles": dict(col.quantiles),
            }
            if lo is not None:
                gen["min"] = float(lo)
            if hi is not None:
                gen["max"] = float(hi)
            return gen

        # 3. Normal from mean/std, clipped to bounds.
        if col.mean is not None and col.std is not None:
            params = {"mean": float(col.mean), "std_dev": max(float(col.std), 1e-9)}
            _inject_bounds(params, lo, hi)
            return {"strategy": "distribution", "distribution": "normal", "params": params}

        # 4. Uniform across bounds (or a degenerate constant if bounds absent).
        if lo is not None and hi is not None:
            return {
                "strategy": "distribution",
                "distribution": "uniform",
                "params": {"min": float(lo), "max": float(hi)},
            }
        const = float(lo if lo is not None else (hi if hi is not None else 0.0))
        return {
            "strategy": "distribution",
            "distribution": "uniform",
            "params": {"min": const, "max": const},
        }

    # ------------------------------------------------------------------
    # Relationships / generation config
    # ------------------------------------------------------------------

    def _build_relationships(self, profile: SafeProfile) -> list[RelationshipDef]:
        relationships: list[RelationshipDef] = []
        for rel in profile.relationships:
            try:
                relationships.append(
                    RelationshipDef(
                        name=rel["name"],
                        parent=rel["parent"],
                        child=rel["child"],
                        parent_columns=rel["parent_columns"],
                        child_columns=rel["child_columns"],
                        type=rel.get("type", "one_to_many"),
                    )
                )
            except (KeyError, TypeError):
                # Malformed relationship entry — skip rather than fail the bridge.
                continue
        return relationships

    def _build_generation_config(self, profile: SafeProfile) -> GenerationConfig:
        small: dict[str, int] = {}
        medium: dict[str, int] = {}
        large: dict[str, int] = {}
        for tname, tprofile in profile.tables.items():
            rc = max(int(tprofile.row_count), 0)
            small[tname] = max(rc, 100)
            medium[tname] = max(rc * 10, 1000)
            large[tname] = max(rc * 100, 10000)
        return GenerationConfig(
            scale="small",
            scales={"small": small, "medium": medium, "large": large},
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

# Pattern -> faker provider. Mirrors SchemaBuilder's pattern map so the adapter
# routes detected non-PII patterns consistently. (PII suppression is STORY-008.)
_PATTERN_TO_FAKER: dict[str, str] = {
    "uuid": "uuid4",
    "email": "email",
    "phone": "phone_number",
    "ssn": "ssn",
    "ip_address": "ipv4",
    "mac_address": "mac_address",
    "iban": "iban",
    "postal_code": "postcode",
    "currency_code": "currency_code",
    "language_code": "language_code",
}


def _has_full_quantiles(quantiles: dict[str, float]) -> bool:
    """The empirical strategy requires the full p1..p99 fingerprint."""
    required = ("p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99")
    return all(k in quantiles and quantiles[k] is not None for k in required)


def _inject_bounds(
    params: dict[str, Any],
    lo: float | None,
    hi: float | None,
) -> None:
    """Thread winsorized bounds into engine ``min``/``max`` for clipping (ADR-002)."""
    if lo is not None:
        params["min"] = float(lo)
    if hi is not None:
        params["max"] = float(hi)


def _translate_distribution(
    name: str | None,
    params: dict[str, float] | None,
) -> tuple[str, dict[str, float]] | None:
    """Translate a profiler-fitted distribution to engine name + param keys.

    The profiler fits scipy distributions (normal/uniform/exponential/lognormal)
    with scipy-style ``loc``/``scale``/``s`` params. The engine's
    DistributionStrategy implements a different vocabulary with its own param
    keys. Returns ``None`` when there is no faithful engine equivalent (caller
    falls back to empirical / mean-std).
    """
    if not name or not params:
        return None

    if name == "normal":
        loc = params.get("loc")
        scale = params.get("scale")
        if loc is None or scale is None:
            return None
        return "normal", {"mean": float(loc), "std_dev": max(float(scale), 1e-9)}

    if name == "uniform":
        # scipy uniform(loc, scale) spans [loc, loc+scale].
        loc = params.get("loc")
        scale = params.get("scale")
        if loc is None or scale is None:
            return None
        return "uniform", {"min": float(loc), "max": float(loc) + float(scale)}

    if name == "lognormal":
        # scipy lognorm(s, loc, scale): the engine's log_normal takes
        # (mean, sigma) of the underlying normal. scipy ``s`` is that sigma and
        # ``scale`` == exp(mean).
        s = params.get("s")
        scale = params.get("scale")
        if s is None or scale is None or scale <= 0:
            return None
        return "log_normal", {
            "mean": float(math.log(scale)),
            "sigma": max(float(s), 1e-9),
        }

    # exponential and anything else: the engine has no faithful generator —
    # signal the caller to fall back (empirical / mean-std).
    return None


def _guess_provider(column_name: str) -> str:
    """Lightweight provider guess; the engine's FakerStrategy refines further."""
    lower = column_name.lower().strip()
    if "email" in lower:
        return "email"
    if "phone" in lower:
        return "phone_number"
    if "name" in lower:
        return "name"
    if "city" in lower:
        return "city"
    if "country" in lower:
        return "country"
    return "pystr"


def safe_profile_to_schema(
    profile: SafeProfile,
    domain_name: str = "safe_inferred",
) -> SpindleSchema:
    """Convenience wrapper around :meth:`SafeProfileAdapter.to_schema`."""
    return SafeProfileAdapter().to_schema(profile, domain_name=domain_name)
