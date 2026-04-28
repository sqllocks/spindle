"""Schema builder — convert a DatasetProfile into a SpindleSchema.

Takes the statistical profile produced by DataProfiler and maps each
column's characteristics to the appropriate Spindle generator strategy,
producing a complete .spindle.json-compatible schema.
"""

from __future__ import annotations

from typing import Any

from sqllocks_spindle.inference.profiler import ColumnProfile, DatasetProfile
from sqllocks_spindle.schema.parser import (
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)
from sqllocks_spindle.streaming.anomaly import AnomalyRegistry, PointAnomaly


# ---------------------------------------------------------------------------
# Heuristic maps for fallback Faker provider selection
# ---------------------------------------------------------------------------

_FAKER_NAME_HINTS: dict[str, str] = {
    "name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "full_name": "name",
    "email": "email",
    "phone": "phone_number",
    "phone_number": "phone_number",
    "address": "street_address",
    "street": "street_address",
    "city": "city",
    "state": "state",
    "zip": "zipcode",
    "zipcode": "zipcode",
    "zip_code": "zipcode",
    "postal_code": "zipcode",
    "country": "country",
    "company": "company",
    "company_name": "company",
    "url": "url",
    "website": "url",
    "username": "user_name",
    "user_name": "user_name",
    "description": "sentence",
    "comment": "sentence",
    "notes": "paragraph",
    "title": "catch_phrase",
    "job": "job",
    "job_title": "job",
    "ssn": "ssn",
    "sku": "bothify",
    "color": "color_name",
    "ip": "ipv4",
    "ip_address": "ipv4",
}


class SchemaBuilder:
    """Convert a DatasetProfile into a SpindleSchema."""

    def build(
        self,
        profile: DatasetProfile,
        domain_name: str = "inferred",
        fit_threshold: float = 0.80,
        correlation_threshold: float = 0.5,
        include_anomaly_registry: bool = False,
    ) -> "SpindleSchema | tuple[SpindleSchema, Any]":
        """Build a complete SpindleSchema from a dataset profile."""
        # Build parent PK lookup for FK references
        parent_pk_map: dict[str, str] = {}
        for tname, tprofile in profile.tables.items():
            if tprofile.primary_key:
                parent_pk_map[tname] = tprofile.primary_key[0]

        tables: dict[str, TableDef] = {}
        for tname, tprofile in profile.tables.items():
            columns: dict[str, ColumnDef] = {}
            for cname, cprofile in tprofile.columns.items():
                gen = self._column_to_generator(cprofile, parent_pk_map, fit_threshold=fit_threshold)
                spindle_type = _spindle_to_column_type(cprofile.dtype)

                columns[cname] = ColumnDef(
                    name=cname,
                    type=spindle_type,
                    generator=gen,
                    nullable=cprofile.null_rate > 0,
                    null_rate=cprofile.null_rate,
                )

            pk = tprofile.primary_key
            if not pk:
                # No PK detected — inject a synthetic surrogate key so the
                # schema passes validation and generation can proceed.
                # The _row_id column is skipped by the fidelity scorer
                # (is_primary_key=True → cardinality check bypassed).
                columns["_row_id"] = ColumnDef(
                    name="_row_id",
                    type="integer",
                    generator={"strategy": "sequence", "start": 1},
                    nullable=False,
                    null_rate=0.0,
                )
                pk = ["_row_id"]

            tables[tname] = TableDef(
                name=tname,
                columns=columns,
                primary_key=pk,
                description=f"Inferred from {tprofile.row_count} rows",
            )

        relationships = self._build_relationships(profile)
        generation = self._build_generation_config(profile)

        model = ModelDef(
            name=f"{domain_name}_inferred",
            description=f"Schema inferred from existing data ({len(tables)} tables)",
            domain=domain_name,
            schema_mode="3nf",
        )

        schema = SpindleSchema(
            model=model,
            tables=tables,
            relationships=relationships,
            business_rules=[],
            generation=generation,
        )

        # Compute correlated column pairs across all tables
        correlated_columns: dict[str, list[tuple[str, str, float]]] = {}
        for tname, tprofile in profile.tables.items():
            if tprofile.correlation_matrix:
                pairs: list[tuple[str, str, float]] = []
                seen: set[frozenset] = set()
                for col_a, row in tprofile.correlation_matrix.items():
                    for col_b, r_val in row.items():
                        if col_a == col_b:
                            continue
                        key = frozenset([col_a, col_b])
                        if key in seen:
                            continue
                        seen.add(key)
                        if abs(r_val) >= correlation_threshold:
                            pairs.append((col_a, col_b, r_val))
                if pairs:
                    correlated_columns[tname] = pairs

        if correlated_columns:
            schema.correlated_columns = correlated_columns

        if include_anomaly_registry:
            registry = self._build_anomaly_registry(profile)
            return schema, registry

        return schema

    # -----------------------------------------------------------------
    # Anomaly registry
    # -----------------------------------------------------------------

    def _build_anomaly_registry(self, profile: DatasetProfile) -> AnomalyRegistry:
        """Build an AnomalyRegistry from profiled outlier rates."""
        registry = AnomalyRegistry()
        for tname, tprofile in profile.tables.items():
            for cname, col in tprofile.columns.items():
                if col.outlier_rate and col.outlier_rate > 0.001:
                    registry.add(
                        PointAnomaly(
                            name=f"{tname}_{cname}_outlier",
                            column=cname,
                            multiplier_range=(3.0, 10.0),
                            fraction=col.outlier_rate,
                        )
                    )
        return registry

    # -----------------------------------------------------------------
    # Generator mapping
    # -----------------------------------------------------------------

    def _column_to_generator(self, col: ColumnProfile, parent_pk_map: dict[str, str] | None = None, fit_threshold: float = 0.80) -> dict:
        """Map a ColumnProfile to a Spindle generator dict."""

        # 1. Primary key — sequence or uuid
        if col.is_primary_key:
            if col.pattern == "uuid" or col.dtype == "string":
                return {"strategy": "uuid"}
            return {
                "strategy": "sequence",
                "start": int(col.min_value) if col.min_value is not None else 1,
            }

        # 2. Foreign key
        if col.is_foreign_key and col.fk_ref_table:
            parent_pk = (parent_pk_map or {}).get(col.fk_ref_table, f"{col.fk_ref_table}_id")
            return {"strategy": "foreign_key", "ref": f"{col.fk_ref_table}.{parent_pk}"}

        # 3. UUID pattern
        if col.pattern == "uuid":
            return {"strategy": "uuid"}

        # 4. Email pattern
        if col.pattern == "email":
            return {"strategy": "faker", "provider": "email"}

        # 5. Phone pattern
        if col.pattern == "phone":
            return {"strategy": "faker", "provider": "phone_number"}

        # 6. Extended string patterns → Faker providers
        _pattern_to_faker: dict[str, str] = {
            "ssn": "ssn",
            "ip_address": "ipv4",
            "mac_address": "mac_address",
            "iban": "iban",
            "postal_code": "postcode",
            "currency_code": "currency_code",
            "language_code": "language_code",
        }
        if col.pattern in _pattern_to_faker:
            return {"strategy": "faker", "provider": _pattern_to_faker[col.pattern]}

        # 7. Date string pattern
        if col.pattern == "date":
            return {"strategy": "temporal", "type": "date"}

        # 8. Date / datetime with temporal histograms
        if col.dtype in ("date", "datetime"):
            gen: dict = {"strategy": "temporal", "type": col.dtype}
            if col.min_value is not None:
                gen["start"] = str(col.min_value)
            if col.max_value is not None:
                gen["end"] = str(col.max_value)
            if col.hour_histogram or col.dow_histogram:
                gen["pattern"] = "seasonal"
                profiles: dict = {}
                if col.hour_histogram:
                    profiles["hour_of_day"] = {str(h): w for h, w in enumerate(col.hour_histogram)}
                if col.dow_histogram:
                    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    profiles["day_of_week"] = {dow_names[i]: w for i, w in enumerate(col.dow_histogram)}
                gen["profiles"] = profiles
            return gen

        # 9. Categorical / enum — prefer value_counts_ext for exact frequencies
        if col.is_enum:
            values = col.value_counts_ext if col.value_counts_ext else col.enum_values
            if values:
                return {"strategy": "weighted_enum", "values": values}

        # 10. Boolean
        if col.dtype == "boolean":
            return {"strategy": "weighted_enum", "values": {"true": 0.5, "false": 0.5}}

        # 11. Numeric with distribution fit
        if col.dtype in ("integer", "float"):
            if col.fit_score is not None and col.fit_score < fit_threshold:
                if col.quantiles:
                    return {"strategy": "empirical", "quantiles": col.quantiles}
                # No quantiles available despite low fit — fall through to distribution

            if col.distribution and col.distribution_params:
                return {
                    "strategy": "distribution",
                    "type": col.distribution,
                    "params": col.distribution_params,
                }

            # Numeric fallback — normal from observed stats
            params: dict = {}
            if col.mean is not None and col.std is not None:
                params = {"loc": col.mean, "scale": max(col.std, 0.01)}
            elif col.min_value is not None and col.max_value is not None:
                params = {
                    "loc": float(col.min_value),
                    "scale": float(col.max_value) - float(col.min_value),
                }
            return {"strategy": "distribution", "type": "normal", "params": params}

        # 12. String with length bounds → constrained faker
        if col.dtype == "string":
            if col.string_length:
                max_len = int(col.string_length.get("p95", col.string_length.get("max", 255)))
                provider = _guess_faker_provider(col.name)
                return {"strategy": "faker", "provider": provider, "max_length": max_len}
            provider = _guess_faker_provider(col.name)
            return {"strategy": "faker", "provider": provider}

        # 13. Ultimate fallback
        return {"strategy": "faker", "provider": "pystr"}

    # -----------------------------------------------------------------
    # Relationships
    # -----------------------------------------------------------------

    def _build_relationships(
        self,
        profile: DatasetProfile,
    ) -> list[RelationshipDef]:
        """Convert detected FK relationships into RelationshipDef objects."""
        relationships: list[RelationshipDef] = []
        for rel in profile.relationships:
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
        return relationships

    # -----------------------------------------------------------------
    # Generation config
    # -----------------------------------------------------------------

    def _build_generation_config(
        self,
        profile: DatasetProfile,
    ) -> GenerationConfig:
        """Derive scale presets from the observed row counts."""
        small: dict[str, int] = {}
        medium: dict[str, int] = {}
        large: dict[str, int] = {}

        for tname, tprofile in profile.tables.items():
            rc = tprofile.row_count
            small[tname] = max(rc, 100)
            medium[tname] = max(rc * 10, 1000)
            large[tname] = max(rc * 100, 10000)

        return GenerationConfig(
            scale="small",
            scales={
                "small": small,
                "medium": medium,
                "large": large,
            },
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spindle_to_column_type(dtype: str) -> str:
    """Map profiler dtype string to Spindle column type string."""
    mapping = {
        "integer": "integer",
        "float": "decimal",
        "string": "string",
        "date": "date",
        "datetime": "datetime",
        "boolean": "boolean",
    }
    return mapping.get(dtype, "string")


def _guess_faker_provider(column_name: str) -> str:
    """Guess a Faker provider from a column name using heuristics."""
    lower = column_name.lower().strip()

    # Direct match
    if lower in _FAKER_NAME_HINTS:
        return _FAKER_NAME_HINTS[lower]

    # Suffix match
    for hint, provider in _FAKER_NAME_HINTS.items():
        if lower.endswith(f"_{hint}") or lower.endswith(hint):
            return provider

    # Contains match (less specific)
    if "email" in lower:
        return "email"
    if "phone" in lower:
        return "phone_number"
    if "name" in lower:
        return "name"
    if "addr" in lower:
        return "street_address"
    if "city" in lower:
        return "city"
    if "state" in lower:
        return "state"
    if "country" in lower:
        return "country"
    if "date" in lower:
        return "date"
    if "url" in lower or "link" in lower:
        return "url"

    return "pystr"
