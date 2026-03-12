"""Star schema transform — convert 3NF GenerationResult to fact/dim layout."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class DimSpec:
    """Specification for building one dimension table."""
    source: str                          # Source table name
    sk: str                              # Surrogate key column (e.g. "sk_customer")
    nk: str                              # Natural key column in source (e.g. "customer_id")
    enrich: list[dict[str, str]] | None = None  # Left joins: [{"table": t, "on": col}]
    include: list[str] | None = None     # Columns to keep (None = all)


@dataclass
class FactSpec:
    """Specification for building one fact table."""
    primary: str                                 # Primary source table
    joins: list[dict[str, str]] | None = None    # [{table, left_on, right_on}]
    fk_map: dict[str, str] | None = None         # col → dim_name (swap NK for SK)
    date_cols: list[str] | None = None           # datetime cols to add sk_date for
    include: list[str] | None = None             # Columns to keep (None = all)


class StarSchemaMap:
    """Describes how to transform a domain result into a star schema."""

    def __init__(
        self,
        dims: dict[str, DimSpec],
        facts: dict[str, FactSpec],
        generate_date_dim: bool = True,
        fiscal_year_start: int = 1,
    ):
        self.dims = dims
        self.facts = facts
        self.generate_date_dim = generate_date_dim
        self.fiscal_year_start = fiscal_year_start


@dataclass
class StarSchemaResult:
    """Result of a star schema transform."""

    dimensions: dict[str, pd.DataFrame]
    facts: dict[str, pd.DataFrame]
    date_dim: pd.DataFrame | None

    def __repr__(self) -> str:
        n_dims = len(self.dimensions)
        n_facts = len(self.facts)
        date_rows = len(self.date_dim) if self.date_dim is not None else 0
        return (
            f"StarSchemaResult("
            f"{n_dims} dimensions, {n_facts} facts, "
            f"dim_date={date_rows:,} rows)"
        )

    def summary(self) -> str:
        lines = ["Star Schema Result", "=" * 40]
        lines.append("DIMENSIONS:")
        for name, df in self.dimensions.items():
            lines.append(f"  {name:<25} {len(df):>10,} rows  {len(df.columns)} cols")
        if self.date_dim is not None:
            lines.append(
                f"  {'dim_date':<25} {len(self.date_dim):>10,} rows  {len(self.date_dim.columns)} cols"
            )
        lines.append("FACTS:")
        for name, df in self.facts.items():
            lines.append(f"  {name:<25} {len(df):>10,} rows  {len(df.columns)} cols")
        return "\n".join(lines)

    def all_tables(self) -> dict[str, pd.DataFrame]:
        """Return all tables (dimensions + date_dim + facts) as a flat dict."""
        result = {}
        result.update(self.dimensions)
        if self.date_dim is not None:
            result["dim_date"] = self.date_dim
        result.update(self.facts)
        return result


class StarSchemaTransform:
    """Transform a 3NF GenerationResult into a star schema using a StarSchemaMap."""

    def transform(
        self,
        tables: dict[str, pd.DataFrame],
        schema_map: StarSchemaMap,
    ) -> StarSchemaResult:
        """Apply the star schema transform.

        Args:
            tables: Dict of table_name → DataFrame (from GenerationResult.tables).
            schema_map: Mapping spec defining dims and facts.

        Returns:
            StarSchemaResult with dimensions, facts, and date_dim.
        """
        # 1. Build dimensions with surrogate keys
        dimensions: dict[str, pd.DataFrame] = {}
        sk_lookups: dict[str, dict[Any, int]] = {}  # dim_name → {nk_value: sk_int}

        for dim_name, spec in schema_map.dims.items():
            dim_df, sk_lookup = self._build_dimension(tables, spec)
            dimensions[dim_name] = dim_df
            sk_lookups[dim_name] = sk_lookup

        # 2. Build date dimension from actual data
        date_dim: pd.DataFrame | None = None
        date_sk_lookup: dict[int, int] = {}  # yyyymmdd → sk_date (same value)
        if schema_map.generate_date_dim:
            all_dates = self._collect_all_dates(tables, schema_map)
            if all_dates:
                date_dim = self._build_date_dim(
                    all_dates, schema_map.fiscal_year_start
                )
                # sk_date is yyyymmdd int — self-referencing (sk_date == yyyymmdd)
                date_sk_lookup = {row["sk_date"]: row["sk_date"] for _, row in date_dim.iterrows()}

        # 3. Build facts with SK replacements
        facts: dict[str, pd.DataFrame] = {}
        for fact_name, spec in schema_map.facts.items():
            fact_df = self._build_fact(
                tables, spec, sk_lookups, date_sk_lookup, dimensions, schema_map
            )
            facts[fact_name] = fact_df

        return StarSchemaResult(
            dimensions=dimensions,
            facts=facts,
            date_dim=date_dim,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_dimension(
        self,
        tables: dict[str, pd.DataFrame],
        spec: DimSpec,
    ) -> tuple[pd.DataFrame, dict[Any, int]]:
        """Build a single dimension table and return its NK→SK lookup."""
        if spec.source not in tables:
            return pd.DataFrame(), {}

        df = tables[spec.source].copy()

        # Enrich via left joins
        if spec.enrich:
            for enrich_spec in spec.enrich:
                right_table = enrich_spec["table"]
                # Support both "on" (shorthand, same col name both sides) and left_on/right_on
                left_on = enrich_spec.get("left_on", enrich_spec.get("on"))
                right_on = enrich_spec.get("right_on", left_on)
                prefix = enrich_spec.get("prefix", "")

                if right_table not in tables or left_on not in df.columns:
                    continue

                right_df = tables[right_table].copy()
                if prefix:
                    right_df = right_df.rename(
                        columns={c: f"{prefix}{c}" for c in right_df.columns}
                    )
                    right_on_actual = f"{prefix}{right_on}"
                else:
                    right_on_actual = right_on

                df = df.merge(
                    right_df, left_on=left_on, right_on=right_on_actual, how="left"
                )
                # Drop the right-side join key if it's a duplicate column
                if right_on_actual != left_on and right_on_actual in df.columns:
                    df = df.drop(columns=[right_on_actual])

        # Select columns
        if spec.include:
            existing = [c for c in spec.include if c in df.columns]
            df = df[existing].copy()

        # Build NK→SK lookup (1-based integer)
        if spec.nk not in df.columns:
            sk_lookup: dict[Any, int] = {}
        else:
            nk_values = df[spec.nk].tolist()
            sk_lookup = {nk: i + 1 for i, nk in enumerate(nk_values)}

        # Add surrogate key as first column
        df.insert(0, spec.sk, range(1, len(df) + 1))

        return df.reset_index(drop=True), sk_lookup

    def _build_fact(
        self,
        tables: dict[str, pd.DataFrame],
        spec: FactSpec,
        sk_lookups: dict[str, dict[Any, int]],
        date_sk_lookup: dict[int, int],
        dimensions: dict[str, pd.DataFrame],
        schema_map: StarSchemaMap,
    ) -> pd.DataFrame:
        """Build a single fact table with surrogate key replacements."""
        if spec.primary not in tables:
            return pd.DataFrame()

        df = tables[spec.primary].copy()

        # Apply joins
        if spec.joins:
            for join_spec in spec.joins:
                right_table = join_spec["table"]
                left_on = join_spec["left_on"]
                right_on = join_spec["right_on"]

                if right_table not in tables:
                    continue

                right_df = tables[right_table].copy()
                # Prefix joined columns to avoid collisions, except join key
                suffix = f"_{right_table}"
                df = df.merge(
                    right_df,
                    left_on=left_on,
                    right_on=right_on,
                    how="left",
                    suffixes=("", suffix),
                )
                # Drop duplicate join key from right
                dup = f"{right_on}{suffix}"
                if dup in df.columns:
                    df = df.drop(columns=[dup])

        # Replace natural FKs with surrogate keys
        if spec.fk_map:
            for col, dim_name in spec.fk_map.items():
                if col not in df.columns:
                    continue
                if dim_name not in sk_lookups:
                    continue
                lookup = sk_lookups[dim_name]
                # Keep original as nk_ column
                df[f"nk_{col}"] = df[col]
                # Find the SK column name from the dim spec
                dim_spec = schema_map.dims.get(dim_name)
                sk_col = dim_spec.sk if dim_spec else f"sk_{col}"
                df[sk_col] = df[col].map(lookup).astype("Int64")
                df = df.drop(columns=[col])

        # Add sk_date for date columns
        if spec.date_cols:
            for col in spec.date_cols:
                if col not in df.columns:
                    continue
                # Compute yyyymmdd from datetime col
                dates = pd.to_datetime(df[col], errors="coerce")
                sk_date_vals = (
                    dates.dt.year * 10000
                    + dates.dt.month * 100
                    + dates.dt.day
                ).astype("Int64")
                df["sk_date"] = sk_date_vals

        # Select columns
        if spec.include:
            existing = [c for c in spec.include if c in df.columns]
            df = df[existing].copy()

        return df.reset_index(drop=True)

    def _collect_all_dates(
        self,
        tables: dict[str, pd.DataFrame],
        schema_map: StarSchemaMap,
    ) -> list[pd.Timestamp]:
        """Collect all distinct dates across date columns referenced in facts."""
        dates: set[pd.Timestamp] = set()
        for spec in schema_map.facts.values():
            if not spec.date_cols:
                continue
            table_df = tables.get(spec.primary)
            if table_df is None:
                continue
            for col in spec.date_cols:
                if col not in table_df.columns:
                    continue
                col_dates = pd.to_datetime(table_df[col], errors="coerce").dropna()
                dates.update(col_dates.dt.normalize().unique())
        return sorted(dates)

    def _build_date_dim(
        self,
        dates: list[pd.Timestamp],
        fiscal_year_start: int = 1,
    ) -> pd.DataFrame:
        """Generate a date dimension spanning the full range of dates."""
        if not dates:
            return pd.DataFrame()

        start = dates[0].normalize()
        end = dates[-1].normalize()
        date_range = pd.date_range(start=start, end=end, freq="D")

        month_names = [
            "", "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        rows = []
        for d in date_range:
            month = d.month
            day_of_week = d.dayofweek  # 0=Mon, 6=Sun

            # Fiscal year: offset by fiscal_year_start (month 1-12)
            if month >= fiscal_year_start:
                fiscal_year = d.year
                fiscal_month = month - fiscal_year_start + 1
            else:
                fiscal_year = d.year - 1
                fiscal_month = month + 12 - fiscal_year_start + 1
            fiscal_quarter = (fiscal_month - 1) // 3 + 1

            rows.append({
                "sk_date": int(d.strftime("%Y%m%d")),
                "date": d.date(),
                "year": d.year,
                "quarter": (month - 1) // 3 + 1,
                "month": month,
                "month_name": month_names[month],
                "week_of_year": int(d.strftime("%V")),
                "day_of_month": d.day,
                "day_of_week": day_of_week + 1,  # 1=Mon, 7=Sun
                "day_of_week_name": day_names[day_of_week],
                "is_weekend": day_of_week >= 5,
                "is_weekday": day_of_week < 5,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
            })

        return pd.DataFrame(rows)
