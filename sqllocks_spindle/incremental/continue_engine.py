"""ContinueEngine — incremental delta generation from existing Spindle data."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.incremental.continue_config import ContinueConfig


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class DeltaResult:
    """Result of incremental generation."""

    inserts: dict[str, pd.DataFrame]
    updates: dict[str, pd.DataFrame]
    deletes: dict[str, pd.DataFrame]
    combined: dict[str, pd.DataFrame]
    stats: dict[str, dict[str, int]]

    def summary(self) -> str:
        lines = ["Incremental Generation Result", "=" * 40]
        for table, s in self.stats.items():
            lines.append(
                f"  {table}: +{s['inserts']} inserts, "
                f"~{s['updates']} updates, -{s['deletes']} deletes"
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class ContinueEngine:
    """Generate incremental deltas (inserts, updates, deletes) from existing data."""

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def continue_from(
        self,
        existing: Any,  # GenerationResult | dict[str, pd.DataFrame]
        schema: Any | None = None,
        config: ContinueConfig | None = None,
    ) -> DeltaResult:
        """Generate incremental changes from existing data.

        Parameters
        ----------
        existing:
            Either a ``GenerationResult`` (from ``Spindle.generate()``) or a
            plain ``dict[str, pd.DataFrame]``.
        schema:
            Optional ``SpindleSchema``.  If *existing* is a ``GenerationResult``
            the schema is extracted automatically.
        config:
            ``ContinueConfig`` controlling insert/update/delete volumes and
            state-transition rules.  Defaults are used when ``None``.
        """
        config = config or ContinueConfig()

        # Normalise input ------------------------------------------------
        tables, resolved_schema = self._normalise_input(existing, schema)

        # RNG
        rng = np.random.default_rng(config.seed)

        # Determine table processing order (use schema dep order if available)
        table_order = self._table_order(tables, resolved_schema)

        # Collect PK column names per table
        pk_map = self._pk_map(tables, resolved_schema)

        # FK column -> parent table mapping
        fk_map = self._fk_map(tables, resolved_schema)

        # Timestamp for all deltas in this batch
        now = pd.Timestamp.now()

        # Accumulate results per table
        all_inserts: dict[str, pd.DataFrame] = {}
        all_updates: dict[str, pd.DataFrame] = {}
        all_deletes: dict[str, pd.DataFrame] = {}
        all_combined: dict[str, pd.DataFrame] = {}
        all_stats: dict[str, dict[str, int]] = {}

        # Track new PKs so child inserts can reference them
        new_pk_pools: dict[str, pd.Series] = {}

        for table_name in table_order:
            df_existing = tables[table_name]
            pk_cols = pk_map.get(table_name, [])
            fk_cols = fk_map.get(table_name, {})

            # --- INSERTS -----------------------------------------------
            ins_df = self._generate_inserts(
                table_name, df_existing, pk_cols, fk_cols,
                config, rng, tables, new_pk_pools,
            )

            # Record new PKs for downstream FK sampling
            if pk_cols and len(ins_df) > 0:
                new_pk_pools[table_name] = ins_df[pk_cols[0]]

            # --- UPDATES -----------------------------------------------
            upd_df = self._generate_updates(
                table_name, df_existing, pk_cols, fk_cols,
                config, rng,
            )

            # --- DELETES -----------------------------------------------
            del_df = self._generate_deletes(
                table_name, df_existing, config, rng,
            )

            # --- Tag & combine -----------------------------------------
            ins_df = self._tag(ins_df, "INSERT", config, now)
            upd_df = self._tag(upd_df, "UPDATE", config, now)
            del_df = self._tag(del_df, "DELETE", config, now)

            parts = [p for p in [ins_df, upd_df, del_df] if len(p) > 0]
            if parts:
                combined = pd.concat(parts, ignore_index=True)
            else:
                # Empty DataFrame with correct columns
                extra = [config.delta_type_column, config.timestamp_column]
                combined = pd.DataFrame(
                    columns=list(df_existing.columns) + extra,
                )

            all_inserts[table_name] = ins_df
            all_updates[table_name] = upd_df
            all_deletes[table_name] = del_df
            all_combined[table_name] = combined
            all_stats[table_name] = {
                "inserts": len(ins_df),
                "updates": len(upd_df),
                "deletes": len(del_df),
            }

        return DeltaResult(
            inserts=all_inserts,
            updates=all_updates,
            deletes=all_deletes,
            combined=all_combined,
            stats=all_stats,
        )

    # ------------------------------------------------------------------ #
    # INSERT generation
    # ------------------------------------------------------------------ #

    def _generate_inserts(
        self,
        table_name: str,
        df_existing: pd.DataFrame,
        pk_cols: list[str],
        fk_cols: dict[str, str],
        config: ContinueConfig,
        rng: np.random.Generator,
        all_tables: dict[str, pd.DataFrame],
        new_pk_pools: dict[str, pd.Series],
    ) -> pd.DataFrame:
        n = config.insert_count
        if n <= 0 or len(df_existing) == 0:
            return pd.DataFrame(columns=df_existing.columns)

        # Sample rows from existing data as templates
        sample_idx = rng.choice(len(df_existing), size=n, replace=True)
        new_df = df_existing.iloc[sample_idx].copy().reset_index(drop=True)

        # Offset integer PK columns so they are unique / higher than existing
        for pk in pk_cols:
            if pk not in new_df.columns:
                continue
            col = df_existing[pk]
            if pd.api.types.is_integer_dtype(col):
                max_pk = int(col.max())
                new_df[pk] = np.arange(max_pk + 1, max_pk + 1 + n)

        # Re-sample FK columns from available parent PKs (existing + new)
        for fk_col, parent_table in fk_cols.items():
            if fk_col in pk_cols:
                continue  # Don't overwrite PK even if it looks like an FK
            if fk_col not in new_df.columns:
                continue
            parent_pks = self._parent_pk_pool(
                parent_table, all_tables, new_pk_pools,
            )
            if parent_pks is not None and len(parent_pks) > 0:
                new_df[fk_col] = rng.choice(
                    parent_pks.values, size=n, replace=True,
                )

        # Perturb non-key columns slightly so rows aren't exact copies
        non_key = [
            c for c in new_df.columns
            if c not in pk_cols and c not in fk_cols
        ]
        self._perturb_columns(new_df, non_key, rng, fraction=1.0)

        return new_df

    # ------------------------------------------------------------------ #
    # UPDATE generation
    # ------------------------------------------------------------------ #

    def _generate_updates(
        self,
        table_name: str,
        df_existing: pd.DataFrame,
        pk_cols: list[str],
        fk_cols: dict[str, str],
        config: ContinueConfig,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        n = max(1, int(len(df_existing) * config.update_fraction))
        if n <= 0 or len(df_existing) == 0:
            return pd.DataFrame(columns=df_existing.columns)

        idx = rng.choice(len(df_existing), size=min(n, len(df_existing)), replace=False)
        upd_df = df_existing.iloc[idx].copy().reset_index(drop=True)

        # Apply state transitions first
        transitioned_cols: set[str] = set()
        for key, transitions in config.state_transitions.items():
            tbl, col = key.split(".", 1) if "." in key else ("", key)
            if tbl != table_name:
                continue
            if col not in upd_df.columns:
                continue
            transitioned_cols.add(col)
            self._apply_transitions(upd_df, col, transitions, rng)

        # For columns without explicit transitions: perturb
        non_key = [
            c for c in upd_df.columns
            if c not in pk_cols
            and c not in fk_cols
            and c not in transitioned_cols
        ]
        self._perturb_columns(upd_df, non_key, rng, fraction=0.3)

        return upd_df

    # ------------------------------------------------------------------ #
    # DELETE generation
    # ------------------------------------------------------------------ #

    def _generate_deletes(
        self,
        table_name: str,
        df_existing: pd.DataFrame,
        config: ContinueConfig,
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        n = max(1, int(len(df_existing) * config.delete_fraction))
        if n <= 0 or len(df_existing) == 0:
            return pd.DataFrame(columns=df_existing.columns)

        idx = rng.choice(len(df_existing), size=min(n, len(df_existing)), replace=False)
        return df_existing.iloc[idx].copy().reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _tag(
        df: pd.DataFrame,
        delta_type: str,
        config: ContinueConfig,
        timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """Add _delta_type and _delta_timestamp columns."""
        if len(df) == 0:
            return df
        df = df.copy()
        df[config.delta_type_column] = delta_type
        df[config.timestamp_column] = timestamp
        return df

    @staticmethod
    def _normalise_input(existing: Any, schema: Any | None):
        """Return (tables dict, schema or None)."""
        # Avoid importing at module level so the engine works without full
        # Spindle install when using plain dict input.
        from sqllocks_spindle.engine.generator import GenerationResult

        if isinstance(existing, GenerationResult):
            return existing.tables, existing.schema if schema is None else schema
        if isinstance(existing, dict):
            return existing, schema
        raise TypeError(
            f"existing must be GenerationResult or dict[str, DataFrame], "
            f"got {type(existing).__name__}"
        )

    @staticmethod
    def _table_order(
        tables: dict[str, pd.DataFrame],
        schema: Any | None,
    ) -> list[str]:
        """Determine generation order.  Fall back to sorted keys."""
        if schema is not None:
            try:
                from sqllocks_spindle.schema.dependency import DependencyResolver
                resolver = DependencyResolver()
                order = resolver.resolve(schema)
                # Filter to tables present in data
                return [t for t in order if t in tables]
            except Exception:
                pass
        return sorted(tables.keys())

    @staticmethod
    def _pk_map(
        tables: dict[str, pd.DataFrame],
        schema: Any | None,
    ) -> dict[str, list[str]]:
        """Return {table: [pk_col, ...]}."""
        pk: dict[str, list[str]] = {}
        if schema is not None:
            for tname, tdef in schema.tables.items():
                if tname in tables:
                    pk[tname] = list(tdef.primary_key)
        # Fallback: guess columns ending with _id that are integer sequences
        for tname, df in tables.items():
            if tname not in pk:
                candidates = [
                    c for c in df.columns
                    if c.endswith("_id")
                    and pd.api.types.is_integer_dtype(df[c])
                ]
                pk[tname] = candidates[:1]
        return pk

    @staticmethod
    def _fk_map(
        tables: dict[str, pd.DataFrame],
        schema: Any | None,
    ) -> dict[str, dict[str, str]]:
        """Return {table: {fk_col: parent_table}}."""
        fk: dict[str, dict[str, str]] = {}
        if schema is not None:
            for tname, tdef in schema.tables.items():
                if tname not in tables:
                    continue
                col_map: dict[str, str] = {}
                for cname, cdef in tdef.columns.items():
                    if cdef.is_foreign_key and cdef.fk_ref_table:
                        col_map[cname] = cdef.fk_ref_table
                if col_map:
                    fk[tname] = col_map
        # No fallback heuristic — plain dicts just won't have FK info
        for tname in tables:
            fk.setdefault(tname, {})
        return fk

    @staticmethod
    def _parent_pk_pool(
        parent_table: str,
        all_tables: dict[str, pd.DataFrame],
        new_pk_pools: dict[str, pd.Series],
    ) -> pd.Series | None:
        """Combine existing + newly inserted PKs for a parent table."""
        parts: list[pd.Series] = []
        if parent_table in all_tables:
            df = all_tables[parent_table]
            # Use first column ending with _id as PK heuristic
            pk_candidates = [
                c for c in df.columns
                if c.endswith("_id") and pd.api.types.is_integer_dtype(df[c])
            ]
            if pk_candidates:
                parts.append(df[pk_candidates[0]])
        if parent_table in new_pk_pools:
            parts.append(new_pk_pools[parent_table])
        if parts:
            return pd.concat(parts, ignore_index=True)
        return None

    @staticmethod
    def _apply_transitions(
        df: pd.DataFrame,
        column: str,
        transitions: dict[str, dict[str, float]],
        rng: np.random.Generator,
    ) -> None:
        """Apply Markov state transitions in-place."""
        for i in range(len(df)):
            current = df.at[i, column]
            if pd.isna(current):
                continue
            current_str = str(current)
            if current_str not in transitions:
                continue
            next_states = transitions[current_str]
            states = list(next_states.keys())
            probs = list(next_states.values())
            # Normalise probabilities
            total = sum(probs)
            if total <= 0:
                continue
            probs = [p / total for p in probs]
            df.at[i, column] = rng.choice(states, p=probs)

    @staticmethod
    def _perturb_columns(
        df: pd.DataFrame,
        columns: list[str],
        rng: np.random.Generator,
        fraction: float = 0.3,
    ) -> None:
        """Perturb a fraction of values in the given columns in-place.

        - Numeric columns: multiply by a random factor in [0.9, 1.1].
        - Datetime columns: shift by 1-30 days.
        - Other columns: shuffle within the column.
        """
        if len(df) == 0:
            return

        n_perturb = max(1, int(len(df) * fraction))

        for col in columns:
            if col not in df.columns:
                continue

            idx = rng.choice(len(df), size=min(n_perturb, len(df)), replace=False)

            if pd.api.types.is_numeric_dtype(df[col]):
                factors = rng.uniform(0.9, 1.1, size=len(idx))
                vals = df[col].values.copy()
                # Preserve int type if applicable
                if pd.api.types.is_integer_dtype(df[col]):
                    perturbed = np.round(vals[idx].astype(float) * factors).astype(
                        vals.dtype
                    )
                else:
                    perturbed = vals[idx] * factors
                vals[idx] = perturbed
                df[col] = vals

            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                shifts = pd.to_timedelta(rng.integers(1, 31, size=len(idx)), unit="D")
                vals = df[col].copy()
                vals.iloc[idx] = vals.iloc[idx] + shifts
                df[col] = vals

            else:
                # Shuffle the selected positions among themselves
                subset = df[col].values[idx].copy()
                rng.shuffle(subset)
                vals = df[col].values.copy()
                vals[idx] = subset
                df[col] = vals
