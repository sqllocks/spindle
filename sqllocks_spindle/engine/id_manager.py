"""ID Manager — tracks generated PKs and provides FK resolution."""

from __future__ import annotations

import threading
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd


@runtime_checkable
class PKPool(Protocol):
    """Protocol for PK pool implementations."""

    def __len__(self) -> int: ...
    def __getitem__(self, indices: np.ndarray) -> np.ndarray: ...


class RangePKPool:
    """Memory-efficient PK pool for contiguous integer ranges.

    Instead of storing all PK values (16 GB for 2B int64s), stores only
    (start, count) — 16 bytes total. FK sampling uses rng.integers()
    for O(1) memory.
    """

    def __init__(self, start: int, count: int):
        self._start = start
        self._count = count

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, indices: np.ndarray) -> np.ndarray:
        return np.asarray(indices, dtype=np.int64) + self._start

    def extend(self, additional_count: int) -> None:
        """Grow the range by additional_count contiguous values."""
        self._count += additional_count

    @property
    def start(self) -> int:
        return self._start

    @property
    def end(self) -> int:
        """Exclusive end of the range."""
        return self._start + self._count

    @property
    def count(self) -> int:
        return self._count


class IDManager:
    """Track all generated primary keys and provide foreign key resolution.

    The ID Manager is the backbone of relational integrity in Spindle.
    It maintains pools of generated PK values per table and provides
    various FK lookup strategies (random, weighted, constrained).
    """

    def __init__(self, rng: np.random.Generator):
        self._rng = rng
        # table_name -> numpy array of PK values
        self._pk_pools: dict[str, np.ndarray] = {}
        # table_name -> full DataFrame (for constrained/filtered lookups)
        self._table_data: dict[str, pd.DataFrame] = {}
        self._groupby_cache: dict[tuple[str, str], dict] = {}
        # (table_name, pk_column, lookup_column) -> indexed Series for fast reindex
        self._lookup_cache: dict[tuple[str, str, str], pd.Series] = {}
        # Lock for concurrent registration from parallel table generators
        self._lock = threading.Lock()

    def register_table(self, table_name: str, df: pd.DataFrame, pk_columns: list[str]) -> None:
        """Register a generated table's PKs for FK resolution (thread-safe).

        Tables with no primary_key (pk_columns=[]) are registered as data-only
        so lookups and constrained FKs still work, but get_random_fks() will
        raise KeyError since they have no PK pool.
        """
        if not pk_columns:
            with self._lock:
                self._table_data[table_name] = df
            return
        if len(pk_columns) == 1:
            pool = df[pk_columns[0]].values
        else:
            # Composite PK — vectorized via to_numpy() instead of itertuples
            pool = df[pk_columns].to_numpy()
        with self._lock:
            self._pk_pools[table_name] = pool
            self._table_data[table_name] = df
            self._groupby_cache.clear()
            self._lookup_cache = {k: v for k, v in self._lookup_cache.items() if k[0] != table_name}

    def register_range(self, table_name: str, start: int, count: int) -> None:
        """Register a contiguous integer PK range without storing all values.

        Uses RangePKPool for O(1) memory regardless of count.
        """
        self._pk_pools[table_name] = RangePKPool(start, count)

    def append_pks(self, table_name: str, new_pks: np.ndarray) -> None:
        """Grow a table's PK pool incrementally with new values (thread-safe).

        For RangePKPool, extends the range. For ndarray pools, concatenates.
        """
        with self._lock:
            pool = self._pk_pools.get(table_name)
            if pool is None:
                self._pk_pools[table_name] = new_pks
            elif isinstance(pool, RangePKPool):
                pool.extend(len(new_pks))
            else:
                self._pk_pools[table_name] = np.concatenate([pool, new_pks])
            self._groupby_cache.clear()
            self._lookup_cache = {k: v for k, v in self._lookup_cache.items() if k[0] != table_name}

    def get_random_fks(
        self,
        table_name: str,
        count: int,
        distribution: str = "uniform",
        params: dict | None = None,
    ) -> np.ndarray:
        """Get random FK values from a parent table's PK pool.

        Supports uniform, zipf, and pareto-weighted selection.
        """
        pool = self._pk_pools.get(table_name)
        if pool is None:
            raise KeyError(f"No PK pool registered for table '{table_name}'")

        pool_size = len(pool)
        params = params or {}

        if distribution == "uniform":
            indices = self._rng.integers(0, pool_size, size=count)
        elif distribution == "zipf":
            alpha = params.get("alpha", 1.5)
            raw = self._rng.zipf(alpha, size=count * 2)
            # Filter to valid range and take what we need
            valid = raw[raw <= pool_size] - 1  # zipf is 1-based
            while len(valid) < count:
                more = self._rng.zipf(alpha, size=count)
                valid = np.concatenate([valid, more[more <= pool_size] - 1])
            indices = valid[:count]
        elif distribution == "pareto":
            alpha = params.get("alpha", 1.2)
            max_per_parent = params.get("max_per_parent")
            raw = self._rng.pareto(alpha, size=count)
            # Cap extreme outliers before normalizing — using raw.max() causes all
            # indices to collapse near 0 at small pool sizes because Pareto tails
            # can be 50-100x the median with alpha=1.2. The 99.5th percentile cap
            # preserves the heavy-tail shape while preventing single-parent dominance.
            cap = float(np.percentile(raw, 99.5))
            capped = np.minimum(raw, cap)
            indices = (capped / (cap + 1e-9) * pool_size).astype(int)
            indices = np.clip(indices, 0, pool_size - 1)
            if max_per_parent is not None:
                indices = self._enforce_max_per_parent(indices, pool_size, int(max_per_parent))
        else:
            indices = self._rng.integers(0, pool_size, size=count)

        return pool[indices]

    def get_constrained_fks(
        self,
        table_name: str,
        constraint_column: str,
        constraint_values: np.ndarray,
        nullable: bool = False,
    ) -> np.ndarray:
        """Get FK values constrained by a parent column value.

        Example: get address_id values that belong to specific customer_ids.

        When nullable=True, rows where no matching parent exists get None instead
        of a random fallback. Set nullable=True for nullable FK columns.
        """
        df = self._table_data.get(table_name)
        if df is None:
            raise KeyError(f"No table data registered for '{table_name}'")

        pk_col = self._pk_pools[table_name]

        if constraint_column not in df.columns:
            return self.get_random_fks(table_name, len(constraint_values))

        cache_key = (table_name, constraint_column)
        if cache_key not in self._groupby_cache:
            self._groupby_cache[cache_key] = df.groupby(constraint_column).indices
        grouped = self._groupby_cache[cache_key]

        result_dtype = object if nullable else (pk_col.dtype if hasattr(pk_col, 'dtype') else object)
        result = np.empty(len(constraint_values), dtype=result_dtype)

        # Vectorized: group constraint_values and batch-process each group
        cv_series = pd.Series(constraint_values)
        for val, positions in cv_series.groupby(cv_series).groups.items():
            pos_array = positions.to_numpy()
            if val is not None and val in grouped:
                candidates = grouped[val]
                chosen_idx = self._rng.choice(candidates, size=len(pos_array))
                # Safe indexing — cap to pool bounds
                safe_idx = np.clip(chosen_idx, 0, len(pk_col) - 1)
                result[pos_array] = pk_col[safe_idx]
            elif nullable:
                result[pos_array] = None
            else:
                result[pos_array] = self._rng.choice(pk_col, size=len(pos_array))

        return result

    def get_filtered_fks(
        self,
        table_name: str,
        filter_column: str,
        filter_value: str,
        count: int,
    ) -> np.ndarray:
        """Get FK values filtered by a column condition.

        Example: get order_ids where status='completed'.
        """
        df = self._table_data.get(table_name)
        pool = self._pk_pools.get(table_name)
        if df is None or pool is None:
            raise KeyError(f"No table data registered for '{table_name}'")

        mask = df[filter_column] == filter_value
        # Use the PK pool (aligned with df rows) rather than df.columns[0]
        # so the correct PK column is returned regardless of column order.
        filtered_indices = np.where(mask.values)[0]

        if len(filtered_indices) == 0:
            raise ValueError(
                f"No rows in '{table_name}' match filter {filter_column}='{filter_value}'"
            )

        filtered_pks = pool[filtered_indices]
        chosen = self._rng.integers(0, len(filtered_pks), size=count)
        return filtered_pks[chosen]

    def get_sampled_fks(
        self,
        table_name: str,
        sample_rate: float,
        filter_column: str | None = None,
        filter_value: str | None = None,
    ) -> np.ndarray:
        """Get a sample of PKs (e.g., 8% of orders for returns).

        Returns the actual FK values, not indices.
        """
        df = self._table_data.get(table_name)
        if df is None:
            raise KeyError(f"No table data registered for '{table_name}'")

        if filter_column and filter_value:
            mask = df[filter_column] == filter_value
            pool = self._pk_pools[table_name][mask.values]
        else:
            pool = self._pk_pools[table_name]

        count = max(1, int(len(pool) * sample_rate))
        indices = self._rng.choice(len(pool), size=count, replace=False)
        return pool[indices]

    def lookup_values(
        self,
        table_name: str,
        lookup_column: str,
        fk_values: np.ndarray,
        pk_column: str,
    ) -> np.ndarray:
        """Look up column values from a parent table via FK.

        Example: get product.unit_price for each order_line.product_id.
        """
        df = self._table_data.get(table_name)
        if df is None:
            raise KeyError(f"No table data registered for '{table_name}'")

        cache_key = (table_name, pk_column, lookup_column)
        if cache_key not in self._lookup_cache:
            self._lookup_cache[cache_key] = df.set_index(pk_column)[lookup_column]
        return self._lookup_cache[cache_key].reindex(fk_values).values

    def _enforce_max_per_parent(
        self,
        indices: np.ndarray,
        pool_size: int,
        max_per_parent: int,
    ) -> np.ndarray:
        """Redistribute excess assignments when a parent exceeds max_per_parent.

        Iterates until no parent exceeds the limit. Excess rows are reassigned
        only to parents that still have capacity, preserving the heavy-tail shape
        while guaranteeing no single parent exceeds max_per_parent.
        """
        indices = indices.copy()
        for _ in range(10):  # safety cap on iterations
            counts = np.bincount(indices, minlength=pool_size)
            over_mask = counts > max_per_parent
            if not over_mask.any():
                break

            # Collect positions to reassign
            reassign_positions = []
            for idx in np.where(over_mask)[0]:
                excess = counts[idx] - max_per_parent
                positions = np.where(indices == idx)[0]
                chosen = self._rng.choice(positions, size=excess, replace=False)
                reassign_positions.append(chosen)

            if not reassign_positions:
                break

            to_reassign = np.concatenate(reassign_positions)
            # Only pick targets that are below the limit
            under_capacity = np.where(counts < max_per_parent)[0]
            if len(under_capacity) == 0:
                # All parents at limit — spread uniformly
                under_capacity = np.arange(pool_size)
            indices[to_reassign] = self._rng.choice(under_capacity, size=len(to_reassign))
        return indices

    def get_pool_size(self, table_name: str) -> int:
        pool = self._pk_pools.get(table_name)
        return len(pool) if pool is not None else 0

    @property
    def registered_tables(self) -> list[str]:
        return list(self._pk_pools.keys())
