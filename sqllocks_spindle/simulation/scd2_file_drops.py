"""SCD Type 2 file-drop simulator — generate initial full loads and daily deltas
with SCD2-style versioning (valid_from / valid_to / is_current tracking).

Produces an initial snapshot followed by *num_delta_days* daily delta files,
each containing INSERT rows for new business entities and UPDATE pairs for
changed entities (expired old row + new current row).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class SCD2FileDropConfig:
    """Configuration for SCD2 file-drop simulation.

    Args:
        domain: Domain name used in path construction (e.g. ``"retail"``).
        base_path: Root directory for landing files.
        business_key_column: Column that identifies the business entity.
        scd2_columns: Columns to track for changes.
        effective_date_column: Name of the valid-from column.
        end_date_column: Name of the valid-to column.
        is_current_column: Name of the is-current flag column.
        initial_load_date: Date string for the initial snapshot (``YYYY-MM-DD``).
        num_delta_days: Number of daily delta files to generate.
        daily_change_rate: Fraction of records that change per day.
        daily_new_rate: Fraction of new records per day (relative to initial count).
        formats: File formats to write (``"parquet"``, ``"csv"``, ``"jsonl"``).
        manifest_enabled: Write a ``_manifest.json`` alongside each drop.
        seed: Random seed for reproducibility.
    """

    domain: str = "default"
    base_path: str = "Files/landing"
    business_key_column: str = "id"
    scd2_columns: list[str] = field(default_factory=list)
    effective_date_column: str = "valid_from"
    end_date_column: str = "valid_to"
    is_current_column: str = "is_current"
    initial_load_date: str = "2024-01-01"
    num_delta_days: int = 30
    daily_change_rate: float = 0.05
    daily_new_rate: float = 0.02
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    manifest_enabled: bool = True
    seed: int = 42


@dataclass
class SCD2FileDropResult:
    """Result of an SCD2 file-drop simulation run.

    Attributes:
        initial_load_path: Path to the initial full-load file.
        delta_paths: Paths to daily delta files.
        manifest_paths: Paths to ``_manifest.json`` files.
        stats: Aggregate statistics for the simulation run.
    """

    initial_load_path: Path
    delta_paths: list[Path]
    manifest_paths: list[Path]
    stats: dict[str, Any]

    def __repr__(self) -> str:
        return (
            f"SCD2FileDropResult(initial={self.initial_load_path.name}, "
            f"deltas={len(self.delta_paths)}, "
            f"stats={self.stats})"
        )


class SCD2FileDropSimulator:
    """Simulate an upstream source landing SCD2-versioned files over time.

    Generates an initial full snapshot and then daily delta files containing
    INSERT rows (new entities) and UPDATE rows (changed entities with
    valid_from / valid_to / is_current tracking).

    Args:
        tables: Mapping of ``entity_name -> DataFrame`` from a generation result.
        config: :class:`SCD2FileDropConfig` controlling paths and simulation
            parameters.

    Example::

        from sqllocks_spindle.simulation.scd2_file_drops import (
            SCD2FileDropSimulator,
            SCD2FileDropConfig,
        )

        cfg = SCD2FileDropConfig(
            domain="retail",
            business_key_column="customer_id",
            scd2_columns=["status", "address", "tier"],
        )
        result = SCD2FileDropSimulator(tables=gen_result.tables, config=cfg).run()
    """

    def __init__(
        self,
        tables: dict[str, pd.DataFrame],
        config: SCD2FileDropConfig,
    ) -> None:
        self._tables = tables
        self._config = config
        self._rng = np.random.default_rng(config.seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> SCD2FileDropResult:
        """Execute the SCD2 file-drop simulation and return results."""
        cfg = self._config

        # We simulate against the first (or only) entity.  If more than one
        # table is provided, each is processed and results are merged.
        entity_names = list(self._tables.keys())

        all_delta_paths: list[Path] = []
        all_manifest_paths: list[Path] = []
        initial_load_path: Path | None = None

        total_initial_rows = 0
        total_new = 0
        total_updates = 0
        total_deltas = 0

        for entity in entity_names:
            df = self._tables[entity].copy()

            # Write initial load
            init_path, init_manifests = self._write_initial_load(df, entity)
            if initial_load_path is None:
                initial_load_path = init_path
            all_manifest_paths.extend(init_manifests)
            total_initial_rows += len(df)

            # Build current state: only "current" rows keyed by business key
            current_state = self._build_initial_state(df, entity)

            # Counter for version suffixes per business-key
            version_counters: dict[Any, int] = {
                key: 1 for key in current_state.keys()
            }
            next_bk_id = self._compute_next_bk_id(current_state)

            base_date = datetime.strptime(cfg.initial_load_date, "%Y-%m-%d")

            for day_offset in range(1, cfg.num_delta_days + 1):
                day_date = base_date + timedelta(days=day_offset)

                delta_df, day_new, day_updates, current_state, version_counters, next_bk_id = (
                    self._generate_daily_delta(
                        day_date, current_state, entity, version_counters, next_bk_id,
                    )
                )

                if delta_df.empty:
                    continue

                delta_files, delta_manifests = self._write_delta(
                    delta_df, day_date, entity,
                )
                all_delta_paths.extend(delta_files)
                all_manifest_paths.extend(delta_manifests)

                total_new += day_new
                total_updates += day_updates
                total_deltas += len(delta_df)

        stats: dict[str, Any] = {
            "initial_rows": total_initial_rows,
            "total_deltas": total_deltas,
            "total_new": total_new,
            "total_updates": total_updates,
            "days_simulated": cfg.num_delta_days,
        }

        return SCD2FileDropResult(
            initial_load_path=initial_load_path or Path(cfg.base_path),
            delta_paths=all_delta_paths,
            manifest_paths=all_manifest_paths,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Initial load
    # ------------------------------------------------------------------

    def _write_initial_load(
        self, df: pd.DataFrame, entity: str,
    ) -> tuple[Path, list[Path]]:
        """Write the full initial snapshot with SCD2 metadata columns."""
        cfg = self._config
        initial_date = datetime.strptime(cfg.initial_load_date, "%Y-%m-%d")

        snapshot = df.copy()
        snapshot[cfg.effective_date_column] = initial_date
        snapshot[cfg.end_date_column] = None
        snapshot[cfg.is_current_column] = True

        base = Path(cfg.base_path) / cfg.domain / entity / "initial"
        base.mkdir(parents=True, exist_ok=True)

        manifest_paths: list[Path] = []
        written_path: Path | None = None

        for fmt in cfg.formats:
            ext = fmt
            filename = f"{entity}_initial.{ext}"
            path = base / filename
            self._write_df(snapshot, path, fmt)
            if written_path is None:
                written_path = path

        if cfg.manifest_enabled:
            mp = self._write_manifest(
                base,
                [base / f"{entity}_initial.{fmt}" for fmt in cfg.formats],
                initial_date,
                entity,
            )
            manifest_paths.append(mp)

        return written_path or base / f"{entity}_initial.{cfg.formats[0]}", manifest_paths

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def _build_initial_state(
        self, df: pd.DataFrame, entity: str,
    ) -> dict[Any, dict[str, Any]]:
        """Build a dict of business_key -> current row values from the initial load."""
        cfg = self._config
        bk_col = cfg.business_key_column
        state: dict[Any, dict[str, Any]] = {}

        for _, row in df.iterrows():
            key = row[bk_col]
            state[key] = row.to_dict()

        return state

    def _compute_next_bk_id(self, current_state: dict[Any, dict[str, Any]]) -> int:
        """Compute the next integer business key for new inserts.

        Falls back to len(current_state) + 1 if keys are not numeric.
        """
        numeric_keys = [k for k in current_state if isinstance(k, (int, float, np.integer))]
        if numeric_keys:
            return int(max(numeric_keys)) + 1
        return len(current_state) + 1

    # ------------------------------------------------------------------
    # Daily delta generation
    # ------------------------------------------------------------------

    def _generate_daily_delta(
        self,
        day_date: datetime,
        current_state: dict[Any, dict[str, Any]],
        entity: str,
        version_counters: dict[Any, int],
        next_bk_id: int,
    ) -> tuple[pd.DataFrame, int, int, dict[Any, dict[str, Any]], dict[Any, int], int]:
        """Generate a single day's delta containing INSERTs and UPDATEs.

        Returns:
            Tuple of (delta_df, new_count, update_count, updated_state,
            updated_version_counters, updated_next_bk_id).
        """
        cfg = self._config
        bk_col = cfg.business_key_column
        all_keys = list(current_state.keys())
        delta_rows: list[dict[str, Any]] = []

        # --- Updates: pick a fraction of existing records to change ---
        num_updates = max(1, int(len(all_keys) * cfg.daily_change_rate))
        if num_updates > len(all_keys):
            num_updates = len(all_keys)

        update_keys = self._rng.choice(
            all_keys, size=num_updates, replace=False,
        ).tolist()

        update_count = 0
        for key in update_keys:
            old_row = current_state[key].copy()
            version_counters[key] = version_counters.get(key, 1) + 1
            version_num = version_counters[key]

            # Expire the old row
            expired_row = old_row.copy()
            expired_row[cfg.end_date_column] = day_date
            expired_row[cfg.is_current_column] = False
            expired_row["_delta_type"] = "update"
            delta_rows.append(expired_row)

            # Create new current row with modified SCD2 columns
            new_row = old_row.copy()
            new_row = self._mutate_scd2_columns(new_row, version_num)
            new_row[cfg.effective_date_column] = day_date
            new_row[cfg.end_date_column] = None
            new_row[cfg.is_current_column] = True
            new_row["_delta_type"] = "update"
            delta_rows.append(new_row)

            # Update current state
            current_state[key] = new_row.copy()
            update_count += 1

        # --- Inserts: new business entities ---
        num_new = max(1, int(len(all_keys) * cfg.daily_new_rate))
        new_count = 0

        # Use a template row to know what columns exist
        template_key = all_keys[0]
        template_row = current_state[template_key]

        for i in range(num_new):
            new_row = self._generate_new_row(
                template_row, next_bk_id, day_date,
            )
            new_row["_delta_type"] = "insert"
            delta_rows.append(new_row)

            current_state[next_bk_id] = new_row.copy()
            version_counters[next_bk_id] = 1
            next_bk_id += 1
            new_count += 1

        if not delta_rows:
            return pd.DataFrame(), 0, 0, current_state, version_counters, next_bk_id

        delta_df = pd.DataFrame(delta_rows)
        return delta_df, new_count, update_count, current_state, version_counters, next_bk_id

    def _mutate_scd2_columns(
        self, row: dict[str, Any], version_num: int,
    ) -> dict[str, Any]:
        """Apply random mutations to tracked SCD2 columns in a row."""
        cfg = self._config

        for col in cfg.scd2_columns:
            if col not in row:
                continue
            row[col] = self._mutate_value(row[col], col, version_num)

        return row

    def _mutate_value(self, value: Any, col_name: str, version_num: int) -> Any:
        """Produce a changed value for a single SCD2 column.

        - Strings: append ``_v{N}`` or strip existing suffix and re-append.
        - Numerics: multiply by a random factor in [0.8, 1.2].
        - Booleans: flip.
        - None / other: return unchanged.
        """
        if isinstance(value, bool) or (isinstance(value, (np.bool_,))):
            return not value

        if isinstance(value, str):
            # Strip existing version suffix if present
            base = value
            for i in range(version_num, 0, -1):
                suffix = f"_v{i}"
                if base.endswith(suffix):
                    base = base[: -len(suffix)]
                    break
            return f"{base}_v{version_num}"

        if isinstance(value, (int, float, np.integer, np.floating)):
            factor = float(self._rng.uniform(0.8, 1.2))
            mutated = value * factor
            if isinstance(value, (int, np.integer)):
                return int(round(mutated))
            return mutated

        # Fallback: return unchanged
        return value

    def _generate_new_row(
        self,
        template_row: dict[str, Any],
        new_bk_id: int,
        day_date: datetime,
    ) -> dict[str, Any]:
        """Create a brand-new entity row based on the template structure."""
        cfg = self._config
        new_row: dict[str, Any] = {}

        for col, value in template_row.items():
            if col == cfg.business_key_column:
                new_row[col] = new_bk_id
            elif col == cfg.effective_date_column:
                new_row[col] = day_date
            elif col == cfg.end_date_column:
                new_row[col] = None
            elif col == cfg.is_current_column:
                new_row[col] = True
            elif col == "_delta_type":
                continue
            elif isinstance(value, bool) or isinstance(value, (np.bool_,)):
                new_row[col] = bool(self._rng.choice([True, False]))
            elif isinstance(value, str):
                new_row[col] = f"{value}_new{new_bk_id}"
            elif isinstance(value, (int, np.integer)):
                new_row[col] = int(self._rng.integers(1, max(int(value) * 2, 10)))
            elif isinstance(value, (float, np.floating)):
                new_row[col] = float(self._rng.uniform(0.5, 1.5) * value) if value else float(self._rng.uniform(0.0, 100.0))
            else:
                new_row[col] = value

        return new_row

    # ------------------------------------------------------------------
    # File writing
    # ------------------------------------------------------------------

    def _write_delta(
        self,
        delta_df: pd.DataFrame,
        day_date: datetime,
        entity: str,
    ) -> tuple[list[Path], list[Path]]:
        """Write a delta DataFrame to a date-partitioned path."""
        cfg = self._config
        dt_str = day_date.strftime("%Y-%m-%d")
        base = Path(cfg.base_path) / cfg.domain / entity / "delta" / f"dt={dt_str}"
        base.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        manifest_paths: list[Path] = []

        for fmt in cfg.formats:
            ext = fmt
            filename = f"{entity}_delta.{ext}"
            path = base / filename
            self._write_df(delta_df, path, fmt)
            written.append(path)

        if cfg.manifest_enabled:
            mp = self._write_manifest(base, written, day_date, entity)
            manifest_paths.append(mp)

        return written, manifest_paths

    @staticmethod
    def _write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
        """Write a DataFrame to *path* in the specified format."""
        path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "parquet":
            df.to_parquet(path, index=False)
        elif fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "jsonl":
            df.to_json(path, orient="records", lines=True, date_format="iso")
        else:
            raise ValueError(f"Unsupported file format: {fmt!r}")

    def _write_manifest(
        self,
        directory: Path,
        data_files: list[Path],
        date: datetime,
        entity: str,
    ) -> Path:
        """Write a JSON manifest summarising the file drop."""
        manifest: dict[str, Any] = {
            "entity": entity,
            "domain": self._config.domain,
            "date": date.isoformat(),
            "files": [str(f.name) for f in data_files],
            "file_count": len(data_files),
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "correlation_id": str(uuid.uuid4()),
        }
        path = directory / "_manifest.json"
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return path
