"""File-drop simulator — simulate upstream sources landing files over a date range."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class FileDropConfig:
    """Configuration for file-drop simulation.

    Args:
        domain: Domain name used in path construction (e.g. ``"retail"``).
        base_path: Root directory for landing files. Maps to
            ``Files/landing`` in a Fabric lakehouse.
        cadence: Drop cadence — ``"daily"``, ``"hourly"``, or ``"every_15m"``.
        date_range_start: Inclusive start date as ``YYYY-MM-DD``.
        date_range_end: Inclusive end date as ``YYYY-MM-DD``.
        partitioning: Partition folder template.
        formats: File formats to write (``"parquet"``, ``"csv"``, ``"jsonl"``).
        file_naming: File naming template.  Placeholders:
            ``{domain}``, ``{entity}``, ``{dt}``, ``{seq}``, ``{ext}``.
        entities: Restrict simulation to these table names.  Empty = all tables.
        manifest_enabled: Write a ``_manifest.json`` per partition folder.
        done_flag_enabled: Write a ``_done`` sentinel per partition folder.
        lateness_enabled: Inject late-arriving rows (data from previous days).
        lateness_probability: Per-row probability of being marked late.
        max_days_late: Maximum staleness for late rows.
        duplicates_enabled: Inject duplicate rows.
        duplicate_probability: Per-row probability of duplication.
        backfill_enabled: Re-drop historical partitions.
        max_days_back: How far back a backfill can reach.
        seed: Random seed for reproducibility.
    """

    domain: str = "default"
    base_path: str = "Files/landing"
    cadence: str = "daily"
    date_range_start: str = ""
    date_range_end: str = ""
    partitioning: str = "dt=YYYY-MM-DD"
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    file_naming: str = "{domain}_{entity}_{dt}_{seq}.{ext}"
    entities: list[str] = field(default_factory=list)
    manifest_enabled: bool = True
    done_flag_enabled: bool = True
    lateness_enabled: bool = True
    lateness_probability: float = 0.10
    max_days_late: int = 3
    duplicates_enabled: bool = False
    duplicate_probability: float = 0.02
    backfill_enabled: bool = False
    max_days_back: int = 0
    seed: int = 42


@dataclass
class FileDropResult:
    """Result of a file-drop simulation run.

    Attributes:
        files_written: All data file paths written.
        manifest_paths: Paths to ``_manifest.json`` files.
        done_flag_paths: Paths to ``_done`` sentinel files.
        stats: Per-entity statistics dict.
    """

    files_written: list[Path]
    manifest_paths: list[Path]
    done_flag_paths: list[Path]
    stats: dict[str, Any]

    def __repr__(self) -> str:
        total = len(self.files_written)
        manifests = len(self.manifest_paths)
        return (
            f"FileDropResult(files={total}, manifests={manifests}, "
            f"entities={list(self.stats.keys())})"
        )


class FileDropSimulator:
    """Simulate an upstream source dropping files on a cadence over a date range.

    For each simulated time slot the simulator:
      1. Slices rows belonging to that slot (temporal column or round-robin).
      2. Writes partitioned data files to disk.
      3. Optionally writes a manifest and done-flag.
      4. Optionally injects late arrivals, duplicates, and backfills.

    Args:
        tables: Mapping of ``table_name -> DataFrame`` (from
            :class:`~sqllocks_spindle.engine.generator.GenerationResult`).
        config: :class:`FileDropConfig` controlling paths, cadence, and
            data-quality anomalies.

    Example::

        from sqllocks_spindle.simulation import FileDropSimulator, FileDropConfig

        cfg = FileDropConfig(
            domain="retail",
            date_range_start="2024-01-01",
            date_range_end="2024-01-31",
        )
        result = FileDropSimulator(tables=gen_result.tables, config=cfg).run()
    """

    # Mapping from format name to pandas write method args
    _FORMAT_WRITERS: dict[str, str] = {
        "parquet": "parquet",
        "csv": "csv",
        "jsonl": "jsonl",
    }

    def __init__(
        self,
        tables: dict[str, pd.DataFrame],
        config: FileDropConfig,
    ) -> None:
        self._tables = tables
        self._config = config
        self._rng = np.random.default_rng(config.seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> FileDropResult:
        """Execute the file-drop simulation and return results."""
        cfg = self._config
        slots = self._build_time_slots()
        entity_names = cfg.entities if cfg.entities else list(self._tables.keys())

        files_written: list[Path] = []
        manifest_paths: list[Path] = []
        done_flag_paths: list[Path] = []
        stats: dict[str, Any] = {}

        for entity in entity_names:
            if entity not in self._tables:
                continue

            df = self._tables[entity].copy()
            ts_col = self._detect_temporal_column(df)
            sliced = self._slice_by_slots(df, ts_col, slots)

            entity_files: list[Path] = []
            entity_rows = 0

            for slot_dt, slot_df in sliced.items():
                if slot_df.empty:
                    continue

                partition_dir = self._partition_path(entity, slot_dt)
                partition_dir.mkdir(parents=True, exist_ok=True)

                # Late arrivals: move some rows to a later slot
                if cfg.lateness_enabled and len(slots) > 1:
                    slot_df, late_rows = self._extract_late_rows(slot_df, slot_dt)
                    for late_dt, late_df in late_rows.items():
                        late_dir = self._partition_path(entity, late_dt)
                        late_dir.mkdir(parents=True, exist_ok=True)
                        late_files = self._write_data(
                            late_df, entity, late_dt, late_dir, seq_start=900,
                        )
                        entity_files.extend(late_files)

                # Duplicates: clone a fraction of rows
                if cfg.duplicates_enabled and not slot_df.empty:
                    slot_df = self._inject_duplicates(slot_df)

                written = self._write_data(slot_df, entity, slot_dt, partition_dir)
                entity_files.extend(written)
                entity_rows += len(slot_df)

                # Manifest
                if cfg.manifest_enabled:
                    mp = self._write_manifest(partition_dir, written, slot_dt, entity)
                    manifest_paths.append(mp)

                # Done flag
                if cfg.done_flag_enabled:
                    dp = partition_dir / "_done"
                    dp.write_text(
                        datetime.utcnow().isoformat(), encoding="utf-8",
                    )
                    done_flag_paths.append(dp)

            # Backfills
            if cfg.backfill_enabled and cfg.max_days_back > 0 and slots:
                bf_files = self._generate_backfills(entity, df, ts_col, slots)
                entity_files.extend(bf_files)

            files_written.extend(entity_files)
            stats[entity] = {
                "files": len(entity_files),
                "rows_written": entity_rows,
                "formats": cfg.formats,
            }

        return FileDropResult(
            files_written=files_written,
            manifest_paths=manifest_paths,
            done_flag_paths=done_flag_paths,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Time-slot helpers
    # ------------------------------------------------------------------

    def _build_time_slots(self) -> list[datetime]:
        """Build a sorted list of time-slot start datetimes from the config."""
        cfg = self._config
        start = datetime.strptime(cfg.date_range_start, "%Y-%m-%d")
        end = datetime.strptime(cfg.date_range_end, "%Y-%m-%d")

        cadence_map = {
            "daily": timedelta(days=1),
            "hourly": timedelta(hours=1),
            "every_15m": timedelta(minutes=15),
        }
        delta = cadence_map.get(cfg.cadence, timedelta(days=1))

        slots: list[datetime] = []
        current = start
        while current <= end:
            slots.append(current)
            current += delta
        return slots

    def _detect_temporal_column(self, df: pd.DataFrame) -> str | None:
        """Return the first datetime-typed column name, or ``None``."""
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
        # Heuristic: look for columns named like dates
        for col in df.columns:
            lower = col.lower()
            if any(kw in lower for kw in ("date", "timestamp", "created", "updated")):
                try:
                    pd.to_datetime(df[col].dropna().head(5))
                    return col
                except (ValueError, TypeError):
                    continue
        return None

    def _slice_by_slots(
        self,
        df: pd.DataFrame,
        ts_col: str | None,
        slots: list[datetime],
    ) -> dict[datetime, pd.DataFrame]:
        """Distribute rows across time slots.

        If a temporal column exists, rows are assigned to the slot whose start
        they fall into.  Otherwise rows are distributed round-robin.
        """
        if not slots:
            return {}

        result: dict[datetime, pd.DataFrame] = {s: pd.DataFrame() for s in slots}
        cfg = self._config
        cadence_map = {
            "daily": timedelta(days=1),
            "hourly": timedelta(hours=1),
            "every_15m": timedelta(minutes=15),
        }
        delta = cadence_map.get(cfg.cadence, timedelta(days=1))

        if ts_col is not None and ts_col in df.columns:
            ts_series = pd.to_datetime(df[ts_col], errors="coerce")
            for slot_start in slots:
                slot_end = slot_start + delta
                mask = (ts_series >= slot_start) & (ts_series < slot_end)
                result[slot_start] = df.loc[mask].copy()
        else:
            # Round-robin distribution
            indices = np.arange(len(df))
            assignments = indices % len(slots)
            for i, slot in enumerate(slots):
                mask = assignments == i
                result[slot] = df.iloc[mask].copy()

        return result

    # ------------------------------------------------------------------
    # Data writing
    # ------------------------------------------------------------------

    def _partition_path(self, entity: str, slot_dt: datetime) -> Path:
        """Build the partition directory path."""
        cfg = self._config
        base = Path(cfg.base_path)
        partition_folder = cfg.partitioning.replace("YYYY-MM-DD", slot_dt.strftime("%Y-%m-%d"))
        if cfg.cadence in ("hourly", "every_15m"):
            partition_folder += f"/hr={slot_dt.strftime('%H')}"
            if cfg.cadence == "every_15m":
                partition_folder += f"/m={slot_dt.strftime('%M')}"
        return base / cfg.domain / entity / partition_folder

    def _write_data(
        self,
        df: pd.DataFrame,
        entity: str,
        slot_dt: datetime,
        partition_dir: Path,
        seq_start: int = 1,
    ) -> list[Path]:
        """Write a DataFrame slice to disk in configured format(s)."""
        cfg = self._config
        written: list[Path] = []

        for fmt in cfg.formats:
            ext = fmt if fmt != "jsonl" else "jsonl"
            dt_str = slot_dt.strftime("%Y-%m-%d")
            if cfg.cadence in ("hourly", "every_15m"):
                dt_str = slot_dt.strftime("%Y-%m-%dT%H%M")

            filename = cfg.file_naming.format(
                domain=cfg.domain,
                entity=entity,
                dt=dt_str,
                seq=f"{seq_start:05d}",
                ext=ext,
            )
            path = partition_dir / filename
            self._write_df(df, path, fmt)
            written.append(path)

        return written

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
        partition_dir: Path,
        data_files: list[Path],
        slot_dt: datetime,
        entity: str,
    ) -> Path:
        """Write a JSON manifest summarising the partition drop."""
        manifest = {
            "entity": entity,
            "domain": self._config.domain,
            "slot": slot_dt.isoformat(),
            "cadence": self._config.cadence,
            "files": [str(f.name) for f in data_files],
            "file_count": len(data_files),
            "created_utc": datetime.utcnow().isoformat(),
            "correlation_id": str(uuid.uuid4()),
        }
        path = partition_dir / "_manifest.json"
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return path

    # ------------------------------------------------------------------
    # Anomaly injection
    # ------------------------------------------------------------------

    def _extract_late_rows(
        self,
        df: pd.DataFrame,
        slot_dt: datetime,
    ) -> tuple[pd.DataFrame, dict[datetime, pd.DataFrame]]:
        """Pull out rows that will arrive late (in a future slot).

        Returns the trimmed DataFrame and a mapping of target_slot -> late rows.
        """
        cfg = self._config
        if df.empty:
            return df, {}

        mask = self._rng.random(len(df)) < cfg.lateness_probability
        late_df = df[mask].copy()
        remaining = df[~mask].copy()

        if late_df.empty:
            return remaining, {}

        late_buckets: dict[datetime, pd.DataFrame] = {}
        delays = self._rng.integers(1, cfg.max_days_late + 1, size=len(late_df))
        for delay_val in np.unique(delays):
            target_dt = slot_dt + timedelta(days=int(delay_val))
            bucket_mask = delays == delay_val
            late_buckets[target_dt] = late_df.iloc[bucket_mask].copy()

        return remaining, late_buckets

    def _inject_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Duplicate a fraction of rows in the DataFrame."""
        cfg = self._config
        mask = self._rng.random(len(df)) < cfg.duplicate_probability
        dupes = df[mask].copy()
        if dupes.empty:
            return df
        return pd.concat([df, dupes], ignore_index=True)

    def _generate_backfills(
        self,
        entity: str,
        df: pd.DataFrame,
        ts_col: str | None,
        slots: list[datetime],
    ) -> list[Path]:
        """Re-drop a historical partition as a backfill."""
        cfg = self._config
        files: list[Path] = []

        if len(slots) <= cfg.max_days_back:
            return files

        # Pick a random historical slot to backfill
        backfill_idx = int(self._rng.integers(0, min(cfg.max_days_back, len(slots))))
        slot_dt = slots[backfill_idx]

        # Re-slice for that slot
        cadence_map = {
            "daily": timedelta(days=1),
            "hourly": timedelta(hours=1),
            "every_15m": timedelta(minutes=15),
        }
        delta = cadence_map.get(cfg.cadence, timedelta(days=1))

        if ts_col and ts_col in df.columns:
            ts_series = pd.to_datetime(df[ts_col], errors="coerce")
            mask = (ts_series >= slot_dt) & (ts_series < slot_dt + delta)
            backfill_df = df.loc[mask].copy()
        else:
            chunk_size = max(1, len(df) // len(slots))
            start_idx = backfill_idx * chunk_size
            backfill_df = df.iloc[start_idx : start_idx + chunk_size].copy()

        if backfill_df.empty:
            return files

        partition_dir = self._partition_path(entity, slot_dt)
        partition_dir.mkdir(parents=True, exist_ok=True)
        files.extend(
            self._write_data(backfill_df, entity, slot_dt, partition_dir, seq_start=990)
        )
        return files
