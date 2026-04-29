"""Profile registry — CRUD, search, tagging, bulk import, diff, and reindex."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterator

from sqllocks_spindle.profiles.profile import RegistryProfile

_DEFAULT_ROOT = Path.home() / ".spindle" / "profiles"

# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------


def _index_path(root: Path) -> Path:
    return root / "_index.json"


def _read_index(root: Path) -> dict[str, dict[str, Any]]:
    p = _index_path(root)
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def _write_index(root: Path, index: dict[str, dict[str, Any]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _index_path(root).write_text(json.dumps(index, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class ProfileRegistry:
    """Manages named, tagged profiles under a configurable root directory.

    Directory layout::

        <root>/
          <system>/
            <table>/
              <profile_name>.json
          _index.json          ← auto-maintained index

    """

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root) if root else _DEFAULT_ROOT
        self.root.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------------

    def _profile_path(self, system: str, table: str, name: str) -> Path:
        return self.root / system / table / f"{name}.json"

    def _all_profile_paths(self) -> Iterator[Path]:
        for p in self.root.rglob("*.json"):
            if p.name == "_index.json":
                continue
            yield p

    # ---------------------------------------------------------------------------
    # CRUD
    # ---------------------------------------------------------------------------

    def save(self, profile: RegistryProfile) -> None:
        """Save a profile to disk and update the index."""
        path = self._profile_path(profile.system, profile.table, profile.name)
        profile.save(path)
        idx = _read_index(self.root)
        idx[profile.identity] = {
            "system": profile.system,
            "table": profile.table,
            "name": profile.name,
            "description": profile.description,
            "tags": profile.tags,
            "source_rows": profile.source_rows,
            "path": str(path.relative_to(self.root)),
        }
        _write_index(self.root, idx)

    def load(self, identity: str) -> RegistryProfile:
        """Load a profile by identity (``system/table/name``)."""
        parts = identity.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid identity '{identity}' — expected system/table/name")
        system, table, name = parts
        path = self._profile_path(system, table, name)
        if not path.exists():
            raise FileNotFoundError(f"Profile not found: {identity}")
        return RegistryProfile.load(path)

    def delete(self, identity: str) -> None:
        """Delete a profile from disk and index."""
        parts = identity.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid identity '{identity}'")
        system, table, name = parts
        path = self._profile_path(system, table, name)
        if path.exists():
            path.unlink()
        idx = _read_index(self.root)
        idx.pop(identity, None)
        _write_index(self.root, idx)

    def exists(self, identity: str) -> bool:
        idx = _read_index(self.root)
        return identity in idx

    # ---------------------------------------------------------------------------
    # Listing and search
    # ---------------------------------------------------------------------------

    def list_all(self) -> list[dict[str, Any]]:
        """Return all index entries sorted by identity."""
        return sorted(_read_index(self.root).values(), key=lambda x: x.get("system", "") + "/" + x.get("table", "") + "/" + x.get("name", ""))

    def list_systems(self) -> list[str]:
        return sorted({e["system"] for e in _read_index(self.root).values()})

    def list_tables(self, system: str | None = None) -> list[str]:
        entries = _read_index(self.root).values()
        if system:
            entries = [e for e in entries if e["system"] == system]
        return sorted({e["table"] for e in entries})

    def search(
        self,
        query: str | None = None,
        system: str | None = None,
        table: str | None = None,
        tags: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Filter index entries by query string, system, table, and/or tags."""
        results = list(_read_index(self.root).values())
        if system:
            results = [e for e in results if e["system"] == system]
        if table:
            results = [e for e in results if e["table"] == table]
        if tags:
            results = [e for e in results if all(t in e.get("tags", []) for t in tags)]
        if query:
            pattern = re.compile(re.escape(query), re.IGNORECASE)
            results = [
                e for e in results
                if pattern.search(e.get("name", ""))
                or pattern.search(e.get("description", ""))
                or pattern.search(e.get("system", ""))
                or pattern.search(e.get("table", ""))
            ]
        return sorted(results, key=lambda x: x.get("system", "") + "/" + x.get("table", "") + "/" + x.get("name", ""))

    # ---------------------------------------------------------------------------
    # Tagging
    # ---------------------------------------------------------------------------

    def add_tags(self, identity: str, tags: list[str]) -> None:
        """Add tags to a profile (in-place, no duplicates)."""
        profile = self.load(identity)
        existing = set(profile.tags)
        for t in tags:
            existing.add(t)
        profile.tags = sorted(existing)
        self.save(profile)

    def remove_tags(self, identity: str, tags: list[str]) -> None:
        """Remove tags from a profile."""
        profile = self.load(identity)
        profile.tags = [t for t in profile.tags if t not in tags]
        self.save(profile)

    # ---------------------------------------------------------------------------
    # Bulk import
    # ---------------------------------------------------------------------------

    def import_from_dir(self, source_dir: Path, overwrite: bool = False) -> list[str]:
        """Import all *.json profile files from a directory tree.

        Returns a list of imported identity strings.
        """
        imported: list[str] = []
        for path in source_dir.rglob("*.json"):
            if path.name == "_index.json":
                continue
            try:
                profile = RegistryProfile.load(path)
                if not overwrite and self.exists(profile.identity):
                    continue
                self.save(profile)
                imported.append(profile.identity)
            except Exception:
                continue
        return imported

    # ---------------------------------------------------------------------------
    # Diff
    # ---------------------------------------------------------------------------

    def diff(self, identity_a: str, identity_b: str) -> dict[str, Any]:
        """Compare two profiles column by column.

        Returns a dict with keys: ``added``, ``removed``, ``changed``.
        """
        a = self.load(identity_a)
        b = self.load(identity_b)
        cols_a = set(a.columns)
        cols_b = set(b.columns)
        added = sorted(cols_b - cols_a)
        removed = sorted(cols_a - cols_b)
        changed: dict[str, dict[str, Any]] = {}
        for col in cols_a & cols_b:
            if a.columns[col] != b.columns[col]:
                changed[col] = {"from": a.columns[col], "to": b.columns[col]}
        return {"added": added, "removed": removed, "changed": changed}

    # ---------------------------------------------------------------------------
    # Reindex
    # ---------------------------------------------------------------------------

    def reindex(self) -> int:
        """Rebuild _index.json from all .json files on disk. Returns count."""
        idx: dict[str, dict[str, Any]] = {}
        for path in self._all_profile_paths():
            try:
                profile = RegistryProfile.load(path)
                idx[profile.identity] = {
                    "system": profile.system,
                    "table": profile.table,
                    "name": profile.name,
                    "description": profile.description,
                    "tags": profile.tags,
                    "source_rows": profile.source_rows,
                    "path": str(path.relative_to(self.root)),
                }
            except Exception:
                continue
        _write_index(self.root, idx)
        return len(idx)

    # ---------------------------------------------------------------------------
    # Integration: build from DatasetProfile
    # ---------------------------------------------------------------------------

    def save_from_dataset_profile(
        self,
        dataset_profile: Any,
        system: str,
        name: str,
        tags: list[str] | None = None,
        description: str = "",
    ) -> list[RegistryProfile]:
        """Convert a DatasetProfile (from inference.profiler) into registry profiles.

        One RegistryProfile is created per table in the DatasetProfile.
        Returns the list of saved profiles.
        """
        saved: list[RegistryProfile] = []
        for table_name, table_profile in dataset_profile.tables.items():
            columns: dict[str, dict[str, Any]] = {}
            for col_name, col_profile in table_profile.columns.items():
                col_data = {
                    "dtype": str(col_profile.dtype) if hasattr(col_profile, "dtype") else "object",
                    "null_rate": getattr(col_profile, "null_rate", 0.0),
                    "cardinality": getattr(col_profile, "cardinality", 0),
                }
                if hasattr(col_profile, "mean") and col_profile.mean is not None:
                    col_data["mean"] = col_profile.mean
                if hasattr(col_profile, "std") and col_profile.std is not None:
                    col_data["std"] = col_profile.std
                if hasattr(col_profile, "min") and col_profile.min is not None:
                    col_data["min"] = col_profile.min
                if hasattr(col_profile, "max") and col_profile.max is not None:
                    col_data["max"] = col_profile.max
                if hasattr(col_profile, "top_values") and col_profile.top_values:
                    col_data["top_values"] = col_profile.top_values
                columns[col_name] = col_data
            profile = RegistryProfile(
                system=system,
                table=table_name,
                name=name,
                columns=columns,
                tags=tags or [],
                description=description,
                source_rows=getattr(table_profile, "row_count", 0),
            )
            self.save(profile)
            saved.append(profile)
        return saved

    # ---------------------------------------------------------------------------
    # Validate: run fidelity check against a GenerationResult
    # ---------------------------------------------------------------------------

    @staticmethod
    def _reconstruct_reference(profile: "RegistryProfile", n_rows: int = 500) -> "Any":
        """Build an approximate reference DataFrame from stored profile statistics.

        Numeric columns: sampled from N(mean, std) clipped to [min, max].
        Categorical columns: sampled from stored top_values frequencies.
        Other columns: null-filled.
        """
        import numpy as np
        import pandas as pd

        rng = np.random.default_rng(0)
        data: dict[str, Any] = {}

        for col_name, col_stats in profile.columns.items():
            dtype = col_stats.get("dtype", "object")
            null_rate = col_stats.get("null_rate", 0.0)
            n_null = max(0, int(round(null_rate * n_rows)))
            n_valid = n_rows - n_null

            if dtype in {"int64", "float64", "int32", "float32", "Int64", "Float64",
                         "int8", "int16", "uint8", "uint16", "uint32", "uint64"}:
                mean = col_stats.get("mean")
                std = col_stats.get("std")
                if mean is not None and std is not None and float(std) > 0:
                    vals = rng.normal(float(mean), float(std), n_valid)
                    mn = col_stats.get("min")
                    mx = col_stats.get("max")
                    if mn is not None and mx is not None:
                        vals = np.clip(vals, float(mn), float(mx))
                else:
                    vals = np.full(n_valid, float(mean) if mean is not None else 0.0)
                col_series = np.concatenate([vals, np.full(n_null, np.nan)])
            elif col_stats.get("top_values"):
                top = col_stats["top_values"]
                if isinstance(top, dict):
                    cats = list(top.keys())
                    weights = np.array(list(top.values()), dtype=float)
                elif isinstance(top, list):
                    cats = top
                    weights = np.ones(len(cats), dtype=float)
                else:
                    cats = [str(top)]
                    weights = np.array([1.0])
                weights = weights / weights.sum()
                vals = rng.choice(cats, size=n_valid, p=weights)
                col_series = list(vals) + [None] * n_null
            else:
                col_series = [None] * n_rows

            rng.shuffle(col_series if isinstance(col_series, list) else col_series)
            data[col_name] = col_series

        df = pd.DataFrame(data)
        return df

    def validate(
        self,
        identity: str,
        result: Any,
        sample_rows: int = 500,
    ) -> Any:
        """Compare a GenerationResult against a stored profile.

        Reconstructs an approximate reference DataFrame from stored column
        statistics and runs FidelityComparator against the new generation.
        Returns a FidelityReport. Requires scipy.
        """
        from sqllocks_spindle.inference.comparator import FidelityComparator

        profile = self.load(identity)
        table_name = profile.table

        if table_name not in result.tables:
            raise KeyError(f"Table '{table_name}' not in GenerationResult")

        synth_df = result.tables[table_name]
        ref_df = self._reconstruct_reference(profile, n_rows=min(sample_rows, len(synth_df)))

        comparator = FidelityComparator()
        return comparator.compare({table_name: ref_df}, {table_name: synth_df})
