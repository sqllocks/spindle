"""Reference data strategy — pick from built-in datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from sqllocks_spindle.engine.strategies.base import GenerationContext, Strategy
from sqllocks_spindle.schema.parser import ColumnDef

# Cache loaded datasets
_dataset_cache: dict[str, list] = {}


def _load_dataset(dataset_name: str, domain_path: Path | list[Path] | None = None) -> list:
    """Load a reference dataset from JSON file."""
    if dataset_name in _dataset_cache:
        return _dataset_cache[dataset_name]

    # Search paths: domain-specific → shared → global data dir
    search_paths = []
    if domain_path:
        # Support multiple domain paths (composite domains)
        paths = domain_path if isinstance(domain_path, list) else [domain_path]
        for dp in paths:
            search_paths.append(Path(dp) / "reference_data" / f"{dataset_name}.json")

    # Shared reference data (cross-domain datasets like us_zip_locations)
    shared_dir = Path(__file__).parent.parent.parent / "domains" / "_shared" / "reference_data"
    search_paths.append(shared_dir / f"{dataset_name}.json")

    # Global data directory
    data_dir = Path(__file__).parent.parent.parent / "data"
    search_paths.append(data_dir / f"{dataset_name}.json")

    # Last-resort fallback: walk every installed domain's reference_data/.
    # Needed when domain_path was set to a local path that doesn't exist on the
    # cluster (e.g. /Users/x/... from a Mac, in a Linux Spark notebook). The
    # installed package contains all domain reference data, so we can find it
    # without knowing which domain we're in.
    domains_root = Path(__file__).parent.parent.parent / "domains"
    if domains_root.exists():
        for entry in domains_root.iterdir():
            if not entry.is_dir() or entry.name.startswith("_") or entry.name.startswith("."):
                continue
            candidate = entry / "reference_data" / f"{dataset_name}.json"
            if candidate not in search_paths:
                search_paths.append(candidate)

    for path in search_paths:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            _dataset_cache[dataset_name] = data
            return data

    raise FileNotFoundError(
        f"Reference dataset '{dataset_name}' not found. "
        f"Searched: {[str(p) for p in search_paths]}"
    )


class ReferenceDataStrategy(Strategy):
    """Pick values from a built-in reference dataset."""

    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        dataset_name = config.get("dataset", "")
        if not dataset_name:
            raise ValueError(
                f"reference_data strategy requires 'dataset' for column '{column.name}'"
            )

        # Try to resolve domain path from context
        domain_path = ctx.model_config.get("_domain_path")

        data = _load_dataset(dataset_name, domain_path)

        # If data is a list of strings, pick randomly
        if isinstance(data, list) and all(isinstance(x, str) for x in data):
            data_arr = np.array(data, dtype=object)
            indices = ctx.rng.integers(0, len(data), size=ctx.row_count)
            return data_arr[indices]

        # If data is a list of dicts with "name" and "weight"
        if isinstance(data, list) and all(isinstance(x, dict) for x in data):
            names = np.array([d.get("name", d.get("value", "")) for d in data], dtype=object)
            weights = np.array([d.get("weight", 1.0) for d in data])
            weights = weights / weights.sum()
            indices = ctx.rng.choice(len(names), size=ctx.row_count, p=weights)
            return names[indices]

        raise ValueError(
            f"Unsupported reference data format for dataset '{dataset_name}'"
        )
