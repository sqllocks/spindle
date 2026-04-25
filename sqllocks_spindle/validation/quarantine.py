"""Quarantine manager for failed validation artifacts."""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class QuarantineEntry:
    """Metadata for a single quarantined artifact."""

    original_path: str | None
    reason: str
    gate_name: str
    timestamp: str
    run_id: str
    table_name: str | None = None
    extra: dict[str, Any] | None = None


META_SUFFIX = "._quarantine_meta.json"


class QuarantineManager:
    """Move or copy failed artifacts to a quarantine directory.

    Quarantine directory layout::

        <quarantine_root>/<domain>/<run_id>/
            <filename>
            <filename>._quarantine_meta.json
    """

    def __init__(self, domain: str = "default") -> None:
        self.domain = domain

    def _run_dir(self, quarantine_root: str | Path, run_id: str) -> Path:
        return Path(quarantine_root) / self.domain / run_id

    @staticmethod
    def _write_meta(meta_path: Path, entry: QuarantineEntry) -> None:
        data = asdict(entry)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def quarantine_file(
        self,
        source_path: str | Path,
        quarantine_root: str | Path,
        run_id: str,
        reason: str,
        gate_name: str = "unknown",
    ) -> Path:
        """Copy a file into the quarantine directory with metadata.

        Returns the path to the quarantined copy.
        """
        source = Path(source_path)
        dest_dir = self._run_dir(quarantine_root, run_id)
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_file = dest_dir / source.name
        shutil.copy2(source, dest_file)

        entry = QuarantineEntry(
            original_path=str(source.resolve()),
            reason=reason,
            gate_name=gate_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id=run_id,
        )
        meta_path = dest_dir / f"{source.name}{META_SUFFIX}"
        self._write_meta(meta_path, entry)

        return dest_file

    def quarantine_dataframe(
        self,
        df: pd.DataFrame,
        quarantine_root: str | Path,
        run_id: str,
        table_name: str,
        reason: str,
        gate_name: str = "unknown",
        fmt: str = "parquet",
    ) -> Path:
        """Write a DataFrame to quarantine with metadata.

        Args:
            df: The DataFrame to quarantine.
            quarantine_root: Root quarantine directory.
            run_id: Unique identifier for the generation run.
            table_name: Logical table name.
            reason: Why this artifact was quarantined.
            gate_name: Which validation gate triggered quarantine.
            fmt: Output format — "parquet", "csv", or "jsonl".

        Returns the path to the quarantined file.
        """
        dest_dir = self._run_dir(quarantine_root, run_id)
        dest_dir.mkdir(parents=True, exist_ok=True)

        ext_map = {"parquet": ".parquet", "csv": ".csv", "jsonl": ".jsonl"}
        ext = ext_map.get(fmt, ".parquet")
        filename = f"{table_name}{ext}"
        dest_file = dest_dir / filename

        if fmt == "csv":
            df.to_csv(dest_file, index=False)
        elif fmt == "jsonl":
            df.to_json(dest_file, orient="records", lines=True, date_format="iso")
        else:
            df.to_parquet(dest_file, index=False)

        entry = QuarantineEntry(
            original_path=None,
            reason=reason,
            gate_name=gate_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id=run_id,
            table_name=table_name,
            extra={"rows": len(df), "columns": len(df.columns), "format": fmt},
        )
        meta_path = dest_dir / f"{filename}{META_SUFFIX}"
        self._write_meta(meta_path, entry)

        return dest_file

    def list_quarantined(
        self,
        quarantine_root: str | Path,
    ) -> list[dict[str, Any]]:
        """List all quarantined items across all domains and runs.

        Returns a list of dicts with quarantine metadata.
        """
        root = Path(quarantine_root)
        items: list[dict[str, Any]] = []

        if not root.exists():
            return items

        for meta_path in root.rglob(f"*{META_SUFFIX}"):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                # Derive the artifact path from the meta path
                artifact_name = meta_path.name.replace(META_SUFFIX, "")
                artifact_path = meta_path.parent / artifact_name
                meta["quarantine_path"] = str(artifact_path)
                meta["exists"] = artifact_path.exists()
                items.append(meta)
            except (json.JSONDecodeError, OSError):
                continue

        return items

    def get_quarantine_report(
        self,
        quarantine_root: str | Path,
        run_id: str,
    ) -> dict[str, Any]:
        """Get a detailed report for a specific run's quarantined artifacts.

        Returns a dict with run-level summary and per-artifact details.
        """
        root = Path(quarantine_root)
        all_items = self.list_quarantined(root)
        run_items = [item for item in all_items if item.get("run_id") == run_id]

        gates_triggered: dict[str, int] = {}
        for item in run_items:
            gate = item.get("gate_name", "unknown")
            gates_triggered[gate] = gates_triggered.get(gate, 0) + 1

        return {
            "run_id": run_id,
            "domain": self.domain,
            "total_quarantined": len(run_items),
            "gates_triggered": gates_triggered,
            "artifacts": run_items,
        }
