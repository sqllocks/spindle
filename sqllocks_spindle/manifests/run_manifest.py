"""Run Manifest — capture metadata from a generation run."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqllocks_spindle import __version__


# ---------------------------------------------------------------------------
# RunManifest dataclass
# ---------------------------------------------------------------------------

@dataclass
class RunManifest:
    """Complete metadata record for a single generation run.

    The run_id follows the format: ``YYYYMMDD_HHMMSS_{domain}_{scale}_s{seed}``.
    """
    run_id: str
    spec_hash: str  # sha256 of the spec file (empty if no spec file)
    pack_id: str
    domain: str
    scale: str
    seed: int
    engine_version: str
    outputs: dict[str, Any] = field(default_factory=dict)
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    validation: dict[str, bool] = field(default_factory=dict)
    chaos: dict[str, Any] = field(default_factory=dict)
    timestamps: dict[str, str] = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            f"Run Manifest: {self.run_id}",
            f"  Engine:  Spindle v{self.engine_version}",
            f"  Pack:    {self.pack_id}",
            f"  Domain:  {self.domain}",
            f"  Scale:   {self.scale}",
            f"  Seed:    {self.seed}",
        ]
        if self.tables:
            total_rows = sum(t.get("rows", 0) for t in self.tables.values())
            lines.append(f"  Tables:  {len(self.tables)} ({total_rows:,} total rows)")
        if self.validation:
            passed = sum(1 for v in self.validation.values() if v)
            lines.append(f"  Gates:   {passed}/{len(self.validation)} passed")
        if self.timestamps:
            elapsed = self.timestamps.get("elapsed_seconds", "?")
            lines.append(f"  Elapsed: {elapsed}s")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# ManifestBuilder
# ---------------------------------------------------------------------------

class ManifestBuilder:
    """Incrementally build a RunManifest during a generation run.

    Usage::

        builder = ManifestBuilder()
        builder.start(spec, pack, "retail", "small", 42)
        builder.record_output("customer", rows=1000, columns=8, paths=["..."])
        builder.record_validation("referential_integrity", True)
        manifest = builder.finish()
        ManifestBuilder.to_file(manifest, "run_manifest.json")
    """

    def __init__(self) -> None:
        self._run_id: str = ""
        self._spec_hash: str = ""
        self._pack_id: str = ""
        self._domain: str = ""
        self._scale: str = ""
        self._seed: int = 0
        self._outputs: dict[str, Any] = {}
        self._tables: dict[str, dict[str, Any]] = {}
        self._validation: dict[str, bool] = {}
        self._chaos: dict[str, Any] = {}
        self._start_time: float = 0.0
        self._started_iso: str = ""

    def start(
        self,
        spec: Any,
        pack: Any,
        domain_name: str,
        scale: str,
        seed: int,
    ) -> None:
        """Begin tracking a new generation run.

        Args:
            spec: A GenerationSpec instance (or None if running pack directly).
            pack: A ScenarioPack instance (or None).
            domain_name: Name of the domain being generated.
            scale: Scale preset being used.
            seed: Random seed.
        """
        now = datetime.now(timezone.utc)
        self._started_iso = now.isoformat()
        self._start_time = time.time()

        ts = now.strftime("%Y%m%d_%H%M%S")
        self._run_id = f"{ts}_{domain_name}_{scale}_s{seed}"
        self._domain = domain_name
        self._scale = scale
        self._seed = seed

        # Compute spec hash if a spec object with a file path is available
        self._spec_hash = ""
        if spec is not None:
            base_dir = getattr(spec, "_base_dir", None)
            if base_dir:
                # Hash the raw spec content if we can find the file
                # (best effort — not critical)
                self._spec_hash = self._hash_path(base_dir)

        self._pack_id = ""
        if pack is not None:
            self._pack_id = getattr(pack, "id", "")

        # Reset accumulators
        self._outputs = {}
        self._tables = {}
        self._validation = {}
        self._chaos = {}

    def record_output(
        self,
        table_name: str,
        rows: int,
        columns: int,
        paths: list[str] | None = None,
    ) -> None:
        """Record metadata for a generated table."""
        self._tables[table_name] = {
            "rows": rows,
            "columns": columns,
            "file_paths": paths or [],
        }

    def record_validation(self, gate: str, result: bool) -> None:
        """Record the result of a validation gate."""
        self._validation[gate] = result

    def record_chaos(self, category: str, count: int) -> None:
        """Record chaos injection statistics."""
        self._chaos[category] = self._chaos.get(category, 0) + count

    def finish(self) -> RunManifest:
        """Finalize the manifest with timing information and return it."""
        elapsed = time.time() - self._start_time if self._start_time else 0.0
        finished_iso = datetime.now(timezone.utc).isoformat()

        return RunManifest(
            run_id=self._run_id,
            spec_hash=self._spec_hash,
            pack_id=self._pack_id,
            domain=self._domain,
            scale=self._scale,
            seed=self._seed,
            engine_version=__version__,
            outputs=self._outputs,
            tables=self._tables,
            validation=self._validation,
            chaos=self._chaos,
            timestamps={
                "started": self._started_iso,
                "finished": finished_iso,
                "elapsed_seconds": round(elapsed, 2),
            },
        )

    # ------------------------------------------------------------------
    # Serialization helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def to_json(manifest: RunManifest) -> str:
        """Serialize a RunManifest to a JSON string."""
        return json.dumps(_manifest_to_dict(manifest), indent=2, default=str)

    @staticmethod
    def to_file(manifest: RunManifest, path: str | Path) -> None:
        """Write a RunManifest to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(_manifest_to_dict(manifest), f, indent=2, default=str)

    @staticmethod
    def from_file(path: str | Path) -> RunManifest:
        """Load a RunManifest from a JSON file."""
        path = Path(path)
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return RunManifest(
            run_id=raw.get("run_id", ""),
            spec_hash=raw.get("spec_hash", ""),
            pack_id=raw.get("pack_id", ""),
            domain=raw.get("domain", ""),
            scale=raw.get("scale", ""),
            seed=raw.get("seed", 0),
            engine_version=raw.get("engine_version", ""),
            outputs=raw.get("outputs", {}),
            tables=raw.get("tables", {}),
            validation=raw.get("validation", {}),
            chaos=raw.get("chaos", {}),
            timestamps=raw.get("timestamps", {}),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_path(path: Path) -> str:
        """SHA-256 hash of a file's contents (best-effort)."""
        try:
            if path.is_file():
                return hashlib.sha256(path.read_bytes()).hexdigest()
        except Exception:
            pass
        return ""


def _manifest_to_dict(m: RunManifest) -> dict[str, Any]:
    """Convert a RunManifest to a plain dict for JSON serialization."""
    return {
        "run_id": m.run_id,
        "spec_hash": m.spec_hash,
        "pack_id": m.pack_id,
        "domain": m.domain,
        "scale": m.scale,
        "seed": m.seed,
        "engine_version": m.engine_version,
        "outputs": m.outputs,
        "tables": m.tables,
        "validation": m.validation,
        "chaos": m.chaos,
        "timestamps": m.timestamps,
    }
