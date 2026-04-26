"""DemoManifest — record every artifact created during a demo session."""
from __future__ import annotations
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass
class ArtifactRecord:
    target: str
    name: str
    row_count: int = 0
    detail: str = ""


@dataclass
class DemoManifest:
    session_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    scenario: str = ""
    mode: str = ""
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    finished_at: Optional[str] = None
    success: bool = False
    error: Optional[str] = None
    artifacts: list = field(default_factory=list)
    params: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)
    scale_mode: Optional[str] = None
    fabric_run_id: Optional[str] = None
    workspace_id: Optional[str] = None
    notebook_item_id: Optional[str] = None
    _path: Optional[Any] = field(default=None, repr=False, compare=False)

    def add_artifact(self, target: str, name: str, row_count: int = 0, detail: str = "") -> None:
        self.artifacts.append(ArtifactRecord(target=target, name=name, row_count=row_count, detail=detail))

    def finish(self, success: bool, error: Optional[str] = None) -> None:
        self.finished_at = datetime.utcnow().isoformat()
        self.success = success
        self.error = error

    def save(self, directory: Optional[Path] = None) -> Path:
        dir_ = directory or (Path.home() / ".spindle" / "sessions")
        dir_.mkdir(parents=True, exist_ok=True)
        path = dir_ / f"demo-{self.session_id}.json"
        d = asdict(self)
        d.pop("_path", None)
        path.write_text(json.dumps(d, indent=2))
        self._path = path
        return path

    @classmethod
    def load(cls, session_id: str, directory: Optional[Path] = None) -> "DemoManifest":
        dir_ = directory or (Path.home() / ".spindle" / "sessions")
        path = dir_ / f"demo-{session_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"No session '{session_id}' found in {dir_}")
        data = json.loads(path.read_text())
        artifacts_raw = data.pop("artifacts", [])
        artifacts = [ArtifactRecord(**a) for a in artifacts_raw]
        data.pop("_path", None)
        m = cls(**{k: v for k, v in data.items()})
        m.artifacts = artifacts
        m._path = path
        return m

    def export(self, format: str = "md") -> str:
        total_rows = sum(a.row_count for a in self.artifacts)
        status = "SUCCESS" if self.success else f"FAILED: {self.error or 'unknown'}"
        targets = sorted({a.target for a in self.artifacts})

        if format == "md":
            lines = [
                f"# Spindle Demo Report — Session {self.session_id}",
                "",
                f"| Field | Value |",
                f"|---|---|",
                f"| Scenario | {self.scenario} |",
                f"| Mode | {self.mode} |",
                f"| Status | {status} |",
                f"| Started | {self.started_at} |",
                f"| Finished | {self.finished_at or 'running'} |",
                f"| Total rows | {total_rows:,} |",
                f"| Targets | {', '.join(targets)} |",
                "",
                f"## Artifacts Created",
                "",
                f"| Target | Name | Rows |",
                f"|---|---|---|",
            ]
            for a in self.artifacts:
                lines.append(f"| {a.target} | {a.name} | {a.row_count:,} |")
            if self.metrics:
                lines += ["", "## Metrics", ""]
                for k, v in self.metrics.items():
                    lines.append(f"- **{k}**: {v}")
            return "\n".join(lines)

        elif format == "html":
            rows_html = "\n".join(
                f"<tr><td>{a.target}</td><td>{a.name}</td><td>{a.row_count:,}</td></tr>"
                for a in self.artifacts
            )
            return f"""<!DOCTYPE html>
<html><head><title>Spindle Demo — {self.session_id}</title>
<style>body{{font-family:sans-serif;padding:24px}} table{{border-collapse:collapse;width:100%}} td,th{{border:1px solid #ddd;padding:8px}}</style>
</head><body>
<h1>Spindle Demo Report — {self.session_id}</h1>
<p>Scenario: <strong>{self.scenario}</strong> | Mode: <strong>{self.mode}</strong> | Status: <strong>{status}</strong></p>
<p>Total rows: <strong>{total_rows:,}</strong></p>
<h2>Artifacts</h2>
<table><tr><th>Target</th><th>Name</th><th>Rows</th></tr>{rows_html}</table>
</body></html>"""
        else:
            raise ValueError(f"Unknown format: {format!r}. Use 'md' or 'html'.")
