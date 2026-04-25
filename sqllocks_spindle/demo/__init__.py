"""Spindle Demo Engine — one-command demos for conference, client, and workshop use."""
from sqllocks_spindle.demo.params import DemoParams, DemoMode, OutputFormat
from sqllocks_spindle.demo.manifest import DemoManifest, ArtifactRecord
from sqllocks_spindle.demo.connections import ConnectionRegistry, ConnectionProfile
from sqllocks_spindle.demo.catalog import ScenarioCatalog, ScenarioMeta, get_catalog
from sqllocks_spindle.demo.orchestrator import DemoOrchestrator, DemoResult
from sqllocks_spindle.demo.estimator import CostEstimator, CostEstimate
from sqllocks_spindle.demo.cleanup import CleanupEngine
from sqllocks_spindle.demo.notebook_gen import NotebookGenerator


class SpindleDemo:
    """High-level facade for running Spindle demos."""

    def __init__(self, connection: str | None = None):
        self._orch = DemoOrchestrator()
        self._connection = connection

    def run(self, scenario: str = "retail", mode: str = "inference",
            rows: int = 100_000, **kwargs) -> DemoResult:
        params = DemoParams(
            scenario=scenario,
            mode=mode,
            rows=rows,
            connection=self._connection,
            **kwargs,
        )
        return self._orch.run(params)

    def cleanup(self, session_id: str) -> None:
        manifest = DemoManifest.load(session_id)
        CleanupEngine().cleanup(manifest)

    def notebook(self, scenario: str, mode: str = "inference", output_path=None):
        meta = get_catalog().get(scenario)
        gen = NotebookGenerator()
        from pathlib import Path
        return gen.generate(meta, mode, Path(output_path) if output_path else None)

    def list_scenarios(self):
        return get_catalog().list()


__all__ = [
    "SpindleDemo",
    "DemoParams", "DemoMode", "OutputFormat",
    "DemoManifest", "ArtifactRecord",
    "ConnectionRegistry", "ConnectionProfile",
    "ScenarioCatalog", "ScenarioMeta", "get_catalog",
    "DemoOrchestrator", "DemoResult",
    "CostEstimator", "CostEstimate",
    "CleanupEngine",
    "NotebookGenerator",
]
