"""DemoOrchestrator — multi-step demo execution with rollback support."""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.catalog import ScenarioCatalog, get_catalog
from sqllocks_spindle.demo.cleanup import CleanupEngine
from sqllocks_spindle.demo.connections import ConnectionRegistry, ConnectionProfile
from sqllocks_spindle.demo.output.dashboard import ProgressDashboard

logger = logging.getLogger(__name__)


@dataclass
class DemoResult:
    success: bool
    session_id: str
    scenario: str
    mode: str
    fidelity_score: Optional[float] = None
    error: Optional[str] = None
    manifest: Optional[DemoManifest] = None


class DemoOrchestrator:
    """Coordinate a full demo run with rollback support."""

    def __init__(self, catalog: Optional[ScenarioCatalog] = None,
                 registry: Optional[ConnectionRegistry] = None):
        self._catalog = catalog or get_catalog()
        self._registry = registry or ConnectionRegistry()

    def run(self, params: DemoParams) -> DemoResult:
        scenario_meta = self._catalog.get(params.scenario)
        if params.mode not in scenario_meta.supported_modes:
            raise ValueError(
                f"Scenario '{params.scenario}' does not support mode '{params.mode}'. "
                f"Supported: {scenario_meta.supported_modes}"
            )

        conn_profile: Optional[ConnectionProfile] = None
        if params.connection:
            try:
                conn_profile = self._registry.load(params.connection)
            except KeyError as e:
                raise ValueError(str(e)) from e

        manifest = DemoManifest(scenario=params.scenario, mode=params.mode)
        manifest.params = {k: str(v) for k, v in vars(params).items() if v is not None}

        dashboard = ProgressDashboard(params.scenario, params.mode, params.rows)

        try:
            result_data = self._execute_mode(params, manifest, dashboard, conn_profile)
            manifest.finish(result_data.get("success", True), result_data.get("error"))
            manifest.save()
            return DemoResult(
                success=result_data.get("success", True),
                session_id=manifest.session_id,
                scenario=params.scenario,
                mode=params.mode,
                fidelity_score=result_data.get("fidelity_score"),
                error=result_data.get("error"),
                manifest=manifest,
            )
        except Exception as e:
            logger.exception("Demo failed — initiating rollback")
            manifest.finish(False, str(e))
            manifest.save()
            try:
                cleaner = CleanupEngine(conn_profile)
                removed = cleaner.cleanup(manifest, dry_run=False)
                logger.info("Rollback removed: %s", removed)
            except Exception as cleanup_err:
                logger.warning("Rollback failed: %s", cleanup_err)
            return DemoResult(
                success=False,
                session_id=manifest.session_id,
                scenario=params.scenario,
                mode=params.mode,
                error=str(e),
                manifest=manifest,
            )

    def _execute_mode(self, params: DemoParams, manifest: DemoManifest,
                      dashboard: ProgressDashboard, conn_profile) -> dict:
        if params.mode == "inference":
            from sqllocks_spindle.demo.modes.inference import InferenceDemoMode
            handler = InferenceDemoMode(params, manifest, dashboard, conn_profile)
        elif params.mode == "streaming":
            from sqllocks_spindle.demo.modes.streaming import StreamingDemoMode
            handler = StreamingDemoMode(params, manifest, dashboard, conn_profile)
        elif params.mode == "seeding":
            from sqllocks_spindle.demo.modes.seeding import SeedingDemoMode
            handler = SeedingDemoMode(params, manifest, dashboard, conn_profile)
        else:
            raise ValueError(f"Unknown mode: {params.mode}")
        return handler.run()
