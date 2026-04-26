"""SeedingDemoMode — instant multi-target Fabric environment setup."""
from __future__ import annotations
import logging
from typing import Optional

from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.output.dashboard import ProgressDashboard, DemoStep
from sqllocks_spindle.demo.estimator import CostEstimator

logger = logging.getLogger(__name__)


def _get_domain_instance(name: str):
    import importlib
    import pkgutil
    import sqllocks_spindle.domains as _pkg
    from sqllocks_spindle.domains.base import Domain

    for _, mod_name, is_pkg in pkgutil.iter_modules(_pkg.__path__, _pkg.__name__ + "."):
        if not is_pkg:
            continue
        try:
            module = importlib.import_module(mod_name)
        except Exception:
            continue
        for attr in getattr(module, "__all__", dir(module)):
            cls = getattr(module, attr, None)
            if isinstance(cls, type) and issubclass(cls, Domain) and cls is not Domain:
                try:
                    inst = cls.__new__(cls)
                    dname = cls.name.fget(inst)
                    if dname == name:
                        return cls()
                except Exception:
                    pass
    raise ValueError(f"Domain '{name}' not found.")


def _rows_to_scale(rows: int) -> str:
    if rows <= 2_000:
        return "small"
    if rows <= 50_000:
        return "medium"
    if rows <= 500_000:
        return "large"
    return "xlarge"


_SPARK_AUTO_THRESHOLD = 500_000


def _resolve_scale_mode(requested: str, conn_profile, rows: int) -> str:
    """Resolve 'auto' to 'local' or 'spark' based on connection and row count.

    'spark' requires a connection profile with a non-empty lakehouse_id.
    'local' always works.
    'auto' picks 'spark' when connection is present, lakehouse_id is set,
    and rows >= _SPARK_AUTO_THRESHOLD; otherwise 'local'.
    """
    if requested == "local":
        return "local"
    if requested == "spark":
        if conn_profile is None:
            raise ValueError("Spark mode requires a connection profile")
        if not getattr(conn_profile, "lakehouse_id", ""):
            raise ValueError("Spark mode requires lakehouse_id in connection profile")
        return "spark"
    # auto
    if (conn_profile is not None
            and getattr(conn_profile, "lakehouse_id", "")
            and rows >= _SPARK_AUTO_THRESHOLD):
        return "spark"
    return "local"


def _import_sink_class(kind: str):
    """Indirection so monkeypatch can intercept individual sink imports."""
    if kind == "lakehouse":
        from sqllocks_spindle.engine.sinks import LakehouseSink
        return LakehouseSink
    if kind == "warehouse":
        from sqllocks_spindle.engine.sinks import WarehouseSink
        return WarehouseSink
    if kind == "sql_db":
        from sqllocks_spindle.engine.sinks import SQLDatabaseSink
        return SQLDatabaseSink
    if kind == "kql":
        from sqllocks_spindle.engine.sinks import KQLSink
        return KQLSink
    raise ValueError(f"Unknown sink kind: {kind!r}")


def _build_sinks(conn, token: str) -> tuple[list, list, dict]:
    """Build sink instances + sinks_list (for FabricSparkRouter) + sink_config dict.

    Returns three values:
      sinks       — list[Sink] for ScaleRouter (local mode)
      sinks_list  — list[{"type", "config"}] passed to FabricSparkRouter
      sink_config — flat dict of common config (workspace_id, lakehouse_id, token)

    A sink whose construction fails or lacks required config is logged and skipped —
    other sinks proceed.
    """
    sinks: list = []
    sinks_list: list = []
    sink_config: dict = {
        "workspace_id": getattr(conn, "workspace_id", "") if conn else "",
        "lakehouse_id": getattr(conn, "lakehouse_id", "") if conn else "",
        "token": token,
    }
    if conn is None:
        return sinks, sinks_list, sink_config

    targets: list[tuple[str, dict, dict]] = []
    # Each target: (kind, init_kwargs_for_local_sink, config_for_spark_sinks_list)
    if conn.lakehouse_id:
        targets.append((
            "lakehouse",
            {},  # LakehouseSink takes optional args; defaults are fine for local
            {"workspace_id": conn.workspace_id, "lakehouse_id": conn.lakehouse_id},
        ))
    if conn.warehouse_conn_str and getattr(conn, "warehouse_staging_path", ""):
        targets.append((
            "warehouse",
            {
                "connection_string": conn.warehouse_conn_str,
                "staging_lakehouse_path": conn.warehouse_staging_path,
                "auth_method": getattr(conn, "auth_method", "cli"),
            },
            {
                "connection_string": conn.warehouse_conn_str,
                "staging_lakehouse_path": conn.warehouse_staging_path,
            },
        ))
    elif conn.warehouse_conn_str:
        logger.warning("Skipping warehouse sink — warehouse_staging_path is required but empty")
    if conn.sql_db_conn_str:
        targets.append((
            "sql_db",
            {
                "connection_string": conn.sql_db_conn_str,
                "auth_method": getattr(conn, "auth_method", "cli"),
            },
            {"connection_string": conn.sql_db_conn_str},
        ))
    if conn.eventhouse_uri and getattr(conn, "eventhouse_database", ""):
        targets.append((
            "kql",
            {
                "cluster_uri": conn.eventhouse_uri,
                "database": conn.eventhouse_database,
                "auth_method": getattr(conn, "auth_method", "cli"),
            },
            {
                "cluster_uri": conn.eventhouse_uri,
                "database": conn.eventhouse_database,
            },
        ))
    elif conn.eventhouse_uri:
        logger.warning("Skipping kql sink — eventhouse_database is required but empty")

    for kind, init_kwargs, spark_cfg in targets:
        try:
            cls = _import_sink_class(kind)
            sinks.append(cls(**init_kwargs))
            sinks_list.append({"type": kind, "config": spark_cfg})
        except Exception as e:
            logger.warning("Skipping %s sink — construction failed: %s", kind, e)

    return sinks, sinks_list, sink_config


def _acquire_token(scope: str = "https://api.fabric.microsoft.com/.default") -> str:
    """Acquire an Entra bearer token for the Fabric API.

    Uses AzureCliCredential (matches existing patterns in fabric/credentials.py).
    The caller must have run `az login`.
    """
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential()
    return cred.get_token(scope).token


class SeedingDemoMode:
    """Generate and write data to all configured Fabric targets."""

    def __init__(self, params: DemoParams, manifest: DemoManifest,
                 dashboard: Optional[ProgressDashboard] = None,
                 connection_profile=None):
        self._params = params
        self._manifest = manifest
        self._dashboard = dashboard or ProgressDashboard(params.scenario, "seeding", params.rows)
        self._conn = connection_profile

    def run(self) -> dict:
        dashboard = self._dashboard
        targets = self._available_targets()

        if self._params.estimate_only or not self._params.dry_run:
            estimator = CostEstimator()
            estimate = estimator.estimate(self._params.rows, targets)
            print(f"\nCost estimate for {self._params.scenario} ({self._params.rows:,} rows):")
            print(str(estimate))

        if self._params.estimate_only:
            return {"success": True, "estimate_only": True}

        if self._params.dry_run:
            print(f"[dry-run] Would write {self._params.rows:,} rows to: {', '.join(targets)}")
            return {"success": True, "dry_run": True}

        dashboard.start()
        try:
            dashboard.step(DemoStep.GENERATING, f"{self._params.rows:,} rows (approx)")
            from sqllocks_spindle.engine.generator import Spindle
            domain_name = self._params.domain or "retail"
            domain = _get_domain_instance(domain_name)
            scale = _rows_to_scale(self._params.rows)
            sp = Spindle()
            result = sp.generate(domain=domain, scale=scale, seed=self._params.seed)
            data = result.tables
            total = sum(len(df) for df in data.values())
            dashboard.info(f"Generated {total:,} total rows across {len(data)} tables")

            dashboard.step(DemoStep.WRITING, f"to {', '.join(targets)}")
            for tname, df in data.items():
                self._manifest.add_artifact("generated", tname, row_count=len(df))

            dashboard.step(DemoStep.DONE)
            dashboard.finish(True)
            self._manifest.finish(True)
            saved_path = self._manifest.save()
            dashboard.info(f"Manifest saved to {saved_path}")
            print(f"\nSession ID: {self._manifest.session_id}")
            print(f"Run `spindle demo report {self._manifest.session_id}` for a full report.")
            print(f"Run `spindle demo cleanup {self._manifest.session_id}` when done.")
            return {"success": True, "session_id": self._manifest.session_id}

        except Exception as e:
            logger.exception("Seeding demo failed")
            dashboard.finish(False, str(e))
            self._manifest.finish(False, str(e))
            self._manifest.save()
            return {"success": False, "error": str(e)}

    def _available_targets(self) -> list:
        if self._conn is None:
            return ["generated"]
        targets = []
        if self._conn.warehouse_conn_str:
            targets.append("warehouse")
        if self._conn.lakehouse_id:
            targets.append("lakehouse")
        if self._conn.sql_db_conn_str:
            targets.append("sql_db")
        if self._conn.eventhouse_uri:
            targets.append("eventhouse")
        return targets or ["generated"]
