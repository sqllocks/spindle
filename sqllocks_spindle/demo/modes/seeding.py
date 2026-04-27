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
    """Generate and write data to all configured Fabric targets.

    Routes generation through ScaleRouter (local multi-process) or
    FabricSparkRouter (Fabric notebook) based on scale_mode.
    """

    def __init__(self, params: DemoParams, manifest: DemoManifest,
                 dashboard: Optional[ProgressDashboard] = None,
                 connection_profile=None):
        self._params = params
        self._manifest = manifest
        self._dashboard = dashboard
        self._conn = connection_profile

    def run(self) -> dict:
        dashboard = self._dashboard
        try:
            scale_mode = _resolve_scale_mode(
                self._params.scale_mode, self._conn, self._params.rows,
            )
        except ValueError as e:
            self._manifest.scale_mode = self._params.scale_mode
            return {"success": False, "error": str(e)}

        self._manifest.scale_mode = scale_mode
        targets = self._available_targets()

        # estimate_only / dry_run paths bypass any sink work
        if self._params.estimate_only or self._params.dry_run:
            estimator = CostEstimator()
            estimate = estimator.estimate(self._params.rows, targets)
            print(f"\nCost estimate for {self._params.scenario} ({self._params.rows:,} rows):")
            print(str(estimate))
            if self._params.estimate_only:
                return {"success": True, "estimate_only": True}
            print(f"[dry-run] Would write {self._params.rows:,} rows to: "
                  f"{', '.join(targets)} via scale_mode={scale_mode}")
            return {"success": True, "dry_run": True}

        if dashboard is not None:
            dashboard.start()
            dashboard.step(DemoStep.GENERATING, f"{self._params.rows:,} rows ({scale_mode})")

        try:
            if scale_mode == "local":
                stats = self._run_local()
            else:
                stats = self._run_spark()
        except Exception as e:
            logger.exception("Seeding failed")
            if dashboard is not None:
                dashboard.finish(False, str(e))
            self._manifest.finish(False, str(e))
            self._manifest.save()
            return {"success": False, "error": str(e)}

        if dashboard is not None:
            dashboard.step(DemoStep.DONE)
            dashboard.finish(True)
        self._manifest.metrics.update(stats.get("metrics", {}))
        self._manifest.finish(True)
        saved_path = self._manifest.save()
        if dashboard is not None:
            dashboard.info(f"Manifest saved to {saved_path}")

        result: dict = {"success": True, "session_id": self._manifest.session_id}
        result.update(stats.get("result", {}))
        return result

    def _run_local(self) -> dict:
        """Local multi-process generation via ScaleRouter."""
        import dataclasses
        import json
        import os
        import tempfile
        from sqllocks_spindle.engine import scale_router as _scale_router_mod

        token = ""  # local does not need a Fabric token (sinks needing it will skip)
        sinks, _sinks_list, _sink_config = _build_sinks(self._conn, token=token)

        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        from sqllocks_spindle.engine.generator import Spindle
        sp = Spindle()
        parsed = sp._resolve_schema(domain, None)
        parsed.generation.scale = _rows_to_scale(self._params.rows)
        if self._params.seed is not None:
            parsed.model.seed = self._params.seed

        schema_dict = dataclasses.asdict(parsed)
        if hasattr(domain, "domain_path"):
            schema_dict["_domain_path"] = str(domain.domain_path)

        tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
        try:
            json.dump(schema_dict, tmp, default=str)
            tmp.close()
            router = _scale_router_mod.ScaleRouter(
                schema_path=tmp.name,
                sinks=sinks,
                chunk_size=500_000,
                max_workers=1 if self._params.rows < 500_000 else None,
            )
            stats = router.run(
                total_rows=self._params.rows,
                seed=self._params.seed if self._params.seed is not None else 42,
            )
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

        # Record one artifact per table that was generated
        target_label = (
            sinks[0].__class__.__name__.replace("Sink", "").lower()
            if sinks else "generated"
        )
        for tname in schema_dict.get("tables", {}):
            self._manifest.add_artifact(
                target_label, tname,
                row_count=stats.get("rows_generated", 0),
            )

        return {
            "result": {"stats": stats},
            "metrics": {"rows_generated": stats.get("rows_generated", 0)},
        }

    def _run_spark(self) -> dict:
        """Async Fabric Spark generation via FabricSparkRouter."""
        import dataclasses
        from sqllocks_spindle.engine import spark_router as _spark_router_mod

        if self._conn is None:
            raise ValueError("Spark mode requires a connection profile")

        token = _acquire_token()
        _sinks, sinks_list, sink_config = _build_sinks(self._conn, token=token)

        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        from sqllocks_spindle.engine.generator import Spindle
        sp = Spindle()
        parsed = sp._resolve_schema(domain, None)
        # Set scale based on requested rows — same as _run_local. Without this,
        # the schema stays at its default cardinality and the Spark path never
        # generates the row counts the user asked for.
        parsed.generation.scale = _rows_to_scale(self._params.rows)
        if self._params.seed is not None:
            parsed.model.seed = self._params.seed
        schema_dict = dataclasses.asdict(parsed)
        if hasattr(domain, "domain_path"):
            schema_dict["_domain_path"] = str(domain.domain_path)

        # Per-domain + per-scale prefix lets concurrent submissions write to
        # disjoint Delta tables (no collision). Override with params.table_prefix
        # to use a custom name.
        domain_name = self._params.domain or "retail"
        scale_label = _rows_to_scale(self._params.rows)
        default_prefix = f"spindle_{domain_name}_{scale_label}_"
        table_prefix = getattr(self._params, "table_prefix", None) or default_prefix

        router = _spark_router_mod.FabricSparkRouter(
            workspace_id=self._conn.workspace_id,
            lakehouse_id=self._conn.lakehouse_id,
            token=token,
            sinks=[s["type"] for s in sinks_list],
            sink_config=sink_config,
            chunk_size=500_000,
            table_prefix=table_prefix,
        )
        job = router.submit(
            schema_dict=schema_dict,
            total_rows=self._params.rows,
            seed=self._params.seed if self._params.seed is not None else 42,
        )

        # Stash job in shared registry so cmd_demo_status can find it
        from sqllocks_spindle.mcp_bridge import _job_store
        _job_store.put(job)

        self._manifest.fabric_run_id = job.fabric_run_id
        self._manifest.workspace_id = job.workspace_id
        self._manifest.notebook_item_id = job.notebook_item_id

        return {
            "result": {
                "fabric_run_id": job.fabric_run_id,
                "job_id": job.job_id,
                "status": "submitted",
                "schema_temp_path": job.schema_temp_path,
            },
            "metrics": {},
        }

    def _available_targets(self) -> list:
        if self._conn is None:
            return ["generated"]
        targets = []
        if self._conn.lakehouse_id:
            targets.append("lakehouse")
        if self._conn.warehouse_conn_str:
            targets.append("warehouse")
        if self._conn.sql_db_conn_str:
            targets.append("sql_db")
        if self._conn.eventhouse_uri:
            targets.append("eventhouse")
        return targets or ["generated"]
