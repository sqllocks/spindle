"""InferenceDemoMode — learn from real data, generate synthetic, compare."""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.output.dashboard import ProgressDashboard, DemoStep
from sqllocks_spindle.demo.output.terminal import FidelityReport
from sqllocks_spindle.demo.output.charts import ChartRenderer

import warnings
logger = logging.getLogger(__name__)


def _rows_to_scale(rows: int) -> str:
    """Map approximate row count to Spindle scale preset."""
    if rows <= 2_000:
        return "small"
    if rows <= 50_000:
        return "medium"
    if rows <= 500_000:
        return "large"
    return "xlarge"


def _get_domain_instance(name: str):
    """Resolve a domain name string to a Domain instance."""
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
    raise ValueError(f"Domain '{name}' not found. Available domains: retail, healthcare, etc.")


class InferenceDemoMode:
    """Profile source data, generate synthetic dataset, compare distributions."""

    def __init__(self, params: DemoParams, manifest: DemoManifest,
                 dashboard: Optional[ProgressDashboard] = None,
                 connection_profile=None):
        self._params = params
        self._manifest = manifest
        self._dashboard = dashboard or ProgressDashboard(params.scenario, "inference", params.rows)
        self._conn = connection_profile

    def run(self) -> dict:
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")
        dashboard = self._dashboard
        dashboard.start()
        try:
            dashboard.step(DemoStep.PROFILING, self._describe_input())
            real_profile = self._profile_source()
            dashboard.info(f"Profiled {len(real_profile.tables)} table(s)")

            from sqllocks_spindle.inference.schema_builder import SchemaBuilder
            schema = SchemaBuilder().build(real_profile)
            dashboard.info(f"Built schema: {len(schema.tables)} tables")

            dashboard.step(DemoStep.GENERATING, f"{self._params.rows:,} rows (approx)")
            gen_result = self._generate(schema)
            total = sum(len(df) for df in gen_result.values())
            dashboard.info(f"Generated {total:,} total rows")

            from sqllocks_spindle.inference.profiler import DataProfiler
            syn_profile = DataProfiler().profile_dataset(gen_result)

            dashboard.step(DemoStep.COMPARING)
            report = FidelityReport(real_profile, syn_profile)
            score = report.overall_score()
            dashboard.info(f"Fidelity score: {score:.1%}")

            self._render_output(report, real_profile, syn_profile, score)

            self._manifest.metrics["fidelity_score"] = round(score, 4)
            self._manifest.metrics["tables_profiled"] = len(real_profile.tables)
            for tname, df in gen_result.items():
                self._manifest.add_artifact("synthetic", tname, row_count=len(df))

            dashboard.step(DemoStep.DONE)
            dashboard.finish(True)
            return {
                "success": True,
                "fidelity_score": score,
                "real_profile": real_profile,
                "synthetic_profile": syn_profile,
                "generated_data": gen_result,
            }
        except Exception as e:
            logger.exception("Inference demo failed")
            dashboard.step(DemoStep.FAILED, str(e))
            dashboard.finish(False, str(e))
            return {"success": False, "error": str(e)}

    def _describe_input(self) -> str:
        if self._params.input_file == "live-db":
            return f"live DB via {self._params.connection or 'default'}"
        elif self._params.input_file:
            return str(self._params.input_file)
        return "domain defaults"

    def _profile_source(self):
        if self._params.input_file == "live-db":
            return self._profile_live_db()
        elif self._params.input_file:
            return self._profile_file(self._params.input_file)
        else:
            return self._profile_domain_defaults()

    def _profile_live_db(self):
        from sqllocks_spindle.inference.database_profiler import DatabaseProfiler
        if self._conn is None:
            raise ValueError("live-db mode requires a connection profile. Use --connection.")
        conn_str = self._conn.warehouse_conn_str or self._conn.sql_db_conn_str
        profiler = DatabaseProfiler(
            connection_string=conn_str,
            auth_method=self._conn.auth_method,
            tenant_id=self._conn.tenant_id or None,
            client_id=self._conn.client_id or None,
            client_secret=self._conn.client_secret or None,
        )
        return profiler.profile(
            schema=self._params.db_schema,
            sample_rows=self._params.sample_rows,
            tables=self._params.db_tables,
        )

    def _profile_file(self, path: str):
        import pandas as pd
        from sqllocks_spindle.inference.profiler import DataProfiler
        p = Path(path)
        if p.suffix == ".csv":
            df = pd.read_csv(p)
        elif p.suffix in (".parquet", ".pq"):
            df = pd.read_parquet(p)
        else:
            raise ValueError(f"Unsupported file type: {p.suffix}. Use .csv or .parquet.")
        return DataProfiler().profile_dataset({p.stem: df})

    def _profile_domain_defaults(self):
        """Generate a small reference dataset from the domain and profile it."""
        from sqllocks_spindle.engine.generator import Spindle
        from sqllocks_spindle.inference.profiler import DataProfiler
        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        sp = Spindle()
        result = sp.generate(domain=domain, scale="small", seed=self._params.seed)
        return DataProfiler().profile_dataset(result.tables)

    def _generate(self, schema) -> dict:
        from sqllocks_spindle.engine.generator import Spindle
        scale = _rows_to_scale(self._params.rows)
        sp = Spindle()
        result = sp.generate(schema=schema, scale=scale, seed=self._params.seed)
        return result.tables

    def _render_output(self, report: FidelityReport, real_profile, syn_profile, score: float) -> None:
        formats = self._params.output_formats or ["terminal"]
        if "all" in formats:
            formats = ["terminal", "charts", "semantic_model"]
        if "terminal" in formats:
            report.render()
        if "charts" in formats:
            renderer = ChartRenderer(real_profile, syn_profile)
            renderer.render_all()
            renderer.render_summary_card(score)
