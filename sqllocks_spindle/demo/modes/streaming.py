"""StreamingDemoMode — seed targets then stream live data."""
from __future__ import annotations
import logging
import time
from typing import Optional

from sqllocks_spindle.demo.params import DemoParams
from sqllocks_spindle.demo.manifest import DemoManifest
from sqllocks_spindle.demo.output.dashboard import ProgressDashboard, DemoStep

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


class StreamingDemoMode:
    """Seed initial data then stream continuous events."""

    def __init__(self, params: DemoParams, manifest: DemoManifest,
                 dashboard: Optional[ProgressDashboard] = None,
                 connection_profile=None):
        self._params = params
        self._manifest = manifest
        self._dashboard = dashboard or ProgressDashboard(params.scenario, "streaming", params.rows)
        self._conn = connection_profile
        self._running = False

    def run(self) -> dict:
        dashboard = self._dashboard
        dashboard.start()
        try:
            dashboard.step(DemoStep.GENERATING, f"Seeding {self._params.rows:,} rows")
            seed_data = self._seed()

            dashboard.step(DemoStep.WRITING, "Streaming live — press Ctrl+C to stop")
            self._stream(seed_data)

            dashboard.step(DemoStep.DONE)
            dashboard.finish(True)
            return {"success": True}
        except KeyboardInterrupt:
            dashboard.info("Interrupted by user")
            dashboard.finish(True)
            if self._params.auto_cleanup:
                dashboard.info("Auto-cleanup enabled — removing artifacts")
            else:
                print("\nDemo stopped. Run `spindle demo cleanup` to remove artifacts.")
            return {"success": True, "stopped_by_user": True}
        except Exception as e:
            logger.exception("Streaming demo failed")
            dashboard.finish(False, str(e))
            return {"success": False, "error": str(e)}

    def _seed(self) -> dict:
        from sqllocks_spindle.engine.generator import Spindle
        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        sp = Spindle()
        result = sp.generate(domain=domain, scale="small", seed=self._params.seed)
        data = result.tables
        for tname, df in data.items():
            self._manifest.add_artifact("generated", tname, row_count=len(df))
        return data

    def _stream(self, seed_data: dict) -> None:
        from sqllocks_spindle.streaming import SpindleStreamer, ConsoleSink, StreamConfig
        domain_name = self._params.domain or "retail"
        domain = _get_domain_instance(domain_name)
        config = StreamConfig(events_per_second=10.0, realtime=False)
        sink = ConsoleSink()
        streamer = SpindleStreamer(domain=domain, sink=sink, config=config, seed=self._params.seed or 42)
        self._running = True

        import signal

        def _stop(sig, frame):
            self._running = False

        try:
            signal.signal(signal.SIGINT, _stop)
        except (ValueError, OSError):
            pass  # signal only works from main thread

        # Find first streamable table
        tables = list(seed_data.keys())
        if not tables:
            logger.warning("No tables available to stream")
            return

        table_name = tables[0]
        count = 0
        try:
            while self._running:
                streamer.stream(table_name)
                count += 1
                if count % 5 == 0:
                    self._dashboard.info(f"Streamed batch {count}")
                time.sleep(0.5)
                if count >= 10:  # stop after 10 batches in non-interactive mode
                    break
        except KeyboardInterrupt:
            self._running = False
