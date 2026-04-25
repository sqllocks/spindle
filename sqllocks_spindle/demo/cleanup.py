"""CleanupEngine — remove all artifacts recorded in a DemoManifest."""
from __future__ import annotations
import logging
from typing import Optional

from sqllocks_spindle.demo.manifest import DemoManifest

logger = logging.getLogger(__name__)


class CleanupEngine:
    def __init__(self, connection_profile=None):
        self._conn = connection_profile

    def cleanup(self, manifest: DemoManifest, dry_run: bool = False) -> dict:
        removed: dict = {}
        for artifact in manifest.artifacts:
            target = artifact.target
            name = artifact.name
            if dry_run:
                removed.setdefault(target, []).append(name)
                logger.info("[dry-run] Would remove %s: %s", target, name)
                continue
            try:
                if target == "file":
                    self._cleanup_file(artifact.detail or name)
                elif target in ("warehouse", "sql_db"):
                    self._cleanup_sql_table(name, target)
                elif target == "lakehouse":
                    self._cleanup_lakehouse(artifact.detail or name)
                elif target == "eventhouse":
                    self._cleanup_eventhouse(name)
                else:
                    logger.warning("Unknown target type %r — skipping %s", target, name)
                    continue
                removed.setdefault(target, []).append(name)
                logger.info("Removed %s: %s", target, name)
            except Exception as e:
                logger.error("Failed to remove %s %s: %s", target, name, e)
        return removed

    def _cleanup_file(self, path: str) -> None:
        from pathlib import Path
        import shutil
        p = Path(path)
        if p.exists():
            if p.is_file():
                p.unlink()
            else:
                shutil.rmtree(p)

    def _cleanup_sql_table(self, table_name: str, target: str) -> None:
        if self._conn is None:
            logger.warning("No connection profile — cannot remove SQL table %s", table_name)
            return
        conn_str = (self._conn.warehouse_conn_str if target == "warehouse"
                    else self._conn.sql_db_conn_str)
        if not conn_str:
            logger.warning("No connection string for %s — cannot remove %s", target, table_name)
            return
        import pyodbc
        import re
        if not re.match(r'^[\w.]+$', table_name):
            raise ValueError(f"Unsafe table name: {table_name!r}")
        with pyodbc.connect(conn_str, timeout=30) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()

    def _cleanup_lakehouse(self, path: str) -> None:
        logger.warning("Lakehouse cleanup not implemented for path %s — remove manually", path)

    def _cleanup_eventhouse(self, table_name: str) -> None:
        logger.warning("Eventhouse cleanup not implemented for table %s — drop manually", table_name)
