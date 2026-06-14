"""LakehouseProfiler — profile Fabric Lakehouse tables without a Spark session.

Uses the `deltalake` library (part of the [fabric] extra) to read Delta tables
locally via ABFSS. Falls back to a REST API for table listing when deltalake
is unavailable.

Requires: sqllocks-spindle[fabric] — deltalake>=0.17.0, pyarrow>=14.0
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from sqllocks_spindle.inference.profiler import DataProfiler, TableProfile

logger = logging.getLogger(__name__)

try:
    import deltalake as _deltalake
    HAS_DELTALAKE = True
except ImportError:
    HAS_DELTALAKE = False

try:
    from azure.identity import DefaultAzureCredential as _DefaultAzureCredential
    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False


class LakehouseProfiler:
    """Profile Fabric Lakehouse Delta tables and return TableProfile objects.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Fabric lakehouse GUID.
        token_provider: A callable returning an Azure access token string.
            Defaults to DefaultAzureCredential when azure-identity is installed.
        default_sample_rows: Row limit for profiling. Pass None to scan entire table.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token_provider: Any | None = None,
        default_sample_rows: int | None = 100_000,
    ):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.token_provider = token_provider
        self.default_sample_rows = default_sample_rows

    def profile_table(
        self,
        table_name: str,
        sample_rows: int | None | str = "default",
    ) -> TableProfile:
        """Profile a single Delta table."""
        if sample_rows == "default":
            sample_rows = self.default_sample_rows

        df = self._read_table(table_name, sample_rows=sample_rows)
        profiler = DataProfiler(sample_rows=None)
        return profiler.profile(df, table_name=table_name)

    def profile_all(
        self,
        sample_rows: int | None | str = "default",
    ) -> dict[str, TableProfile]:
        """Profile all tables in the lakehouse."""
        table_names = self._list_tables()
        profiles: dict[str, TableProfile] = {}
        for tname in table_names:
            try:
                profiles[tname] = self.profile_table(tname, sample_rows=sample_rows)
            except Exception as exc:
                logger.warning("Skipping table '%s': %s", tname, exc)
        return profiles

    def detect_foreign_keys(
        self,
        table_names: list[str] | None = None,
        overlap_threshold: float = 0.9,
        sample_rows: int | None | str = "default",
        full_scan: bool = False,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Sampled cross-table FK detection (advisory). ADR-009 / STORY-016.

        Reads each table's columns (sampled by default) and runs the proven
        ``DataProfiler._detect_foreign_keys_advisory`` core (naming ``*_id`` plus
        value-overlap >= ``overlap_threshold``) across every table pair. Detected
        FKs are advisory and reported with the measured overlap; a declared
        ``star_map`` / ``RelationshipDef`` remains authoritative and overrides
        (resolved by the caller, not here).

        Args:
            table_names: Tables to scan. Defaults to all tables in the lakehouse.
            overlap_threshold: Minimum child-to-parent value overlap to report a
                FK (default 0.9, configurable per ADR-009).
            sample_rows: Per-table row cap used when reading key columns.
                ``"default"`` uses ``self.default_sample_rows``; ``None`` reads
                the full table. Ignored when ``full_scan=True``.
            full_scan: Read entire tables (no sampling) to confirm a sampled
                result (ADR-009 full-scan option).

        Returns:
            ``{child_table: {col_name: {"parent_table": str, "overlap": float,
            "advisory": True, "full_scan": bool}}}`` for every detected FK.
        """
        if sample_rows == "default":
            sample_rows = self.default_sample_rows
        effective_sample = None if full_scan else sample_rows

        names = table_names if table_names is not None else self._list_tables()

        # Read each table once (sampled unless full_scan); skip unreadable tables.
        frames: dict[str, pd.DataFrame] = {}
        for tname in names:
            try:
                frames[tname] = self._read_table(tname, sample_rows=effective_sample)
            except Exception as exc:
                logger.warning("Skipping table '%s' during FK detection: %s", tname, exc)

        profiler = DataProfiler(sample_rows=None)

        result: dict[str, dict[str, dict[str, Any]]] = {}
        for child_name, child_df in frames.items():
            advisory = profiler._detect_foreign_keys_advisory(
                child_name, child_df, frames, overlap_threshold=overlap_threshold
            )
            if not advisory:
                continue
            result[child_name] = {
                col: {
                    "parent_table": parent,
                    "overlap": overlap,
                    "advisory": True,
                    "full_scan": full_scan,
                }
                for col, (parent, overlap) in advisory.items()
            }

        return result

    @staticmethod
    def reconcile_declared_foreign_keys(
        detected: dict[str, dict[str, dict[str, Any]]],
        declared: Any,
    ) -> dict[str, Any]:
        """Declared FKs override detected advisory FKs (ADR-009 / STORY-017).

        A declared ``star_map`` / ``RelationshipDef`` is AUTHORITATIVE: where a
        declaration exists for a ``(child_table, child_col)`` it wins over any
        detected FK, even a high-overlap one. Detected FKs that a declaration
        overrode are REPORTED (not silently dropped) for transparency.

        Args:
            detected: the output of :meth:`detect_foreign_keys`.
            declared: iterable of ``(child_table, child_col, parent_table)``
                tuples, or dicts with those keys.

        Returns:
            ``{"foreign_keys": <resolved map>, "overridden": [<reports>]}``.
            Resolved declared entries carry ``advisory=False, declared=True``.
        """
        import copy

        decl: dict[tuple[str, str], str] = {}
        for d in declared or []:
            if isinstance(d, dict):
                decl[(d["child_table"], d["child_col"])] = d["parent_table"]
            else:
                child, col, parent = d
                decl[(child, col)] = parent

        resolved = copy.deepcopy(detected) if detected else {}
        overridden: list[dict[str, Any]] = []
        for (child, col), parent in decl.items():
            existing = resolved.get(child, {}).get(col)
            if existing and existing.get("parent_table") != parent:
                overridden.append(
                    {
                        "child_table": child,
                        "child_col": col,
                        "detected_parent": existing.get("parent_table"),
                        "detected_overlap": existing.get("overlap"),
                        "declared_parent": parent,
                    }
                )
            resolved.setdefault(child, {})[col] = {
                "parent_table": parent,
                "overlap": None,
                "advisory": False,
                "declared": True,
            }
        return {"foreign_keys": resolved, "overridden": overridden}

    def _abfss_tables_root(self) -> str:
        return (
            f"abfss://{self.workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com"
            f"/{self.lakehouse_id}/Tables"
        )

    def _get_token(self) -> str | None:
        if self.token_provider is not None:
            return self.token_provider()
        if HAS_AZURE_IDENTITY:
            cred = _DefaultAzureCredential()
            token = cred.get_token("https://storage.azure.com/.default")
            return token.token
        return None

    def _storage_options(self) -> dict[str, str]:
        token = self._get_token()
        if token:
            return {"bearer_token": token, "use_emulator": "false"}
        return {}

    def _read_table(
        self,
        table_name: str,
        sample_rows: int | None = None,
    ) -> pd.DataFrame:
        """Read a Delta table into a pandas DataFrame."""
        if not HAS_DELTALAKE:
            raise ImportError(
                "LakehouseProfiler requires 'deltalake'. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )

        import re
        if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
            raise ValueError(
                f"Unsafe Delta table name {table_name!r}; expected [A-Za-z0-9_]+. "
                "Rejecting to prevent path traversal into other OneLake locations."
            )
        table_uri = f"{self._abfss_tables_root()}/{table_name}"
        storage_options = self._storage_options()

        try:
            dt = _deltalake.DeltaTable(table_uri, storage_options=storage_options)
            df = dt.to_pandas()
            if sample_rows is not None and len(df) > sample_rows:
                # Random sample, not head(): head() returns physical/partition
                # order (often sorted by date/key), which biases every inferred
                # distribution, quantile, and FK-overlap statistic.
                df = df.sample(n=sample_rows, random_state=42).reset_index(drop=True)
            return df
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read table '{table_name}' from {table_uri}: {exc}"
            ) from exc

    def _list_tables(self) -> list[str]:
        """List table names in the lakehouse."""
        if not HAS_DELTALAKE:
            logger.warning(
                "deltalake not installed — cannot list lakehouse tables. "
                "Install with: pip install 'sqllocks-spindle[fabric-inference]'"
            )
            return []

        try:
            from pyarrow import fs as _fs
            az_fs = _fs.AzureFileSystem(
                self.workspace_id,
                dfs_storage_authority="onelake.dfs.fabric.microsoft.com",
            )
            file_info = az_fs.get_file_info(
                _fs.FileSelector(f"{self.lakehouse_id}/Tables", recursive=False)
            )
            return [fi.base_name for fi in file_info if fi.type.name == "Directory"]
        except Exception as exc:
            logger.warning("Could not list lakehouse tables: %s", exc)
            return []
