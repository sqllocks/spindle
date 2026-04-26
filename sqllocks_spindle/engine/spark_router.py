"""FabricSparkRouter — submits Spindle generation jobs to Fabric Spark notebooks."""
from __future__ import annotations

import json
import logging
import uuid

import requests

from sqllocks_spindle.engine.async_job_store import JobRecord
from sqllocks_spindle.engine.scale_router import (
    _SpindleJSONEncoder,
    _classify_tables,
    _generate_static_tables,
)

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


class NotebookNotFoundError(RuntimeError):
    """Raised when the Spindle Spark worker notebook cannot be found or created."""


class FabricAPIError(RuntimeError):
    """Raised when the Fabric REST API returns an unexpected error."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Fabric API error {status_code}: {message}")
        self.status_code = status_code


class FabricSparkRouter:
    """Submits Spindle generation jobs to a Fabric Spark notebook.

    Args:
        workspace_id: Fabric workspace GUID.
        lakehouse_id: Lakehouse GUID for temp file staging and output.
        token: Entra bearer token with Workspace.Write + Lakehouse.ReadWrite permissions.
        notebook_name: Display name of the Spark worker notebook (default: spindle_spark_worker).
        sinks: List of sink names to pass into the notebook (default: ["lakehouse"]).
        sink_config: Per-sink configuration dict passed as JSON to the notebook.
        chunk_size: Rows per Spark partition. Default 500_000.
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        token: str,
        notebook_name: str = "spindle_spark_worker",
        sinks: list[str] | None = None,
        sink_config: dict | None = None,
        chunk_size: int = 500_000,
    ) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._token = token
        self._notebook_name = notebook_name
        self._sinks = sinks or ["lakehouse"]
        self._sink_config = sink_config or {}
        self._chunk_size = chunk_size

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _json_headers(self) -> dict[str, str]:
        return {**self._auth_headers(), "Content-Type": "application/json"}

    def _get_or_create_notebook(self) -> str:
        """Return the item ID for the Spark worker notebook, creating if absent."""
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/notebooks"
        resp = requests.get(url, headers=self._json_headers(), timeout=30)
        resp.raise_for_status()
        for item in resp.json().get("value", []):
            if item.get("displayName") == self._notebook_name:
                logger.info("Found existing notebook '%s' (id=%s)", self._notebook_name, item["id"])
                return item["id"]
        logger.info("Notebook '%s' not found — creating.", self._notebook_name)
        return self._create_notebook()

    def _create_notebook(self) -> str:
        """Create the spindle_spark_worker notebook via the Fabric Items API."""
        import base64

        from sqllocks_spindle.notebooks import SPARK_WORKER_IPYNB

        payload_b64 = base64.b64encode(json.dumps(SPARK_WORKER_IPYNB).encode()).decode()
        body = {
            "displayName": self._notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        url = f"{_FABRIC_API}/workspaces/{self._workspace_id}/items"
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=60)
        resp.raise_for_status()
        item_id = resp.json()["id"]
        logger.info("Created notebook '%s' (id=%s)", self._notebook_name, item_id)
        return item_id

    def _upload_schema(self, schema_dict: dict, run_id: str) -> str:
        """Upload augmented schema JSON to OneLake Files via the ADLS Gen2 DFS API.

        Three-step protocol: create → append → flush.
        Returns the OneLake-relative file path.
        """
        data: bytes = json.dumps(schema_dict, cls=_SpindleJSONEncoder).encode()
        rel_path = f"spindle_temp/{run_id}_schema.json"
        base_url = (
            f"{_ONELAKE_DFS}/{self._workspace_id}/{self._lakehouse_id}/files/{rel_path}"
        )

        requests.put(
            f"{base_url}?resource=file",
            headers=self._auth_headers(),
            timeout=30,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=append&position=0",
            headers={**self._auth_headers(), "Content-Length": str(len(data))},
            data=data,
            timeout=120,
        ).raise_for_status()

        requests.patch(
            f"{base_url}?action=flush&position={len(data)}",
            headers=self._auth_headers(),
            timeout=30,
        ).raise_for_status()

        logger.info("Schema uploaded to OneLake: %s", rel_path)
        return rel_path

    def _submit_notebook_run(
        self,
        notebook_item_id: str,
        schema_path: str,
        total_rows: int,
        seed: int,
    ) -> str:
        """Submit a Fabric notebook run and return the Fabric run ID."""
        url = (
            f"{_FABRIC_API}/workspaces/{self._workspace_id}"
            f"/items/{notebook_item_id}/jobs/instances?jobType=RunNotebook"
        )
        body = {
            "executionData": {
                "parameters": {
                    "schema_path": {"value": schema_path, "cellLanguage": "Python"},
                    "chunk_size": {"value": str(self._chunk_size), "cellLanguage": "Python"},
                    "seed": {"value": str(seed), "cellLanguage": "Python"},
                    "total_rows": {"value": str(total_rows), "cellLanguage": "Python"},
                    "sinks_json": {
                        "value": json.dumps(
                            {"sinks": self._sinks, "sink_config": self._sink_config}
                        ),
                        "cellLanguage": "Python",
                    },
                    "workspace_id": {"value": self._workspace_id, "cellLanguage": "Python"},
                    "lakehouse_id": {"value": self._lakehouse_id, "cellLanguage": "Python"},
                }
            }
        }
        resp = requests.post(url, headers=self._json_headers(), json=body, timeout=30)
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            return location.rstrip("/").split("/")[-1]
        if resp.status_code == 200:
            return resp.json().get("id", uuid.uuid4().hex)
        raise FabricAPIError(resp.status_code, resp.text)

    def submit(self, schema_dict: dict, total_rows: int, seed: int) -> JobRecord:
        """Generate static tables, upload schema, and submit a Fabric notebook run.

        Returns a JobRecord immediately.
        """
        from sqllocks_spindle.engine.generator import calculate_row_counts
        from sqllocks_spindle.schema.parser import SchemaParser

        schema = SchemaParser().parse_dict(schema_dict)
        schema_counts = calculate_row_counts(schema)
        static_tables, dynamic_tables = _classify_tables(schema_counts, self._chunk_size)

        if static_tables:
            logger.info("Generating %d static tables in main process.", len(static_tables))
            static_chunk = _generate_static_tables(
                schema_path="",
                static_tables=static_tables,
                schema_counts=schema_counts,
                seed=seed,
                schema_dict=schema_dict,
            )
            schema_dict["_static_tables"] = list(static_tables)
            schema_dict["_static_pk_data"] = static_chunk
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts
        else:
            schema_dict["_static_tables"] = []
            schema_dict["_static_pk_data"] = {}
            schema_dict["_dynamic_tables"] = list(dynamic_tables)
            schema_dict["_schema_counts"] = schema_counts

        schema_dict["_base_seed"] = seed

        run_id = uuid.uuid4().hex
        schema_path = self._upload_schema(schema_dict, run_id)
        notebook_item_id = self._get_or_create_notebook()
        fabric_run_id = self._submit_notebook_run(
            notebook_item_id=notebook_item_id,
            schema_path=schema_path,
            total_rows=total_rows,
            seed=seed,
        )

        job_id = f"spindle-{run_id[:8]}"
        return JobRecord(
            job_id=job_id,
            fabric_run_id=fabric_run_id,
            workspace_id=self._workspace_id,
            notebook_item_id=notebook_item_id,
            schema_temp_path=schema_path,
            lakehouse_id=self._lakehouse_id,
            token=self._token,
        )
