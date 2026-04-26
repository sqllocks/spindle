"""Fabric REST API client for polling and cancelling notebook job runs."""
from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"

_STATUS_MAP: dict[str, str] = {
    "NotStarted": "submitted",
    "InProgress": "running",
    "Deduplicating": "running",
    "Completed": "succeeded",
    "Failed": "failed",
    "Cancelled": "cancelled",
}


class FabricJobTracker:
    """Thin wrapper around the Fabric Jobs REST API.

    Args:
        token: Bearer token for Entra authentication.
    """

    def __init__(self, token: str) -> None:
        self._token = token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def get_status(self, workspace_id: str, item_id: str, run_id: str) -> dict:
        """Poll Fabric for the current status of a notebook run.

        Returns:
            dict with keys: status (Spindle-normalized), fabric_status (raw),
            fabric_run_id.
        """
        url = (
            f"{_FABRIC_API}/workspaces/{workspace_id}"
            f"/items/{item_id}/jobs/instances/{run_id}"
        )
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        fabric_status = data.get("status", "Unknown")
        return {
            "status": _STATUS_MAP.get(fabric_status, fabric_status.lower()),
            "fabric_status": fabric_status,
            "fabric_run_id": run_id,
        }

    def cancel(self, workspace_id: str, item_id: str, run_id: str) -> dict:
        """Cancel an in-flight Fabric notebook run.

        Returns:
            dict with keys: cancelled (True), fabric_run_id.
        """
        url = (
            f"{_FABRIC_API}/workspaces/{workspace_id}"
            f"/items/{item_id}/jobs/instances/{run_id}/cancel"
        )
        resp = requests.post(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return {"cancelled": True, "fabric_run_id": run_id}
