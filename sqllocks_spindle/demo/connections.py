"""ConnectionRegistry — store and retrieve named Fabric connection profiles."""
from __future__ import annotations
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


@dataclass
class ConnectionProfile:
    name: str
    workspace_id: str = ""
    warehouse_conn_str: str = ""
    eventhouse_uri: str = ""
    sql_db_conn_str: str = ""
    lakehouse_id: str = ""
    auth_method: str = "cli"
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""


class ConnectionRegistry:
    DEFAULT_PATH = Path.home() / ".spindle" / "connections.json"

    def __init__(self, path: Optional[Path] = None):
        self._path = path or self.DEFAULT_PATH

    def _load_all(self) -> dict:
        if not self._path.exists():
            return {}
        with open(self._path) as f:
            return json.load(f)

    def _save_all(self, data: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(data, f, indent=2)

    def save(self, profile: ConnectionProfile) -> None:
        data = self._load_all()
        data[profile.name] = asdict(profile)
        self._save_all(data)

    def load(self, name: str) -> ConnectionProfile:
        data = self._load_all()
        if name not in data:
            raise KeyError(f"No connection profile '{name}'. Run: spindle demo init")
        return ConnectionProfile(**data[name])

    def list(self) -> list:
        return list(self._load_all().keys())

    def delete(self, name: str) -> None:
        data = self._load_all()
        data.pop(name, None)
        self._save_all(data)

    def exists(self, name: str) -> bool:
        return name in self._load_all()
