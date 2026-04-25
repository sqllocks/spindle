from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import numpy as np

if TYPE_CHECKING:
    from sqllocks_spindle.schema.parser import SpindleSchema


@runtime_checkable
class Sink(Protocol):
    """Protocol all sinks must implement."""

    def open(self, schema: SpindleSchema) -> None:
        """Called once before any chunks arrive. Create directories/tables/connections."""
        ...

    def write_chunk(self, table: str, arrays: dict[str, np.ndarray]) -> None:
        """Write one chunk of data for a table. Called once per table per chunk."""
        ...

    def close(self) -> None:
        """Flush, commit, teardown. Called once after all chunks."""
        ...


@dataclass
class FabricConnectionProfile:
    """Auth profile shared by all Fabric-backed sinks."""

    token: str
    endpoint: str
