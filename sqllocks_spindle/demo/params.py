"""DemoParams — configuration for all demo modes."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, Optional


DemoMode = Literal["inference", "streaming", "seeding"]
OutputFormat = Literal["terminal", "charts", "semantic_model", "all"]
ScaleMode = Literal["auto", "local", "spark"]


@dataclass
class DemoParams:
    """Unified parameter bag passed to every demo mode and scenario."""
    scenario: str = "retail"
    mode: DemoMode = "inference"
    connection: Optional[str] = None
    input_file: Optional[str] = None
    db_schema: str = "dbo"
    db_tables: Optional[list] = None
    sample_rows: int = 1000
    rows: int = 100_000
    domain: Optional[str] = None
    domains: Optional[list] = None
    output_formats: list = field(default_factory=lambda: ["terminal"])
    env_name: Optional[str] = None
    dry_run: bool = False
    estimate_only: bool = False
    auto_cleanup: bool = False
    seed: Optional[int] = None
    scale_mode: ScaleMode = "auto"
