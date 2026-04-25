"""ProgressDashboard — rich terminal progress tracking for demo steps."""
from __future__ import annotations
from enum import Enum
from typing import Optional
import time


class DemoStep(Enum):
    CONNECTING = "Connecting to targets"
    PROFILING = "Profiling source data"
    GENERATING = "Generating synthetic data"
    COMPARING = "Comparing distributions"
    WRITING = "Writing to targets"
    NOTEBOOK = "Generating notebook"
    DONE = "Complete"
    FAILED = "Failed"


class ProgressDashboard:
    def __init__(self, scenario: str, mode: str, total_rows: int = 0):
        self._scenario = scenario
        self._mode = mode
        self._total_rows = total_rows
        self._current_step: Optional[DemoStep] = None
        self._step_times: dict = {}
        self._start_time = time.time()
        self._use_rich = self._check_rich()
        self._console = None

    def _check_rich(self) -> bool:
        try:
            import rich  # noqa: F401
            return True
        except ImportError:
            return False

    def start(self) -> None:
        if self._use_rich:
            from rich.console import Console
            self._console = Console()
            self._console.rule(f"[bold cyan]Spindle Demo — {self._scenario} ({self._mode})[/]")
        else:
            print(f"=== Spindle Demo — {self._scenario} ({self._mode}) ===")

    def step(self, s: DemoStep, detail: str = "") -> None:
        self._current_step = s
        self._step_times[s.name] = time.time()
        elapsed = time.time() - self._start_time
        msg = f"[{elapsed:.1f}s] {s.value}"
        if detail:
            msg += f": {detail}"
        if self._use_rich and self._console:
            icon = "✅" if s == DemoStep.DONE else ("❌" if s == DemoStep.FAILED else "⏳")
            self._console.print(f"  {icon} [bold]{s.value}[/bold]" + (f" — {detail}" if detail else ""))
        else:
            print(f"  >> {msg}")

    def info(self, message: str) -> None:
        if self._use_rich and self._console:
            self._console.print(f"     [dim]{message}[/dim]")
        else:
            print(f"     {message}")

    def finish(self, success: bool, error: Optional[str] = None) -> None:
        elapsed = time.time() - self._start_time
        if success:
            msg = f"Done in {elapsed:.1f}s"
            if self._use_rich and self._console:
                self._console.rule(f"[bold green]{msg}[/]")
            else:
                print(f"=== {msg} ===")
        else:
            msg = f"Failed after {elapsed:.1f}s: {error or 'unknown error'}"
            if self._use_rich and self._console:
                self._console.rule(f"[bold red]{msg}[/]")
            else:
                print(f"=== ERROR: {msg} ===")
