# sqllocks_spindle/verify/runner.py
"""VerifyRunner — orchestrates validation gates and produces a VerifyResult."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from sqllocks_spindle import __version__
from sqllocks_spindle.schema.parser import SpindleSchema
from sqllocks_spindle.validation.gates import (
    DistributionGate,
    GateResult,
    NullConstraintGate,
    ReferentialIntegrityGate,
    SchemaConformanceGate,
    UniqueConstraintGate,
    ValidationContext,
)
from sqllocks_spindle.verify.report import VerifyResult


class VerifyRunner:
    """Run a curated set of validation gates against synthetic data tables.

    Without a schema: only records row counts (nothing to validate against).
    With a schema: adds schema conformance, null constraint, PK uniqueness, and FK checks.
    With statistical=True: also runs distribution gate (requires scipy).

    Args:
        schema:      Parsed SpindleSchema for validation. Optional.
        statistical: Include distribution drift checks (KS test / chi-squared).
        data_path:   Original data path (for reporting). Optional.
        schema_path: Original schema path (for reporting). Optional.
    """

    def __init__(
        self,
        schema: SpindleSchema | None = None,
        statistical: bool = False,
        data_path: str = "",
        schema_path: str | None = None,
    ) -> None:
        self._schema = schema
        self._statistical = statistical
        self._data_path = data_path
        self._schema_path = schema_path

    def run(self, tables: dict[str, pd.DataFrame]) -> VerifyResult:
        """Run gates against `tables` and return a VerifyResult.

        Args:
            tables: Dict mapping table name to DataFrame.

        Returns:
            VerifyResult with gate outcomes, row counts, and metadata.
        """
        row_counts = {name: len(df) for name, df in tables.items()}
        ctx = ValidationContext(tables=tables, schema=self._schema)
        gate_results: list[GateResult] = []

        if self._schema is not None:
            gate_results.append(SchemaConformanceGate().check(ctx))
            gate_results.append(NullConstraintGate().check(ctx))
            gate_results.append(UniqueConstraintGate().check(ctx))
            gate_results.append(ReferentialIntegrityGate().check(ctx))

        if self._statistical:
            gate_results.append(DistributionGate().check(ctx))

        passed = all(gr.passed for gr in gate_results)

        return VerifyResult(
            passed=passed,
            gate_results=gate_results,
            row_counts=row_counts,
            run_at=datetime.now(timezone.utc).isoformat(),
            data_path=self._data_path,
            schema_path=self._schema_path,
            statistical=self._statistical,
            spindle_version=__version__,
        )
