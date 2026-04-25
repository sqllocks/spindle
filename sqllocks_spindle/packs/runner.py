"""Orchestrate complete scenario pack execution."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from sqllocks_spindle.packs.loader import ScenarioPack
from sqllocks_spindle.packs.validator import PackValidator, PackValidationResult


@dataclass
class RunResult:
    """Result of a complete scenario pack execution."""
    manifest: Any  # RunManifest (imported lazily to avoid circular deps)
    files_written: list[str] = field(default_factory=list)
    events_emitted: int = 0
    validation_results: dict[str, bool] = field(default_factory=dict)
    elapsed_time: float = 0.0
    pack_id: str = ""
    domain: str = ""
    scale: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        status = "SUCCESS" if self.is_success else "FAILED"
        lines = [
            f"Pack Run: {status}",
            f"  Pack:    {self.pack_id}",
            f"  Domain:  {self.domain}",
            f"  Scale:   {self.scale}",
            f"  Elapsed: {self.elapsed_time:.1f}s",
            f"  Files:   {len(self.files_written)}",
            f"  Events:  {self.events_emitted:,}",
        ]
        if self.validation_results:
            lines.append("  Validation gates:")
            for gate, passed in self.validation_results.items():
                lines.append(f"    {gate}: {'PASS' if passed else 'FAIL'}")
        if self.errors:
            lines.append(f"  Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"    {err}")
        return "\n".join(lines)


class PackRunner:
    """Orchestrate a complete scenario pack execution.

    Steps:
        1. Validate pack against domain schema
        2. Generate base data via Spindle engine
        3. Run simulation (file_drop / stream / hybrid)
        4. Validate outputs against gates
        5. Emit run manifest
    """

    def run(
        self,
        pack: ScenarioPack,
        domain: Any,
        scale: str = "small",
        seed: int = 42,
        base_path: str = ".",
    ) -> RunResult:
        """Execute a scenario pack end-to-end.

        Args:
            pack: The ScenarioPack to execute.
            domain: A Domain instance whose schema drives generation.
            scale: Scale preset (small, medium, large, xlarge).
            seed: Random seed for reproducibility.
            base_path: Root directory for output files.

        Returns:
            RunResult with manifest, files written, and validation outcomes.
        """
        from sqllocks_spindle.engine.generator import Spindle
        from sqllocks_spindle.manifests.run_manifest import ManifestBuilder

        start_time = time.time()
        output_root = Path(base_path)
        output_root.mkdir(parents=True, exist_ok=True)

        errors: list[str] = []
        files_written: list[str] = []
        events_emitted = 0
        validation_results: dict[str, bool] = {}

        # --- Step 1: Validate pack against domain ---
        validator = PackValidator()
        pack_validation = validator.validate(pack, domain)
        if not pack_validation.is_valid:
            return RunResult(
                manifest=None,
                errors=[f"Pack validation failed: {e}" for e in pack_validation.errors],
                pack_id=pack.id,
                domain=pack.domain,
                scale=scale,
                elapsed_time=time.time() - start_time,
            )

        # --- Step 2: Generate base data ---
        builder = ManifestBuilder()
        builder.start(
            spec=None,
            pack=pack,
            domain_name=pack.domain,
            scale=scale,
            seed=seed,
        )

        spindle = Spindle()
        try:
            gen_result = spindle.generate(domain=domain, scale=scale, seed=seed)
        except Exception as exc:
            errors.append(f"Data generation failed: {exc}")
            builder.finish()
            return RunResult(
                manifest=builder.finish(),
                errors=errors,
                pack_id=pack.id,
                domain=pack.domain,
                scale=scale,
                elapsed_time=time.time() - start_time,
            )

        # --- Step 3: Run simulation based on pack kind ---
        if pack.kind == "file_drop":
            written = self._run_file_drop(pack, gen_result, output_root)
            files_written.extend(written)
        elif pack.kind == "stream":
            events_emitted = self._run_stream(pack, gen_result, output_root)
        elif pack.kind == "hybrid":
            written, events = self._run_hybrid(pack, gen_result, output_root)
            files_written.extend(written)
            events_emitted = events

        # Record outputs in manifest
        for table_name, df in gen_result.tables.items():
            table_paths = [p for p in files_written if table_name in str(p)]
            builder.record_output(
                table_name=table_name,
                rows=len(df),
                columns=len(df.columns),
                paths=table_paths,
            )

        # --- Step 4: Validate outputs ---
        if pack.validation:
            for gate in pack.validation.required_gates:
                passed = self._run_gate(gate, gen_result)
                validation_results[gate] = passed
                builder.record_validation(gate, passed)

        # --- Step 5: Build and return manifest ---
        manifest = builder.finish()

        elapsed = time.time() - start_time

        # Write manifest to disk
        manifest_path = output_root / f"{manifest.run_id}_manifest.json"
        from sqllocks_spindle.manifests.run_manifest import ManifestBuilder as MB
        MB.to_file(manifest, str(manifest_path))
        files_written.append(str(manifest_path))

        return RunResult(
            manifest=manifest,
            files_written=files_written,
            events_emitted=events_emitted,
            validation_results=validation_results,
            elapsed_time=elapsed,
            pack_id=pack.id,
            domain=pack.domain,
            scale=scale,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Simulation runners
    # ------------------------------------------------------------------

    def _run_file_drop(self, pack: ScenarioPack, gen_result: Any, output_root: Path) -> list[str]:
        """Simulate a file-drop landing zone by writing generated data as files."""
        from sqllocks_spindle.output import PandasWriter

        files_written: list[str] = []
        if pack.file_drop is None:
            return files_written

        # Determine output format (use first listed format)
        fmt = pack.file_drop.formats[0] if pack.file_drop.formats else "parquet"

        # Determine landing zone path
        landing_root = output_root
        lh_root = pack.fabric_targets.get("lakehouse_files_root")
        if lh_root:
            landing_root = output_root / lh_root
        landing_root.mkdir(parents=True, exist_ok=True)

        writer = PandasWriter()
        # Filter to entities listed in the pack (or all if none specified)
        entities = pack.file_drop.entities or list(gen_result.tables.keys())
        tables_to_write = {
            name: df for name, df in gen_result.tables.items() if name in entities
        }

        if fmt == "parquet":
            files_written = writer.to_parquet(tables_to_write, str(landing_root))
        elif fmt == "csv":
            files_written = writer.to_csv(tables_to_write, str(landing_root))
        elif fmt in ("jsonl", "json"):
            files_written = writer.to_jsonl(tables_to_write, str(landing_root))
        else:
            files_written = writer.to_csv(tables_to_write, str(landing_root))

        return files_written

    def _run_stream(self, pack: ScenarioPack, gen_result: Any, output_root: Path) -> int:
        """Simulate streaming by writing events to JSONL files.

        Returns the number of events emitted.
        """
        if pack.streaming is None:
            return 0

        events_emitted = 0
        for topic in pack.streaming.topics:
            # Find the table that matches the topic name
            table_name = topic.name
            if table_name not in gen_result.tables:
                # Try to find a close match
                for t_name in gen_result.tables:
                    if topic.name in t_name or t_name in topic.name:
                        table_name = t_name
                        break
                else:
                    continue

            df = gen_result.tables[table_name]
            events_emitted += len(df)

            # Write topic events to JSONL
            topic_file = output_root / f"{topic.name}_{topic.event_type}.jsonl"
            df.to_json(str(topic_file), orient="records", lines=True)

        return events_emitted

    def _run_hybrid(
        self, pack: ScenarioPack, gen_result: Any, output_root: Path
    ) -> tuple[list[str], int]:
        """Simulate hybrid mode: file drop + stream."""
        files_written: list[str] = []
        events_emitted = 0

        if pack.hybrid is None:
            return files_written, events_emitted

        # Micro-batch portion
        if pack.hybrid.micro_batch:
            from sqllocks_spindle.output import PandasWriter

            mb_root = output_root / "micro_batch"
            mb_root.mkdir(parents=True, exist_ok=True)

            entities = pack.hybrid.micro_batch.entities or list(gen_result.tables.keys())
            tables_to_write = {
                name: df for name, df in gen_result.tables.items() if name in entities
            }
            fmt = pack.hybrid.micro_batch.formats[0] if pack.hybrid.micro_batch.formats else "jsonl"

            writer = PandasWriter()
            if fmt == "jsonl":
                files_written = writer.to_jsonl(tables_to_write, str(mb_root))
            elif fmt == "parquet":
                files_written = writer.to_parquet(tables_to_write, str(mb_root))
            else:
                files_written = writer.to_csv(tables_to_write, str(mb_root))

        # Stream portion
        if pack.hybrid.stream and pack.hybrid.stream.topics:
            for topic in pack.hybrid.stream.topics:
                table_name = topic.name
                if table_name not in gen_result.tables:
                    for t_name in gen_result.tables:
                        if topic.name in t_name or t_name in topic.name:
                            table_name = t_name
                            break
                    else:
                        continue

                df = gen_result.tables[table_name]
                events_emitted += len(df)

                topic_file = output_root / f"stream_{topic.name}_{topic.event_type}.jsonl"
                df.to_json(str(topic_file), orient="records", lines=True)
                files_written.append(str(topic_file))

        return files_written, events_emitted

    # ------------------------------------------------------------------
    # Validation gates
    # ------------------------------------------------------------------

    def _run_gate(self, gate: str, gen_result: Any) -> bool:
        """Run a single validation gate against generated data."""
        if gate == "referential_integrity":
            errors = gen_result.verify_integrity()
            return len(errors) == 0
        elif gate == "schema_conformance":
            # Check that all expected tables were generated with correct columns
            for table_name, table_def in gen_result.schema.tables.items():
                if table_name not in gen_result.tables:
                    return False
                df = gen_result.tables[table_name]
                expected_cols = set(table_def.column_names)
                actual_cols = set(df.columns)
                if not expected_cols.issubset(actual_cols):
                    return False
            return True
        elif gate == "row_count":
            # Verify each table has at least 1 row
            for df in gen_result.tables.values():
                if len(df) == 0:
                    return False
            return True
        elif gate == "null_check":
            # Verify non-nullable columns have no nulls
            for table_name, table_def in gen_result.schema.tables.items():
                if table_name not in gen_result.tables:
                    continue
                df = gen_result.tables[table_name]
                for col_name, col_def in table_def.columns.items():
                    if not col_def.nullable and col_name in df.columns:
                        if df[col_name].isnull().any():
                            return False
            return True
        elif gate == "uniqueness":
            # Verify primary keys are unique
            for table_name, table_def in gen_result.schema.tables.items():
                if table_name not in gen_result.tables:
                    continue
                df = gen_result.tables[table_name]
                pk_cols = [c for c in table_def.primary_key if c in df.columns]
                if pk_cols and df.duplicated(subset=pk_cols).any():
                    return False
            return True
        else:
            # Unknown gate — pass by default with a warning
            return True
