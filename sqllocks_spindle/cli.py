"""Spindle CLI — command-line interface for data generation."""

from __future__ import annotations

import sys

import click

from sqllocks_spindle import __version__


# ---------------------------------------------------------------------------
# Domain registry — auto-discovered from sqllocks_spindle.domains.*
# ---------------------------------------------------------------------------

def _discover_domains() -> dict[str, tuple[str, str, str]]:
    """Scan the domains package for Domain subclasses and build the registry."""
    import importlib
    import pkgutil

    import sqllocks_spindle.domains as _pkg
    from sqllocks_spindle.domains.base import Domain

    registry: dict[str, tuple[str, str, str]] = {}
    for _, mod_name, is_pkg in pkgutil.iter_modules(_pkg.__path__, _pkg.__name__ + "."):
        if not is_pkg:
            continue
        try:
            module = importlib.import_module(mod_name)
        except Exception:
            continue
        for attr in getattr(module, "__all__", dir(module)):
            cls = getattr(module, attr, None)
            if (
                isinstance(cls, type)
                and issubclass(cls, Domain)
                and cls is not Domain
            ):
                instance = cls.__new__(cls)
                name = cls.name.fget(instance)  # type: ignore[attr-defined]
                desc = cls.description.fget(instance)  # type: ignore[attr-defined]
                registry[name] = (mod_name, cls.__name__, desc)
    return registry


_DOMAIN_REGISTRY: dict[str, tuple[str, str, str]] | None = None


def _get_domain_registry() -> dict[str, tuple[str, str, str]]:
    global _DOMAIN_REGISTRY
    if _DOMAIN_REGISTRY is None:
        _DOMAIN_REGISTRY = _discover_domains()
    return _DOMAIN_REGISTRY


@click.group()
@click.version_option(version=__version__, prog_name="spindle")
def main():
    """Spindle by SQLLocks — Multi-domain synthetic data generator for Microsoft Fabric."""
    pass


@main.command()
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset: small, medium, large, xlarge")
@click.option("--seed", default=42, type=int, help="Random seed for reproducibility")
@click.option("--output", "-o", default=None, help="Output directory for generated files")
@click.option("--format", "fmt", default="summary", type=click.Choice(["summary", "csv", "tsv", "jsonl", "parquet", "excel", "sql", "sql-database", "delta"]))
@click.option("--mode", "-m", default="3nf", help="Schema mode: 3nf, star")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be generated without generating")
@click.option("--schema-name", default=None, help="SQL schema prefix (e.g. dbo)")
@click.option("--sql-ddl/--no-sql-ddl", default=True, help="Include CREATE TABLE DDL in SQL output")
@click.option("--sql-drop/--no-sql-drop", default=True, help="Include DROP IF EXISTS before CREATE")
@click.option("--sql-go/--no-sql-go", default=True, help="Include GO batch separators (T-SQL)")
@click.option("--sql-dialect", default="tsql", type=click.Choice(["tsql", "tsql-fabric-warehouse", "postgres", "mysql"]), help="SQL dialect for DDL output")
@click.option("--connection-string", default=None, envvar="SPINDLE_SQL_CONNECTION", help="SQL connection string (for sql-database format)")
@click.option("--auth", "auth_method", default="cli", type=click.Choice(["cli", "msi", "spn", "sql", "device-code"]), help="Auth method for sql-database")
@click.option("--write-mode", default="create_insert", type=click.Choice(["create_insert", "insert_only", "truncate_insert", "append"]), help="SQL write mode")
@click.option("--batch-size", default=5000, type=int, help="Rows per INSERT batch")
@click.option("--staging-path", default=None, envvar="SPINDLE_STAGING_PATH", help="OneLake staging path for Warehouse COPY INTO (abfss://...)")
def generate(
    domain_name: str, scale: str, seed: int, output: str | None, fmt: str,
    mode: str, dry_run: bool, schema_name: str | None,
    sql_ddl: bool, sql_drop: bool, sql_go: bool, sql_dialect: str,
    connection_string: str | None, auth_method: str, write_mode: str, batch_size: int,
    staging_path: str | None,
):
    """Generate synthetic data for a domain.

    Example: spindle generate retail --scale small --seed 42
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.schema.dependency import DependencyResolver

    # Resolve domain
    domain = _resolve_domain(domain_name, mode)

    click.echo(f"Spindle v{__version__} — {'[DRY RUN] ' if dry_run else ''}Generating {domain_name} ({mode}) at scale '{scale}'")
    click.echo(f"Seed: {seed}")

    if dry_run:
        # Parse schema and show planned row counts without generating
        spindle = Spindle()
        schema = spindle.describe(domain=domain)
        schema.generation.scale = scale
        row_counts = spindle._calculate_row_counts(schema)
        resolver = DependencyResolver()
        gen_order = resolver.resolve(schema)

        click.echo()
        click.echo(f"  {'Table':<25} {'Planned Rows':>12}")
        click.echo(f"  {'-' * 37}")
        total = 0
        for table_name in gen_order:
            count = row_counts.get(table_name, 100)
            total += count
            click.echo(f"  {table_name:<25} {count:>12,}")
        click.echo(f"  {'-' * 37}")
        click.echo(f"  {'TOTAL':<25} {total:>12,}")
        click.echo()
        click.echo(f"  Profile: {domain.profile_name}")
        click.echo(f"  Output format: {fmt}")
        if output:
            click.echo(f"  Output directory: {output}")
        click.echo()
        click.echo("No data generated. Remove --dry-run to execute.")
        return

    click.echo()

    spindle = Spindle()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)

    click.echo()
    click.echo(result.summary())

    # Verify integrity
    errors = result.verify_integrity()
    if errors:
        click.echo()
        click.echo("Referential Integrity Issues:")
        for err in errors:
            click.echo(f"  WARNING: {err}")
    else:
        click.echo()
        click.echo("Referential integrity: PASS (all FKs resolve)")

    # Output
    if fmt == "sql-database":
        # Write directly to SQL database
        if not connection_string:
            click.echo("--connection-string or SPINDLE_SQL_CONNECTION env var is required for sql-database format", err=True)
            sys.exit(1)

        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

        db_writer = FabricSqlDatabaseWriter(
            connection_string=connection_string,
            auth_method=auth_method,
            staging_lakehouse_path=staging_path,
        )

        strategy = "COPY INTO" if db_writer._bulk_writer else "INSERT"
        click.echo()
        click.echo(f"Writing to SQL database (mode={write_mode}, auth={auth_method}, strategy={strategy})...")

        write_result = db_writer.write(
            result,
            schema_name=schema_name or "dbo",
            mode=write_mode,
            batch_size=batch_size,
        )

        click.echo()
        click.echo(write_result.summary())
        if write_result.errors:
            sys.exit(1)

    elif fmt != "summary" and output:
        format_labels = {
            "csv": "CSV", "tsv": "TSV", "jsonl": "JSON Lines",
            "parquet": "Parquet", "excel": "Excel", "sql": "SQL",
            "delta": "Delta Lake",
        }

        if fmt == "delta":
            from sqllocks_spindle.output import DeltaWriter
            partition_by = getattr(result.schema.generation, "partition_by", None) or {}
            delta_writer = DeltaWriter(output_dir=output, partition_by=partition_by)
            files = delta_writer.write_all(result.tables)
        else:
            from sqllocks_spindle.output import PandasWriter
            writer = PandasWriter()

            if fmt == "csv":
                files = writer.to_csv(result.tables, output)
            elif fmt == "tsv":
                files = writer.to_tsv(result.tables, output)
            elif fmt == "jsonl":
                files = writer.to_jsonl(result.tables, output)
            elif fmt == "parquet":
                files = writer.to_parquet(result.tables, output)
            elif fmt == "excel":
                files = writer.to_excel(result.tables, output)
            elif fmt == "sql":
                # Build schema metadata from the SpindleSchema for DDL generation
                meta = {}
                pks = {}
                for tname, tdef in result.schema.tables.items():
                    pks[tname] = tdef.primary_key
                    col_meta = {}
                    for cname, cdef in tdef.columns.items():
                        col_meta[cname] = {
                            "type": cdef.type,
                            "nullable": cdef.nullable,
                            "max_length": cdef.max_length,
                            "precision": cdef.precision,
                            "scale": cdef.scale,
                        }
                    meta[tname] = col_meta
                files = writer.to_sql_inserts(
                    result.tables, output,
                    schema_name=schema_name,
                    batch_size=batch_size,
                    include_ddl=sql_ddl,
                    include_drop=sql_drop,
                    include_go=sql_go,
                    sql_dialect=sql_dialect,
                    schema_meta=meta,
                    primary_keys=pks,
                    domain_name=domain_name,
                    scale=scale,
                    seed=seed,
                )
            else:
                files = []

        click.echo()
        click.echo(f"Written {len(files)} {format_labels.get(fmt, fmt)} files to {output}/")
    elif fmt != "summary" and fmt != "sql-database" and not output:
        click.echo()
        click.echo("Hint: use --output/-o to write files to disk")


@main.command()
@click.argument("domain_name")
@click.option("--mode", "-m", default="3nf", help="Schema mode: 3nf, star")
def describe(domain_name: str, mode: str):
    """Describe a domain's schema without generating data.

    Example: spindle describe retail
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.schema.dependency import DependencyResolver

    domain = _resolve_domain(domain_name, mode)
    spindle = Spindle()
    schema = spindle.describe(domain=domain)
    resolver = DependencyResolver()
    gen_order = resolver.resolve(schema)

    click.echo(f"Domain: {schema.model.domain} ({schema.model.schema_mode})")
    click.echo(f"Tables: {len(schema.tables)}")
    click.echo(f"Relationships: {len(schema.relationships)}")
    click.echo(f"Business Rules: {len(schema.business_rules)}")
    click.echo()

    # Profile info
    click.echo(f"Profile: {domain.profile_name}")
    click.echo(f"Available profiles: {', '.join(domain.available_profiles)}")
    click.echo()

    click.echo("Generation Order:")
    for i, table in enumerate(gen_order, 1):
        t = schema.tables[table]
        deps = t.fk_dependencies
        dep_str = f" (depends on: {', '.join(deps)})" if deps else ""
        click.echo(f"  {i}. {table} — {len(t.columns)} columns{dep_str}")

    click.echo()
    click.echo("Scale Presets:")
    for scale_name, scale_def in schema.generation.scales.items():
        total = sum(scale_def.values())
        click.echo(f"  {scale_name}: {total:,} anchor rows")

    # Show distribution keys from active profile
    profile = domain._profile
    dist_keys = sorted(profile.get("distributions", {}).keys())
    ratio_keys = sorted(profile.get("ratios", {}).keys())
    if dist_keys:
        click.echo()
        click.echo(f"Distribution keys ({len(dist_keys)}):")
        for key in dist_keys:
            click.echo(f"  {key}")
    if ratio_keys:
        click.echo()
        click.echo(f"Ratio keys ({len(ratio_keys)}):")
        for key in ratio_keys:
            val = profile["ratios"][key]
            click.echo(f"  {key}: {val}")


@main.command(name="list")
def list_cmd():
    """List available domains and their profiles.

    Example: spindle list
    """
    click.echo(f"Spindle v{__version__} — Available Domains")
    click.echo()

    for name, (_, __, desc) in _get_domain_registry().items():
        try:
            domain = _resolve_domain(name, "3nf")
            profiles = domain.available_profiles
            click.echo(f"  {name:<15} {desc}")
            click.echo(f"  {'':15} Profiles: {', '.join(profiles)}")
            click.echo()
        except Exception:
            click.echo(f"  {name:<15} {desc} [failed to load]")
            click.echo()


@main.command()
@click.argument("schema_path")
def validate(schema_path: str):
    """Validate a .spindle.json schema file.

    Example: spindle validate my_schema.spindle.json
    """
    from sqllocks_spindle.schema.parser import SchemaParser
    from sqllocks_spindle.schema.validator import SchemaValidator

    parser = SchemaParser()
    validator = SchemaValidator()

    try:
        schema = parser.parse_file(schema_path)
    except Exception as e:
        click.echo(f"Parse error: {e}", err=True)
        sys.exit(1)

    errors = validator.validate(schema)
    if not errors:
        click.echo("Schema is valid!")
        click.echo(f"Tables: {len(schema.tables)}")
        click.echo(f"Relationships: {len(schema.relationships)}")
    else:
        real_errors = [e for e in errors if e.level == "error"]
        warnings = [e for e in errors if e.level == "warning"]
        if warnings:
            click.echo("Warnings:")
            for w in warnings:
                click.echo(f"  [{w.location}] {w.message}")
        if real_errors:
            click.echo("Errors:")
            for e in real_errors:
                click.echo(f"  [{e.location}] {e.message}")
            sys.exit(1)


@main.command()
@click.argument("domain_name")
@click.option("--table", "-t", required=True, help="Table to stream events from")
@click.option("--scale", "-s", default="small", help="Scale preset: small, medium, large, xlarge")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--rate", default=10.0, type=float, help="Target events per second (realtime mode only)")
@click.option("--max-events", default=None, type=int, help="Stop after N events")
@click.option("--duration", default=None, type=float, help="Stop after N seconds (realtime mode only)")
@click.option("--out-of-order", default=0.0, type=float, help="Fraction of events to reorder (0.0–1.0)")
@click.option("--sink", "sink_type", default="console", type=click.Choice(["console", "file"]), help="Sink type")
@click.option("--output", "-o", default=None, help="Output file path (sink=file only)")
@click.option("--mode", "-m", default="3nf", help="Schema mode: 3nf, star")
@click.option("--realtime/--no-realtime", default=False, help="Rate-limit output to --rate events/second")
@click.option("--burst", "burst_spec", default=None, multiple=True, help="Burst spec: START:DURATION:MULTIPLIER e.g. 30:60:10")
@click.option("--anomaly-fraction", default=0.0, type=float, help="Fraction of rows to inject as point anomalies")
def stream(
    domain_name: str,
    table: str,
    scale: str,
    seed: int,
    rate: float,
    max_events: int | None,
    duration: float | None,
    out_of_order: float,
    sink_type: str,
    output: str | None,
    mode: str,
    realtime: bool,
    burst_spec: tuple,
    anomaly_fraction: float,
):
    """Stream synthetic events row-by-row from a domain table.

    Example: spindle stream retail --table order --max-events 1000 --sink file --output events.jsonl
    """
    from sqllocks_spindle.streaming import (
        AnomalyRegistry,
        BurstWindow,
        ConsoleSink,
        FileSink,
        PointAnomaly,
        SpindleStreamer,
        StreamConfig,
    )

    domain = _resolve_domain(domain_name, mode)

    # Parse burst specs  "start:duration:multiplier"
    burst_windows = []
    for spec in burst_spec:
        parts = spec.split(":")
        if len(parts) != 3:
            click.echo(f"Invalid burst spec '{spec}' — expected START:DURATION:MULTIPLIER", err=True)
            sys.exit(1)
        try:
            bw = BurstWindow(
                start_offset_seconds=float(parts[0]),
                duration_seconds=float(parts[1]),
                multiplier=float(parts[2]),
            )
            burst_windows.append(bw)
        except ValueError:
            click.echo(f"Invalid burst spec '{spec}' — values must be numbers", err=True)
            sys.exit(1)

    config = StreamConfig(
        events_per_second=rate,
        max_events=max_events,
        duration_seconds=duration,
        out_of_order_fraction=out_of_order,
        burst_windows=burst_windows,
        realtime=realtime,
    )

    # Anomaly registry
    anomaly_registry = None
    if anomaly_fraction > 0:
        anomaly_registry = AnomalyRegistry()
        # Auto-detect numeric columns for point anomaly injection
        # (column is resolved at inject time — use a placeholder detected at runtime)
        anomaly_registry.add(PointAnomaly("auto", column="_auto_", fraction=anomaly_fraction))

    # Build sink
    if sink_type == "file":
        if not output:
            click.echo("--output/-o is required when --sink=file", err=True)
            sys.exit(1)
        sink = FileSink(output, mode="w")
    else:
        sink = ConsoleSink()

    label = f"  rate={rate} eps" if realtime else "  (fast mode — no rate limiting)"
    click.echo(f"Spindle v{__version__} — Streaming {domain_name}.{table}", err=True)
    click.echo(f"  scale={scale}, seed={seed}", err=True)
    click.echo(label, err=True)
    if max_events:
        click.echo(f"  max-events={max_events:,}", err=True)
    if duration:
        click.echo(f"  duration={duration}s", err=True)
    if out_of_order > 0:
        click.echo(f"  out-of-order={out_of_order:.0%}", err=True)
    click.echo("", err=True)

    # Use a simple registry with no anomalies if fraction=0
    streamer = SpindleStreamer(
        domain=domain,
        sink=sink,
        config=config,
        anomaly_registry=AnomalyRegistry() if anomaly_fraction == 0 else None,
        scale=scale,
        seed=seed,
    )

    result = streamer.stream(table)

    if sink_type == "file":
        sink.close()

    click.echo("", err=True)
    click.echo(f"  Streamed:    {result.events_sent:,} events", err=True)
    click.echo(f"  Anomalies:   {result.anomaly_count}", err=True)
    click.echo(f"  Out-of-order:{result.out_of_order_count}", err=True)
    click.echo(f"  Elapsed:     {result.elapsed_seconds:.2f}s", err=True)
    click.echo(f"  Throughput:  {result.events_per_second_actual:,.0f} eps", err=True)
    if output:
        click.echo(f"  Output:      {output}", err=True)


@main.command(name="to-star")
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--output", "-o", required=True, help="Output directory for star schema files")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv", "parquet"]), help="Output format")
def to_star(domain_name: str, scale: str, seed: int, output: str, fmt: str):
    """Generate data and export as a star schema (dim_* + fact_* tables).

    Example: spindle to-star retail --scale small --output ./star/
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.transform import StarSchemaTransform
    from sqllocks_spindle.output import PandasWriter

    domain = _resolve_domain(domain_name, "3nf")

    if not hasattr(domain, "star_schema_map"):
        click.echo(
            f"Domain '{domain_name}' does not implement star_schema_map(). "
            "Star schema export is supported for: retail, healthcare",
            err=True,
        )
        sys.exit(1)

    click.echo(f"Spindle v{__version__} — Generating {domain_name} at scale '{scale}'")
    click.echo()
    spindle = Spindle()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)

    click.echo()
    click.echo("Transforming to star schema...")
    schema_map = domain.star_schema_map()
    transform = StarSchemaTransform()
    star = transform.transform(result.tables, schema_map)

    click.echo()
    click.echo(star.summary())

    all_tables = star.all_tables()
    writer = PandasWriter()
    if fmt == "parquet":
        files = writer.to_parquet(all_tables, output)
    else:
        files = writer.to_csv(all_tables, output)

    click.echo()
    click.echo(f"Written {len(files)} {fmt.upper()} files to {output}/")


@main.command(name="to-cdm")
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--output", "-o", required=True, help="Output directory for CDM folder")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv", "parquet"]), help="Data file format")
@click.option("--model-name", default=None, help="CDM model name (defaults to Spindle<DomainName>)")
def to_cdm(domain_name: str, scale: str, seed: int, output: str, fmt: str, model_name: str | None):
    """Generate data and export as a Microsoft CDM folder (model.json + data files).

    Produces a CDM folder compatible with Fabric CDM connectors, Dataverse,
    Power Platform, and Azure Data Lake Storage.

    Example: spindle to-cdm retail --scale small --output ./cdm/
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.transform import CdmMapper

    domain = _resolve_domain(domain_name, "3nf")

    cdm_name = model_name or f"Spindle{domain_name.replace('_', ' ').title().replace(' ', '')}"
    entity_map = None
    if hasattr(domain, "cdm_map"):
        entity_map = domain.cdm_map()

    click.echo(f"Spindle v{__version__} — Generating {domain_name} at scale '{scale}'")
    click.echo()
    spindle = Spindle()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)

    click.echo()
    click.echo(f"Exporting CDM folder '{cdm_name}' ({fmt.upper()})...")
    mapper = CdmMapper()
    files = mapper.write_cdm_folder(
        tables=result.tables,
        output_dir=output,
        domain_name=cdm_name,
        entity_map=entity_map,
        fmt=fmt,
    )

    n_entities = len(files) - 1  # subtract model.json
    click.echo()
    click.echo(f"CDM folder written to {output}/")
    click.echo(f"  {n_entities} entities + model.json ({len(files)} files total)")


@main.command()
@click.argument("input_path")
@click.option("--output", "-o", default=None, help="Output .spindle.json file")
@click.option("--format", "input_fmt", default="csv", type=click.Choice(["csv", "parquet", "jsonl"]))
@click.option("--domain", default="inferred", help="Domain name for generated schema")
def learn(input_path: str, output: str | None, input_fmt: str, domain: str):
    """Infer a .spindle.json schema from existing data files.

    Reads CSV/Parquet/JSONL files from INPUT_PATH (file or directory),
    profiles column types, distributions, and relationships,
    then generates a ready-to-use Spindle schema.

    Example: spindle learn ./data/ --output my_schema.spindle.json
    """
    import json
    from pathlib import Path

    from sqllocks_spindle.inference import DataProfiler, SchemaBuilder

    input_p = Path(input_path)

    # Collect files to read
    if input_p.is_dir():
        ext_map = {"csv": "*.csv", "parquet": "*.parquet", "jsonl": "*.jsonl"}
        pattern = ext_map.get(input_fmt, "*.csv")
        files = sorted(input_p.glob(pattern))
        if not files:
            click.echo(f"No {input_fmt} files found in {input_path}", err=True)
            sys.exit(1)
    elif input_p.is_file():
        files = [input_p]
    else:
        click.echo(f"Path not found: {input_path}", err=True)
        sys.exit(1)

    # Read data into DataFrames
    import pandas as pd

    tables: dict[str, pd.DataFrame] = {}
    for fp in files:
        table_name = fp.stem
        if input_fmt == "csv":
            df = pd.read_csv(fp)
        elif input_fmt == "parquet":
            df = pd.read_parquet(fp)
        elif input_fmt == "jsonl":
            df = pd.read_json(fp, lines=True)
        else:
            click.echo(f"Unsupported format: {input_fmt}", err=True)
            sys.exit(1)
        tables[table_name] = df
        click.echo(f"  Read {table_name}: {len(df):,} rows x {len(df.columns)} columns")

    click.echo()

    # Profile
    profiler = DataProfiler()
    if len(tables) == 1:
        tname, df = next(iter(tables.items()))
        profile_result = profiler.profile_dataset({tname: df})
    else:
        profile_result = profiler.profile_dataset(tables)

    # Build schema
    builder = SchemaBuilder()
    schema = builder.build(profile_result, domain_name=domain)

    # Serialize
    schema_dict = _schema_to_dict(schema)

    # Determine output path
    if not output:
        if input_p.is_dir():
            output = str(input_p / f"{domain}.spindle.json")
        else:
            output = str(input_p.with_suffix(".spindle.json"))

    with open(output, "w", encoding="utf-8") as f:
        json.dump(schema_dict, f, indent=2)

    # Summary
    click.echo(f"Spindle v{__version__} — Schema Inference")
    click.echo()
    click.echo(f"  Domain:        {domain}")
    click.echo(f"  Tables:        {len(schema.tables)}")
    click.echo(f"  Relationships: {len(schema.relationships)}")
    click.echo()
    for tname, tdef in schema.tables.items():
        pk_str = f" (PK: {', '.join(tdef.primary_key)})" if tdef.primary_key else ""
        n_fk = sum(1 for c in tdef.columns.values() if c.is_foreign_key)
        fk_str = f", {n_fk} FKs" if n_fk else ""
        click.echo(f"  {tname}: {len(tdef.columns)} columns{pk_str}{fk_str}")
    click.echo()
    click.echo(f"Schema written to {output}")


@main.command(name="export-model")
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset (for row count context)")
@click.option("--source-type", default="lakehouse", type=click.Choice(["lakehouse", "warehouse", "sql_database"]), help="Data source type for M expressions")
@click.option("--source-name", default="", help="Lakehouse/Warehouse/Server name for M expressions")
@click.option("--output", "-o", default="model.bim", help="Output .bim file path")
@click.option("--include-measures/--no-measures", default=True, help="Generate DAX measures")
@click.option("--schema-name", default="dbo", help="SQL schema for warehouse/sql_database sources")
def export_model(
    domain_name: str, scale: str, source_type: str, source_name: str,
    output: str, include_measures: bool, schema_name: str,
):
    """Export a domain schema as a Power BI / Fabric semantic model (.bim).

    Generates TOM JSON at compatibilityLevel 1604 with typed columns,
    relationships, M expressions, and auto-generated DAX measures.

    Example: spindle export-model retail --source-type lakehouse --output retail.bim
    """
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter

    domain = _resolve_domain(domain_name, "3nf")
    spindle = Spindle()
    schema = spindle.describe(domain=domain)
    schema.generation.scale = scale

    exporter = SemanticModelExporter()
    output_path = exporter.export_bim(
        schema=schema,
        source_type=source_type,
        source_name=source_name,
        output_path=output,
        include_measures=include_measures,
        schema_name=schema_name,
    )

    # Count tables and measures
    tom = exporter.to_dict(schema=schema, source_type=source_type, source_name=source_name, include_measures=include_measures, schema_name=schema_name)
    n_tables = len(tom["model"]["tables"])
    n_rels = len(tom["model"]["relationships"])
    n_measures = sum(len(t.get("measures", [])) for t in tom["model"]["tables"])

    click.echo(f"Spindle v{__version__} — Semantic Model Export")
    click.echo()
    click.echo(f"  Domain:        {domain_name}")
    click.echo(f"  Source type:   {source_type}")
    click.echo(f"  Tables:        {n_tables}")
    click.echo(f"  Relationships: {n_rels}")
    click.echo(f"  DAX measures:  {n_measures}")
    click.echo(f"  Output:        {output_path}")
    click.echo()
    click.echo("Import this .bim file into Tabular Editor or deploy via XMLA endpoint.")


@main.command(name="from-ddl")
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output", "-o", default=None, help="Output path for .spindle.json file")
@click.option("--domain", default="custom", help="Domain name for the generated schema")
@click.option("--scale", "-s", default=None, help="Scale override: small:table1=N,table2=N")
@click.option("--smart/--no-smart", default=True, help="Enable smart inference (realistic distributions, FK patterns, business rules)")
@click.option("--explain", is_flag=True, help="Print inference explanation report")
def from_ddl(input_file: str, output: str | None, domain: str, scale: str | None,
             smart: bool, explain: bool):
    """Import SQL DDL (CREATE TABLE) into a .spindle.json schema.

    Parses SQL Server, PostgreSQL, MySQL, and ANSI SQL dialects.
    With --smart (default), infers realistic distributions, FK patterns,
    temporal seasonality, and business rules from schema structure.

    Example: spindle from-ddl adventureworks.sql --output aw.spindle.json
    """
    import json

    from sqllocks_spindle.schema.ddl_parser import DdlParser

    parser = DdlParser()

    try:
        schema = parser.parse_file(input_file)
    except Exception as e:
        click.echo(f"DDL parse error: {e}", err=True)
        sys.exit(1)

    # Override domain name
    schema.model.domain = domain
    schema.model.name = f"{domain}_ddl_import"

    # Smart inference: upgrade strategies based on schema structure
    annotations = []
    if smart:
        from sqllocks_spindle.schema.inference import SchemaInferenceEngine
        engine = SchemaInferenceEngine()
        schema, annotations = engine.infer_with_report(schema)

    # Apply scale overrides if provided
    if scale:
        _apply_scale_overrides(schema, scale)

    # Convert to JSON-serializable dict
    schema_dict = _schema_to_dict(schema)

    # Determine output path
    if not output:
        from pathlib import Path
        input_path = Path(input_file)
        output = str(input_path.with_suffix(".spindle.json"))

    with open(output, "w", encoding="utf-8") as f:
        json.dump(schema_dict, f, indent=2)

    click.echo(f"Spindle v{__version__} — DDL Import{' (Smart)' if smart else ''}")
    click.echo()
    click.echo(f"  Source: {input_file}")
    click.echo(f"  Output: {output}")
    click.echo(f"  Tables: {len(schema.tables)}")
    click.echo(f"  Relationships: {len(schema.relationships)}")
    click.echo(f"  Business rules: {len(schema.business_rules)}")
    if smart:
        click.echo(f"  Inferences: {len(annotations)}")
    click.echo()
    for tname, tdef in schema.tables.items():
        pk_str = f" (PK: {', '.join(tdef.primary_key)})" if tdef.primary_key else ""
        click.echo(f"  {tname}: {len(tdef.columns)} columns{pk_str}")
    click.echo()
    click.echo(f"Schema written to {output}")
    click.echo(f"Run: spindle generate custom --schema {output} --scale small")

    # Print explain report if requested
    if explain and annotations:
        click.echo()
        click.echo("--- Inference Report ---")
        click.echo()
        for ann in annotations:
            col_str = f".{ann.column}" if ann.column else ""
            click.echo(f"  [{ann.rule_id}] {ann.table}{col_str}: {ann.description} (confidence: {ann.confidence:.0%})")


@main.command(name="continue")
@click.argument("domain_name")
@click.option("--input", "input_dir", required=True, help="Directory with existing CSV/Parquet files")
@click.option("--output", "-o", required=True, help="Output directory for delta files")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv", "parquet", "jsonl"]))
@click.option("--inserts", default=100, type=int, help="Number of new rows to insert per anchor table")
@click.option("--update-fraction", default=0.1, type=float, help="Fraction of existing rows to update")
@click.option("--delete-fraction", default=0.02, type=float, help="Fraction of existing rows to soft-delete")
@click.option("--seed", default=None, type=int, help="Random seed")
def continue_cmd(domain_name, input_dir, output, fmt, inserts, update_fraction, delete_fraction, seed):
    """Generate incremental changes (inserts, updates, deletes) from existing data.

    Reads existing data files, then generates new rows, status updates,
    and soft deletes tagged with _delta_type and _delta_timestamp.

    Example: spindle continue retail --input ./data/ --output ./deltas/ --inserts 50
    """
    from pathlib import Path

    import pandas as pd

    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.incremental import ContinueConfig, ContinueEngine

    # 1. Read existing data from input_dir
    input_path = Path(input_dir)
    if not input_path.is_dir():
        click.echo(f"Input directory not found: {input_dir}", err=True)
        sys.exit(1)

    tables: dict[str, pd.DataFrame] = {}
    for ext, reader in [("*.csv", pd.read_csv), ("*.parquet", pd.read_parquet)]:
        for fp in sorted(input_path.glob(ext)):
            tables[fp.stem] = reader(fp)

    if not tables:
        click.echo(f"No CSV or Parquet files found in {input_dir}", err=True)
        sys.exit(1)

    click.echo(f"Spindle v{__version__} — Incremental Generation")
    click.echo(f"  Source: {input_dir} ({len(tables)} tables)")

    # 2. Resolve domain schema
    domain = _resolve_domain(domain_name, "3nf")
    spindle = Spindle()
    schema = spindle.describe(domain=domain)

    # 3. Build config
    cfg = ContinueConfig(
        insert_count=inserts,
        update_fraction=update_fraction,
        delete_fraction=delete_fraction,
        seed=seed,
    )

    # 4. Run engine
    engine = ContinueEngine()
    delta = engine.continue_from(tables, schema=schema, config=cfg)

    # 5. Write combined delta files to output_dir
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    files_written = []
    for table_name, df in delta.combined.items():
        if len(df) == 0:
            continue
        if fmt == "csv":
            fp = output_path / f"{table_name}.csv"
            df.to_csv(fp, index=False)
        elif fmt == "parquet":
            fp = output_path / f"{table_name}.parquet"
            df.to_parquet(fp, index=False)
        elif fmt == "jsonl":
            fp = output_path / f"{table_name}.jsonl"
            df.to_json(fp, orient="records", lines=True)
        else:
            continue
        files_written.append(fp)

    click.echo()
    click.echo(delta.summary())
    click.echo()
    click.echo(f"Written {len(files_written)} delta files to {output}/")


@main.command(name="time-travel")
@click.argument("domain_name")
@click.option("--months", default=12, type=int, help="Number of monthly snapshots")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--output", "-o", required=True, help="Output directory for snapshot files")
@click.option("--format", "fmt", default="parquet", type=click.Choice(["csv", "parquet"]))
@click.option("--growth-rate", default=0.05, type=float, help="Monthly growth rate")
@click.option("--churn-rate", default=0.02, type=float, help="Monthly churn rate")
@click.option("--seed", default=42, type=int)
def time_travel(domain_name, months, scale, output, fmt, growth_rate, churn_rate, seed):
    """Generate monthly point-in-time snapshots showing data evolution.

    Produces N+1 snapshots (month 0 = initial, then N months of evolution)
    with configurable growth, churn, and update rates.

    Example: spindle time-travel retail --months 6 --output ./snapshots/
    """
    from pathlib import Path

    from sqllocks_spindle.incremental.time_travel import TimeTravelConfig, TimeTravelEngine

    domain = _resolve_domain(domain_name, "3nf")

    config = TimeTravelConfig(
        months=months,
        growth_rate=growth_rate,
        churn_rate=churn_rate,
        seed=seed,
    )

    click.echo(f"Spindle v{__version__} — Time-Travel Snapshots")
    click.echo(f"  Domain: {domain_name}, Scale: {scale}, Months: {months}")
    click.echo(f"  Growth: {growth_rate:.0%}/mo, Churn: {churn_rate:.0%}/mo")
    click.echo()

    engine = TimeTravelEngine()
    result = engine.generate(domain=domain, config=config, scale=scale)

    click.echo(result.summary())

    # Write snapshot files to output/month_N/table.fmt
    output_path = Path(output)
    files_written = 0
    for snap in result.snapshots:
        month_dir = output_path / f"month_{snap.month_index}"
        month_dir.mkdir(parents=True, exist_ok=True)
        for table_name, df in snap.tables.items():
            if fmt == "csv":
                fp = month_dir / f"{table_name}.csv"
                df.to_csv(fp, index=False)
            else:
                fp = month_dir / f"{table_name}.parquet"
                df.to_parquet(fp, index=False)
            files_written += 1

    click.echo()
    click.echo(f"Written {files_written} files to {output}/")


@main.command()
@click.argument("real_path")
@click.argument("synth_path")
@click.option(
    "--format",
    "input_fmt",
    default="csv",
    type=click.Choice(["csv", "parquet"]),
    help="Input file format",
)
@click.option("--output", "-o", default=None, help="Output file for report (markdown)")
def compare(real_path: str, synth_path: str, input_fmt: str, output: str | None):
    """Compare real vs synthetic data and generate a fidelity report.

    Compares column distributions, null rates, cardinality, and statistical
    tests to produce a 0-100 fidelity score.

    REAL_PATH and SYNTH_PATH should be directories containing data files
    (one file per table) in the specified format.

    Example: spindle compare ./real_data/ ./synth_data/ --output report.md
    """
    from pathlib import Path

    import pandas as pd

    from sqllocks_spindle.inference.comparator import FidelityComparator

    def _load_tables(dir_path: str, fmt: str) -> dict[str, pd.DataFrame]:
        p = Path(dir_path)
        ext = "*.csv" if fmt == "csv" else "*.parquet"
        tables: dict[str, pd.DataFrame] = {}

        if p.is_file():
            # Single file
            if fmt == "csv":
                tables[p.stem] = pd.read_csv(p)
            else:
                tables[p.stem] = pd.read_parquet(p)
        elif p.is_dir():
            files = sorted(p.glob(ext))
            if not files:
                click.echo(f"No {fmt} files found in {dir_path}", err=True)
                sys.exit(1)
            for fp in files:
                if fmt == "csv":
                    tables[fp.stem] = pd.read_csv(fp)
                else:
                    tables[fp.stem] = pd.read_parquet(fp)
        else:
            click.echo(f"Path not found: {dir_path}", err=True)
            sys.exit(1)
        return tables

    click.echo(f"Spindle v{__version__} — Fidelity Comparison")
    click.echo()

    real_tables = _load_tables(real_path, input_fmt)
    synth_tables = _load_tables(synth_path, input_fmt)

    click.echo(f"  Real tables:  {', '.join(real_tables.keys())}")
    click.echo(f"  Synth tables: {', '.join(synth_tables.keys())}")
    click.echo()

    comparator = FidelityComparator()
    report = comparator.compare(real_tables, synth_tables)

    click.echo(report.summary())

    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(report.to_markdown())
        click.echo()
        click.echo(f"Markdown report written to {output}")


@main.command()
@click.argument("preset_or_domains")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int)
@click.option("--output", "-o", default=None, help="Output directory")
@click.option("--format", "fmt", default="summary", type=click.Choice(["summary", "csv", "parquet", "jsonl"]))
def composite(preset_or_domains, scale, seed, output, fmt):
    """Generate data from a composite preset or ad-hoc domain combination.

    Use a preset name or combine domains with '+':

    \b
    Examples:
      spindle composite enterprise --scale small
      spindle composite retail+hr+financial --scale small --output ./data/
    """
    from sqllocks_spindle.domains.composite import CompositeDomain
    from sqllocks_spindle.engine.generator import Spindle

    # Check if it's a preset name
    try:
        from sqllocks_spindle.presets import get_preset
        preset = get_preset(preset_or_domains)
        domain_names = preset.domains
        shared_entities = preset.shared_entities
        click.echo(f"Using preset: {preset.name} — {preset.description}")
    except (KeyError, ImportError):
        # Ad-hoc: parse "retail+hr+financial"
        domain_names = [d.strip() for d in preset_or_domains.split("+")]
        shared_entities = None
        click.echo(f"Ad-hoc composite: {' + '.join(domain_names)}")

    domains = [_resolve_domain(d, "3nf") for d in domain_names]
    comp = CompositeDomain(
        domains=domains,
        shared_entities=shared_entities if shared_entities else None,
    )

    spindle = Spindle()
    result = spindle.generate(domain=comp, scale=scale, seed=seed)

    click.echo()
    click.echo(result.summary())

    if fmt != "summary" and output:
        from sqllocks_spindle.output import PandasWriter
        writer = PandasWriter()
        if fmt == "csv":
            files = writer.to_csv(result.tables, output)
        elif fmt == "parquet":
            files = writer.to_parquet(result.tables, output)
        elif fmt == "jsonl":
            files = writer.to_jsonl(result.tables, output)
        else:
            files = []
        click.echo(f"\nWritten {len(files)} files to {output}/")
    elif fmt != "summary" and not output:
        click.echo()
        click.echo("Hint: use --output/-o to write files to disk")


@main.command(name="presets")
def list_presets_cmd():
    """List available composite presets."""
    from sqllocks_spindle.presets import list_presets
    presets = list_presets()
    click.echo(f"Available Presets ({len(presets)}):")
    click.echo()
    for p in presets:
        click.echo(f"  {p.name:<20} {p.description}")
        click.echo(f"  {'':20} Domains: {', '.join(p.domains)}")
        click.echo()


@main.command()
@click.argument("input_path")
@click.option("--output", "-o", required=True, help="Output directory for masked files")
@click.option(
    "--format",
    "input_fmt",
    default="csv",
    type=click.Choice(["csv", "parquet"]),
    help="Input file format",
)
@click.option("--seed", default=42, type=int, help="Random seed for reproducibility")
@click.option(
    "--exclude",
    multiple=True,
    help="Column names to exclude from masking",
)
def mask(input_path: str, output: str, input_fmt: str, seed: int, exclude: tuple):
    """Replace PII in data files with synthetic values.

    Detects PII columns (email, phone, name, SSN, etc.) via column name
    heuristics and replaces values with realistic synthetic data while
    preserving null patterns and distributions.

    Example: spindle mask ./real_data/ --output ./masked/
    """
    from pathlib import Path

    import pandas as pd

    from sqllocks_spindle.inference.masker import DataMasker, MaskConfig

    input_p = Path(input_path)

    # Collect files to read
    if input_p.is_dir():
        ext = "*.csv" if input_fmt == "csv" else "*.parquet"
        files = sorted(input_p.glob(ext))
        if not files:
            click.echo(f"No {input_fmt} files found in {input_path}", err=True)
            sys.exit(1)
    elif input_p.is_file():
        files = [input_p]
    else:
        click.echo(f"Path not found: {input_path}", err=True)
        sys.exit(1)

    # Read data into DataFrames
    tables: dict[str, pd.DataFrame] = {}
    for fp in files:
        table_name = fp.stem
        if input_fmt == "csv":
            df = pd.read_csv(fp)
        else:
            df = pd.read_parquet(fp)
        tables[table_name] = df
        click.echo(f"  Read {table_name}: {len(df):,} rows x {len(df.columns)} columns")

    click.echo()

    # Mask
    config = MaskConfig(
        seed=seed,
        exclude_columns=list(exclude),
    )
    masker = DataMasker()
    result = masker.mask(tables, config=config)

    # Write output
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    from sqllocks_spindle.output import PandasWriter

    writer = PandasWriter()
    if input_fmt == "csv":
        out_files = writer.to_csv(result.tables, output)
    else:
        out_files = writer.to_parquet(result.tables, output)

    click.echo(f"Spindle v{__version__} — PII Masking")
    click.echo()
    click.echo(result.summary())
    click.echo()
    click.echo(f"Written {len(out_files)} {input_fmt.upper()} files to {output}/")


@main.group()
def profile():
    """Manage domain profiles — export, import, and list."""
    pass


@profile.command(name="export")
@click.argument("domain_name")
@click.option("--output", "-o", required=True, help="Output JSON file path")
@click.option("--profile-name", default="default", help="Profile to export")
def profile_export(domain_name, output, profile_name):
    """Export a domain profile to a portable JSON file."""
    from sqllocks_spindle.inference.profile_io import ProfileIO

    domain = _resolve_domain(domain_name, "3nf")
    if profile_name != "default":
        domain._profile = domain._load_profile(profile_name)
    io = ProfileIO()
    path = io.export_profile(domain, output, profile_name)
    click.echo(f"Profile '{profile_name}' exported to {path}")


@profile.command(name="import")
@click.argument("profile_path")
@click.argument("domain_name")
@click.option("--save-as", default=None, help="Name to save the profile as")
def profile_import(profile_path, domain_name, save_as):
    """Import a profile into a domain's profiles/ directory."""
    from sqllocks_spindle.inference.profile_io import ProfileIO

    domain = _resolve_domain(domain_name, "3nf")
    io = ProfileIO()
    name = io.import_profile(profile_path, domain, save_as=save_as)
    click.echo(f"Profile imported as '{name}' into {domain.name}")


@profile.command(name="list")
@click.argument("domain_name")
def profile_list(domain_name):
    """List available profiles for a domain."""
    from sqllocks_spindle.inference.profile_io import ProfileIO

    domain = _resolve_domain(domain_name, "3nf")
    io = ProfileIO()
    profiles = io.list_profiles(domain)
    click.echo(f"Profiles for {domain_name}:")
    for p in profiles:
        click.echo(
            f"  {p['name']:<20} {p['description']:<40} "
            f"({p['distributions']} dists, {p['ratios']} ratios)"
        )


def _apply_scale_overrides(schema, scale_spec: str):
    """Parse scale spec like 'small:customer=5000,order=25000' and apply."""
    if ":" in scale_spec:
        scale_name, overrides = scale_spec.split(":", 1)
    else:
        scale_name = scale_spec
        overrides = ""

    schema.generation.scale = scale_name

    if overrides:
        scale_dict = schema.generation.scales.get(scale_name, {})
        for pair in overrides.split(","):
            if "=" in pair:
                table, count = pair.split("=", 1)
                scale_dict[table.strip()] = int(count.strip())
        schema.generation.scales[scale_name] = scale_dict


def _schema_to_dict(schema) -> dict:
    """Convert a SpindleSchema to a JSON-serializable dict."""
    tables = {}
    for tname, tdef in schema.tables.items():
        columns = {}
        for cname, cdef in tdef.columns.items():
            col = {"type": cdef.type, "generator": cdef.generator}
            if cdef.nullable:
                col["nullable"] = True
            if cdef.null_rate > 0:
                col["null_rate"] = cdef.null_rate
            if cdef.max_length is not None:
                col["max_length"] = cdef.max_length
            if cdef.precision is not None:
                col["precision"] = cdef.precision
            if cdef.scale is not None:
                col["scale"] = cdef.scale
            columns[cname] = col
        tables[tname] = {
            "columns": columns,
            "primary_key": tdef.primary_key,
        }
        if tdef.description:
            tables[tname]["description"] = tdef.description

    relationships = []
    for r in schema.relationships:
        rel = {
            "name": r.name,
            "parent": r.parent,
            "child": r.child,
            "parent_columns": r.parent_columns,
            "child_columns": r.child_columns,
            "type": r.type,
        }
        relationships.append(rel)

    return {
        "model": {
            "name": schema.model.name,
            "description": schema.model.description,
            "domain": schema.model.domain,
            "schema_mode": schema.model.schema_mode,
            "locale": schema.model.locale,
            "seed": schema.model.seed,
            "date_range": schema.model.date_range,
        },
        "tables": tables,
        "relationships": relationships,
        "business_rules": [],
        "generation": {
            "scale": schema.generation.scale,
            "scales": schema.generation.scales,
        },
    }


@main.command()
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--mode", "-m", default="3nf", help="Schema mode: 3nf, star")
@click.option("--target", "-t", required=True,
              type=click.Choice(["lakehouse", "eventhouse", "sql-database"]),
              help="Fabric target to publish to")
@click.option("--workspace-id", default=None, envvar="SPINDLE_WORKSPACE_ID",
              help="Fabric workspace ID")
@click.option("--lakehouse-id", default=None, envvar="SPINDLE_LAKEHOUSE_ID",
              help="Fabric lakehouse ID")
@click.option("--base-path", default=None, envvar="SPINDLE_LAKEHOUSE_PATH",
              help="Lakehouse Files base path or abfss:// URI")
@click.option("--connection-string", default=None, envvar="SPINDLE_SQL_CONNECTION",
              help="SQL or Eventhouse connection string")
@click.option("--database", default=None, help="KQL database name (eventhouse target)")
@click.option("--auth", "auth_method", default="cli",
              type=click.Choice(["cli", "msi", "spn", "sql", "device-code"]),
              help="Authentication method")
@click.option("--format", "fmt", default="parquet",
              type=click.Choice(["parquet", "csv", "jsonl", "delta"]),
              help="File format for lakehouse target")
@click.option("--credential", "credential_ref", default=None,
              help="Credential reference (env://, kv://, file://) for connection string")
@click.option("--dry-run", is_flag=True, default=False,
              help="Show what would be published without publishing")
def publish(
    domain_name: str, scale: str, seed: int, mode: str,
    target: str, workspace_id: str | None, lakehouse_id: str | None,
    base_path: str | None, connection_string: str | None,
    database: str | None, auth_method: str, fmt: str,
    credential_ref: str | None, dry_run: bool,
):
    """Generate and publish data to a Fabric workspace.

    Generates synthetic data and pushes it directly to a Fabric Lakehouse,
    Eventhouse, or SQL Database endpoint.

    Examples:

        spindle publish retail --target lakehouse --base-path abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse

        spindle publish retail --target sql-database --connection-string "env://SPINDLE_SQL_CONNECTION"

        spindle publish retail --target eventhouse --connection-string "https://eh.kusto.fabric.microsoft.com" --database mydb
    """
    from sqllocks_spindle.engine.generator import Spindle

    domain = _resolve_domain(domain_name, mode)

    # Resolve credentials if provided
    actual_connection = connection_string
    if credential_ref:
        from sqllocks_spindle.fabric.credentials import CredentialResolver
        resolver = CredentialResolver()
        actual_connection = resolver.resolve(credential_ref)
    elif connection_string and connection_string.startswith(("env://", "kv://", "file://")):
        from sqllocks_spindle.fabric.credentials import CredentialResolver
        resolver = CredentialResolver()
        actual_connection = resolver.resolve(connection_string)

    click.echo(f"Spindle v{__version__} — {'[DRY RUN] ' if dry_run else ''}Publishing {domain_name} → {target}")
    click.echo(f"Scale: {scale} | Seed: {seed} | Format: {fmt}")
    if workspace_id:
        click.echo(f"Workspace: {workspace_id}")

    # Generate
    click.echo()
    click.echo("Generating data...")
    spindle = Spindle()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)
    click.echo(result.summary())

    if dry_run:
        click.echo()
        click.echo("Dry run complete. No data published.")
        return

    click.echo()

    # Publish based on target
    if target == "lakehouse":
        if not base_path:
            click.echo(
                "Error: --base-path or SPINDLE_LAKEHOUSE_PATH is required for lakehouse target",
                err=True,
            )
            sys.exit(1)

        is_remote = base_path.startswith("abfss://")

        if is_remote:
            # Remote OneLake: write via azure-storage-file-datalake
            try:
                from azure.identity import DefaultAzureCredential
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError:
                click.echo("Error: 'azure-storage-file-datalake' and 'azure-identity' are required for remote OneLake writes.", err=True)
                click.echo("Install with: pip install azure-storage-file-datalake azure-identity", err=True)
                sys.exit(1)

            import io

            credential = DefaultAzureCredential(
                exclude_shared_token_cache_credential=True,
            )

            # Parse abfss path: abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse
            service_client = DataLakeServiceClient(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                credential=credential,
            )
            # Container = workspace ID, path prefix = lakehouse GUID (strip .Lakehouse suffix)
            parts = base_path.replace("abfss://", "").split("@")
            container = parts[0]  # workspace ID
            raw_prefix = parts[1].split("/", 1)[1] if "/" in parts[1] else ""
            # OneLake DFS API needs just the GUID, not {guid}.Lakehouse
            lakehouse_prefix = raw_prefix.replace(".Lakehouse", "").replace(".lakehouse", "")
            fs_client = service_client.get_file_system_client(container)

            click.echo(f"Publishing to OneLake ({fmt})...")
            published_files = []
            for table_name, df in result.tables.items():
                dir_path = f"{lakehouse_prefix}/Files/landing/{domain_name}/{table_name}/latest"
                ext = "jsonl" if fmt == "jsonl" else fmt
                file_name = f"part-0001.{ext}"
                file_path = f"{dir_path}/{file_name}"

                # Serialize DataFrame to bytes
                buf = io.BytesIO()
                if fmt == "parquet":
                    df.to_parquet(buf, index=False, engine="pyarrow")
                elif fmt == "csv":
                    buf.write(df.to_csv(index=False).encode("utf-8"))
                elif fmt == "jsonl":
                    buf.write(df.to_json(orient="records", lines=True, date_format="iso").encode("utf-8"))
                data = buf.getvalue()

                # Create directory and upload
                try:
                    dir_client = fs_client.get_directory_client(dir_path)
                    dir_client.create_directory()
                except Exception:
                    pass  # Directory may already exist
                file_client = fs_client.get_file_client(file_path)
                file_client.upload_data(data, overwrite=True)

                published_files.append(file_path)
                click.echo(f"  {table_name}: {len(df):,} rows → onelake://.../{file_path}")

            click.echo()
            click.echo(f"Published {len(published_files)} tables to OneLake.")
        else:
            # Local: use LakehouseFilesWriter
            from sqllocks_spindle.fabric import LakehouseFilesWriter

            writer = LakehouseFilesWriter(base_path=base_path, default_format=fmt)

            click.echo(f"Publishing to Lakehouse ({fmt})...")
            published_files = []
            for table_name, df in result.tables.items():
                path = writer.paths.landing_zone_path(domain_name, table_name, "latest")
                file_path = writer.write_partition(df, path, format=fmt)
                published_files.append(str(file_path))
                click.echo(f"  {table_name}: {len(df):,} rows → {file_path}")

            click.echo()
            click.echo(f"Published {len(published_files)} tables to lakehouse.")

        # Write run manifest
        from sqllocks_spindle.manifests import ManifestBuilder
        builder = ManifestBuilder()
        builder.start(spec=None, pack=None, domain_name=domain_name, scale=scale, seed=seed)
        if workspace_id:
            builder.set_fabric_ids(workspace_id=workspace_id or "", lakehouse_id=lakehouse_id or "")
        for table_name, df in result.tables.items():
            builder.record_output(table_name, rows=len(df), columns=len(df.columns))
        manifest = builder.finish()

        if is_remote:
            manifest_dir = f"{lakehouse_prefix}/Files/_control/{domain_name}"
            manifest_file = f"{manifest_dir}/run_manifest.json"
            try:
                dir_client = fs_client.get_directory_client(manifest_dir)
                dir_client.create_directory()
            except Exception:
                pass
            manifest_json = ManifestBuilder.to_json(manifest).encode("utf-8")
            file_client = fs_client.get_file_client(manifest_file)
            file_client.upload_data(manifest_json, overwrite=True)
            click.echo(f"Manifest written to OneLake _control/")
        else:
            manifest_path = writer.paths.control_path(domain_name, "manifest") / "run_manifest.json"
            ManifestBuilder.to_file(manifest, manifest_path)
            click.echo(f"Manifest: {manifest_path}")

    elif target == "sql-database":
        if not actual_connection:
            click.echo(
                "Error: --connection-string or SPINDLE_SQL_CONNECTION is required",
                err=True,
            )
            sys.exit(1)

        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

        db_writer = FabricSqlDatabaseWriter(
            connection_string=actual_connection,
            auth_method=auth_method,
            staging_lakehouse_path=base_path,
        )

        click.echo(f"Publishing to SQL Database (auth={auth_method})...")
        write_result = db_writer.write(
            result,
            schema_name="dbo",
            mode="create_insert",
            on_table_complete=lambda t, r: click.echo(f"  {t}: {r:,} rows"),
        )
        click.echo(write_result.summary())
        if write_result.errors:
            sys.exit(1)

    elif target == "eventhouse":
        if not actual_connection or not database:
            click.echo(
                "Error: --connection-string and --database are required for eventhouse target",
                err=True,
            )
            sys.exit(1)

        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter

        eh_writer = EventhouseWriter(
            cluster_uri=actual_connection,
            database=database,
            auth_method=auth_method,
        )

        click.echo(f"Publishing to Eventhouse ({database})...")
        write_result = eh_writer.write(result)
        click.echo(write_result.summary())
        if write_result.errors:
            sys.exit(1)

    click.echo()
    click.echo("Publish complete.")


@main.command()
@click.argument("domain_name")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--output", "-o", default=None, help="Output .ipynb file path")
@click.option("--target", default="lakehouse",
              type=click.Choice(["lakehouse", "display", "csv"]),
              help="Notebook output target")
def notebook(domain_name: str, scale: str, seed: int, output: str | None, target: str):
    """Generate a ready-to-run Fabric notebook for a domain.

    Creates a .ipynb notebook that installs Spindle, generates data,
    and writes to a Lakehouse (or other target).

    Example: spindle notebook retail --scale medium -o ./notebooks/retail_demo.ipynb
    """
    from sqllocks_spindle.fabric.notebook_template import generate_notebook, save_notebook

    # Verify domain exists
    registry = _get_domain_registry()
    if domain_name not in registry:
        click.echo(f"Unknown domain: '{domain_name}'", err=True)
        click.echo(f"Available domains: {', '.join(registry.keys())}", err=True)
        sys.exit(1)

    nb = generate_notebook(domain=domain_name, scale=scale, seed=seed, output_target=target)

    if output:
        path = save_notebook(nb, output)
        click.echo(f"Spindle v{__version__} — Notebook Generated")
        click.echo(f"  Domain: {domain_name}")
        click.echo(f"  Scale:  {scale}")
        click.echo(f"  Target: {target}")
        click.echo(f"  Cells:  {len(nb['cells'])}")
        click.echo(f"  Output: {path}")
    else:
        import json
        click.echo(json.dumps(nb, indent=1))


@main.command(name="deploy-notebook")
@click.argument("domain_name")
@click.option("--workspace", required=True, help="Fabric workspace name or ID")
@click.option("--scale", "-s", default="small", help="Scale preset")
@click.option("--seed", default=42, type=int, help="Random seed")
@click.option("--auth", "auth_method", default="cli",
              type=click.Choice(["cli", "device-code"]),
              help="Authentication method")
@click.option("--notebook-name", default=None, help="Name for the notebook in Fabric")
def deploy_notebook(
    domain_name: str, workspace: str, scale: str, seed: int,
    auth_method: str, notebook_name: str | None,
):
    """Generate and deploy a notebook to a Fabric workspace.

    Creates a Spindle notebook and uploads it to the specified Fabric workspace
    using the Fabric REST API.

    Example: spindle deploy-notebook retail --workspace "Demo" --auth cli
    """
    from sqllocks_spindle.fabric.notebook_template import generate_notebook

    registry = _get_domain_registry()
    if domain_name not in registry:
        click.echo(f"Unknown domain: '{domain_name}'", err=True)
        sys.exit(1)

    nb = generate_notebook(domain=domain_name, scale=scale, seed=seed, output_target="lakehouse")
    name = notebook_name or f"Spindle_{domain_name}_{scale}"

    click.echo(f"Spindle v{__version__} — Deploy Notebook")
    click.echo(f"  Domain:    {domain_name}")
    click.echo(f"  Workspace: {workspace}")
    click.echo(f"  Name:      {name}")
    click.echo()

    # Deploy via Fabric REST API
    try:
        import base64
        import json
        import requests
        from azure.identity import (
            AzureCliCredential,
            DeviceCodeCredential,
        )

        if auth_method == "cli":
            credential = AzureCliCredential()
        else:
            credential = DeviceCodeCredential(
                client_id="ea0616ba-638b-4df5-95b9-636659ae5121",
            )

        token = credential.get_token("https://api.fabric.microsoft.com/.default").token
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Resolve workspace ID
        if len(workspace) == 36 and "-" in workspace:
            ws_id = workspace
        else:
            resp = requests.get(
                "https://api.fabric.microsoft.com/v1/workspaces",
                headers=headers,
            )
            resp.raise_for_status()
            ws_list = resp.json().get("value", [])
            ws_match = [w for w in ws_list if w["displayName"] == workspace]
            if not ws_match:
                click.echo(f"Workspace '{workspace}' not found", err=True)
                sys.exit(1)
            ws_id = ws_match[0]["id"]

        # Create notebook item
        nb_content = base64.b64encode(json.dumps(nb).encode()).decode()
        payload = {
            "displayName": name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": nb_content,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        resp = requests.post(
            f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        item = resp.json()

        click.echo(f"  Notebook created: {item.get('displayName', name)}")
        click.echo(f"  Item ID: {item.get('id', 'unknown')}")
        click.echo()
        click.echo("Open the notebook in Fabric and click Run All to generate data.")

    except ImportError as e:
        click.echo(f"Missing dependency: {e}", err=True)
        click.echo("Install with: pip install azure-identity requests", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Deploy failed: {e}", err=True)
        sys.exit(1)


@main.command(name="setup-fabric")
@click.option("--workspace", required=True, help="Fabric workspace name or ID")
@click.option("--auth", "auth_method", default="cli",
              type=click.Choice(["cli", "device-code"]),
              help="Authentication method")
@click.option("--create-lakehouse", is_flag=True, default=False,
              help="Also create a Lakehouse for output")
@click.option("--env-name", default="spindle-env",
              help="Name for the Fabric Environment item")
@click.option("--snippet", is_flag=True, default=False,
              help="Just print the copy-paste setup snippet instead of deploying")
def setup_fabric(
    workspace: str, auth_method: str, create_lakehouse: bool,
    env_name: str, snippet: bool,
):
    """Set up a Fabric environment with Spindle pre-installed.

    Creates a Fabric Environment item with sqllocks-spindle and dependencies,
    and optionally creates a Lakehouse for output.

    Example: spindle setup-fabric --workspace "Demo" --auth cli --create-lakehouse
    """
    from sqllocks_spindle.fabric.setup_environment import (
        get_environment_library_spec,
        print_setup_snippet,
    )

    if snippet:
        print_setup_snippet()
        return

    click.echo(f"Spindle v{__version__} — Fabric Environment Setup")
    click.echo(f"  Workspace:   {workspace}")
    click.echo(f"  Environment: {env_name}")
    click.echo(f"  Lakehouse:   {'yes' if create_lakehouse else 'no'}")
    click.echo()

    try:
        import requests
        from azure.identity import (
            AzureCliCredential,
            DeviceCodeCredential,
        )

        if auth_method == "cli":
            credential = AzureCliCredential()
        else:
            credential = DeviceCodeCredential(
                client_id="ea0616ba-638b-4df5-95b9-636659ae5121",
            )

        token = credential.get_token("https://api.fabric.microsoft.com/.default").token
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Resolve workspace
        if len(workspace) == 36 and "-" in workspace:
            ws_id = workspace
        else:
            resp = requests.get(
                "https://api.fabric.microsoft.com/v1/workspaces",
                headers=headers,
            )
            resp.raise_for_status()
            ws_list = resp.json().get("value", [])
            ws_match = [w for w in ws_list if w["displayName"] == workspace]
            if not ws_match:
                click.echo(f"Workspace '{workspace}' not found", err=True)
                sys.exit(1)
            ws_id = ws_match[0]["id"]

        created_items = []

        # Create Environment
        lib_spec = get_environment_library_spec()
        env_payload = {
            "displayName": env_name,
            "type": "Environment",
        }
        resp = requests.post(
            f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items",
            headers=headers,
            json=env_payload,
        )
        resp.raise_for_status()
        env_item = resp.json()
        created_items.append(f"Environment: {env_item.get('displayName', env_name)}")
        click.echo(f"  Created Environment: {env_item.get('displayName')}")

        # Create Lakehouse if requested
        if create_lakehouse:
            lh_payload = {
                "displayName": "spindle-lakehouse",
                "type": "Lakehouse",
            }
            resp = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items",
                headers=headers,
                json=lh_payload,
            )
            resp.raise_for_status()
            lh_item = resp.json()
            created_items.append(f"Lakehouse: {lh_item.get('displayName')}")
            click.echo(f"  Created Lakehouse:   {lh_item.get('displayName')}")

        click.echo()
        click.echo("Setup complete:")
        for item in created_items:
            click.echo(f"  {item}")
        click.echo()
        click.echo("Next steps:")
        click.echo("  1. Open the Environment in Fabric")
        click.echo("  2. Add these PyPI libraries:")
        for lib in lib_spec["customLibraries"]["pypi"]:
            click.echo(f"     - {lib['name']} {lib['version']}")
        click.echo("  3. Publish the environment")
        click.echo("  4. Attach to notebooks and run 'spindle notebook' to generate data")

    except ImportError as e:
        click.echo(f"Missing dependency: {e}", err=True)
        click.echo("Install with: pip install azure-identity requests", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Setup failed: {e}", err=True)
        sys.exit(1)


def _resolve_domain(domain_name: str, mode: str):
    """Resolve a domain name to a Domain instance."""
    registry = _get_domain_registry()
    if domain_name not in registry:
        click.echo(f"Unknown domain: '{domain_name}'", err=True)
        click.echo(f"Available domains: {', '.join(registry.keys())}", err=True)
        sys.exit(1)

    module_path, class_name, _ = registry[domain_name]
    import importlib
    module = importlib.import_module(module_path)
    domain_class = getattr(module, class_name)
    return domain_class(schema_mode=mode)



# ---------------------------------------------------------------------------
# spindle demo — Demo Engine commands
# ---------------------------------------------------------------------------

@main.group()
def demo():
    """Demo engine — run Spindle demos for conference, client, and workshop use."""


@demo.command("init")
@click.option("--name", prompt="Connection profile name", help="Name for this connection profile")
@click.option("--workspace-id", prompt="Fabric workspace ID", default="", help="Fabric workspace ID")
@click.option("--warehouse-conn", prompt="Warehouse connection string (blank to skip)", default="", help="ODBC connection string")
@click.option("--eventhouse-uri", prompt="Eventhouse URI (blank to skip)", default="", help="KQL cluster URI")
@click.option("--sql-db-conn", prompt="SQL Database connection string (blank to skip)", default="", help="ODBC connection string")
@click.option("--lakehouse-id", prompt="Lakehouse ID (blank to skip)", default="", help="Fabric Lakehouse item ID")
@click.option("--auth", default="cli", type=click.Choice(["cli", "msi", "spn", "fabric"]), help="Auth method")
def demo_init(name, workspace_id, warehouse_conn, eventhouse_uri, sql_db_conn, lakehouse_id, auth):
    """Configure a named connection profile for Fabric targets."""
    from sqllocks_spindle.demo.connections import ConnectionRegistry, ConnectionProfile
    registry = ConnectionRegistry()
    profile = ConnectionProfile(
        name=name, workspace_id=workspace_id, warehouse_conn_str=warehouse_conn,
        eventhouse_uri=eventhouse_uri, sql_db_conn_str=sql_db_conn,
        lakehouse_id=lakehouse_id, auth_method=auth,
    )
    registry.save(profile)
    click.echo(f"Connection profile '{name}' saved. Use with: spindle demo run SCENARIO --connection {name}")


@demo.command("list")
def demo_list():
    """Show all available demo scenarios."""
    from sqllocks_spindle.demo.catalog import get_catalog
    try:
        from rich.table import Table
        from rich.console import Console
        catalog = get_catalog()
        table = Table(title="Spindle Demo Scenarios")
        table.add_column("Name", style="cyan bold")
        table.add_column("Modes")
        table.add_column("Domains")
        table.add_column("Default rows", justify="right")
        table.add_column("Description")
        for s in catalog.list():
            table.add_row(s.name, ", ".join(s.supported_modes), ", ".join(s.domains),
                          f"{s.default_rows:,}", s.description[:60] + ("..." if len(s.description) > 60 else ""))
        Console().print(table)
    except ImportError:
        for s in get_catalog().list():
            click.echo(f"{s.name:20} {', '.join(s.supported_modes):30} {s.description[:60]}")


@demo.command("run")
@click.argument("scenario")
@click.option("--mode", default="inference", type=click.Choice(["inference", "streaming", "seeding"]))
@click.option("--connection", default=None)
@click.option("--input-file", default=None)
@click.option("--rows", default=None, type=int)
@click.option("--domain", default=None)
@click.option("--domains", default=None)
@click.option("--env-name", default=None)
@click.option("--output", "output_formats", default="terminal")
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--estimate", "estimate_only", is_flag=True, default=False)
@click.option("--seed", default=None, type=int)
@click.option("--scale-mode", "scale_mode", default="auto",
              type=click.Choice(["auto", "local", "spark"]),
              help="local: ProcessPoolExecutor; spark: Fabric notebook; auto: pick by row count + connection")
def demo_run(scenario, mode, connection, input_file, rows, domain, domains,
             env_name, output_formats, dry_run, estimate_only, seed, scale_mode):
    """Run a demo scenario."""
    from sqllocks_spindle.demo.params import DemoParams
    from sqllocks_spindle.demo.orchestrator import DemoOrchestrator
    meta = __import__('sqllocks_spindle.demo.catalog', fromlist=['get_catalog']).get_catalog().get(scenario)
    effective_rows = rows or meta.default_rows
    domain_list = [d.strip() for d in domains.split(",")] if domains else None
    fmt_list = [f.strip() for f in output_formats.split(",")]
    params = DemoParams(scenario=scenario, mode=mode, connection=connection, input_file=input_file,
                        rows=effective_rows, domain=domain, domains=domain_list, env_name=env_name,
                        output_formats=fmt_list, dry_run=dry_run, estimate_only=estimate_only,
                        seed=seed, scale_mode=scale_mode)
    result = DemoOrchestrator().run(params)
    if result.success:
        click.echo(f"\nSession: {result.session_id}")
        if result.fidelity_score is not None:
            click.echo(f"Fidelity: {result.fidelity_score:.1%}")
    else:
        click.echo(f"\nFailed: {result.error}", err=True)
        raise SystemExit(1)


@demo.command("preflight")
@click.option("--connection", default=None)
def demo_preflight(connection):
    """Validate connections to configured Fabric targets."""
    from sqllocks_spindle.demo.connections import ConnectionRegistry
    registry = ConnectionRegistry()
    if not registry.list():
        click.echo("No connection profiles found. Run: spindle demo init")
        return
    profiles = [connection] if connection else registry.list()
    for name in profiles:
        click.echo(f"\nChecking '{name}'...")
        try:
            profile = registry.load(name)
            checks = []
            if profile.warehouse_conn_str:
                try:
                    import pyodbc
                    with pyodbc.connect(profile.warehouse_conn_str, timeout=10): pass
                    checks.append(("Warehouse", True, ""))
                except Exception as e:
                    checks.append(("Warehouse", False, str(e)[:60]))
            if profile.eventhouse_uri:
                checks.append(("Eventhouse", True, "URI set"))
            if profile.sql_db_conn_str:
                checks.append(("SQL DB", True, "Connection string set"))
            if profile.lakehouse_id:
                checks.append(("Lakehouse", True, f"ID: {profile.lakehouse_id}"))
            for target, ok, msg in checks:
                click.echo(f"  [{'OK' if ok else 'FAIL'}] {target}" + (f": {msg}" if msg else ""))
            if not checks:
                click.echo("  (no targets configured)")
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)


@demo.command("cleanup")
@click.argument("session_id")
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--connection", default=None)
def demo_cleanup(session_id, dry_run, connection):
    """Remove all artifacts from a demo session."""
    from sqllocks_spindle.demo.manifest import DemoManifest
    from sqllocks_spindle.demo.cleanup import CleanupEngine
    from sqllocks_spindle.demo.connections import ConnectionRegistry
    conn_profile = ConnectionRegistry().load(connection) if connection else None
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        click.echo(f"Session '{session_id}' not found.", err=True)
        raise SystemExit(1)
    removed = CleanupEngine(conn_profile).cleanup(manifest, dry_run=dry_run)
    prefix = "[dry-run] Would remove" if dry_run else "Removed"
    for target, names in removed.items():
        for name in names:
            click.echo(f"  {prefix}: {target}/{name}")
    if not any(removed.values()):
        click.echo("Nothing to remove.")


@demo.command("status")
@click.argument("session_id")
def demo_status(session_id):
    """Show status of a demo session."""
    from sqllocks_spindle.demo.manifest import DemoManifest
    try:
        m = DemoManifest.load(session_id)
        click.echo(f"Session: {m.session_id}")
        click.echo(f"Scenario: {m.scenario} ({m.mode})")
        click.echo(f"Status: {'Success' if m.success else 'Failed'}")
        click.echo(f"Started: {m.started_at}")
        click.echo(f"Artifacts: {len(m.artifacts)}")
    except FileNotFoundError:
        click.echo(f"Session '{session_id}' not found.", err=True)
        raise SystemExit(1)


@demo.command("notebook")
@click.argument("scenario")
@click.option("--mode", default="inference", type=click.Choice(["inference", "streaming", "seeding"]))
@click.option("--output", default=None)
def demo_notebook(scenario, mode, output):
    """Generate a Fabric notebook for a demo scenario."""
    from sqllocks_spindle.demo.catalog import get_catalog
    from sqllocks_spindle.demo.notebook_gen import NotebookGenerator
    from pathlib import Path
    meta = get_catalog().get(scenario)
    out = NotebookGenerator().generate(meta, mode, Path(output) if output else None)
    click.echo(f"Notebook written to: {out}")


@demo.command("report")
@click.argument("session_id")
@click.option("--format", "fmt", default="md", type=click.Choice(["md", "html"]))
@click.option("--output", default=None)
def demo_report(session_id, fmt, output):
    """Generate a report for a completed demo session."""
    from sqllocks_spindle.demo.manifest import DemoManifest
    from pathlib import Path
    try:
        manifest = DemoManifest.load(session_id)
    except FileNotFoundError:
        click.echo(f"Session '{session_id}' not found.", err=True)
        raise SystemExit(1)
    content = manifest.export(fmt)
    if output:
        Path(output).write_text(content)
        click.echo(f"Report written to: {output}")
    else:
        click.echo(content)


if __name__ == "__main__":
    main()

