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


_DOMAIN_REGISTRY: dict[str, tuple[str, str, str]] = _discover_domains()


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
@click.option("--auth", "auth_method", default="cli", type=click.Choice(["cli", "msi", "spn", "sql"]), help="Auth method for sql-database")
@click.option("--write-mode", default="create_insert", type=click.Choice(["create_insert", "insert_only", "truncate_insert", "append"]), help="SQL write mode")
@click.option("--batch-size", default=1000, type=int, help="Rows per INSERT batch")
def generate(
    domain_name: str, scale: str, seed: int, output: str | None, fmt: str,
    mode: str, dry_run: bool, schema_name: str | None,
    sql_ddl: bool, sql_drop: bool, sql_go: bool, sql_dialect: str,
    connection_string: str | None, auth_method: str, write_mode: str, batch_size: int,
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
        )

        click.echo()
        click.echo(f"Writing to SQL database (mode={write_mode}, auth={auth_method})...")

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

    for name, (_, __, desc) in _DOMAIN_REGISTRY.items():
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
def from_ddl(input_file: str, output: str | None, domain: str, scale: str | None):
    """Import SQL DDL (CREATE TABLE) into a .spindle.json schema.

    Parses SQL Server, PostgreSQL, MySQL, and ANSI SQL dialects.
    Automatically infers generator strategies from column types and names.

    Example: spindle from-ddl my_tables.sql --output my_schema.spindle.json
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

    click.echo(f"Spindle v{__version__} — DDL Import")
    click.echo()
    click.echo(f"  Source: {input_file}")
    click.echo(f"  Output: {output}")
    click.echo(f"  Tables: {len(schema.tables)}")
    click.echo(f"  Relationships: {len(schema.relationships)}")
    click.echo()
    for tname, tdef in schema.tables.items():
        pk_str = f" (PK: {', '.join(tdef.primary_key)})" if tdef.primary_key else ""
        click.echo(f"  {tname}: {len(tdef.columns)} columns{pk_str}")
    click.echo()
    click.echo(f"Schema written to {output}")
    click.echo("Run: spindle generate custom --schema {output} --scale small")


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


def _resolve_domain(domain_name: str, mode: str):
    """Resolve a domain name to a Domain instance."""
    if domain_name not in _DOMAIN_REGISTRY:
        click.echo(f"Unknown domain: '{domain_name}'", err=True)
        click.echo(f"Available domains: {', '.join(_DOMAIN_REGISTRY.keys())}", err=True)
        sys.exit(1)

    module_path, class_name, _ = _DOMAIN_REGISTRY[domain_name]
    import importlib
    module = importlib.import_module(module_path)
    domain_class = getattr(module, class_name)
    return domain_class(schema_mode=mode)


if __name__ == "__main__":
    main()
