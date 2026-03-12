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
@click.option("--format", "fmt", default="summary", type=click.Choice(["summary", "csv", "tsv", "jsonl", "parquet", "excel", "sql", "delta"]))
@click.option("--mode", "-m", default="3nf", help="Schema mode: 3nf, star")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be generated without generating")
def generate(domain_name: str, scale: str, seed: int, output: str | None, fmt: str, mode: str, dry_run: bool):
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
    if fmt != "summary" and output:
        format_labels = {
            "csv": "CSV", "tsv": "TSV", "jsonl": "JSON Lines",
            "parquet": "Parquet", "excel": "Excel", "sql": "SQL INSERT",
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
                files = writer.to_sql_inserts(result.tables, output)
            else:
                files = []

        click.echo()
        click.echo(f"Written {len(files)} {format_labels.get(fmt, fmt)} files to {output}/")
    elif fmt != "summary" and not output:
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
