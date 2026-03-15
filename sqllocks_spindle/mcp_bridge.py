"""MCP bridge — JSON stdin/stdout protocol for spindle-forge MCP server.

Usage:
    echo '{"command": "list"}' | python -m sqllocks_spindle.mcp_bridge

Commands:
    list           — List available domains with metadata
    describe       — Full schema description for a domain
    generate       — Generate data, return summary + file paths
    dry_run        — Preview planned row counts without generating
    validate       — Validate a .spindle.json schema file
    preview        — Generate small sample, return first N rows as JSON
    profile_info   — Get distribution keys and ratios for a domain
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

from sqllocks_spindle import __version__


def _discover_domains() -> dict:
    """Discover all available domains."""
    import importlib
    import pkgutil

    import sqllocks_spindle.domains as _pkg
    from sqllocks_spindle.domains.base import Domain

    registry = {}
    for _, mod_name, is_pkg in pkgutil.iter_modules(_pkg.__path__, _pkg.__name__ + "."):
        if not is_pkg:
            continue
        try:
            module = importlib.import_module(mod_name)
        except Exception:
            continue
        for attr in getattr(module, "__all__", dir(module)):
            cls = getattr(module, attr, None)
            if isinstance(cls, type) and issubclass(cls, Domain) and cls is not Domain:
                instance = cls.__new__(cls)
                name = cls.name.fget(instance)
                desc = cls.description.fget(instance)
                registry[name] = {
                    "module": mod_name,
                    "class": cls.__name__,
                    "description": desc,
                }
    return registry


_REGISTRY = _discover_domains()


def _resolve_domain(domain_name: str, mode: str = "3nf", profile: str | None = None):
    """Resolve domain name to instance."""
    if domain_name not in _REGISTRY:
        raise ValueError(f"Unknown domain: '{domain_name}'. Available: {', '.join(sorted(_REGISTRY.keys()))}")

    import importlib
    info = _REGISTRY[domain_name]
    module = importlib.import_module(info["module"])
    cls = getattr(module, info["class"])
    kwargs = {"schema_mode": mode}
    if profile:
        kwargs["profile"] = profile
    return cls(**kwargs)


def cmd_list(_params: dict) -> dict:
    """List all available domains."""
    result = []
    for name, info in sorted(_REGISTRY.items()):
        try:
            domain = _resolve_domain(name)
            profiles = domain.available_profiles
        except Exception:
            profiles = ["default"]
        result.append({
            "name": name,
            "description": info["description"],
            "profiles": profiles,
        })
    return {"version": __version__, "domains": result, "count": len(result)}


def cmd_describe(params: dict) -> dict:
    """Describe a domain's schema."""
    domain_name = params.get("domain", "")
    mode = params.get("mode", "3nf")

    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.schema.dependency import DependencyResolver

    domain = _resolve_domain(domain_name, mode)
    spindle = Spindle()
    schema = spindle.describe(domain=domain)
    resolver = DependencyResolver()
    gen_order = resolver.resolve(schema)

    tables = {}
    for tname, tdef in schema.tables.items():
        columns = []
        for col_name, col_def in tdef.columns.items():
            columns.append({
                "name": col_def.name,
                "type": col_def.type,
                "nullable": col_def.nullable,
            })
        tables[tname] = {
            "description": tdef.description or "",
            "primary_key": list(tdef.primary_key),
            "columns": columns,
            "column_count": len(columns),
            "dependencies": list(tdef.fk_dependencies),
        }

    relationships = []
    for rel in schema.relationships:
        relationships.append({
            "name": rel.name,
            "parent": rel.parent,
            "child": rel.child,
            "parent_columns": rel.parent_columns,
            "child_columns": rel.child_columns,
        })

    rules = []
    for rule in schema.business_rules:
        rules.append({
            "name": rule.name,
            "rule": rule.rule,
            "type": rule.type,
        })

    scales = {}
    for scale_name, scale_def in schema.generation.scales.items():
        scales[scale_name] = dict(scale_def)

    return {
        "domain": domain_name,
        "mode": mode,
        "table_count": len(tables),
        "tables": tables,
        "generation_order": gen_order,
        "relationships": relationships,
        "business_rules": rules,
        "scales": scales,
    }


def cmd_generate(params: dict) -> dict:
    """Generate data and optionally write to disk."""
    domain_name = params.get("domain", "")
    scale = params.get("scale", "small")
    seed = params.get("seed", 42)
    fmt = params.get("format", "summary")
    output_dir = params.get("output_dir")
    mode = params.get("mode", "3nf")
    profile = params.get("profile")

    from sqllocks_spindle.engine.generator import Spindle

    domain = _resolve_domain(domain_name, mode, profile)
    spindle = Spindle()
    result = spindle.generate(domain=domain, scale=scale, seed=seed)

    # Integrity check
    errors = result.verify_integrity()

    # Build summary
    table_info = {}
    total_rows = 0
    for tname in result.tables:
        count = len(result[tname])
        total_rows += count
        table_info[tname] = {"rows": count, "columns": len(result[tname].columns)}

    response = {
        "domain": domain_name,
        "scale": scale,
        "seed": seed,
        "total_rows": total_rows,
        "tables": table_info,
        "integrity_errors": errors,
        "integrity_pass": len(errors) == 0,
    }

    # Write files if requested
    if fmt != "summary" and output_dir:
        files = _write_output(result, fmt, output_dir)
        response["output_format"] = fmt
        response["output_dir"] = output_dir
        response["files"] = files

    return response


def cmd_dry_run(params: dict) -> dict:
    """Preview planned row counts."""
    domain_name = params.get("domain", "")
    scale = params.get("scale", "small")
    mode = params.get("mode", "3nf")

    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.schema.dependency import DependencyResolver

    domain = _resolve_domain(domain_name, mode)
    spindle = Spindle()
    schema = spindle.describe(domain=domain)
    schema.generation.scale = scale
    row_counts = spindle._calculate_row_counts(schema)
    resolver = DependencyResolver()
    gen_order = resolver.resolve(schema)

    tables = {}
    total = 0
    for tname in gen_order:
        count = row_counts.get(tname, 100)
        tables[tname] = count
        total += count

    return {
        "domain": domain_name,
        "scale": scale,
        "generation_order": gen_order,
        "planned_rows": tables,
        "total_rows": total,
    }


def cmd_validate(params: dict) -> dict:
    """Validate a .spindle.json schema file."""
    schema_path = params.get("schema_path", "")
    if not schema_path:
        raise ValueError("schema_path is required")

    from sqllocks_spindle.schema.parser import SchemaParser
    from sqllocks_spindle.schema.validator import SchemaValidator

    parser = SchemaParser()
    validator = SchemaValidator()

    schema = parser.parse_file(schema_path)
    issues = validator.validate(schema)

    errors = [{"location": e.location, "message": e.message} for e in issues if e.level == "error"]
    warnings = [{"location": w.location, "message": w.message} for w in issues if w.level == "warning"]

    return {
        "valid": len(errors) == 0,
        "table_count": len(schema.tables),
        "relationship_count": len(schema.relationships),
        "errors": errors,
        "warnings": warnings,
    }


def cmd_preview(params: dict) -> dict:
    """Generate small sample and return as JSON."""
    domain_name = params.get("domain", "")
    rows = params.get("rows", 5)
    seed = params.get("seed", 42)
    tables_filter = params.get("tables")  # optional list of table names
    mode = params.get("mode", "3nf")

    from sqllocks_spindle.engine.generator import Spindle

    domain = _resolve_domain(domain_name, mode)
    spindle = Spindle()
    result = spindle.generate(domain=domain, scale="small", seed=seed)

    preview = {}
    for tname in result.tables:
        if tables_filter and tname not in tables_filter:
            continue
        df = result[tname].head(rows)
        # Convert to JSON-serializable records
        records = json.loads(df.to_json(orient="records", date_format="iso"))
        preview[tname] = {
            "total_rows": len(result[tname]),
            "preview_rows": len(records),
            "columns": list(df.columns),
            "data": records,
        }

    return {"domain": domain_name, "seed": seed, "tables": preview}


def cmd_profile_info(params: dict) -> dict:
    """Get distribution keys and ratios for a domain's profile."""
    domain_name = params.get("domain", "")
    profile_name = params.get("profile")
    mode = params.get("mode", "3nf")

    domain = _resolve_domain(domain_name, mode, profile_name)
    profile = domain._profile

    return {
        "domain": domain_name,
        "profile": domain.profile_name,
        "available_profiles": domain.available_profiles,
        "distribution_keys": sorted(profile.get("distributions", {}).keys()),
        "distributions": profile.get("distributions", {}),
        "ratio_keys": sorted(profile.get("ratios", {}).keys()),
        "ratios": profile.get("ratios", {}),
    }


def _write_output(result, fmt: str, output_dir: str) -> list[str]:
    """Write generated data to files."""
    if fmt == "delta":
        from sqllocks_spindle.output import DeltaWriter
        partition_by = getattr(result.schema.generation, "partition_by", None) or {}
        writer = DeltaWriter(output_dir=output_dir, partition_by=partition_by)
        return writer.write_all(result.tables)
    else:
        from sqllocks_spindle.output import PandasWriter
        writer = PandasWriter()
        dispatch = {
            "csv": writer.to_csv,
            "tsv": writer.to_tsv,
            "jsonl": writer.to_jsonl,
            "parquet": writer.to_parquet,
            "excel": writer.to_excel,
            "sql": writer.to_sql_inserts,
        }
        fn = dispatch.get(fmt)
        if not fn:
            raise ValueError(f"Unknown format: {fmt}")
        return fn(result.tables, output_dir)


# ── Main dispatch ────────────────────────────────────────────────────────────

COMMANDS = {
    "list": cmd_list,
    "describe": cmd_describe,
    "generate": cmd_generate,
    "dry_run": cmd_dry_run,
    "validate": cmd_validate,
    "preview": cmd_preview,
    "profile_info": cmd_profile_info,
}


def main():
    """Read JSON from stdin, dispatch command, write JSON to stdout."""
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            _respond_error("Empty input")
            return

        request = json.loads(raw)
        command = request.get("command", "")

        if command not in COMMANDS:
            _respond_error(f"Unknown command: '{command}'. Available: {', '.join(COMMANDS.keys())}")
            return

        params = request.get("params", {})
        result = COMMANDS[command](params)
        _respond_ok(result)

    except json.JSONDecodeError as e:
        _respond_error(f"Invalid JSON: {e}")
    except Exception as e:
        _respond_error(f"{type(e).__name__}: {e}", traceback.format_exc())


def _respond_ok(data: dict):
    """Write success response to stdout."""
    response = {"status": "ok", "data": data}
    sys.stdout.write(json.dumps(response, default=str) + "\n")
    sys.stdout.flush()


def _respond_error(message: str, trace: str | None = None):
    """Write error response to stdout."""
    response = {"status": "error", "error": message}
    if trace:
        response["trace"] = trace
    sys.stdout.write(json.dumps(response, default=str) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
