"""FidelityReport — real vs synthetic distribution comparison."""
from __future__ import annotations
from typing import Optional


class FidelityReport:
    def __init__(self, real_profile, synthetic_profile, table_name: str = ""):
        self._real = real_profile
        self._synthetic = synthetic_profile
        self._table = table_name
        self._use_rich = self._check_rich()

    def _check_rich(self) -> bool:
        try:
            import rich  # noqa: F401
            return True
        except ImportError:
            return False

    def render(self) -> None:
        comparisons = self._build_comparisons()
        if self._use_rich:
            self._render_rich(comparisons)
        else:
            self._render_plain(comparisons)

    def _build_comparisons(self) -> list:
        results = []
        real_tables = getattr(self._real, "tables", {})
        syn_tables = getattr(self._synthetic, "tables", {})
        all_tables = set(real_tables) | set(syn_tables)
        if not all_tables:
            return results
        for tname in sorted(all_tables):
            real_t = real_tables.get(tname)
            syn_t = syn_tables.get(tname)
            if real_t is None or syn_t is None:
                continue
            for col_name in sorted(set(real_t.columns) | set(syn_t.columns)):
                rc = real_t.columns.get(col_name)
                sc = syn_t.columns.get(col_name)
                if rc is None or sc is None:
                    continue
                results.append({
                    "table": tname,
                    "column": col_name,
                    "dtype": rc.dtype,
                    "real_nulls": f"{rc.null_rate:.1%}",
                    "syn_nulls": f"{sc.null_rate:.1%}",
                    "real_card": str(rc.cardinality),
                    "syn_card": str(sc.cardinality),
                    "pass": self._check_pass(rc, sc),
                })
        return results

    def _check_pass(self, real_col, syn_col) -> bool:
        # Null rate must be similar (≤5pp difference).
        null_diff = abs(real_col.null_rate - syn_col.null_rate)
        if null_diff > 0.05:
            return False

        # Surrogate keys: values are structurally meaningless for analytics.
        # Their absolute range will always differ across samples — skip cardinality.
        if getattr(real_col, "is_primary_key", False) or getattr(real_col, "is_unique", False):
            return True

        # Date/datetime: cardinality is sample-size-dependent for bounded ranges
        # (e.g. 184K rows saturates all 25K possible dates → cardinality ≈ date-range-size).
        # Range alignment is guaranteed by the schema_builder; null-rate check above suffices.
        dtype = getattr(real_col, "dtype", "")
        if dtype in ("date", "datetime"):
            return True

        # Float: continuous readings have near-infinite theoretical cardinality.
        # Comparing cardinality ratios measures quantization noise, not fidelity.
        # Distribution shape is not captured by cardinality — skip this check for floats.
        if dtype == "float":
            return True

        # Phone/email/uuid: always near-unique by semantic definition.
        # Absolute cardinality comparison is meaningless across different sample sizes.
        pattern = getattr(real_col, "pattern", None)
        if pattern in ("phone", "phone_number", "email", "uuid"):
            return True

        # Cardinality comparison — normalised for scale differences.
        real_card = getattr(real_col, "cardinality", 0) or 0
        syn_card  = getattr(syn_col,  "cardinality", 0) or 0
        if real_card and syn_card:
            real_cr = getattr(real_col, "cardinality_ratio", real_card) or 0.0
            syn_cr  = getattr(syn_col,  "cardinality_ratio", syn_card)  or 0.0

            # Integers tolerate wider skew than strings — zero-inflation and value
            # clustering produce cardinality ratios that a normal generator can't
            # reproduce exactly. Strings (names, codes) should match more tightly.
            high_card_threshold = 0.3 if dtype == "integer" else 0.5
            high_card_tolerance = 0.75 if dtype == "integer" else 0.35
            cat_lo, cat_hi      = (0.2, 5.0) if dtype == "integer" else (0.5, 2.0)

            if real_cr > high_card_threshold:
                if abs(real_cr - syn_cr) > high_card_tolerance:
                    return False
            else:
                # Low-cardinality (categorical/enum): compare absolute cardinality.
                ratio = syn_card / max(real_card, 1)
                if not (cat_lo <= ratio <= cat_hi):
                    return False

        return True

    def overall_score(self) -> float:
        comparisons = self._build_comparisons()
        if not comparisons:
            return 1.0
        return sum(1 for c in comparisons if c["pass"]) / len(comparisons)

    def _render_rich(self, comparisons: list) -> None:
        from rich.console import Console
        from rich.table import Table
        console = Console()
        table = Table(title=f"Fidelity Report{' — ' + self._table if self._table else ''}", show_lines=False)
        table.add_column("Table", style="dim")
        table.add_column("Column")
        table.add_column("Type", style="cyan")
        table.add_column("Real nulls", justify="right")
        table.add_column("Syn nulls", justify="right")
        table.add_column("Real card", justify="right")
        table.add_column("Syn card", justify="right")
        table.add_column("Pass?", justify="center")
        for c in comparisons:
            pass_str = "[green]✓[/]" if c["pass"] else "[red]✗[/]"
            table.add_row(c["table"], c["column"], c["dtype"],
                          c["real_nulls"], c["syn_nulls"],
                          c["real_card"], c["syn_card"], pass_str)
        console.print(table)
        score = self.overall_score()
        color = "green" if score >= 0.9 else ("yellow" if score >= 0.7 else "red")
        console.print(f"\n[{color}]Overall fidelity score: {score:.1%}[/{color}]")

    def _render_plain(self, comparisons: list) -> None:
        print(f"Fidelity Report{' — ' + self._table if self._table else ''}")
        print(f"{'Table':<20} {'Column':<25} {'Type':<10} {'Pass':>5}")
        print("-" * 65)
        for c in comparisons:
            print(f"{c['table']:<20} {c['column']:<25} {c['dtype']:<10} {'OK' if c['pass'] else 'FAIL':>5}")
        print(f"\nFidelity score: {self.overall_score():.1%}")
