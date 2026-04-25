"""ChartRenderer — plotly distribution charts for Jupyter inline display."""
from __future__ import annotations


class ChartRenderer:
    def __init__(self, real_profile, synthetic_profile):
        self._real = real_profile
        self._synthetic = synthetic_profile
        self._available = self._check_plotly()

    def _check_plotly(self) -> bool:
        try:
            import plotly  # noqa: F401
            return True
        except ImportError:
            return False

    def render_all(self, max_columns: int = 20) -> None:
        if not self._available:
            print("plotly not installed — skipping charts. Install with: pip install plotly")
            return
        real_tables = getattr(self._real, "tables", {})
        syn_tables = getattr(self._synthetic, "tables", {})
        for tname in sorted(set(real_tables) & set(syn_tables)):
            real_t = real_tables[tname]
            syn_t = syn_tables[tname]
            cols = sorted(set(real_t.columns) & set(syn_t.columns))[:max_columns]
            if cols:
                self._render_table_charts(tname, cols, real_t, syn_t)

    def _render_table_charts(self, table_name: str, cols: list, real_t, syn_t) -> None:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        n = len(cols)
        ncols = min(3, n)
        nrows = (n + ncols - 1) // ncols
        fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=cols)

        for i, col_name in enumerate(cols):
            row = i // ncols + 1
            col = i % ncols + 1
            rc = real_t.columns.get(col_name)
            sc = syn_t.columns.get(col_name)
            if rc is None or sc is None:
                continue
            if rc.enum_values:
                real_vals = list(rc.enum_values.keys())
                real_probs = list(rc.enum_values.values())
                syn_vals = list((sc.enum_values or {}).keys())
                syn_probs = list((sc.enum_values or {}).values())
                fig.add_trace(go.Bar(name="Real", x=real_vals, y=real_probs,
                                     marker_color="steelblue", showlegend=(i == 0)), row=row, col=col)
                fig.add_trace(go.Bar(name="Synthetic", x=syn_vals, y=syn_probs,
                                     marker_color="coral", showlegend=(i == 0)), row=row, col=col)
            elif rc.min_value is not None and rc.max_value is not None:
                for label, cp, color in [("Real", rc, "steelblue"), ("Synthetic", sc, "coral")]:
                    if cp and cp.min_value is not None:
                        fig.add_trace(go.Box(
                            name=label, y=[cp.min_value, cp.mean, cp.max_value],
                            marker_color=color, showlegend=(i == 0 and label == "Real"),
                        ), row=row, col=col)

        fig.update_layout(title_text=f"Distribution Comparison — {table_name}",
                          height=300 * nrows, barmode="group")
        fig.show()

    def render_summary_card(self, fidelity_score: float) -> None:
        if not self._available:
            print(f"Fidelity score: {fidelity_score:.1%}")
            return
        import plotly.graph_objects as go
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=fidelity_score * 100,
            title={"text": "Fidelity Score"},
            gauge={"axis": {"range": [0, 100]},
                   "bar": {"color": "steelblue"},
                   "steps": [{"range": [0, 70], "color": "salmon"},
                              {"range": [70, 90], "color": "khaki"},
                              {"range": [90, 100], "color": "lightgreen"}]},
        ))
        fig.update_layout(height=300)
        fig.show()
