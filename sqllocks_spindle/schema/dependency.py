"""Topological sort of tables by FK dependencies."""

from __future__ import annotations

from sqllocks_spindle.schema.parser import SpindleSchema


class CircularDependencyError(Exception):
    """Raised when tables have circular FK dependencies."""
    pass


class DependencyResolver:
    """Resolve table generation order via topological sort."""

    def resolve(self, schema: SpindleSchema) -> list[str]:
        graph: dict[str, set[str]] = {}
        for table_name, table in schema.tables.items():
            deps = set()
            for col in table.columns.values():
                ref_table = col.fk_ref_table
                if ref_table and ref_table != table_name:
                    deps.add(ref_table)
            graph[table_name] = deps

        # Also add dependency info from relationships (skip self-references)
        for rel in schema.relationships:
            if rel.parent != rel.child and rel.type != "self_referencing" and rel.child in graph:
                graph[rel.child].add(rel.parent)

        return self._topological_sort(graph)

    def _topological_sort(self, graph: dict[str, set[str]]) -> list[str]:
        in_degree: dict[str, int] = {node: 0 for node in graph}
        for node, deps in graph.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[node] = in_degree.get(node, 0)

        # Recalculate: count how many nodes depend on each
        in_degree = {node: 0 for node in graph}
        for node, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise CircularDependencyError(
                        f"Table '{node}' depends on '{dep}' which is not defined"
                    )

        # Kahn's algorithm
        in_degree = {node: len(deps) for node, deps in graph.items()}
        queue = [node for node, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            # Sort for deterministic order
            queue.sort()
            node = queue.pop(0)
            result.append(node)
            for other, deps in graph.items():
                if node in deps:
                    in_degree[other] -= 1
                    if in_degree[other] == 0:
                        queue.append(other)

        if len(result) != len(graph):
            remaining = set(graph.keys()) - set(result)
            raise CircularDependencyError(
                f"Circular dependency detected among tables: {remaining}"
            )

        return result

    def get_dependency_graph(self, schema: SpindleSchema) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {}
        for table_name, table in schema.tables.items():
            graph[table_name] = table.fk_dependencies
        return graph
