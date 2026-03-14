"""Configuration for incremental (continue) generation."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ContinueConfig:
    """Configuration for incremental generation.

    Attributes:
        insert_count: Number of new rows to generate per anchor table.
        update_fraction: Fraction of existing rows to update (0.0 - 1.0).
        delete_fraction: Fraction of existing rows to soft-delete (0.0 - 1.0).
        state_transitions: Per-column Markov transition probabilities.
            Format: ``{"table.column": {"current_state": {"next_state": probability}}}``
            Example::

                {"order.status": {
                    "pending": {"shipped": 0.7, "cancelled": 0.3},
                    "shipped": {"delivered": 0.9, "returned": 0.1},
                }}

        timestamp_column: Name of the delta-timestamp metadata column.
        delta_type_column: Name of the delta-type metadata column.
        seed: Optional random seed for reproducibility.
    """

    insert_count: int = 100
    update_fraction: float = 0.1
    delete_fraction: float = 0.02
    state_transitions: dict[str, dict[str, dict[str, float]]] = field(
        default_factory=dict
    )
    timestamp_column: str = "_delta_timestamp"
    delta_type_column: str = "_delta_type"
    seed: int | None = None

    def __post_init__(self) -> None:
        if self.insert_count < 0:
            raise ValueError("insert_count must be >= 0")
        if not 0.0 <= self.update_fraction <= 1.0:
            raise ValueError("update_fraction must be between 0.0 and 1.0")
        if not 0.0 <= self.delete_fraction <= 1.0:
            raise ValueError("delete_fraction must be between 0.0 and 1.0")
