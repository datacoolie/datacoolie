"""Pure execution-metric values and observed-value aggregation.

Metric values are observations.  Missing contributions are ignored by
aggregates; valid nonnegative integers, including zero, are summed
independently for each field, and fields with no observations remain null.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Optional


METRIC_FIELDS = (
    "rows_read",
    "rows_written",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "files_added",
    "files_removed",
    "bytes_added",
    "bytes_removed",
    "bytes_saved",
)

MetricValue = Optional[int]


def _observed_integer(value: Any) -> bool:
    return type(value) is int and value >= 0


@dataclass
class ExecutionMetrics:
    """Nullable execution measurements at one attempt or aggregate grain."""

    rows_read: MetricValue = None
    rows_written: MetricValue = None
    rows_inserted: MetricValue = None
    rows_updated: MetricValue = None
    rows_deleted: MetricValue = None
    files_added: MetricValue = None
    files_removed: MetricValue = None
    bytes_added: MetricValue = None
    bytes_removed: MetricValue = None
    bytes_saved: MetricValue = None

    @classmethod
    def from_attempt(cls, attempt: Any | None) -> "ExecutionMetrics":
        """Project one attempt without treating an absent phase as zero."""

        if attempt is None:
            return cls()
        source = attempt.source
        destination = attempt.destination
        values: dict[str, Any] = {
            name: None for name in METRIC_FIELDS
        }
        if source is not None:
            values["rows_read"] = source.rows_read
        if destination is not None:
            for name in METRIC_FIELDS:
                if name == "rows_read":
                    continue
                values[name] = getattr(destination, name)
        return cls(**values)

    @classmethod
    def sum(cls, metrics: Iterable["ExecutionMetrics"]) -> "ExecutionMetrics":
        """Sum observed values independently, preserving unobserved fields."""

        accumulator = MetricAccumulator()
        for value in metrics:
            accumulator.add(value)
        return accumulator.metrics


class MetricAccumulator:
    """Aggregate observed values independently for each metric field."""

    def __init__(self) -> None:
        self._sums = {name: 0 for name in METRIC_FIELDS}
        self._observed = {name: 0 for name in METRIC_FIELDS}

    def add(
        self,
        metrics: ExecutionMetrics | Mapping[str, Any] | None,
    ) -> "MetricAccumulator":
        """Add one observation and return this accumulator.

        Missing values do not contribute to the corresponding field.  A
        valid nonnegative integer, including zero, is the only observation.
        """

        values = metrics if isinstance(metrics, Mapping) else (
            {name: getattr(metrics, name) for name in METRIC_FIELDS}
            if metrics is not None
            else {}
        )
        for name in METRIC_FIELDS:
            value = values.get(name)
            if _observed_integer(value):
                self._sums[name] += value
                self._observed[name] += 1
        return self

    @property
    def metrics(self) -> ExecutionMetrics:
        """Return observed totals, leaving fields without observations null."""

        values: dict[str, MetricValue] = {}
        for name in METRIC_FIELDS:
            values[name] = self._sums[name] if self._observed[name] else None
        return ExecutionMetrics(**values)

    def reset(self) -> None:
        """Clear contributions while retaining this reusable reducer object."""

        self.__init__()


__all__ = [
    "METRIC_FIELDS",
    "MetricValue",
    "ExecutionMetrics",
    "MetricAccumulator",
]
