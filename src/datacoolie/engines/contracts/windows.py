"""Pure contracts for resolving physical engine targets.

Destination writers and orchestration must agree on the same primary handle.
This module contains no engine instance, platform I/O, catalog client, or run
configuration; it only projects a destination model into the addressing
information an engine operation will receive.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Literal

from datacoolie.core.exceptions import ConfigurationError


@dataclass(frozen=True, slots=True, eq=False)
class WindowSpec:
    """Execution-only watermark bounds passed to engine replacement calls.

    Bounds are combined with ``OR`` across columns and with ``AND`` within a
    column. The operators are explicit so rolling and replay semantics do not
    depend on a hidden engine default.
    """

    bounds: Mapping[str, tuple[Any, Any]]
    lower_operator: Literal[">", ">="] = ">"
    upper_operator: Literal["<", "<="] = "<="
    combine_operator: Literal["OR"] = "OR"

    def __post_init__(self) -> None:
        if not isinstance(self.bounds, Mapping):
            raise ConfigurationError("WindowSpec.bounds must be a mapping")
        normalized_bounds: dict[str, tuple[Any, Any]] = {}
        for column, pair in self.bounds.items():
            if not isinstance(column, str) or not column.strip():
                raise ConfigurationError(
                    "WindowSpec bound columns must be non-empty strings"
                )
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise ConfigurationError(
                    f"WindowSpec bound for {column!r} must contain exactly two values"
                )
            normalized_bounds[column] = (pair[0], pair[1])
        object.__setattr__(self, "bounds", MappingProxyType(normalized_bounds))
        if self.lower_operator not in {">", ">="}:
            raise ConfigurationError(
                f"Unsupported watermark lower operator: {self.lower_operator!r}"
            )
        if self.upper_operator not in {"<", "<="}:
            raise ConfigurationError(
                f"Unsupported watermark upper operator: {self.upper_operator!r}"
            )
        if self.combine_operator != "OR":
            raise ConfigurationError(
                f"Unsupported watermark combine operator: {self.combine_operator!r}"
            )

    def items(self):
        """Expose mapping-style iteration for engine adapters."""

        return self.bounds.items()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, WindowSpec):
            return (
                self.bounds == other.bounds
                and self.lower_operator == other.lower_operator
                and self.upper_operator == other.upper_operator
                and self.combine_operator == other.combine_operator
            )
        return NotImplemented


def normalize_window(window: WindowSpec) -> WindowSpec:
    """Validate an explicit execution window.

    Window mappings are deliberately not accepted at the engine boundary.  A
    mapping has no way to express replay operators and made equality silently
    ignore execution semantics.
    """
    if isinstance(window, WindowSpec):
        return window

    raise ConfigurationError("Engine window operations require a WindowSpec")


__all__ = [
    "WindowSpec",
    "normalize_window",
]
