"""Structured identifiers used to resolve portable SQL relation names."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


class QualifiedTableNameError(ValueError):
    """Raised when a logical table identifier is invalid."""


NameInput = str | Sequence[str]


def _coerce_parts(value: NameInput, *, allow_empty: bool = False) -> tuple[str, ...]:
    if isinstance(value, str):
        raw_parts = value.split(".") if value else ()
    else:
        raw_parts = tuple(value)

    parts = tuple(str(part).strip() for part in raw_parts)
    if not parts and allow_empty:
        return ()
    if not parts:
        raise QualifiedTableNameError(
            "A qualified table name must contain at least one component"
        )
    if any(not part for part in parts):
        raise QualifiedTableNameError(
            f"Qualified table name contains an empty component: {value!r}"
        )
    return parts


def parse_name_prefix(
    value: NameInput | None, *, default: tuple[str, ...] = ()
) -> tuple[str, ...]:
    """Parse an optional logical prefix without requiring a table component."""

    if value is None:
        return default
    parts = _coerce_parts(value, allow_empty=True)
    if len(parts) > 3:
        raise QualifiedTableNameError(
            f"A logical prefix may contain at most 3 components, got {len(parts)}: {value!r}"
        )
    return parts


@dataclass(frozen=True, slots=True)
class QualifiedTableName:
    """Canonical 1-4 part logical table name.

    Middle components intentionally have no database/schema-specific type;
    their position carries the namespace meaning for the active source.
    """

    parts: tuple[str, ...]

    def __post_init__(self) -> None:
        parts = _coerce_parts(self.parts)
        if len(parts) > 4:
            raise QualifiedTableNameError(
                "A logical SQL table name may contain at most 4 components; "
                f"got {len(parts)} for {'.'.join(parts)!r}. Narrow the discovery root "
                "or supply a shorter logical_prefix."
            )
        object.__setattr__(self, "parts", parts)

    @classmethod
    def parse(cls, value: NameInput) -> QualifiedTableName:
        """Build a logical name from a dotted string or component sequence."""

        return cls(_coerce_parts(value))

    @property
    def normalized(self) -> tuple[str, ...]:
        """Return the case-insensitive key used for unquoted SQL lookup."""

        return tuple(part.casefold() for part in self.parts)

    def suffix(self, levels: int) -> tuple[str, ...]:
        """Return the last *levels* components."""

        if levels < 1 or levels > len(self.parts):
            raise QualifiedTableNameError(
                f"Suffix levels must be between 1 and {len(self.parts)}, got {levels}"
            )
        return self.parts[-levels:]

    def normalized_suffix(self, levels: int) -> tuple[str, ...]:
        """Return a case-insensitive suffix key."""

        return self.normalized[-levels:]

    def __str__(self) -> str:
        return ".".join(self.parts)
