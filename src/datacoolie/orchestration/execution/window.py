"""Attempt-local watermark window construction."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY
from datacoolie.core.exceptions import TransformError
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.transformers.base import ColumnMapping


def build_watermark_window(
    *,
    enabled: bool,
    watermark_effective: Mapping[str, Any] | None,
    watermark_after: Mapping[str, Any] | None,
    explicit_start: Mapping[str, Any] | None,
    explicit_end: Mapping[str, Any] | None,
    start_operator: str,
    end_operator: str,
    watermark_kind: str | None = None,
) -> WindowSpec | None:
    """Build a destructive replacement scope for one execution attempt.

    Explicit replay bounds take precedence over source observations. For a
    rolling run the observed source maximum is the upper bound. A column with
    a missing lower or upper value is omitted; no missing value authorizes a
    broader delete scope.
    """

    if not enabled:
        return None
    lower = dict(explicit_start or watermark_effective or {})
    upper = dict(explicit_end or watermark_after or {})
    bounds = {
        column: (lower[column], upper[column])
        for column in lower.keys() & upper.keys()
        if column != DATE_FOLDER_PARTITION_KEY
        and lower[column] is not None and upper[column] is not None
    }
    if not bounds:
        return None
    # Rolling reads have no explicit upper filter and replace through the
    # observed max. Replay uses its explicit half-open bound unchanged.
    if explicit_end is not None:
        effective_end_operator = end_operator
    elif watermark_kind == "request_end":
        # A request-end state represents an exclusive covered ceiling.  The
        # replacement scope must delete through that ceiling exclusively.
        effective_end_operator = "<"
    else:
        # Observed row maxima are inclusive replacement bounds.
        effective_end_operator = "<="
    return WindowSpec(
        bounds=bounds,
        lower_operator=start_operator,
        upper_operator=effective_end_operator,
        combine_operator="OR",
    )


def map_watermark_window(
    window: WindowSpec | None,
    *,
    column_mapping: ColumnMapping | None,
    output_columns: list[str],
) -> WindowSpec | None:
    """Map source-side bounds to final destination column names.

    A replacement operation is destructive.  If a transformer renamed,
    removed, collided, or otherwise changed an active watermark column without
    reporting a deterministic mapping, fail before the writer can mutate the
    target.  Bounds and operators themselves are preserved verbatim.
    """

    if window is None:
        return None
    if column_mapping is None or not column_mapping.known:
        raise TransformError(
            "Cannot resolve replacement watermark columns after transformation"
        )

    final = {column.casefold(): column for column in output_columns}
    mapped: dict[str, tuple[Any, Any]] = {}
    mapped_keys: set[str] = set()
    for source, bounds in window.items():
        output = column_mapping.resolve(source)
        if output is None:
            raise TransformError(
                f"Replacement watermark column {source!r} was removed or is unmapped"
            )
        output = final.get(output.casefold())
        if output is None:
            raise TransformError(
                f"Replacement watermark column {source!r} is absent from transformed output"
            )
        output_key = output.casefold()
        if output_key in mapped_keys:
            raise TransformError(
                f"Replacement watermark columns collide on transformed output {output!r}"
            )
        mapped_keys.add(output_key)
        mapped[output] = bounds

    return WindowSpec(
        bounds=mapped,
        lower_operator=window.lower_operator,
        upper_operator=window.upper_operator,
        combine_operator=window.combine_operator,
    )


__all__ = ["build_watermark_window", "map_watermark_window"]
