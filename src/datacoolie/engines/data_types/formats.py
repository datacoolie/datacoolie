"""Format-specific native-schema compatibility rules.

This module contains only pure target rules. It does not inspect a table or
import a writer. Native adapters remain responsible for applying the returned
target to their engine's schema.
"""

from __future__ import annotations

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import ConfigurationError


_SUPPORTED_OUTPUT_FORMATS = frozenset(
    {Format.PARQUET.value, Format.DELTA.value, Format.ICEBERG.value}
)


def normalize_output_format(value: str | Format) -> str:
    if isinstance(value, Format):
        value = value.value
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError("Output format must be a non-empty string")
    normalized = value.strip().lower()
    if normalized not in _SUPPORTED_OUTPUT_FORMATS:
        raise ConfigurationError(
            f"Unsupported datatype output format: {value!r}",
            details={"supported": sorted(_SUPPORTED_OUTPUT_FORMATS)},
        )
    return normalized


def output_type_for_format(
    logical_type: str,
    output_format: str | Format,
) -> str:
    """Return the format target for a native logical scalar alias.

    This function does not interpret vendor/source datatype names.  Delta and
    Parquet retain the logical integral widths. Iceberg's Spark compatibility
    layer promotes byte/short/int to ``int``.
    """

    normalized = normalize_output_format(output_format)
    if not isinstance(logical_type, str) or not logical_type.strip():
        raise ConfigurationError("Logical datatype must be a non-empty string")
    target = " ".join(logical_type.strip().lower().split())
    if normalized != Format.ICEBERG.value:
        return target
    if target in {"tinyint", "smallint", "int"}:
        return "int"
    return target
