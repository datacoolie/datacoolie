"""Shared JSON framing policy, independent of parsers and transports."""

from collections.abc import Mapping
from typing import Any

from datacoolie.core.exceptions import ConfigurationError


def resolve_json_multiline(fmt: str, options: Mapping[str, Any] | None) -> bool:
    """Resolve document versus line framing without changing caller options."""
    multiline = fmt == "json"
    if options is not None and "multiLine" in options:
        value = options["multiLine"]
        if isinstance(value, bool):
            multiline = value
        elif isinstance(value, str) and value.strip().lower() in {"true", "false"}:
            multiline = value.strip().lower() == "true"
        else:
            raise ConfigurationError(
                "JSON option 'multiLine' must be boolean or 'true'/'false'"
            )
    if fmt == "jsonl" and multiline:
        raise ConfigurationError(
            "JSONL requires line-oriented reading; multiLine=true is conflicting"
        )
    return multiline
