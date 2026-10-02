"""Stable text/JSON result rendering for the DataCoolie CLI."""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO


def choose_format(requested: str | None, *, stream: TextIO | None = None) -> str:
    if requested in {"text", "json"}:
        return requested
    output = stream or sys.stdout
    return "text" if output.isatty() else "json"


def render(value: Any, *, requested: str | None = None, stream: TextIO | None = None) -> None:
    output = stream or sys.stdout
    mode = choose_format(requested, stream=output)
    if mode == "json":
        encoded = json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False)
        output.write(encoded + "\n")
        return
    if isinstance(value, dict):
        _render_mapping(value, output, 0)
    elif isinstance(value, list):
        _render_list(value, output, 0)
    else:
        output.write(f"{value}\n")


def _render_mapping(value: dict[str, Any], output: TextIO, level: int) -> None:
    prefix = "  " * level
    if not value:
        output.write(f"{prefix}{{}}\n")
        return
    for key, item in value.items():
        if isinstance(item, dict):
            if item:
                output.write(f"{prefix}{key}:\n")
                _render_mapping(item, output, level + 1)
            else:
                output.write(f"{prefix}{key}: {{}}\n")
        elif isinstance(item, list):
            output.write(f"{prefix}{key}:\n")
            _render_list(item, output, level + 1)
        else:
            output.write(f"{prefix}{key}: {item}\n")


def _render_list(value: list[Any], output: TextIO, level: int) -> None:
    prefix = "  " * level
    if not value:
        output.write(f"{prefix}[]\n")
        return
    for child in value:
        if isinstance(child, dict):
            output.write(f"{prefix}-\n")
            _render_mapping(child, output, level + 1)
        elif isinstance(child, list):
            output.write(f"{prefix}-\n")
            _render_list(child, output, level + 1)
        else:
            output.write(f"{prefix}- {child}\n")


def render_error(
    value: dict[str, Any],
    *,
    requested: str | None = None,
    stream: TextIO | None = None,
) -> None:
    """Render a CLI error to the selected machine or human channel."""

    mode = choose_format(requested, stream=sys.stdout)
    output = stream or (sys.stdout if mode == "json" else sys.stderr)
    if mode == "json":
        render(value, requested="json", stream=output)
        return
    error = value.get("error")
    if isinstance(error, dict):
        code = error.get("code", "operation.failed")
        message = error.get("message", "Command failed")
        output.write(f"Error [{code}]: {message}\n")
    else:
        output.write("Command failed\n")
    details = value.get("data")
    if isinstance(details, dict):
        _render_mapping(details, output, 0)
    elif isinstance(details, list):
        _render_list(details, output, 0)
    elif details is not None:
        output.write(f"{details}\n")


__all__ = ["choose_format", "render", "render_error"]
