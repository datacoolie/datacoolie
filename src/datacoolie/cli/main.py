"""The DataCoolie project/metadata CLI entry point."""

from __future__ import annotations

import sys
from typing import Sequence

from .parser import CLIUsageError, create_parser
from .render import choose_format, render, render_error
from .responses import (
    _error_payload,
    _exit_code,
    _failure_from_result,
    _success_payload,
)

def execute(args):
    """Dispatch one parsed command without importing the command graph eagerly."""

    from .commands import execute as _execute

    return _execute(args)

def _format_hint(argv: Sequence[str]) -> str | None:
    selected: str | None = None
    for index, value in enumerate(argv):
        if value == "--format" and index + 1 < len(argv):
            candidate = argv[index + 1]
            if candidate in {"text", "json"}:
                selected = candidate
        if value.startswith("--format="):
            candidate = value.partition("=")[2]
            if candidate in {"text", "json"}:
                selected = candidate
    return selected

def _emit_success(value: object, *, requested: str | None) -> None:
    mode = choose_format(requested)
    if mode == "json":
        render(_success_payload(value), requested="json", stream=sys.stdout)
    else:
        render(value, requested="text", stream=sys.stdout)

def _emit_error(payload: dict[str, object], *, requested: str | None) -> None:
    render_error(payload, requested=requested)

def main(argv: Sequence[str] | None = None) -> int:
    parser = create_parser()
    values = list(sys.argv[1:] if argv is None else argv)
    requested = _format_hint(values)
    try:
        args = parser.parse_args(values)
    except CLIUsageError as exc:
        if exc.usage and choose_format(requested) == "text":
            sys.stderr.write(exc.usage)
        _emit_error(_error_payload(exc), requested=requested)
        return 2
    try:
        result = execute(args)
    except CLIUsageError as exc:
        if exc.usage and choose_format(getattr(args, "format", requested)) == "text":
            sys.stderr.write(exc.usage)
        _emit_error(
            _error_payload(exc),
            requested=getattr(args, "format", requested),
        )
        return 2
    except Exception as exc:
        _emit_error(
            _error_payload(exc),
            requested=getattr(args, "format", requested),
        )
        return _exit_code(exc)
    if isinstance(result, dict):
        failure = _failure_from_result(result)
        if failure is not None:
            _emit_error(failure, requested=getattr(args, "format", None))
            return 1
    try:
        _emit_success(result, requested=getattr(args, "format", None))
    except (TypeError, ValueError) as exc:
        # ``render`` serializes before writing JSON, so this fallback cannot
        # leave a partially written machine-readable document behind.
        _emit_error(_error_payload(exc), requested=getattr(args, "format", requested))
        return 1
    return 0

__all__ = ["execute", "main"]
