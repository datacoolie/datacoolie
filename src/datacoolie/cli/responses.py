"""Machine-readable CLI envelopes and error mapping."""

from __future__ import annotations

from datacoolie import __version__
from datacoolie.project.errors import (
    ProjectConfigError,
    ProjectDependencyError,
    ProjectError,
    ProjectValidationError,
)
from .parser import CLIUsageError


# Additive fields are compatible; increment only for incompatible envelopes.
CLI_SCHEMA_VERSION = 1

def _error_payload(exc: Exception) -> dict[str, object]:
    error: dict[str, object] = {
        "code": _error_code(exc),
        "message": str(exc),
    }
    if isinstance(exc, ProjectDependencyError) and exc.dependency:
        error["dependency"] = exc.dependency
    details = _normalise_report_data(getattr(exc, "details", None))
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "datacoolie_version": __version__,
        "ok": False,
        "data": details,
        "error": error,
    }

def _normalise_report_data(value: object) -> object:
    """Keep domain diagnostic reports below the CLI envelope only once."""

    if isinstance(value, dict) and _is_validation_report(value):
        return {
            key: item
            for key, item in value.items()
            if key not in {"schema_version", "ok"}
        }
    return value

def _error_code(exc: Exception) -> str:
    if isinstance(exc, CLIUsageError):
        return "usage.invalid"
    if isinstance(exc, ProjectDependencyError):
        return "dependency.missing"
    if isinstance(exc, ProjectConfigError):
        return "project.config_invalid"
    if isinstance(exc, ProjectValidationError):
        return "validation.failed"
    if isinstance(exc, ProjectError):
        return "operation.failed"
    return "internal.error"

def _exit_code(exc: Exception) -> int:
    if isinstance(exc, CLIUsageError):
        return 2
    if isinstance(exc, ProjectDependencyError):
        return exc.exit_code
    if isinstance(exc, ProjectConfigError):
        return 2
    return 1

def _success_payload(value: object) -> dict[str, object]:
    data = value
    if isinstance(value, dict) and _is_validation_report(value):
        data = {
            key: item
            for key, item in value.items()
            if key not in {"schema_version", "ok"}
        }
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "datacoolie_version": __version__,
        "ok": True,
        "data": data,
    }

def _is_validation_report(value: dict[str, object]) -> bool:
    return {
        "schema_version",
        "ok",
        "scope",
        "errors",
        "warnings",
        "checks",
        "details",
    }.issubset(value)

def _failure_from_result(value: dict[str, object]) -> dict[str, object] | None:
    if not _is_validation_report(value) or value.get("ok") is not False:
        return None
    data = {
        key: item
        for key, item in value.items()
        if key not in {"schema_version", "ok"}
    }
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "datacoolie_version": __version__,
        "ok": False,
        "data": data,
        "error": {
            "code": "validation.failed",
            "message": "Validation failed",
        },
    }

__all__ = ["CLI_SCHEMA_VERSION"]
