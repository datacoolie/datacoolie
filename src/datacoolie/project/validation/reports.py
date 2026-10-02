"""Diagnostic report types shared by project validation services."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Sequence

@dataclass(frozen=True)
class Diagnostic:
    severity: str
    code: str
    message: str
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        if self.path is None:
            result.pop("path")
        return result

@dataclass
class ValidationReport:
    scope: str
    errors: list[Diagnostic]
    warnings: list[Diagnostic]
    checks: list[str]
    details: dict[str, Any]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "ok": self.ok,
            "scope": self.scope,
            "errors": [item.to_dict() for item in self.errors],
            "warnings": [item.to_dict() for item in self.warnings],
            "checks": self.checks,
            "details": self.details,
        }

def _error(
    errors: list[Diagnostic], code: str, message: str, path: str | None = None
) -> None:
    errors.append(Diagnostic("error", code, message, path))

def _warning(
    warnings: list[Diagnostic], code: str, message: str, path: str | None = None
) -> None:
    warnings.append(Diagnostic("warning", code, message, path))

def _add_schema_summary(
    details: dict[str, Any], reports: Sequence[ValidationReport]
) -> None:
    """Expose the effective schema identities used by nested validations."""

    schema_reports = [
        report.details
        for report in reports
        if report.details.get("schema_version") and report.details.get("schema_url")
    ]
    if not schema_reports:
        return
    details["framework_versions"] = sorted(
        {
            str(item["framework_version"])
            for item in schema_reports
            if item.get("framework_version")
        }
    )
    details["schema_versions"] = sorted(
        {str(item["schema_version"]) for item in schema_reports}
    )
    details["schema_urls"] = sorted(
        {str(item["schema_url"]) for item in schema_reports}
    )
    if len(details["framework_versions"]) == 1:
        details["framework_version"] = details["framework_versions"][0]
    if len(details["schema_versions"]) == 1:
        details["schema_version"] = details["schema_versions"][0]
    if len(details["schema_urls"]) == 1:
        details["schema_url"] = details["schema_urls"][0]


__all__ = ["Diagnostic", "ValidationReport"]
