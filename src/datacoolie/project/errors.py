"""Errors raised by DataCoolie project services and the project CLI."""

from __future__ import annotations


class ProjectError(Exception):
    """A user-actionable project/configuration error."""

    def __init__(self, message: str, *, details: object | None = None) -> None:
        super().__init__(message)
        self.details = details


class ProjectConfigError(ProjectError):
    """The project marker or one of its values is invalid."""


class ProjectValidationError(ProjectError):
    """A project, metadata document, or artifact failed validation."""


class ProjectDependencyError(ProjectError):
    """A selected operation requires an optional dependency that is missing."""

    def __init__(
        self,
        message: str,
        *,
        dependency: str | None = None,
        exit_code: int = 1,
        details: object | None = None,
    ) -> None:
        super().__init__(message, details=details)
        self.dependency = dependency
        self.exit_code = exit_code


__all__ = [
    "ProjectError",
    "ProjectConfigError",
    "ProjectDependencyError",
    "ProjectValidationError",
]
