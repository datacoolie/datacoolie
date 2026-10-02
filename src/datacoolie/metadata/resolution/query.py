"""Pure classification of inline and file-backed SQL metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional

from datacoolie.core.exceptions import MetadataError
from datacoolie.utils.path_utils import ensure_relative_path


_SHORTHAND_RE = re.compile(
    r"^[\w.-]+(?:[/\\][\w.-]+)*\.sql$",
    flags=re.IGNORECASE,
)
_ABSOLUTE_PATH_RE = re.compile(r"^(?:[A-Za-z]:[/\\]|\\\\|/)")
_URI_PATH_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:/")


@dataclass(frozen=True, slots=True)
class QueryReference:
    """Classification result for a declared query value."""

    declared: str
    kind: Literal["inline", "file"]
    relative_path: Optional[str] = None
    scheme: Optional[Literal["shorthand", "artifact"]] = None

    @property
    def is_file(self) -> bool:
        return self.kind == "file"


def _invalid_reference(value: str, reason: str) -> MetadataError:
    return MetadataError(
        f"Invalid SQL file reference {value!r}: {reason}",
        details={"query": value},
    )


def _explicit_reference(value: str, *, declared: Optional[str] = None) -> QueryReference:
    target = value[len("artifact:") :]
    if not target.startswith("/") or target.startswith("//"):
        raise _invalid_reference(value, "expected artifact:/<relative-path>")
    target = target[1:]
    try:
        target = ensure_relative_path(target)
    except ValueError as exc:
        raise _invalid_reference(value, str(exc)) from exc
    return QueryReference(
        declared=value if declared is None else declared,
        kind="file",
        relative_path=target,
        scheme="artifact",
    )


def classify_query(query: Optional[str]) -> QueryReference:
    """Classify *query* using string shape only; never probe storage."""

    if query is None:
        return QueryReference(declared="", kind="inline")
    if not isinstance(query, str):
        raise MetadataError(
            f"Source.query must be a string or null, got {type(query).__name__}",
        )

    value = query.strip()
    if not value:
        return QueryReference(declared=query, kind="inline")
    if value.startswith("--") or value.startswith("/*"):
        return QueryReference(declared=query, kind="inline")
    if value.startswith("artifact:"):
        return _explicit_reference(value, declared=query)

    if _SHORTHAND_RE.fullmatch(value):
        try:
            relative = ensure_relative_path(value)
        except ValueError as exc:
            raise _invalid_reference(value, str(exc)) from exc
        return QueryReference(
            declared=query,
            kind="file",
            relative_path=relative,
            scheme="shorthand",
        )

    lowered = value.lower()
    looks_like_file = (
        lowered.endswith(".sql") or ".sql?" in lowered or ".sql#" in lowered
    )
    if looks_like_file and (
        _ABSOLUTE_PATH_RE.match(value)
        or _URI_PATH_RE.match(value)
        or "://" in value
        or ".sql?" in lowered
        or ".sql#" in lowered
    ):
        raise _invalid_reference(
            value,
            "absolute, URI-shaped, or query/fragment paths are unsupported; "
            "configure a relative path below sql_base_path or artifact_base_path",
        )

    return QueryReference(declared=query, kind="inline")


__all__ = ["QueryReference", "classify_query"]
