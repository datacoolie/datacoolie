"""Resolve classified SQL references to executable SQL content."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from datacoolie.core.exceptions import MetadataError
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.metadata.resolution.query import QueryReference, classify_query
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.component_paths import (
    ComponentPathError,
    normalize_component_paths,
    select_prefixed_root,
)
from datacoolie.utils.path_utils import normalize_path


logger = get_logger(__name__)


def resolve_query(
    query: Optional[str],
    platform: BasePlatform,
    *,
    sql_base_path: str | Sequence[str] | None = None,
    artifact_base_path: Optional[str] = None,
) -> str | None:
    """Resolve a declared query to executable SQL content."""

    reference = classify_query(query)
    if not reference.is_file:
        return query

    assert reference.relative_path is not None
    base_name = "sql_base_path"
    selected_relative = reference.relative_path
    if reference.scheme == "artifact":
        if artifact_base_path is None or not normalize_path(artifact_base_path):
            raise MetadataError(
                f"SQL file reference {reference.declared!r} requires artifact_base_path",
                details={"query": reference.declared, "base": "artifact_base_path"},
            )
        base = normalize_path(artifact_base_path)
        base_name = "artifact_base_path"
    else:
        if sql_base_path is not None:
            if isinstance(sql_base_path, str) and not sql_base_path.strip():
                raise MetadataError(
                    f"SQL file reference {reference.declared!r} requires sql_base_path",
                    details={"query": reference.declared},
                )
            try:
                roots = normalize_component_paths(
                    sql_base_path,
                    name="sql_base_path",
                    artifact_base_path=artifact_base_path,
                    allow_empty=False,
                ) or ()
                selected, selected_relative = select_prefixed_root(
                    roots,
                    reference.relative_path,
                    name="SQL file",
                    allow_unprefixed_single=True,
                )
                base = selected.base_path
            except ComponentPathError as exc:
                raise MetadataError(
                    str(exc), details={"query": reference.declared}
                ) from exc
            base_name = "sql_base_path"
        elif artifact_base_path is not None and normalize_path(artifact_base_path):
            # Artifact-only execution treats the declared path as a path
            # below the artifact root.  No ``sql`` convention or manifest
            # descriptor is consulted, so ``sql/orders.sql`` maps to
            # ``<artifact>/sql/orders.sql`` and nested component paths remain
            # intact.
            base = normalize_path(artifact_base_path)
            base_name = "artifact_base_path"
        else:
            raise MetadataError(
                f"SQL file reference {reference.declared!r} requires "
                f"{'sql_base_path' if sql_base_path is not None else 'artifact_base_path'}",
                details={"query": reference.declared},
            )

    try:
        content = platform.read_file_under_base(base, selected_relative)
    except Exception as exc:
        if isinstance(exc, MetadataError):
            raise
        raise MetadataError(
            f"Cannot read SQL file {reference.relative_path!r} below {base_name}",
            details={"query": reference.declared, "base_path": normalize_path(base)},
        ) from exc

    if not isinstance(content, str) or not content.strip():
        raise MetadataError(
            f"SQL file {reference.relative_path!r} is empty",
            details={"query": reference.declared, "base_path": normalize_path(base)},
        )
    logger.debug(
        "Resolved SQL file reference %s from %s=%s",
        selected_relative,
        base_name,
        _safe_display_path(normalize_path(base)),
        extra={"event_name": LogEvent.QUERY_FILE_RESOLVED.value},
    )
    return content


def _safe_display_path(path: str) -> str:
    """Remove URI credentials/query material from a diagnostic path."""
    try:
        parsed = urlsplit(path)
        if parsed.scheme and parsed.netloc:
            # Do not render userinfo, port, query or fragment in logs.  The
            # hostname is enough to identify the configured storage location.
            return urlunsplit((parsed.scheme, parsed.hostname or "", parsed.path, "", ""))
    except ValueError:
        return "<unavailable>"
    return path.split("?", 1)[0].split("#", 1)[0]


__all__ = ["QueryReference", "classify_query", "resolve_query"]
