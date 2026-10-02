"""Validation of SQL query references in authored metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from datacoolie.utils.component_paths import (
    ComponentPathError,
    normalize_component_paths,
    select_prefixed_root,
)
from .reports import Diagnostic, _error, _warning

def _artifact_relative_candidate(
    roots: Sequence[Path],
    artifact_root: Path | None,
    relative_path: str,
) -> Path | None:
    """Return a full artifact-relative query when it names a configured root.

    A project may configure a nested SQL root such as ``shared/sql2`` while
    authored metadata uses either the root leaf (``sql2/orders.sql``) with an
    explicit ``sql_base_path`` or the full artifact-relative path
    (``shared/sql2/orders.sql``) with artifact-only execution.  Validation can
    accept the latter without weakening the explicit-root rule for unrelated
    paths.
    """

    if artifact_root is None:
        return None
    artifact = artifact_root.expanduser().resolve()
    candidate = artifact.joinpath(*relative_path.split("/"))
    try:
        candidate.relative_to(artifact)
    except ValueError:
        return None
    for root in roots:
        try:
            root_relative = root.expanduser().resolve().relative_to(artifact).as_posix()
        except ValueError:
            continue
        if relative_path == root_relative or relative_path.startswith(
            root_relative + "/"
        ):
            return candidate
    return None

def _validate_queries(
    metadata: dict[str, Any],
    errors: list[Diagnostic],
    warnings: list[Diagnostic],
    *,
    sql_root: Path | Sequence[Path] | None = None,
    artifact_root: Path | None = None,
) -> int:
    # Keep CLI import/help lightweight.  The preparation classifier is loaded
    # only when a metadata validation actually inspects a query reference;
    # merely importing the CLI must not import Driver or optional engines.
    from datacoolie.metadata.resolution.query import classify_query

    checked = 0
    for index, dataflow in enumerate(metadata.get("dataflows", [])):
        if not isinstance(dataflow, dict):
            continue
        source = dataflow.get("source")
        if not isinstance(source, dict) or source.get("query") is None:
            continue
        try:
            reference = classify_query(source.get("query"))
        except Exception as exc:
            _error(
                errors, "query.invalid", str(exc), f"dataflows[{index}].source.query"
            )
            continue
        if not reference.is_file or reference.relative_path is None:
            continue
        base: Path | None
        relative_path = reference.relative_path
        if reference.scheme == "artifact":
            base = artifact_root
        else:
            # A shorthand reference is qualified by the leaf name of one of
            # the configured SQL roots (``sql1/orders.sql`` selects the
            # ``.../sql1`` root).  This keeps multiple roots deterministic and
            # matches runtime query resolution.
            roots: tuple[Path, ...]
            if sql_root is None:
                roots = ()
            elif isinstance(sql_root, Path):
                roots = (sql_root,)
            else:
                roots = tuple(sql_root)
            if roots:
                try:
                    normalized_roots = (
                        normalize_component_paths(
                            [str(root) for root in roots],
                            name="sql_base_path",
                            allow_empty=False,
                        )
                        or ()
                    )
                    selected, relative_path = select_prefixed_root(
                        normalized_roots,
                        relative_path,
                        name="SQL file",
                        allow_unprefixed_single=True,
                    )
                    base = Path(selected.base_path)
                except (ComponentPathError, ValueError) as exc:
                    full_path = _artifact_relative_candidate(
                        roots,
                        artifact_root,
                        reference.relative_path,
                    )
                    if full_path is None or not full_path.is_file():
                        _error(
                            errors,
                            "query.prefix",
                            str(exc),
                            f"dataflows[{index}].source.query",
                        )
                        continue
                    # The declaration names the path from the artifact root;
                    # retain that representation for the existence/read check.
                    base = artifact_root
                    relative_path = reference.relative_path
            elif sql_root is not None:
                _error(
                    errors,
                    "query.base_missing",
                    f"SQL file reference {reference.declared!r} has no configured SQL root",
                    f"dataflows[{index}].source.query",
                )
                continue
            elif artifact_root is not None:
                # Artifact-only runtime keeps the declaration intact and
                # joins the whole relative path below the artifact root.
                base = artifact_root
            else:
                base = None
        if base is None:
            _warning(
                warnings,
                "query.not_checked",
                f"SQL file reference {reference.declared!r} was classified but no query base was supplied",
                f"dataflows[{index}].source.query",
            )
            continue
        candidate = base.joinpath(*relative_path.split("/"))
        try:
            candidate.resolve().relative_to(base.resolve())
        except ValueError:
            _error(
                errors,
                "query.escape",
                "SQL reference escapes its configured base",
                f"dataflows[{index}].source.query",
            )
            continue
        if candidate.is_symlink():
            _error(
                errors,
                "query.symlink",
                f"SQL file must not be a symlink: {candidate}",
                f"dataflows[{index}].source.query",
            )
            continue
        checked += 1
        if not candidate.is_file():
            _error(
                errors,
                "query.missing",
                f"SQL file not found: {candidate}",
                f"dataflows[{index}].source.query",
            )
        else:
            try:
                content = candidate.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                _error(
                    errors,
                    "query.read",
                    f"Cannot read SQL file {candidate}: {exc}",
                    f"dataflows[{index}].source.query",
                )
            else:
                if not content.strip():
                    _error(
                        errors,
                        "query.empty",
                        f"SQL file is empty: {candidate}",
                        f"dataflows[{index}].source.query",
                    )
    return checked
