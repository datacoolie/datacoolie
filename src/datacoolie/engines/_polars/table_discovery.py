"""Delta path and Iceberg catalog discovery for Polars logical relations."""

from __future__ import annotations

from collections import deque
from typing import Any, Callable, Iterable, Literal, Sequence

from datacoolie.core.exceptions import EngineError
from datacoolie.core.qualified_names import (
    NameInput,
    QualifiedTableName,
    parse_name_prefix,
)
from datacoolie.engines._polars.relations import (
    DiscoveredTable,
    PatternInput,
    RegistrationLimitError,
    logical_name_selected,
)
from datacoolie.utils.path_utils import normalize_path


OnError = Literal["raise", "skip"]


def _validate_options(
    *, max_depth: int | None, max_tables: int | None, on_error: OnError
) -> None:
    if max_depth is not None and max_depth < 0:
        raise EngineError(f"max_depth must be non-negative, got {max_depth}")
    if max_tables is not None and max_tables < 1:
        raise EngineError(f"max_tables must be positive, got {max_tables}")
    if on_error not in ("raise", "skip"):
        raise EngineError(f"on_error must be 'raise' or 'skip', got {on_error!r}")


def _raise_or_record(
    on_error: OnError, failures: list[str], message: str, exc: Exception
) -> None:
    if on_error == "raise":
        raise EngineError(f"{message}: {exc}") from exc
    failures.append(f"{message}: {exc}")


def _leaf(path: str) -> str:
    return normalize_path(path).rstrip("/").rsplit("/", 1)[-1]


def discover_path_tables(
    *,
    platform: Any,
    base_path: str,
    marker_directory: str,
    source_kind: str,
    loader_factory: Callable[[str], Any],
    logical_prefix: NameInput | None = (),
    recursive: bool = False,
    max_depth: int | None = None,
    include: PatternInput = None,
    exclude: PatternInput = None,
    max_tables: int | None = None,
    on_error: OnError = "raise",
) -> tuple[list[DiscoveredTable], tuple[str, ...]]:
    """Discover marked table directories without invoking frame loaders."""

    _validate_options(max_depth=max_depth, max_tables=max_tables, on_error=on_error)
    prefix = parse_name_prefix(logical_prefix)
    failures: list[str] = []
    try:
        children = sorted(platform.list_folders(base_path))
    except Exception as exc:  # noqa: BLE001
        _raise_or_record(
            on_error, failures, f"Failed to list table root {base_path!r}", exc
        )
        return [], tuple(failures)

    queue = deque((child, (_leaf(child),), 1) for child in children)
    discovered: list[DiscoveredTable] = []
    while queue:
        path, relative_parts, depth = queue.popleft()
        normalized_path = normalize_path(path).rstrip("/")
        marker_path = f"{normalized_path}/{marker_directory}"
        try:
            is_table = platform.folder_exists(marker_path)
        except Exception as exc:  # noqa: BLE001
            _raise_or_record(
                on_error, failures, f"Failed to inspect {normalized_path!r}", exc
            )
            continue

        if is_table:
            logical_name = QualifiedTableName(prefix + relative_parts)
            if logical_name_selected(logical_name, include=include, exclude=exclude):
                discovered.append(
                    DiscoveredTable(
                        logical_name=logical_name,
                        source_kind=source_kind,
                        source_identifier=normalized_path,
                        loader=lambda path=path: loader_factory(path),
                    )
                )
                if max_tables is not None and len(discovered) > max_tables:
                    raise RegistrationLimitError(
                        f"Discovery under {base_path!r} exceeded max_tables={max_tables}; "
                        "narrow the root or add include/exclude patterns"
                    )
            continue

        if not recursive or (max_depth is not None and depth >= max_depth):
            continue
        try:
            nested = sorted(platform.list_folders(path))
        except Exception as exc:  # noqa: BLE001
            _raise_or_record(
                on_error, failures, f"Failed to list {normalized_path!r}", exc
            )
            continue
        for child in nested:
            queue.append((child, relative_parts + (_leaf(child),), depth + 1))

    discovered.sort(key=lambda item: item.logical_name.normalized)
    return discovered, tuple(failures)


def _identifier_parts(identifier: Any) -> tuple[str, ...]:
    if isinstance(identifier, str):
        return tuple(part for part in identifier.split(".") if part)
    if isinstance(identifier, Sequence):
        return tuple(str(part) for part in identifier)
    return (str(identifier),)


def _relative_identifier(
    identifier: tuple[str, ...], root: tuple[str, ...]
) -> tuple[str, ...]:
    if root and identifier[: len(root)] == root:
        return identifier[len(root) :]
    return identifier


def discover_iceberg_catalog_tables(
    *,
    catalog: Any,
    namespace: NameInput | None,
    loader_factory: Callable[[Any], Any],
    logical_prefix: NameInput | None = None,
    recursive: bool = False,
    max_depth: int | None = None,
    include: PatternInput = None,
    exclude: PatternInput = None,
    max_tables: int | None = None,
    on_error: OnError = "raise",
) -> tuple[list[DiscoveredTable], tuple[str, ...]]:
    """Discover Iceberg identifiers without loading table metadata."""

    _validate_options(max_depth=max_depth, max_tables=max_tables, on_error=on_error)
    root = () if namespace is None else _identifier_parts(namespace)
    if logical_prefix is None:
        raw_catalog_name = getattr(catalog, "name", "")
        catalog_name = (
            raw_catalog_name.strip() if isinstance(raw_catalog_name, str) else ""
        )
        logical_root = ((catalog_name,) if catalog_name else ()) + root
    else:
        logical_root = parse_name_prefix(logical_prefix)

    failures: list[str] = []
    discovered: list[DiscoveredTable] = []
    namespace_queue = deque([(root, 0)])
    visited: set[tuple[str, ...]] = set()

    while namespace_queue:
        current, depth = namespace_queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        try:
            table_ids = sorted(
                catalog.list_tables(current), key=lambda item: _identifier_parts(item)
            )
        except Exception as exc:  # noqa: BLE001
            _raise_or_record(
                on_error, failures, f"Failed to list Iceberg namespace {current!r}", exc
            )
            table_ids = []

        for table_id in table_ids:
            identifier = _identifier_parts(table_id)
            relative = _relative_identifier(identifier, root)
            logical_name = QualifiedTableName(logical_root + relative)
            if not logical_name_selected(
                logical_name, include=include, exclude=exclude
            ):
                continue
            discovered.append(
                DiscoveredTable(
                    logical_name=logical_name,
                    source_kind="iceberg",
                    source_identifier=identifier,
                    loader=lambda table_id=table_id: loader_factory(table_id),
                )
            )
            if max_tables is not None and len(discovered) > max_tables:
                raise RegistrationLimitError(
                    f"Iceberg discovery under {root!r} exceeded max_tables={max_tables}; "
                    "narrow the namespace or add include/exclude patterns"
                )

        if not recursive or (max_depth is not None and depth >= max_depth):
            continue
        try:
            children: Iterable[Any] = catalog.list_namespaces(current)
        except Exception as exc:  # noqa: BLE001
            _raise_or_record(
                on_error,
                failures,
                f"Failed to list child namespaces of {current!r}",
                exc,
            )
            continue
        for child in sorted((_identifier_parts(item) for item in children)):
            namespace_queue.append((child, depth + 1))

    discovered.sort(key=lambda item: item.logical_name.normalized)
    return discovered, tuple(failures)
