"""Logical relation registry for :class:`datacoolie.engines.PolarsEngine`."""

from __future__ import annotations

from dataclasses import dataclass, replace
from fnmatch import fnmatchcase
from hashlib import blake2s
from threading import RLock
from typing import Any, Callable, Iterable, Literal, Sequence

from datacoolie.core.exceptions import EngineError
from datacoolie.core.qualified_names import QualifiedTableName


class RelationRegistryError(EngineError):
    """Base error for logical Polars relation registration."""


class DuplicateTableRegistrationError(RelationRegistryError):
    """Raised when one logical name points to different physical sources."""


class AmbiguousTableReferenceError(RelationRegistryError):
    """Raised when a short SQL name resolves to more than one table."""


class RelationMaterializationError(RelationRegistryError):
    """Raised when a lazy relation cannot be bound to SQLContext."""


class RegistrationLimitError(RelationRegistryError):
    """Raised when discovery exceeds its configured safety ceiling."""


RelationState = Literal["indexed", "materialized", "failed"]
PatternInput = str | Sequence[str] | None


@dataclass(slots=True)
class DiscoveredTable:
    """Source descriptor whose loader creates a Polars frame on first use."""

    logical_name: QualifiedTableName
    source_kind: str
    source_identifier: Any
    loader: Callable[[], Any]
    state: RelationState = "indexed"
    frame: Any | None = None
    error: str | None = None

    @property
    def alias(self) -> str:
        digest = blake2s(
            ".".join(self.logical_name.normalized).encode("utf-8"), digest_size=10
        ).hexdigest()
        return f"__dc_rel_{digest}"


@dataclass(frozen=True, slots=True)
class RegistrationReport:
    """Observable result of the most recent discovery/registration call."""

    indexed: tuple[str, ...] = ()
    materialized: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()

    def with_materialized(self, names: Iterable[str]) -> RegistrationReport:
        return replace(self, materialized=tuple(sorted(set(names))))


def _patterns(value: PatternInput) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _match_components(pattern: tuple[str, ...], value: tuple[str, ...]) -> bool:
    memo: dict[tuple[int, int], bool] = {}

    def visit(pattern_index: int, value_index: int) -> bool:
        key = (pattern_index, value_index)
        if key in memo:
            return memo[key]
        if pattern_index == len(pattern):
            result = value_index == len(value)
        elif pattern[pattern_index] == "**":
            result = visit(pattern_index + 1, value_index) or (
                value_index < len(value) and visit(pattern_index, value_index + 1)
            )
        else:
            result = (
                value_index < len(value)
                and fnmatchcase(value[value_index], pattern[pattern_index])
                and visit(pattern_index + 1, value_index + 1)
            )
        memo[key] = result
        return result

    return visit(0, 0)


def logical_name_matches(name: QualifiedTableName, pattern: str) -> bool:
    """Match a component glob against the full logical name and every suffix."""

    components = tuple(component.strip().casefold() for component in pattern.split("."))
    if not components or any(not component for component in components):
        raise RelationRegistryError(f"Invalid logical table pattern: {pattern!r}")
    normalized = name.normalized
    return any(
        _match_components(components, normalized[start:])
        for start in range(len(normalized))
    )


def logical_name_selected(
    name: QualifiedTableName,
    *,
    include: PatternInput = None,
    exclude: PatternInput = None,
) -> bool:
    """Return whether include/exclude patterns select *name*."""

    include_patterns = _patterns(include)
    exclude_patterns = _patterns(exclude)
    included = not include_patterns or any(
        logical_name_matches(name, pattern) for pattern in include_patterns
    )
    excluded = any(logical_name_matches(name, pattern) for pattern in exclude_patterns)
    return included and not excluded


class PolarsRelationRegistry:
    """Thread-safe logical-name index and SQLContext binding cache."""

    def __init__(self) -> None:
        self._relations: dict[tuple[str, ...], DiscoveredTable] = {}
        self._suffixes: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
        self._version = 0
        self._lock = RLock()
        self._last_report = RegistrationReport()

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def last_report(self) -> RegistrationReport:
        with self._lock:
            return self._last_report

    def __bool__(self) -> bool:
        with self._lock:
            return bool(self._relations)

    def registered_tables(self) -> list[str]:
        with self._lock:
            return sorted(
                str(relation.logical_name) for relation in self._relations.values()
            )

    def add_batch(
        self,
        relations: Iterable[DiscoveredTable],
        *,
        failures: Iterable[str] = (),
    ) -> RegistrationReport:
        """Atomically index descriptors, preserving idempotent duplicates."""

        candidates = list(relations)
        with self._lock:
            pending: dict[tuple[str, ...], DiscoveredTable] = {}
            skipped: list[str] = []
            for relation in candidates:
                key = relation.logical_name.normalized
                existing = self._relations.get(key) or pending.get(key)
                if existing is None:
                    pending[key] = relation
                    continue
                if (
                    existing.source_kind == relation.source_kind
                    and existing.source_identifier == relation.source_identifier
                ):
                    skipped.append(str(relation.logical_name))
                    continue
                raise DuplicateTableRegistrationError(
                    f"Logical table {relation.logical_name!s} is already registered from "
                    f"{existing.source_kind}:{existing.source_identifier!r}; cannot also register "
                    f"{relation.source_kind}:{relation.source_identifier!r}"
                )

            for key, relation in pending.items():
                self._relations[key] = relation
                for levels in range(1, len(key) + 1):
                    self._suffixes.setdefault(key[-levels:], set()).add(key)
            if pending:
                self._version += 1

            report = RegistrationReport(
                indexed=tuple(
                    sorted(str(item.logical_name) for item in pending.values())
                ),
                skipped=tuple(sorted(skipped)),
                failed=tuple(failures),
            )
            self._last_report = report
            return report

    def resolve(
        self,
        parts: Sequence[str],
        *,
        quoted: Sequence[bool] | None = None,
    ) -> DiscoveredTable | None:
        """Resolve a SQL reference by unique suffix, returning ``None`` if unknown."""

        if not 1 <= len(parts) <= 4:
            raise RelationRegistryError(
                f"SQL table references must contain 1-4 components, got {len(parts)}: "
                f"{'.'.join(parts)}"
            )
        normalized = tuple(part.casefold() for part in parts)
        with self._lock:
            keys = set(self._suffixes.get(normalized, ()))
            if quoted:
                keys = {
                    key
                    for key in keys
                    if all(
                        not is_quoted
                        or self._relations[key].logical_name.parts[-len(parts) + index]
                        == part
                        for index, (part, is_quoted) in enumerate(zip(parts, quoted))
                    )
                }
            if not keys:
                return None
            if len(keys) > 1:
                candidates = sorted(
                    str(self._relations[key].logical_name) for key in keys
                )
                raise AmbiguousTableReferenceError(
                    f"Ambiguous table reference {'.'.join(parts)!r}; qualify it further. "
                    f"Candidates: {', '.join(candidates)}"
                )
            return self._relations[next(iter(keys))]

    def get(self, logical_name: QualifiedTableName) -> DiscoveredTable:
        with self._lock:
            return self._relations[logical_name.normalized]

    def materialize(
        self, relations: Iterable[DiscoveredTable], sql_context: Any
    ) -> tuple[str, ...]:
        """Load and bind unmaterialized relations once, rolling back partial binds."""

        requested = list(
            dict.fromkeys(relation.logical_name.normalized for relation in relations)
        )
        with self._lock:
            existing_aliases = set(sql_context.tables())
            pending = [
                self._relations[key]
                for key in requested
                if self._relations[key].state != "materialized"
                or self._relations[key].alias not in existing_aliases
            ]
            if not pending:
                return ()

            loaded: list[tuple[DiscoveredTable, Any]] = []
            try:
                for relation in pending:
                    frame = (
                        relation.frame
                        if relation.state == "materialized"
                        and relation.frame is not None
                        else relation.loader()
                    )
                    loaded.append((relation, frame))
            except Exception as exc:  # noqa: BLE001
                relation.state = "failed"
                relation.error = str(exc)
                raise RelationMaterializationError(
                    f"Failed to create Polars relation for {relation.logical_name!s}: {exc}"
                ) from exc

            conflicting = [
                relation.alias
                for relation, _ in loaded
                if relation.alias in existing_aliases
            ]
            if conflicting:
                raise RelationMaterializationError(
                    "Generated private SQLContext alias already exists: "
                    + ", ".join(conflicting)
                )

            bound: list[str] = []
            try:
                for relation, frame in loaded:
                    sql_context.register(relation.alias, frame)
                    bound.append(relation.alias)
            except Exception as exc:  # noqa: BLE001
                if bound:
                    sql_context.unregister(bound)
                raise RelationMaterializationError(
                    f"Failed to register Polars relation {relation.logical_name!s}: {exc}"
                ) from exc

            names: list[str] = []
            for relation, frame in loaded:
                relation.frame = frame
                relation.state = "materialized"
                relation.error = None
                names.append(str(relation.logical_name))
            return tuple(sorted(names))

    def update_last_report_materialized(self, names: Iterable[str]) -> None:
        with self._lock:
            self._last_report = self._last_report.with_materialized(names)

    def update_last_report_failure(self, message: str) -> None:
        with self._lock:
            self._last_report = replace(
                self._last_report,
                failed=self._last_report.failed + (message,),
            )
