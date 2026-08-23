"""Optional SQLGlot-based source resolution for Polars SQLContext."""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Any

from datacoolie.core.exceptions import EngineError
from datacoolie.core.qualified_names import QualifiedTableName
from datacoolie.engines._polars.relations import (
    DiscoveredTable,
    PolarsRelationRegistry,
)


@dataclass(frozen=True, slots=True)
class _CachedRewrite:
    sql: str
    logical_names: tuple[QualifiedTableName, ...]


class PolarsSqlResolver:
    """Resolve logical SQL sources and rewrite only their original spans."""

    def __init__(self, *, dialect: str | None = None) -> None:
        self._dialect = dialect
        self._cache: dict[tuple[str, int, str | None], _CachedRewrite] = {}
        self._cache_version = -1
        self._lock = RLock()

    def prepare(
        self,
        sql: str,
        *,
        registry: PolarsRelationRegistry,
        sql_context: Any,
    ) -> str:
        """Return SQL with indexed logical sources replaced by private aliases."""

        version = registry.version
        key = (sql, version, self._dialect)
        with self._lock:
            if version != self._cache_version:
                self._cache.clear()
                self._cache_version = version
            cached = self._cache.get(key)

        if cached is not None:
            relations = [registry.get(name) for name in cached.logical_names]
            registry.materialize(relations, sql_context)
            return cached.sql

        try:
            import sqlglot  # noqa: PLC0415
            from sqlglot import exp  # noqa: PLC0415
            from sqlglot.optimizer.scope import traverse_scope  # noqa: PLC0415
        except ImportError as exc:
            raise EngineError(
                "Qualified/indexed Polars SQL requires SQLGlot; install "
                'pip install "datacoolie[polars-sql]"'
            ) from exc

        try:
            expressions = (
                sqlglot.parse(sql, read=self._dialect)
                if self._dialect
                else sqlglot.parse(sql)
            )
        except Exception as exc:  # noqa: BLE001
            raise EngineError(
                f"Unable to parse Polars SQL for table resolution: {exc}"
            ) from exc
        if len(expressions) != 1 or expressions[0] is None:
            raise EngineError(
                "PolarsEngine.execute_sql accepts exactly one SQL statement"
            )

        replacements: dict[tuple[int, int], str] = {}
        relations_by_name: dict[tuple[str, ...], DiscoveredTable] = {}
        expression = expressions[0]
        for scope in traverse_scope(expression):
            for _, (_, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue
                identifiers = source.parts
                if not identifiers:
                    continue
                parts = tuple(identifier.name for identifier in identifiers)
                quoted = tuple(
                    bool(identifier.args.get("quoted")) for identifier in identifiers
                )
                relation = registry.resolve(parts, quoted=quoted)
                if relation is None:
                    continue
                start = identifiers[0].meta.get("start")
                end = identifiers[-1].meta.get("end")
                if not isinstance(start, int) or not isinstance(end, int):
                    raise EngineError(
                        "SQLGlot did not provide source spans for table reference "
                        f"{'.'.join(parts)!r}; use a supported sqlglot version"
                    )
                replacements[(start, end + 1)] = relation.alias
                relations_by_name[relation.logical_name.normalized] = relation

        materialized = registry.materialize(relations_by_name.values(), sql_context)
        if materialized:
            registry.update_last_report_materialized(materialized)

        rewritten = sql
        for (start, end), alias in sorted(replacements.items(), reverse=True):
            rewritten = rewritten[:start] + alias + rewritten[end:]

        logical_names = tuple(
            relation.logical_name
            for relation in sorted(
                relations_by_name.values(),
                key=lambda item: item.logical_name.normalized,
            )
        )
        with self._lock:
            self._cache[key] = _CachedRewrite(rewritten, logical_names)
        return rewritten
