"""Column selection, removal, and atomic rename."""

from __future__ import annotations

from datacoolie.core.constants import TRAILING_COLUMNS
from datacoolie.core.exceptions import ConfigurationError, EngineError
from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.transformers.base import BaseTransformer


class ColumnProjector(BaseTransformer[DF]):
    """Resolve select/drop against pre-rename names, then rename atomically."""

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 85

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        transform = dataflow.transform
        if not (
            transform.select_columns
            or transform.drop_columns
            or transform.rename_columns
        ):
            self._mark_skipped()
            return df

        changed = False

        actual = self._engine.get_columns(df)
        required = {
            *(column.lower() for column in dataflow.merge_keys),
            *(column.lower() for column in dataflow.partition_column_names),
        }
        reserved = {column.lower() for column in TRAILING_COLUMNS}
        trailing = [
            column
            for column in actual
            if column.lower() in reserved
        ]

        if transform.select_columns:
            selected = self._resolve(
                transform.select_columns, actual, transform.missing_column_policy
            )
            selected_lower = {column.lower() for column in selected}
            missing_required = required.difference(selected_lower)
            if missing_required:
                raise ConfigurationError(
                    "select_columns cannot remove merge or partition columns",
                    details={"columns": sorted(missing_required)},
                )
            selected.extend(
                column for column in trailing if column.lower() not in selected_lower
            )
            if selected != actual:
                df = self._engine.select_columns(df, selected)
                changed = True
        elif transform.drop_columns:
            dropped = self._resolve(
                transform.drop_columns, actual, transform.missing_column_policy
            )
            overlap = (required | reserved).intersection(
                column.lower() for column in dropped
            )
            if overlap:
                raise ConfigurationError(
                    "drop_columns cannot remove merge, partition, or framework-reserved columns",
                    details={"columns": sorted(overlap)},
                )
            if dropped:
                df = self._engine.drop_columns(df, dropped)
                changed = True

        current = self._engine.get_columns(df)
        resolved_renames: list[tuple[str, str]] = []
        current_lower = {column.lower(): column for column in current}
        for source, target in transform.rename_columns.items():
            try:
                actual_source = self._engine._resolve_column_name(current, source)
            except EngineError:
                if transform.missing_column_policy == "ignore":
                    continue
                raise
            if (
                actual_source.lower() in required
                or actual_source.lower() in reserved
                or target.lower() in reserved
            ):
                raise ConfigurationError(
                    "rename_columns cannot rename merge, partition, or framework-reserved columns",
                    details={"source": source, "target": target},
                )
            existing_target = current_lower.get(target.lower())
            if (
                existing_target is not None
                and existing_target.lower() != actual_source.lower()
            ):
                raise ConfigurationError(
                    "rename_columns cannot overwrite an existing column",
                    details={"source": source, "target": target},
                )
            resolved_renames.append((actual_source, target))
        if resolved_renames:
            df = self._engine.rename_columns(df, dict(resolved_renames))
            changed = True

        if changed:
            self._mark_applied()
        else:
            self._mark_skipped()
        return df

    def _resolve(
        self, requested: list[str], actual: list[str], policy: str
    ) -> list[str]:
        resolved: list[str] = []
        for column in requested:
            try:
                resolved.append(self._engine._resolve_column_name(actual, column))
            except EngineError:
                if policy != "ignore":
                    raise
        return resolved
