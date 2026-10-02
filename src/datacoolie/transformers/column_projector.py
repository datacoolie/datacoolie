"""Column selection, removal, and atomic rename."""

from __future__ import annotations

from datacoolie.core.constants import TRAILING_COLUMNS
from datacoolie.core.exceptions import ConfigurationError, EngineError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.transformers.base import BaseTransformer, ColumnMapping


logger = get_logger(__name__)


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
        select_requested = len(transform.select_columns)
        drop_requested = len(transform.drop_columns)
        rename_requested = len(transform.rename_columns)
        select_resolved = 0
        drop_resolved = 0
        rename_resolved = 0
        ignored_references = 0

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
            select_resolved = len(selected)
            ignored_references += select_requested - select_resolved
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
            drop_resolved = len(dropped)
            ignored_references += drop_requested - drop_resolved
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
                    ignored_references += 1
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
        rename_resolved = len(resolved_renames)
        if resolved_renames:
            df = self._engine.rename_columns(df, dict(resolved_renames))
            changed = True

        if changed:
            self._mark_applied()
        else:
            self._mark_skipped()
        self._report_column_mapping(
            ColumnMapping.from_columns(actual, self._engine.get_columns(df), dict(resolved_renames))
        )
        logger.debug(
            "ColumnProjector: select=%d/%d, drop=%d/%d, rename=%d/%d, "
            "ignored=%d, changed=%s",
            select_resolved,
            select_requested,
            drop_resolved,
            drop_requested,
            rename_resolved,
            rename_requested,
            ignored_references,
            changed,
        )
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
