"""Schema conversion transformer.

Casts DataFrame columns according to schema hints defined in the
:class:`Transform` configuration.  Also handles ``timestamp_ntz`` →
``timestamp`` conversion.  Datatype interpretation is delegated to the
selected engine's :meth:`~BaseEngine.cast_column` implementation.
"""

from __future__ import annotations

from typing import Dict

from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.transform import SchemaHint
from datacoolie.engines.data_types import infer_type_system
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)


class SchemaConverter(BaseTransformer[DF]):
    """Cast columns per schema hints (order = 10).

    Processing:
        1. If ``source.connection.use_schema_hint`` is truthy and hints
           exist, pass each authored source type and its dialect context to
           the engine, which resolves it to a native target.
        2. Convert ``timestamp_ntz`` columns to ``timestamp`` (engine hook)
           after hint casts so an explicitly hinted NTZ column follows the
           same conversion policy.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 10

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Apply schema conversions."""
        self._mark_skipped()  # assume no-op; override below if work is done

        # Step 1: schema-hint–based casting
        if dataflow.source.connection.use_schema_hint:
            hints_dict = dataflow.transform.schema_hints_dict
            if hints_dict:
                type_system = infer_type_system(
                    database_type=dataflow.source.connection.database_type,
                    explicit_type_system=dataflow.source.connection.schema_hint_type_system,
                )
                df, cast_count = self._apply_conversions(
                    df,
                    hints_dict,
                    type_system=type_system,
                )
                if cast_count:
                    self._mark_applied(f"{cast_count} casts")

        # Step 2: timestamp_ntz → timestamp (after hints so hint-cast columns
        # that resolve to timestamp_ntz are also converted)
        if dataflow.transform.convert_timestamp_ntz:
            converted = self._engine.convert_timestamp_ntz_to_timestamp(
                df, dataflow.transform.timestamp_timezone
            )
            if converted is not df:
                self._mark_applied()
            df = converted

        return df

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_conversions(
        self,
        df: DF,
        hints_dict: Dict[str, SchemaHint],
        *,
        type_system: str,
    ) -> tuple[DF, int]:
        """Cast columns using schema hints.

        Column matching is case-insensitive.
        """
        existing_columns = self._engine.get_columns(df)
        missing_hint_columns: list[str] = []
        cast_count = 0

        for hint_col, hint in hints_dict.items():
            if not hint.is_active:
                continue

            actual_col = next(
                (
                    column
                    for column in existing_columns
                    if column.lower() == hint_col.lower()
                ),
                None,
            )
            if actual_col is None:
                missing_hint_columns.append(hint_col)
                continue

            logger.debug(
                "SchemaConverter: casting %s using source type %s (%s)",
                actual_col,
                hint.data_type,
                type_system,
            )
            df = self._engine.cast_column(
                df,
                actual_col,
                hint.data_type,
                hint.format,
                type_system=type_system,
                precision=hint.precision,
                scale=hint.scale,
            )
            cast_count += 1

        if missing_hint_columns:
            logger.warning(
                "SchemaConverter: %d active schema-hint column(s) not found "
                "and skipped: %s",
                len(missing_hint_columns),
                missing_hint_columns,
            )

        return df, cast_count
