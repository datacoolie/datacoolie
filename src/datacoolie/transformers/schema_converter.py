"""Schema conversion transformer.

Casts DataFrame columns according to schema hints defined in the
:class:`Transform` configuration.  Also handles ``timestamp_ntz`` →
``timestamp`` conversion.  Type resolution (SQL alias → engine-native
type) is delegated to each engine's :meth:`~BaseEngine.cast_column`.
"""

from __future__ import annotations

from typing import Dict

from datacoolie.core.models import DataFlow, SchemaHint
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)



class SchemaConverter(BaseTransformer[DF]):
    """Cast columns per schema hints (order = 10).

    Processing:
        1. Convert ``timestamp_ntz`` columns to ``timestamp`` (engine hook).
        2. If ``source.connection.use_schema_hint`` is truthy and hints
           exist, cast each matching column using the raw type string from
           the hint — type resolution is handled by the engine.
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
                df = self._apply_conversions(df, hints_dict)
                self._mark_applied()

        # Step 2: timestamp_ntz → timestamp (after hints so hint-cast columns
        # that resolve to timestamp_ntz are also converted)
        if dataflow.transform.convert_timestamp_ntz:
            df = self._engine.convert_timestamp_ntz_to_timestamp(df)
            self._mark_applied()

        return df

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_conversions(
        self,
        df: DF,
        hints_dict: Dict[str, SchemaHint],
    ) -> DF:
        """Cast columns using schema hints.

        Column matching is case-insensitive.
        """
        existing_columns = self._engine.get_columns(df)
        # Build case-insensitive lookup
        col_lower_map = {c.lower(): c for c in existing_columns}

        for hint_col, hint in hints_dict.items():
            if not hint.is_active:
                continue

            actual_col = col_lower_map.get(hint_col.lower())
            if actual_col is None:
                logger.debug("SchemaConverter: column %s not found — skipping", hint_col)
                continue

            target_type = self._build_type_string(hint)

            logger.debug(
                "SchemaConverter: casting %s → %s (format=%s)",
                actual_col,
                target_type,
                hint.format,
            )
            df = self._engine.cast_column(df, actual_col, target_type, hint.format)

        return df

    @staticmethod
    def _build_type_string(hint: SchemaHint) -> str:
        """Build the target type string to pass to the engine.

        If the hint carries precision, appends ``(precision,scale)``;
        otherwise returns :attr:`~SchemaHint.data_type` unchanged.
        The engine is responsible for resolving any SQL alias.
        """
        if hint.precision is not None:
            s = hint.scale if hint.scale is not None else 0
            return f"{hint.data_type}({hint.precision},{s})"
        return hint.data_type
