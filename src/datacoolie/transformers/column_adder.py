"""Column adder transformers.

:class:`ColumnAdder` removes stale system columns and adds computed
(user-defined) columns.  :class:`SystemColumnAdder` appends the standard
audit columns as the **final** step in the pipeline.
"""

from __future__ import annotations

from typing import List

from datacoolie.core.constants import DEFAULT_AUTHOR, LoadType, SCD2Column
from datacoolie.core.models import AdditionalColumn, DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)


class ColumnAdder(BaseTransformer[DF]):
    """Remove stale system columns and add computed columns (order = 30).

    Processing steps:
        1. Remove existing system columns (``__created_at``, etc.).
        2. Add user-defined additional columns.
    """

    def __init__(
        self,
        engine: BaseEngine[DF],
    ) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 30

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Apply column additions."""
        # Step 1: remove stale system columns
        df = self._engine.remove_system_columns(df)

        # Step 2: add user-defined columns
        additional: List[AdditionalColumn] = dataflow.transform.additional_columns
        for col_def in additional:
            df = self._add_custom_column(df, col_def)

        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _add_custom_column(self, df: DF, col_def: AdditionalColumn) -> DF:
        """Add a single computed column using its SQL expression."""
        if not col_def.expression:
            logger.debug("ColumnAdder: no expression for %s — skipping", col_def.column)
            return df

        logger.debug("ColumnAdder: adding %s = %s", col_def.column, col_def.expression)
        return self._engine.add_column(df, col_def.column, col_def.expression)


class SystemColumnAdder(BaseTransformer[DF]):
    """Append system audit columns as the final pipeline step (order = 70).

    Adds ``__created_at``, ``__updated_at``, and ``__updated_by`` to every
    DataFrame.  Running last ensures that column name sanitization never
    touches system column names.
    """

    def __init__(
        self,
        engine: BaseEngine[DF],
        author: str = DEFAULT_AUTHOR,
    ) -> None:
        self._engine = engine
        self._author = author

    @property
    def order(self) -> int:
        return 70

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Add system audit columns."""
        return self._engine.add_system_columns(df, self._author)


class SCD2ColumnAdder(BaseTransformer[DF]):
    """Add SCD2 tracking columns for scd2 load type (order = 60).

    Adds ``__valid_from`` (from the effective column), ``__valid_to`` (NULL),
    and ``__is_current`` (true) so downstream MERGE can close old rows and
    insert new versions.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 60

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Add SCD2 columns if load_type is scd2, otherwise no-op."""
        if dataflow.destination.load_type != LoadType.SCD2.value:
            self._mark_skipped()
            return df

        effective_col = dataflow.destination.scd2_effective_column
        if not effective_col:
            self._mark_skipped()
            return df

        logger.debug("SCD2ColumnAdder: adding SCD2 columns from %s", effective_col)
        vf = SCD2Column.VALID_FROM.value
        df = self._engine.add_column(df, vf, effective_col)
        df = self._engine.add_column(
            df, SCD2Column.VALID_TO.value,
            f'CASE WHEN 1=0 THEN "{vf}" END',
        )
        df = self._engine.add_column(df, SCD2Column.IS_CURRENT.value, "true")
        return df
