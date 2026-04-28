"""Partition handler transformer.

Generates partition columns from SQL expressions defined in the
destination ``partition_columns`` configuration.
"""

from __future__ import annotations

from datacoolie.core.models import DataFlow, PartitionColumn
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)


class PartitionHandler(BaseTransformer[DF]):
    """Generate partition columns from expressions (order = 80).

    For each :class:`PartitionColumn` with an ``expression``, adds
    a new column (or overwrites an existing one) using
    :meth:`engine.add_column`.  Columns without expressions are
    assumed to already exist in the DataFrame.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 80

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Add partition columns where expressions are defined."""
        partition_columns = dataflow.destination.partition_columns

        if not partition_columns:
            self._mark_skipped()
            return df

        existing_columns = self._engine.get_columns(df)

        for pc in partition_columns:
            df = self._add_partition_column(df, pc, existing_columns)

        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _add_partition_column(
        self,
        df: DF,
        pc: PartitionColumn,
        existing_columns: list[str],
    ) -> DF:
        """Add a single partition column if it has an expression.

        If the column already exists and has no expression, it is left
        untouched (assumed to be an existing data column).
        """
        if not pc.expression:
            if pc.column not in existing_columns:
                logger.warning(
                    "PartitionHandler: column %s has no expression and is not in DataFrame",
                    pc.column,
                )
            return df

        logger.debug(
            "PartitionHandler: adding partition column %s = %s",
            pc.column,
            pc.expression,
        )
        return self._engine.add_column(df, pc.column, pc.expression)
