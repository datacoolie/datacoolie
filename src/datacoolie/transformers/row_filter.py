"""Row filter transformer.

Applies a SQL ``WHERE``-style expression to discard unwanted rows
after column addition but before SCD2 processing.
"""

from __future__ import annotations

from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)


class RowFilter(BaseTransformer[DF]):
    """Filter rows by a SQL expression (order = 35).

    Reads :attr:`~datacoolie.core.models.Transform.filter_expression`.
    When the expression is absent or empty, the transformer is a no-op.

    The expression is passed verbatim to
    :meth:`engine.filter_rows` which delegates to the engine's native
    ``WHERE``-clause equivalent (Polars ``filter``, Spark ``where``, etc.).

    This transformer complements :attr:`~datacoolie.core.models.Source.filter_expression`
    (applied at read time inside each source reader).  Use
    ``source.filter_expression`` for conditions on raw source columns and
    ``transform.filter_expression`` for conditions on computed or added
    columns that only exist after :class:`ColumnAdder` runs.

    Example (YAML metadata)::

        transform:
          filter_expression: "category_label != 'unknown' AND derived_score > 0.5"

    Ordering rationale:

    * Runs **after** ``ColumnAdder`` (slot 30) so expressions may reference
      computed columns.
    * Runs **before** ``SCD2ColumnAdder`` (slot 60) to reduce the row set
      before the heavier SCD2 logic.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 35

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Apply the filter expression, or skip when none is configured."""
        expr = dataflow.transform.filter_expression
        if not expr:
            self._mark_skipped()
            return df

        logger.debug("RowFilter: applying expression: %s", expr)
        self._mark_applied(expr)
        return self._engine.filter_rows(df, expr)
