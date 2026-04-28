"""Deduplication transformer.

Removes duplicate rows based on partition (dedup) columns and ordering
columns.  Uses ``RANK``-based dedup for ``MERGE_OVERWRITE`` load type
and ``ROW_NUMBER``-based dedup otherwise.
"""

from __future__ import annotations

from datacoolie.core.constants import LoadType
from datacoolie.core.models import DataFlow
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.transformers.base import BaseTransformer

logger = get_logger(__name__)


class Deduplicator(BaseTransformer[DF]):
    """Deduplicate DataFrame rows (order = 20).

    Decision logic:
        * ``order_cols`` = ``dataflow.order_columns`` (``latest_data_columns``
          with fallback to ``source.watermark_columns``).
        * If ``deduplicate_columns`` *or* ``order_cols`` is empty,
          the transformer is a no-op (returns input unchanged).
        * Uses :meth:`engine.deduplicate_by_rank` (keeps ties) when:

          - ``load_type == MERGE_OVERWRITE`` *and* ``merge_keys`` is
            non-empty *and* ``transform.deduplicate_columns`` is ``None``
            (partition resolved implicitly from merge keys).
          - *or* ``transform.configure["deduplicate_by_rank"]`` is ``True``.

        * Otherwise → :meth:`engine.deduplicate` (strict ROW_NUMBER).
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine

    @property
    def order(self) -> int:
        return 20

    def transform(self, df: DF, dataflow: DataFlow) -> DF:
        """Apply deduplication if configured."""
        partition_cols = dataflow.deduplicate_columns
        order_cols = dataflow.order_columns

        if not partition_cols or not order_cols:
            logger.debug("Deduplicator: no partition/order columns — skipping")
            self._mark_skipped()
            return df

        use_rank = (
            dataflow.load_type == LoadType.MERGE_OVERWRITE.value
            and dataflow.merge_keys
            and not dataflow.transform.deduplicate_columns
        ) or dataflow.transform.deduplicate_by_rank

        if use_rank:
            logger.debug(
                "Deduplicator: RANK dedup(%s, %s)",
                partition_cols,
                order_cols,
            )
            self._mark_applied("RANK")
            return self._engine.deduplicate_by_rank(
                df,
                partition_columns=partition_cols,
                order_columns=order_cols,
                order="desc",
            )

        logger.debug(
            "Deduplicator: ROW_NUMBER dedup(%s, %s)",
            partition_cols,
            order_cols,
        )
        return self._engine.deduplicate(
            df,
            partition_columns=partition_cols,
            order_columns=order_cols,
            order="desc",
        )
