"""Delta Lake source reader.

Reads Delta tables via :meth:`engine.read_delta` with support for
watermark-based incremental reads and SQL queries.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)


class DeltaReader(BaseSourceReader[DF]):
    """Source reader for Delta Lake tables.

    Supports:
    * Full and incremental reads (watermark filtering).
    * SQL query mode (``source.query``).
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)

    # ------------------------------------------------------------------
    # Core reading
    # ------------------------------------------------------------------

    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read a Delta table, optionally filtering by watermark.

        Steps:
            1. Read the raw Delta table (or execute a SQL query).
            2. Apply watermark filter (if provided).
            3. Calculate row count and new watermark.
            4. Return ``None`` if zero rows remain.
        """
        action: dict = {"reader": type(self).__name__}
        if source.query:
            action["query"] = source.query
        elif source.full_table_name:
            action["table"] = source.full_table_name
        elif source.path:
            action["path"] = source.path
        self._set_source_action(action)

        df = self._read_data(source)

        # Apply watermark filter
        if watermark and source.watermark_columns:
            df = self._apply_watermark_filter(df, source.watermark_columns, watermark)

        context = f"Table: {source.full_table_name} (format: {source.connection.format}), Source path: {source.path}"
        return self._finalize_read(df, source.watermark_columns, "DeltaReader", context)

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read a Delta table or execute a SQL query.

        Prefers ``source.query``, then ``full_table_name`` (read_table),
        then ``source.path``.

        Raises:
            SourceError: If neither path, query, nor table name is specified.
        """
        if source.query:
            logger.debug("DeltaReader: executing SQL query")
            return self._engine.execute_sql(source.query)

        table_name = source.full_table_name
        path = source.path
        fmt = source.connection.format
        options = source.read_options

        # For path-based sources without a catalog namespace, prefer path-based reads to avoid table-not-found errors.
        if path and not source.namespace:
            table_name = None

        logger.debug("DeltaReader: reading Delta table by name: %s, path: %s", table_name, path)
        return self._engine.read(fmt=fmt, table_name=table_name, path=path, options=options or None)

