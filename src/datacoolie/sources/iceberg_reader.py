"""Iceberg source reader.

Reads Iceberg tables via SQL (``SELECT * FROM table_name``) using
the catalog-aware fully qualified table name. Falls back to path-based
reads when no catalog is configured.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)


class IcebergReader(BaseSourceReader[DF]):
    """Source reader for Apache Iceberg tables.

    Prefers SQL-based reads via ``full_table_name`` (catalog-aware).
    Falls back to path-based reads when no catalog is configured.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)

    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read an Iceberg table, optionally filtering by watermark."""
        action: dict = {"reader": type(self).__name__}
        if source.query:
            action["query"] = source.query
        elif source.full_table_name:
            action["table"] = source.full_table_name
        if source.path:
            action["path"] = source.path
        self._set_source_action(action)

        df = self._read_data(source)

        if watermark and source.watermark_columns:
            df = self._apply_watermark_filter(df, source.watermark_columns, watermark)

        context = f"Table: {source.full_table_name or source.path} (format: {source.connection.format})"
        return self._finalize_read(df, source.watermark_columns, "IcebergReader", context)

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read an Iceberg table via SQL or path.

        Prefers ``source.query``, then ``full_table_name`` (SQL),
        then ``source.path``.
        """
        if source.query:
            logger.debug("IcebergReader: executing SQL query")
            return self._engine.execute_sql(source.query)

        table_name = source.full_table_name
        fmt = source.connection.format
        path = source.path
        options = source.read_options

        logger.debug("IcebergReader: reading Iceberg table by name: %s", table_name)
        return self._engine.read(fmt=fmt, table_name=table_name, path=path, options=options or None)

