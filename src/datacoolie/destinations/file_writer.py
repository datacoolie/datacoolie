"""File-format destination writer (Parquet, CSV, JSON, JSONL, Avro).

Writes DataFrames to storage paths, supporting date-folder partitioning
via ``connection.date_folder_partitions`` resolved with the current UTC
timestamp.  Only ``append``, ``overwrite``, and ``full_load`` load types
are supported.  Maintenance (compact / cleanup) is not applicable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import LoadType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.destinations.load_strategies import get_load_strategy
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.utils.helpers import utc_now

logger = get_logger(__name__)

_SUPPORTED_LOAD_TYPES = {
    LoadType.APPEND.value,
    LoadType.OVERWRITE.value,
    LoadType.FULL_LOAD.value,
}


class FileWriter(BaseDestinationWriter[DF]):
    """Destination writer for flat-file formats (Parquet, CSV, JSON, …).

    Delegates to :func:`get_load_strategy` for the actual write logic.
    Date-folder partitioning is resolved at write time using ``utc_now()``.

    Maintenance operations (compact / cleanup) are **not supported** for
    flat files and will raise :class:`DestinationError` if invoked.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        super().__init__(engine)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def _write_internal(self, df: DF, dataflow: DataFlow) -> None:
        dest = dataflow.destination
        dest_fmt = dest.connection.format

        load_type = dataflow.load_type

        if load_type not in _SUPPORTED_LOAD_TYPES:
            raise DestinationError(
                f"FileWriter only supports {sorted(_SUPPORTED_LOAD_TYPES)} load types, got: {load_type}",
                details={"table": dest.full_table_name, "load_type": load_type, "format": dest_fmt, "path": dest.path},
            )

        base_path = dest.path
        if not base_path:
            raise DestinationError(
                "FileWriter requires a destination path (connection.base_path must be set)",
                details={"table": dest.full_table_name, "format": dest_fmt, "load_type": load_type},
            )

        # Prefer partition_columns over date_folder_partitions — only use one.
        if dest.partition_column_names:
            resolved_path = base_path
        else:
            date_pattern = dest.connection.date_folder_partitions
            resolved_path = self._resolve_date_folder_path(base_path, date_pattern, utc_now())

        strategy = get_load_strategy(load_type)

        logger.info(
            "FileWriter: writing to table %s path %s (format=%s, load_type=%s)",
            dest.full_table_name,
            resolved_path,
            dest_fmt,
            load_type,
        )
        strategy.execute(df, table_name=None, dataflow=dataflow, engine=self._engine, path=resolved_path)

    # ------------------------------------------------------------------
    # Date-folder path resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_date_folder_path(
        base_path: str,
        date_pattern: Optional[str],
        now: datetime,
    ) -> str:
        """Resolve date-folder placeholders and append to *base_path*.

        Replaces ``{year}``, ``{month}``, ``{day}``, ``{hour}`` in
        *date_pattern* with zero-padded values from *now*, then appends
        the result as a sub-path under *base_path*.

        Args:
            base_path: Root output path.
            date_pattern: Pattern string (e.g. ``"{year}/{month}/{day}"``).
                ``None`` means no date partitioning — returns *base_path*
                unchanged.
            now: Timestamp to use for placeholder resolution.

        Returns:
            Resolved output path.
        """
        if not date_pattern:
            return base_path

        resolved = date_pattern
        resolved = resolved.replace("{year}", f"{now.year:04d}")
        resolved = resolved.replace("{month}", f"{now.month:02d}")
        resolved = resolved.replace("{day}", f"{now.day:02d}")
        resolved = resolved.replace("{hour}", f"{now.hour:02d}")

        return f"{base_path.rstrip('/')}/{resolved}"

    # ------------------------------------------------------------------
    # Maintenance — not supported for flat files
    # ------------------------------------------------------------------

    def _maintain_internal(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool,
        do_cleanup: bool,
        retention_hours: int,
    ) -> "tuple[List[Dict[str, Any]], List[str]]":
        dest = dataflow.destination
        raise DestinationError(
            f"Maintenance is not supported for file format: {dest.connection.format}",
            details={
                "table": dest.full_table_name,
                "path": dest.path,
                "format": dest.connection.format,
            },
        )
