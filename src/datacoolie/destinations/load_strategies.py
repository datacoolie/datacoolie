"""Load strategies for destination writers.

Each strategy implements a specific write mode (overwrite, append,
merge_upsert, merge_overwrite, scd2) via the :class:`BaseLoadStrategy`
interface.

The :data:`LOAD_STRATEGIES` registry and :func:`get_load_strategy` helper
provide lookup by load-type string.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.core.constants import LoadType
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.destinations.base import BaseLoadStrategy
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


# ============================================================================
# Shared helpers
# ============================================================================


def _initial_load_fallback(
    df: DF,
    table_name: str,
    dataflow: DataFlow,
    engine: BaseEngine[DF],
    path: Optional[str],
    strategy_name: str,
) -> bool:
    """Fall back to overwrite when the target table does not exist yet.

    Returns ``True`` if the fallback was executed (caller should ``return``),
    ``False`` if the table exists and normal logic should proceed.
    """
    dest = dataflow.destination
    fmt = dest.connection.format
    if engine.exists(table_name=table_name, path=path, fmt=fmt):
        return False

    logger.info(
        "%s: table not found – falling back to overwrite at %s, format=%s",
        strategy_name, table_name, fmt,
    )
    engine.write(
        df,
        table_name=table_name,
        path=path,
        mode=LoadType.OVERWRITE.value,
        fmt=fmt,
        partition_columns=dest.partition_column_names or None,
        options=dest.write_options or None,
    )
    return True


# ============================================================================
# Overwrite
# ============================================================================


class OverwriteStrategy(BaseLoadStrategy):
    """Full overwrite — replace the entire table."""

    @property
    def load_type(self) -> str:
        return LoadType.OVERWRITE.value

    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        dest = dataflow.destination
        fmt = dest.connection.format
        options = dest.write_options

        logger.info("OverwriteStrategy: writing to table %s (format=%s)", table_name or dest.full_table_name, fmt)
        engine.write(
            df,
            table_name=table_name,
            path=path,
            mode=LoadType.OVERWRITE.value,
            fmt=fmt,
            partition_columns=dest.partition_column_names or None,
            options=options or None,
        )


# ============================================================================
# Append
# ============================================================================


class AppendStrategy(BaseLoadStrategy):
    """Append — add new rows to the table."""

    @property
    def load_type(self) -> str:
        return LoadType.APPEND.value

    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        dest = dataflow.destination
        fmt = dest.connection.format
        options = dest.write_options

        logger.info("AppendStrategy: appending to table %s (format=%s)", table_name or dest.full_table_name, fmt)
        engine.write(
            df,
            table_name=table_name,
            path=path,
            mode=LoadType.APPEND.value,
            fmt=fmt,
            partition_columns=dest.partition_column_names or None,
            options=options or None,
        )


# ============================================================================
# Merge Upsert
# ============================================================================


class MergeUpsertStrategy(BaseLoadStrategy):
    """Merge upsert — insert new rows, update existing by merge keys."""

    @property
    def load_type(self) -> str:
        return LoadType.MERGE_UPSERT.value

    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        dest = dataflow.destination
        merge_keys = dest.merge_keys_extended

        if not merge_keys:
            raise DestinationError(
                "MergeUpsertStrategy requires merge_keys",
                details={"table": dest.full_table_name, "path": dest.path},
            )

        fmt = dest.connection.format
        options = dest.write_options

        if _initial_load_fallback(df, table_name, dataflow, engine, path, "MergeUpsertStrategy"):
            return

        logger.info("MergeUpsertStrategy: merging into table %s (keys=%s), format=%s", table_name or dest.full_table_name, merge_keys, fmt)
        engine.merge(
            df,
            table_name=table_name,
            path=path,
            merge_keys=merge_keys,
            fmt=fmt,
            partition_columns=dest.partition_column_names or None,
            options=options or None,
        )


# ============================================================================
# Merge Overwrite
# ============================================================================


class MergeOverwriteStrategy(BaseLoadStrategy):
    """Merge overwrite — delete + re-insert matching rows."""

    @property
    def load_type(self) -> str:
        return LoadType.MERGE_OVERWRITE.value

    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        dest = dataflow.destination
        merge_keys = dest.merge_keys_extended

        if not merge_keys:
            raise DestinationError(
                "MergeOverwriteStrategy requires merge_keys",
                details={"table": dest.full_table_name, "path": dest.path},
            )

        fmt = dest.connection.format
        options = dest.write_options

        if _initial_load_fallback(df, table_name, dataflow, engine, path, "MergeOverwriteStrategy"):
            return

        logger.info("MergeOverwriteStrategy: merge-overwrite table %s (keys=%s), format=%s", table_name or dest.full_table_name, merge_keys, fmt)
        engine.merge_overwrite(
            df,
            table_name=table_name,
            path=path,
            merge_keys=merge_keys,
            fmt=fmt,
            partition_columns=dest.partition_column_names or None,
            options=options or None,
        )


# ============================================================================
# SCD2 (placeholder)
# ============================================================================


class SCD2Strategy(BaseLoadStrategy):
    """Slowly Changing Dimension Type 2.

    Closes existing current records and inserts new versions using
    the staged-updates MERGE pattern.  Requires ``merge_keys`` and
    ``scd2_effective_column`` in ``destination.configure``.
    """

    @property
    def load_type(self) -> str:
        return LoadType.SCD2.value

    def execute(
        self,
        df: DF,
        table_name: str,
        dataflow: DataFlow,
        engine: BaseEngine[DF],
        path: Optional[str] = None,
    ) -> None:
        dest = dataflow.destination
        merge_keys = dest.merge_keys_extended

        if not merge_keys:
            raise DestinationError(
                "SCD2Strategy requires merge_keys",
                details={"table": dest.full_table_name, "path": dest.path},
            )

        effective_col = dest.scd2_effective_column
        if not effective_col:
            raise DestinationError(
                "SCD2Strategy requires scd2_effective_column in destination.configure",
                details={"table": dest.full_table_name, "path": dest.path},
            )

        fmt = dest.connection.format
        options = dest.write_options

        if _initial_load_fallback(df, table_name, dataflow, engine, path, "SCD2Strategy"):
            return

        logger.info("SCD2Strategy: scd2 merge into table %s (keys=%s), format=%s", table_name or dest.full_table_name, merge_keys, fmt)
        engine.scd2(
            df,
            table_name=table_name,
            path=path,
            merge_keys=merge_keys,
            fmt=fmt,
            partition_columns=dest.partition_column_names or None,
            options=options or None,
        )


# ============================================================================
# Registry
# ============================================================================


LOAD_STRATEGIES: Dict[str, BaseLoadStrategy] = {
    LoadType.FULL_LOAD.value: OverwriteStrategy(),  # Alias for overwrite
    LoadType.OVERWRITE.value: OverwriteStrategy(),
    LoadType.APPEND.value: AppendStrategy(),
    LoadType.MERGE_UPSERT.value: MergeUpsertStrategy(),
    LoadType.MERGE_OVERWRITE.value: MergeOverwriteStrategy(),
    LoadType.SCD2.value: SCD2Strategy(),
}


def get_load_strategy(load_type: str) -> BaseLoadStrategy:
    """Look up a load strategy by type string.

    Args:
        load_type: Load type (e.g. ``"overwrite"``, ``"merge_upsert"``).

    Returns:
        Matching :class:`BaseLoadStrategy` instance.

    Raises:
        DestinationError: If the load type is not registered.
    """
    key = load_type.strip().lower()
    strategy = LOAD_STRATEGIES.get(key)
    if strategy is None:
        raise DestinationError(
            f"Unknown load type: {load_type!r}",
            details={"available": list(LOAD_STRATEGIES.keys())},
        )
    return strategy
