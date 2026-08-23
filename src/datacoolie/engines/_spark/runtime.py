"""Spark runtime compatibility helpers shared by private capabilities."""

from __future__ import annotations

from pyspark.sql import DataFrame

from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


def supports_merge_into() -> bool:
    """Return whether the active Spark runtime exposes DataFrame.mergeInto."""
    return hasattr(DataFrame, "mergeInto")


def safe_cache(df: DataFrame) -> DataFrame:
    """Cache *df* when supported, otherwise return it unchanged."""
    try:
        return df.cache()
    except Exception:  # noqa: BLE001
        logger.debug(
            "DataFrame.cache() is not supported on this runtime; continuing without caching."
        )
        return df


def safe_unpersist(df: DataFrame) -> None:
    """Unpersist *df*, ignoring runtimes where caching was unavailable."""
    try:
        df.unpersist()
    except Exception:  # noqa: BLE001
        pass
