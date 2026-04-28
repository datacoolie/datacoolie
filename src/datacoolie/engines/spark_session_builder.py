"""Spark session creation and default configuration.

Provides :func:`get_or_create_spark_session` and the
``DEFAULT_SPARK_CONFIGS`` dictionary used by :class:`SparkEngine`.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Default Spark configurations
# ---------------------------------------------------------------------------

DEFAULT_SPARK_CONFIGS: Dict[str, str] = {
    # Parquet handling
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    # Adaptive query execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


def get_or_create_spark_session(
    app_name: str = "DataCoolie",
    config: Optional[Dict[str, str]] = None,
    existing_session: Optional[SparkSession] = None,
) -> SparkSession:
    """Get or create a :class:`SparkSession` with optimised defaults.

    In **notebook environments** (Fabric, Databricks) pass the existing
    session so only the default configs are applied on top.  For
    **standalone** usage a fresh session is created.

    Callers are responsible for providing Delta/Iceberg catalog,
    extensions, and JARs via the ``config`` dict.

    Args:
        app_name: Spark application name.
        config: Extra Spark configs that override the defaults.
        existing_session: Existing session to re-use.

    Returns:
        Configured ``SparkSession``.
    """
    final_config = {**DEFAULT_SPARK_CONFIGS}
    if config:
        final_config.update(config)

    if existing_session:
        _apply_configs(existing_session, final_config)
        return existing_session

    # Build a new session
    builder = SparkSession.builder.appName(app_name)
    for key, value in final_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def _apply_configs(spark: SparkSession, config: Dict[str, str]) -> None:
    """Apply configuration values to an existing session, ignoring errors."""
    for key, value in config.items():
        try:
            spark.conf.set(key, value)
        except Exception:  # noqa: BLE001
            logger.warning("Could not set Spark config '%s' on existing session.", key)
