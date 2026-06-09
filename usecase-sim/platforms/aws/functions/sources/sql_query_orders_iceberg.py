"""Python function source: SQL query against the Iceberg orders table.

Used by the ``python_fn__iceberg_sql`` dataflow (stage: source_python_fn).

The function queries the Iceberg table via the ``glue_catalog`` Spark catalog
that is configured in the Glue job Spark properties, demonstrating
engine.execute_sql() against an Iceberg table.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame


def sql_query_orders_iceberg(engine, source, watermark_start: Optional[Dict[str, Any]] = None, watermark_end: Optional[Dict[str, Any]] = None, 
                             *args, **kwargs) -> DataFrame:
    """Return a filtered subset of the orders_iceberg_overwritten Iceberg table via SQL.

    Args:
        engine: Active :class:`~datacoolie.engines.SparkEngine` instance.
        source: :class:`~datacoolie.core.models.Source` model for this dataflow.
        watermark_start: Previous watermark dict (``None`` on first run).
        watermark_end: Replay ceiling watermark (``None`` for normal reads).

    Returns:
        A Spark DataFrame containing high-value orders (amount > 100).
    """
    # The glue_catalog is configured via Spark properties in the Glue job:
    #   spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
    #   spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    sql = """
        SELECT
            order_id,
            amount,
            region,
            order_date
        FROM glue_catalog.datacoolie.orders_iceberg_overwritten
        WHERE CAST(amount AS DOUBLE) > 100
    """
    return engine.execute_sql(sql)
