"""Python function source: SQL query against the Delta orders table.

Used by the ``python_fn__delta_sql`` dataflow (stage: source_python_fn).

The function registers the Delta table as a Spark temporary view and runs
a filtered SQL query on it, demonstrating how to use engine.execute_sql()
inside a python_function source.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame


def sql_query_orders(engine, source, watermark: Optional[Dict[str, Any]]) -> DataFrame:
    """Return a filtered subset of the orders_appended Delta table via SQL.

    Args:
        engine: Active :class:`~datacoolie.engines.SparkEngine` instance.
        source: :class:`~datacoolie.core.models.Source` model for this dataflow.
        watermark: Previous watermark dict (``None`` on first run).

    Returns:
        A Spark DataFrame containing orders for the ``US-East`` region.
    """
    # Load the Delta table and register it as a temporary view so SQL can reference it.

    sql = """
        SELECT
            order_id,
            amount,
            region,
            order_date
        FROM datacoolie.orders_appended
        WHERE region = 'US-East'
    """
    return engine.execute_sql(sql)
