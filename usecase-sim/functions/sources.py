"""Engine-agnostic source functions for usecase-sim test cases.

Function signature: ``fn(engine, source, watermark) -> DataFrame | None``

These functions are referenced via ``source.python_function`` in metadata
files (e.g. local_use_cases.json) as dotted paths::

    "functions.sources.sql_query_orders"

They work with **both** PolarsEngine and SparkEngine by using only the
common :class:`BaseEngine` API (``execute_sql``, ``create_dataframe``).
Engine-specific setup (e.g. Polars table registration) is handled via
``hasattr`` checks.
"""

from __future__ import annotations


def sql_query_orders(engine, source, watermark):
    """Query orders from Delta tables via SQL.

    * **Polars**: registers Delta tables from ``base_path`` into the
      SQLContext before querying.
    * **Spark**: tables are already in the catalog — queries directly.

    Pre-requisite stage: ``load_delta`` (writes ``orders_appended``).
    """
    # Hardcoded per-platform fallbacks for testing — connection.base_path
    # in metadata always wins; this only kicks in if it's missing.
    platform_name = type(getattr(engine, "platform", None)).__name__
    default_base_path = {
        "AWSPlatform": "s3a://datacoolie-test/output/delta",
        "FabricPlatform": "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/output/delta",
        "DatabricksPlatform": "dbfs:/datacoolie/output/delta",
    }.get(platform_name, "./usecase-sim/data/output/delta")
    base_path = source.connection.configure.get("base_path", default_base_path)

    # Polars requires explicit table registration.
    if hasattr(engine, "register_delta_tables"):
        registered = engine.register_delta_tables(base_path)
        if not registered:
            return None
    elif hasattr(engine, "spark"):
        # Spark: register the target Delta table as a temp view for SQL.
        # base_path may be a URI (s3a://, abfss://, gs://) — Spark's connector
        # handles them natively, so just join with "/" and let .load() raise
        # if the table doesn't exist.  os.path.* is local-only and corrupts
        # URIs on Windows ("s3a:/datacoolie-test/...").
        table_path = base_path.rstrip("/") + "/orders_appended"
        engine.spark.read.format("delta").load(table_path).createOrReplaceTempView("orders_appended")

    return engine.execute_sql(
        "SELECT order_id, order_date, amount, quantity, region "
        "FROM orders_appended "
        "WHERE amount IS NOT NULL "
        "ORDER BY order_date, order_id"
    )


def sql_query_orders_iceberg(engine, source, watermark):
    """Query orders from Iceberg tables via SQL.

    * **Polars**: registers Iceberg tables from the catalog namespace
      into the SQLContext before querying.
    * **Spark**: uses the fully-qualified catalog name directly.

    Pre-requisite stage: ``load_iceberg`` (writes ``orders_overwritten``).
    """
    namespace = source.connection.database or "default"

    if hasattr(engine, "register_iceberg_tables"):
        # Polars: register into SQLContext, then query by short name.
        registered = engine.register_iceberg_tables(namespace)
        if not registered:
            return None
        table_ref = "orders_overwritten"
    else:
        # Spark: use fully-qualified catalog.namespace.table reference.
        catalog = (
            source.configure.get("catalog")
            or getattr(source.connection, "catalog", None)
            or "local_catalog"
        )
        table_ref = f"{catalog}.{namespace}.orders_overwritten"

    return engine.execute_sql(
        f"SELECT * "
        f"FROM {table_ref} "
        f"WHERE amount IS NOT NULL "
        f"ORDER BY order_date, order_id"
    )


def load_orders_custom(engine, source, watermark):
    """Return a hard-coded set of orders — no external dependency.

    Uses ``engine.create_dataframe`` which is part of the common
    :class:`BaseEngine` API and works for both Polars and Spark.
    """
    records = [
        {"order_id": "PY001", "order_date": "2024-01-15", "amount": 125.50, "quantity": 3, "region": "north"},
        {"order_id": "PY002", "order_date": "2024-02-20", "amount": 89.99,  "quantity": 1, "region": "south"},
        {"order_id": "PY003", "order_date": "2024-03-05", "amount": 340.00, "quantity": 5, "region": "east"},
        {"order_id": "PY004", "order_date": "2024-04-10", "amount": 55.75,  "quantity": 2, "region": "west"},
    ]
    return engine.create_dataframe(records)


def read_iceberg_orders_query(engine, source, watermark):
    """Read from Iceberg table with a filter — used by ``read_iceberg__query``.

    * **Polars**: registers Iceberg tables from the catalog namespace
      into the SQLContext before querying by short name.
    * **Spark**: uses the fully-qualified catalog.namespace.table reference.

    Pre-requisite stage: ``load_iceberg`` (writes ``orders_from_json``).
    """
    namespace = source.connection.database or "default"

    if hasattr(engine, "register_iceberg_tables"):
        registered = engine.register_iceberg_tables(namespace)
        if not registered:
            return None
        table_ref = "orders_from_json"
    else:
        catalog = (
            source.configure.get("catalog")
            or getattr(source.connection, "catalog", None)
            or "local_catalog"
        )
        table_ref = f"{catalog}.{namespace}.orders_from_json"

    return engine.execute_sql(
        f"SELECT order_id, amount, status "
        f"FROM {table_ref} "
        f"WHERE status = 'completed'"
    )
