"""Live YEAR extraction must preserve numeric zero before schema casting."""

from __future__ import annotations

import shutil
import subprocess
from uuid import uuid4

import pytest

from tests.support.data_types.database_matrix import seed_matrix_source
from tests.support.datatype_config import require_docker_services

pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]


@pytest.fixture
def year_source():
    require_docker_services(("mysql",))
    from sqlalchemy import text

    source = seed_matrix_source("mysql", table=f"dc_year_{uuid4().hex[:12]}")
    try:
        with source.database.begin() as connection:
            connection.execute(text(
                f"INSERT INTO {source.table} (my_row_id, my_year) VALUES "
                "(3, 0), (4, 1901), (5, 2155), (6, 2000)"
            ))
        yield source
    finally:
        source.cleanup()


@pytest.mark.parametrize("reader", ["native", "connectorx"])
def test_polars_mysql_year_preserves_integers(year_source, reader: str) -> None:
    import polars as pl
    from datacoolie.engines.polars_engine import PolarsEngine

    if reader == "connectorx":
        pytest.importorskip("connectorx")
    engine = PolarsEngine()
    uri = year_source.database.url.set(drivername="mysql").render_as_string(
        hide_password=False
    )
    frame = engine.read_database(
        query=f"SELECT my_row_id, my_year FROM {year_source.table} ORDER BY my_row_id",
        options={"database_type": "mysql", "url": uri, "database_read_engine": reader},
    )
    assert frame.collect_schema()["my_year"] == pl.Int64
    converted = engine.cast_column(frame, "my_year", "YEAR", type_system="mysql")
    result = converted.collect()
    assert result.schema["my_year"] == pl.Int16
    assert result["my_year"].to_list() == [2024, None, 0, 1901, 2155, 2000]


@pytest.mark.spark
@pytest.mark.parametrize("service", [
    "spark",
    pytest.param("spark4", marks=pytest.mark.spark4_qualification),
])
def test_spark_mysql_year_defaults_to_integer(year_source, service: str) -> None:
    require_docker_services((service,))
    # The simulator owns credentials and the pinned JDBC jar cache.
    script = f"""
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ShortType
from datacoolie.engines.spark_engine import SparkEngine

jar = next(Path('/datacoolie/usecase-sim/.runtime/spark/jars/jars').glob('*mysql*.jar'))
spark = (SparkSession.builder.master('local[2]').appName('mysql-year-regression')
         .config('spark.driver.extraClassPath', str(jar)).getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    engine = SparkEngine(spark_session=spark)
    for connection in (
        {{'host': 'host.docker.internal', 'database': 'datacoolie'}},
        {{'url': 'jdbc:mysql://host.docker.internal:3306/datacoolie'}},
    ):
        frame = engine.read_database(
            query='SELECT my_row_id, my_year FROM {year_source.table}',
            options={{**connection, 'database_type': 'mysql',
                     'user': os.environ['DATACOOLIE_MYSQL_USER'],
                     'password': os.environ['DATACOOLIE_MYSQL_PASS']}},
        )
        assert isinstance(frame.schema['my_year'].dataType, IntegerType), frame.schema
        result = engine.cast_column(frame, 'my_year', 'YEAR', type_system='mysql')
        assert isinstance(result.schema['my_year'].dataType, ShortType), result.schema
        values = [row.my_year for row in result.orderBy('my_row_id').collect()]
        assert values == [2024, None, 0, 1901, 2155, 2000], values
    print('YEAR integer regression passed on Spark', spark.version)
finally:
    spark.stop()
"""
    completed = subprocess.run(
        [shutil.which("docker"), "exec", "-i", f"datacoolie-{service}", "python", "-"],
        input=script, capture_output=True, text=True, timeout=120, check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
