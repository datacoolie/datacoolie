"""Container-side native Spark/Polars observation worker.

The worker only executes bounded Delta/Iceberg read/write directions and emits
structured observations.  It intentionally contains no pytest and no expected
schema assertions; the host integration test owns the contract comparison.
"""

from __future__ import annotations

import argparse
import json
import traceback
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from importlib import metadata
from pathlib import Path
from uuid import uuid4

import polars as pl
import pyspark
from delta import configure_spark_with_delta_pip
from deltalake import DeltaTable
from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession

from datacoolie.engines.polars_engine import PolarsEngine


EXPECTED_INSTANT = datetime(
    2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc
)
EXPECTED_WALL_CLOCK = datetime(2024, 1, 15, 10, 30, 45, 123456)


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([Decimal("12.30")], dtype=pl.Decimal(10, 2)),
            "instant": pl.Series(
                [EXPECTED_INSTANT], dtype=pl.Datetime("us", "UTC")
            ),
            "wall_clock": pl.Series(
                [EXPECTED_WALL_CLOCK], dtype=pl.Datetime("us")
            ),
        }
    )


def _normalise(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [_normalise(item) for item in value]
    return str(value)


def _observe_polars(frame: pl.DataFrame) -> dict[str, object]:
    return {
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "rows": [
            {name: _normalise(value) for name, value in row.items()}
            for row in frame.to_dicts()
        ],
    }


def _observe_spark(frame: object) -> dict[str, object]:
    schema = frame.schema.simpleString()  # type: ignore[attr-defined]
    row = frame.selectExpr(  # type: ignore[attr-defined]
        "CAST(id AS BIGINT) AS id",
        "CAST(amount AS STRING) AS amount",
        "unix_micros(instant) AS instant_us",
        "date_format(wall_clock, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS wall_clock",
    ).first()
    return {
        "schema": schema,
        "rows": [
            {
                "id": row.id,
                "amount": row.amount,
                "instant_us": row.instant_us,
                "wall_clock": row.wall_clock,
            }
        ],
    }


def _spark_session() -> SparkSession:
    spark_version = pyspark.__version__
    spark_major = int(spark_version.split(".")[0])
    spark_major_minor = ".".join(spark_version.split(".")[:2])
    scala_suffix = "2.13" if spark_major >= 4 else "2.12"
    iceberg_version = "1.11.0" if spark_major_minor == "4.1" else "1.10.1"
    delta_version = metadata.version("delta-spark")
    builder = configure_spark_with_delta_pip(
        SparkSession.builder.master("local[1]")
        .appName("datacoolie-spark-cross-engine-observation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local_catalog.type", "rest")
        .config("spark.sql.catalog.local_catalog.uri", "http://iceberg-rest:8181")
        .config(
            "spark.sql.catalog.local_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.local_catalog.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local_catalog.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.local_catalog.s3.secret-access-key", "minioadmin")
        .config("spark.sql.catalog.local_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.local_catalog.s3.region", "us-east-1")
    )
    return builder.config(
        "spark.jars.packages",
        f"io.delta:delta-spark_{scala_suffix}:{delta_version},"
        f"org.apache.iceberg:iceberg-spark-runtime-{spark_major_minor}_{scala_suffix}:{iceberg_version},"
        "software.amazon.awssdk:bundle:2.29.51",
    ).getOrCreate()


def _iceberg_catalog():
    return load_catalog(
        "datacoolie-spark-cross-engine-observation",
        type="rest",
        uri="http://iceberg-rest:8181",
        **{
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def run(run_root: Path) -> dict[str, object]:
    run_root.mkdir(parents=True, exist_ok=True)
    namespace = f"datatype_observation_{uuid4().hex[:10]}"
    catalog = _iceberg_catalog()
    cleanup_errors: list[str] = []
    table_names = (
        f"{namespace}.spark_values",
        f"{namespace}.polars_values",
    )
    spark: SparkSession | None = None
    try:
        catalog.create_namespace_if_not_exists(namespace)
        spark = _spark_session()
        polars_engine = PolarsEngine(
            iceberg_catalog=catalog,
            storage_options={
                "aws_access_key_id": "minioadmin",
                "aws_secret_access_key": "minioadmin",
                "aws_endpoint_url": "http://minio:9000",
                "aws_region": "us-east-1",
                "aws_allow_http": "true",
            },
        )
        delta_spark_path = run_root / "delta-spark-write"
        delta_polars_path = run_root / "delta-polars-write"
        spark_frame = spark.sql(
            """
            SELECT 1 AS id,
                   CAST(12.30 AS DECIMAL(10,2)) AS amount,
                   timestamp '2024-01-15 03:30:45.123456+00:00' AS instant,
                   timestamp_ntz '2024-01-15 10:30:45.123456' AS wall_clock
            """
        )
        spark_frame.write.format("delta").mode("overwrite").save(str(delta_spark_path))
        spark_to_polars = pl.from_arrow(
            DeltaTable(str(delta_spark_path)).to_pyarrow_table()
        )

        _frame().write_delta(str(delta_polars_path), mode="overwrite")
        polars_to_spark = spark.read.format("delta").load(str(delta_polars_path))

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Delete operation did not match any records",
                category=UserWarning,
            )
            polars_engine.write_to_table(
                _frame().lazy(),
                f"{namespace}.polars_values",
                mode="overwrite",
                fmt="iceberg",
            )
        polars_to_iceberg_spark = spark.table(
            f"local_catalog.{namespace}.polars_values"
        )
        spark_frame.writeTo(f"local_catalog.{namespace}.spark_values").using(
            "iceberg"
        ).createOrReplace()
        spark_to_iceberg_polars = polars_engine.read_table(
            f"{namespace}.spark_values", fmt="iceberg"
        ).collect()
        return {
            "status": "succeeded",
            "runtime": {
                "pyspark": pyspark.__version__,
                "delta_spark": metadata.version("delta-spark"),
                "deltalake": metadata.version("deltalake"),
                "pyiceberg": metadata.version("pyiceberg"),
            },
            "observations": {
                "delta_spark_to_polars": _observe_polars(spark_to_polars),
                "delta_polars_to_spark": _observe_spark(polars_to_spark),
                "iceberg_spark_to_polars": _observe_polars(spark_to_iceberg_polars),
                "iceberg_polars_to_spark": _observe_spark(polars_to_iceberg_spark),
            },
            "cleanup_errors": cleanup_errors,
        }
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:  # noqa: BLE001 - report cleanup status
                cleanup_errors.append(f"spark.stop: {exc}")
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Delete operation did not match any records",
                category=UserWarning,
            )
            for table_name in table_names:
                try:
                    catalog.drop_table(table_name)
                except Exception as exc:  # noqa: BLE001 - report cleanup status
                    cleanup_errors.append(f"drop_table {table_name}: {exc}")
            try:
                catalog.drop_namespace(namespace)
            except Exception as exc:  # noqa: BLE001 - report cleanup status
                cleanup_errors.append(f"drop_namespace {namespace}: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument(
        "--run-root",
        type=Path,
        default=Path("/datacoolie/usecase-sim/.runtime/data/datatype-observation"),
    )
    args = parser.parse_args()
    try:
        result = run(args.run_root)
    except Exception as exc:  # noqa: BLE001 - protocol must report failure
        result = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
    args.result.parent.mkdir(parents=True, exist_ok=True)
    args.result.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    if result.get("status") != "succeeded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
