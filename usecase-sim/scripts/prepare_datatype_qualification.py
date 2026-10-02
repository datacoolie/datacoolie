"""Create the small typed/weak input fixture used by datatype scenarios."""

from __future__ import annotations

import csv
import argparse
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUN_ROOT = ROOT / ".runtime" / "data" / "datatype_qualification"
CANONICAL_METADATA = ROOT / "metadata" / "file" / "datatype_qualification.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument(
        "--formats",
        default="parquet,delta,iceberg",
        help="Comma-separated destination formats to materialize",
    )
    parser.add_argument(
        "--iceberg-table-suffix",
        default="",
        help="Optional suffix used to isolate generated Iceberg table names",
    )
    return parser.parse_args()


def _write_engine_metadata(
    canonical: dict,
    metadata_root: Path,
    output_root: Path,
    engine: str,
    output_format: str,
    iceberg_table_suffix: str,
) -> None:
    materialized = json.loads(json.dumps(canonical))
    # Keep each qualification run self-contained.  The canonical fixture uses
    # the stable ``.runtime/data/datatype_qualification`` root so it can also
    # be run directly from the simulator.  Opt-in cross-engine qualification
    # gives every run an isolated root; rewrite source roots together with the
    # destination root so the Spark container sees the same fixture that the
    # host-side setup created.
    input_root = output_root.parent / "input"
    try:
        input_relative = input_root.relative_to(ROOT)
        input_base = f"./usecase-sim/{input_relative.as_posix()}"
    except ValueError:
        input_base = str(input_root)
    for connection in materialized["connections"]:
        name = str(connection.get("name", ""))
        if not name.endswith("_source"):
            continue
        configure = connection.get("configure")
        if not isinstance(configure, dict):
            continue
        source_format = str(connection.get("format", "")).lower()
        if source_format in {"csv", "parquet", "json", "jsonl"}:
            configure["base_path"] = f"{input_base}/{source_format}"
    # Separate catalog tables are required only for Iceberg because both
    # engines share the REST catalog during one qualification run. File and
    # Delta outputs already have engine-addressed roots.
    destination_table_suffix = (
        f"_{engine}_{output_format}{iceberg_table_suffix}"
        if output_format == "iceberg"
        else ""
    )
    try:
        destination = output_root.relative_to(ROOT)
        destination_value = (
            f"./usecase-sim/{destination.as_posix()}/{output_format}/{engine}"
        )
    except ValueError:
        destination_value = str(output_root / output_format / engine)
    for connection in materialized["connections"]:
        if connection["name"] == "datatype_qualification_destination":
            connection["configure"]["base_path"] = destination_value
            connection["format"] = output_format
            connection["connection_type"] = (
                "file" if output_format == "parquet" else "lakehouse"
            )
            if output_format == "iceberg":
                connection["catalog"] = "local_catalog"
                connection["database"] = "default"
            else:
                connection.pop("catalog", None)
                connection.pop("database", None)
    for dataflow in materialized["dataflows"]:
        dataflow["destination"]["table"] = (
            f"{dataflow['destination']['table']}{destination_table_suffix}"
        )
    metadata_root.mkdir(parents=True, exist_ok=True)
    (metadata_root / f"{engine}_{output_format}.json").write_text(
        json.dumps(materialized, indent=2) + "\n", encoding="utf-8"
    )


def _pair(value: object) -> list[object]:
    """Return one deterministic non-null value and one null value."""

    return [value, None]


def _matrix_table() -> pa.Table:
    """Build the shared typed matrix consumed by every source dialect flow.

    The physical fixture intentionally uses Parquet-native values rather than
    strings.  This isolates the source-dialect contract from CSV parsing and
    lets each dataflow exercise its full authored type vocabulary.  Database
    extraction qualification remains a separate opt-in test boundary.
    """

    naive = datetime(2024, 1, 15, 10, 30, 45, 123456)
    instant = datetime(2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc)
    decimal_18_2 = lambda values: pa.array(  # noqa: E731 - compact fixture factory
        values, type=pa.decimal128(18, 2)
    )
    decimal_19_4 = lambda values: pa.array(  # noqa: E731 - compact fixture factory
        values, type=pa.decimal128(19, 4)
    )
    decimal_10_4 = lambda values: pa.array(  # noqa: E731 - compact fixture factory
        values, type=pa.decimal128(10, 4)
    )
    decimal_20_0 = lambda values: pa.array(  # noqa: E731 - compact fixture factory
        values, type=pa.decimal128(20, 0)
    )

    return pa.table(
        {
            # PostgreSQL vocabulary
            "pg_row_id": pa.array([1, 2], type=pa.int64()),
            "pg_bool": pa.array(_pair(True), type=pa.bool_()),
            "pg_int2": pa.array(_pair(32767), type=pa.int64()),
            "pg_int4": pa.array(_pair(2147483647), type=pa.int64()),
            "pg_int8": pa.array(_pair(9223372036854775807), type=pa.int64()),
            "pg_real": pa.array(_pair(1.25), type=pa.float64()),
            "pg_double": pa.array(_pair(2.5), type=pa.float64()),
            "pg_numeric": decimal_18_2([Decimal("12.30"), None]),
            "pg_text": pa.array(_pair("postgres"), type=pa.string()),
            "pg_bytea": pa.array(_pair(b"pg"), type=pa.binary()),
            "pg_date": pa.array(_pair(date(2024, 1, 15)), type=pa.date32()),
            "pg_timestamp": pa.array(_pair(naive), type=pa.timestamp("us")),
            "pg_timestamptz": pa.array(
                _pair(instant), type=pa.timestamp("us", tz="UTC")
            ),
            # MySQL vocabulary
            "my_row_id": pa.array([1, 2], type=pa.int64()),
            "my_tinyint_unsigned": pa.array(_pair(255), type=pa.int64()),
            "my_smallint_unsigned": pa.array(_pair(65535), type=pa.int64()),
            "my_mediumint": pa.array(_pair(8388607), type=pa.int64()),
            "my_bigint_unsigned": decimal_20_0(
                [Decimal("18446744073709551615"), None]
            ),
            "my_float": pa.array(_pair(1.25), type=pa.float64()),
            "my_double": pa.array(_pair(2.5), type=pa.float64()),
            "my_decimal": decimal_18_2([Decimal("12.30"), None]),
            "my_text": pa.array(_pair("mysql"), type=pa.string()),
            "my_binary": pa.array(_pair(b"my"), type=pa.binary()),
            "my_date": pa.array(_pair(date(2024, 1, 15)), type=pa.date32()),
            "my_datetime": pa.array(_pair(naive), type=pa.timestamp("us")),
            "my_timestamp": pa.array(
                _pair(instant), type=pa.timestamp("us", tz="UTC")
            ),
            "my_year": pa.array(_pair(2024), type=pa.int64()),
            # SQL Server vocabulary
            "ms_row_id": pa.array([1, 2], type=pa.int64()),
            "ms_bit": pa.array(_pair(True), type=pa.bool_()),
            "ms_tinyint": pa.array(_pair(255), type=pa.int64()),
            "ms_smallint": pa.array(_pair(32767), type=pa.int64()),
            "ms_int": pa.array(_pair(2147483647), type=pa.int64()),
            "ms_bigint": pa.array(_pair(9223372036854775807), type=pa.int64()),
            "ms_decimal": decimal_18_2([Decimal("12.30"), None]),
            "ms_money": decimal_19_4([Decimal("1234.5678"), None]),
            "ms_smallmoney": decimal_10_4([Decimal("12.3400"), None]),
            "ms_real": pa.array(_pair(1.25), type=pa.float64()),
            "ms_float": pa.array(_pair(2.5), type=pa.float64()),
            "ms_string": pa.array(_pair("mssql"), type=pa.string()),
            "ms_binary": pa.array(_pair(b"ms"), type=pa.binary()),
            "ms_date": pa.array(_pair(date(2024, 1, 15)), type=pa.date32()),
            "ms_datetime": pa.array(_pair(naive), type=pa.timestamp("us")),
            "ms_datetimeoffset": pa.array(
                _pair(instant), type=pa.timestamp("us", tz="UTC")
            ),
            # Oracle vocabulary
            "ora_row_id": pa.array([1, 2], type=pa.int64()),
            "ora_number": decimal_18_2([Decimal("12.30"), None]),
            "ora_binary_float": pa.array(_pair(1.25), type=pa.float64()),
            "ora_binary_double": pa.array(_pair(2.5), type=pa.float64()),
            "ora_float": pa.array(_pair(3.5), type=pa.float64()),
            "ora_string": pa.array(_pair("oracle"), type=pa.string()),
            "ora_binary": pa.array(_pair(b"ora"), type=pa.binary()),
            "ora_date": pa.array(_pair(naive), type=pa.timestamp("us")),
            "ora_timestamp": pa.array(_pair(naive), type=pa.timestamp("us")),
            "ora_timestamptz": pa.array(
                _pair(instant), type=pa.timestamp("us", tz="UTC")
            ),
            # SQLite vocabulary
            "sqlite_row_id": pa.array([1, 2], type=pa.int64()),
            "sqlite_integer": pa.array(_pair(9223372036854775807), type=pa.int64()),
            "sqlite_real": pa.array(_pair(2.5), type=pa.float64()),
            "sqlite_numeric": decimal_18_2([Decimal("12.30"), None]),
            "sqlite_text": pa.array(_pair("sqlite"), type=pa.string()),
            "sqlite_blob": pa.array(_pair(b"sqlite"), type=pa.binary()),
            "sqlite_date": pa.array(_pair(date(2024, 1, 15)), type=pa.date32()),
            "sqlite_datetime": pa.array(_pair(naive), type=pa.timestamp("us")),
            "sqlite_timestamp": pa.array(_pair(naive), type=pa.timestamp("us")),
            # Spark SQL vocabulary
            "spark_row_id": pa.array([1, 2], type=pa.int64()),
            "spark_boolean": pa.array(_pair(True), type=pa.bool_()),
            "spark_tinyint": pa.array(_pair(127), type=pa.int64()),
            "spark_smallint": pa.array(_pair(32767), type=pa.int64()),
            "spark_int": pa.array(_pair(2147483647), type=pa.int64()),
            "spark_bigint": pa.array(_pair(9223372036854775807), type=pa.int64()),
            "spark_float": pa.array(_pair(1.25), type=pa.float64()),
            "spark_double": pa.array(_pair(2.5), type=pa.float64()),
            "spark_decimal": decimal_18_2([Decimal("12.30"), None]),
            "spark_string": pa.array(_pair("spark"), type=pa.string()),
            "spark_binary": pa.array(_pair(b"spark"), type=pa.binary()),
            "spark_date": pa.array(_pair(date(2024, 1, 15)), type=pa.date32()),
            "spark_timestamp": pa.array(
                _pair(instant), type=pa.timestamp("us", tz="UTC")
            ),
            "spark_timestamp_ntz": pa.array(_pair(naive), type=pa.timestamp("us")),
        }
    )


def main() -> None:
    args = _parse_args()
    run_root = args.run_root.resolve()
    input_root = run_root / "input"
    output_root = run_root / "output"
    metadata_root = run_root / "metadata"
    canonical = json.loads(CANONICAL_METADATA.read_text(encoding="utf-8"))
    formats = tuple(item.strip().lower() for item in args.formats.split(",") if item.strip())
    unsupported = set(formats) - {"parquet", "delta", "iceberg"}
    if not formats or unsupported:
        raise SystemExit(f"Unsupported qualification formats: {sorted(unsupported)}")
    parquet_root = input_root / "parquet" / "sample"
    matrix_root = input_root / "parquet" / "matrix"
    csv_root = input_root / "csv" / "sample"
    json_root = input_root / "json" / "sample"
    jsonl_root = input_root / "jsonl" / "sample"
    parquet_root.mkdir(parents=True, exist_ok=True)
    matrix_root.mkdir(parents=True, exist_ok=True)
    csv_root.mkdir(parents=True, exist_ok=True)
    json_root.mkdir(parents=True, exist_ok=True)
    jsonl_root.mkdir(parents=True, exist_ok=True)
    metadata_root.mkdir(parents=True, exist_ok=True)
    for stale_metadata in metadata_root.glob("*.json"):
        stale_metadata.unlink()
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "amount": pa.array(
                    [Decimal("12.30"), None], type=pa.decimal128(18, 2)
                ),
                "event_date": pa.array(
                    [date(2024, 1, 15), None], type=pa.date32()
                ),
            }
        ),
        parquet_root / "sample.parquet",
    )
    pq.write_table(_matrix_table(), matrix_root / "matrix.parquet")
    with (csv_root / "sample.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["identifier", "label"])
        writer.writerows([["0000123", "alpha"], ["0000456", "beta"]])
    json_rows = [
        {"id": 1, "amount": 12.3, "event_date": "2024-01-15"},
        {"id": 2, "amount": None, "event_date": None},
    ]
    (json_root / "sample.json").write_text(
        json.dumps(json_rows, indent=2) + "\n", encoding="utf-8"
    )
    (jsonl_root / "sample.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in json_rows), encoding="utf-8"
    )
    for engine in ("polars", "spark"):
        for output_format in formats:
            _write_engine_metadata(
                canonical,
                metadata_root,
                output_root,
                engine,
                output_format,
                args.iceberg_table_suffix,
            )
    print(f"Datatype qualification fixture prepared: {run_root}")


if __name__ == "__main__":
    main()
