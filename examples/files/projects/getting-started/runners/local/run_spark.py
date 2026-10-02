"""Run one lesson from the canonical local Spark onboarding project."""

from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

from checks import (
    GuardError,
    latest_orders,
    parse_amount,
    parse_int,
    read_fixture,
    require_named_dataflows,
    require_delta_path,
    require_no_newer_rows,
    require_terminal_result,
    resolve_root,
)


LESSONS = ("orders", "customers", "multi-stage")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the canonical DataCoolie getting-started lesson with Spark."
    )
    parser.add_argument(
        "--lesson",
        choices=LESSONS,
        default="orders",
        help="Lesson to execute (default: orders).",
    )
    parser.add_argument(
        "--state-base-path",
        default=".runtime",
        help="Explicit runtime state and log root (default: .runtime).",
    )
    return parser.parse_args()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _orders_input(root: Path) -> tuple[Path, list[dict[str, str]]]:
    path = root / "data" / "input" / "orders" / "orders.csv"
    rows = read_fixture(path, ("order_id", "customer_id", "amount", "updated_at"))
    for row in rows:
        parse_int(row["order_id"], column="order_id")
        parse_int(row["customer_id"], column="customer_id")
        parse_amount(row["amount"])
    latest_orders(rows)
    return path, rows


def _customers_input(root: Path) -> tuple[Path, list[dict[str, str]]]:
    path = root / "data" / "input" / "customers" / "customers.csv"
    rows = read_fixture(path, ("customer_id", "name"))
    seen: set[int] = set()
    for row in rows:
        customer_id = parse_int(row["customer_id"], column="customer_id")
        if customer_id in seen:
            raise GuardError(f"Customers fixture contains duplicate customer_id: {customer_id}")
        if not row["name"]:
            raise GuardError(f"Customers fixture contains an empty name for customer_id {customer_id}")
        seen.add(customer_id)
    return path, rows


def _require_columns(frame: Any, required: dict[str, str], *, label: str) -> None:
    fields = {field.name: field.dataType.simpleString().lower() for field in frame.schema.fields}
    missing = sorted(set(required) - set(fields))
    if missing:
        raise GuardError(f"{label} is missing columns: {', '.join(missing)}")
    for column, kind in required.items():
        dtype = fields[column]
        if kind not in dtype:
            raise GuardError(f"{label}.{column} has type {dtype}; expected a {kind} type")


def _require_system_columns(frame: Any, *, label: str) -> None:
    required = {"__created_at", "__updated_at", "__updated_by"}
    missing = sorted(required - {field.name for field in frame.schema.fields})
    if missing:
        raise GuardError(f"{label} is missing framework columns: {', '.join(missing)}")


def _read_delta(spark: Any, path: Path, *, label: str) -> Any:
    require_delta_path(path, label=label)
    try:
        return spark.read.format("delta").load(str(path))
    except Exception as exc:
        raise GuardError(f"Cannot read {label} Delta output: {path}") from exc


def _validate_orders_output(
    spark: Any,
    path: Path,
    rows: list[dict[str, str]],
    *,
    label: str,
) -> dict[str, Any]:
    frame = _read_delta(spark, path, label=label)
    _require_columns(
        frame,
        {
            "order_id": "bigint",
            "customer_id": "bigint",
            "amount": "decimal",
            "updated_at": "timestamp",
        },
        label=label,
    )
    _require_system_columns(frame, label=label)
    expected = latest_orders(rows)
    actual_rows = frame.select("order_id", "customer_id", "amount").collect()
    actual_ids = {int(row["order_id"]) for row in actual_rows}
    expected_ids = set(expected)
    if actual_ids != expected_ids or len(actual_rows) != len(expected_ids):
        raise GuardError(
            f"{label} IDs/row count mismatch: actual_ids={sorted(actual_ids)} "
            f"rows={len(actual_rows)}, expected_ids={sorted(expected_ids)}"
        )
    actual = {int(row["order_id"]): row.asDict() for row in actual_rows}
    for order_id, source in expected.items():
        output = actual[order_id]
        if int(output["customer_id"]) != parse_int(source["customer_id"], column="customer_id"):
            raise GuardError(f"{label} customer_id mismatch for order_id {order_id}")
        if Decimal(str(output["amount"])).quantize(Decimal("0.01")) != parse_amount(source["amount"]):
            raise GuardError(f"{label} amount mismatch for order_id {order_id}")
    return {"rows": len(actual_rows), "order_ids": sorted(actual_ids)}


def _validate_customers_output(spark: Any, path: Path, rows: list[dict[str, str]]) -> dict[str, Any]:
    frame = _read_delta(spark, path, label="Customers")
    _require_columns(frame, {"customer_id": "bigint", "name": "string"}, label="Customers")
    _require_system_columns(frame, label="Customers")
    expected = {
        parse_int(row["customer_id"], column="customer_id"): row["name"]
        for row in rows
    }
    actual_rows = frame.select("customer_id", "name").collect()
    actual = {int(row["customer_id"]): str(row["name"]) for row in actual_rows}
    if actual != expected:
        raise GuardError(f"Customers output mismatch: actual={actual}, expected={expected}")
    return {"rows": len(actual_rows), "customer_ids": sorted(actual)}


def _validate_silver_output(spark: Any, path: Path, rows: list[dict[str, str]]) -> dict[str, Any]:
    frame = _read_delta(spark, path, label="Silver")
    _require_columns(
        frame,
        {
            "order_id": "bigint",
            "customer_id": "bigint",
            "amount": "decimal",
            "updated_at": "timestamp",
            "order_date": "date",
        },
        label="Silver",
    )
    result = _validate_orders_output(spark, path, rows, label="Silver")
    actual_dates = {
        int(row["order_id"]): str(row["order_date"])
        for row in frame.select("order_id", "order_date").collect()
    }
    expected_dates = {
        order_id: source["updated_at"][:10]
        for order_id, source in latest_orders(rows).items()
    }
    if actual_dates != expected_dates:
        raise GuardError(f"Silver order_date mismatch: actual={actual_dates}, expected={expected_dates}")
    partition_dirs = sorted(
        item.name
        for item in path.rglob("order_date=*")
        if item.is_dir()
    )
    if not partition_dirs:
        raise GuardError(f"Silver output has no order_date partitions: {path}")
    result["partitions"] = partition_dirs
    result["order_dates"] = actual_dates
    return result


def _spark_session() -> Any:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("datacoolie-getting-started")
        .master("local[2]")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _dataflows_for(
    metadata: FileProvider, root: Path, stage: str, expected_name: str
) -> list[Any]:
    """Load one stage and make local paths explicit for Spark's Delta SQL checks."""

    dataflows = require_named_dataflows(
        metadata.get_dataflows(stage=stage, active_only=True),
        expected_name=expected_name,
        stage=stage,
    )
    for dataflow in dataflows:
        for connection in (dataflow.source.connection, dataflow.destination.connection):
            base_path = connection.configure.get("base_path")
            if not isinstance(base_path, str) or not base_path.strip():
                continue
            if "://" in base_path or Path(base_path).is_absolute():
                continue
            connection.configure["base_path"] = str((root / base_path).resolve())
    return dataflows


def _run_lesson(args: argparse.Namespace) -> dict[str, Any]:
    root = _project_root()
    os.chdir(root)
    state_root = resolve_root(root, args.state_base_path).resolve()
    spark = _spark_session()
    platform = LocalPlatform()
    metadata = FileProvider(
        metadata_base_path=str(root / "metadata"),
        platform=platform,
        watermark_base_path=str(state_root / "watermarks"),
    )
    config = DataCoolieRunConfig(
        job_id="getting-started-local-spark",
        max_workers=1,
        stop_on_error=True,
        allowed_function_prefixes=[],
    )
    engine = SparkEngine(spark_session=spark, platform=platform)
    orders_path = root / "data" / "output" / "bronze" / "orders"
    silver_path = root / "data" / "output" / "silver" / "orders"
    customers_path = root / "data" / "output" / "customers" / "customers"

    try:
        with DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=metadata,
            state_base_path=str(state_root),
            log_base_path=str(state_root / "logs"),
            config=config,
        ) as driver:
            if args.lesson == "customers":
                _, customer_rows = _customers_input(root)
                counts = require_terminal_result(
                    driver.run(
                        dataflows=_dataflows_for(
                            metadata,
                            root,
                            "customers_full_refresh",
                            "customers_full_refresh",
                        )
                    ),
                    lesson="customers_full_refresh",
                )
                details = _validate_customers_output(spark, customers_path, customer_rows)
                return {
                    "lesson": args.lesson,
                    "ok": True,
                    "results": [{"stage": "customers_full_refresh", **counts}],
                    "output": details,
                }

            _, order_rows = _orders_input(root)
            bronze_counts = require_terminal_result(
                driver.run(
                    dataflows=_dataflows_for(
                        metadata, root, "ingest2bronze", "orders_to_bronze"
                    )
                ),
                lesson="orders_to_bronze",
                allow_skip=True,
            )
            watermark = None
            if bronze_counts["skipped"]:
                watermark = require_no_newer_rows(
                    order_rows,
                    runtime_root=state_root,
                    output_path=orders_path,
                ).isoformat()
            bronze_details = _validate_orders_output(spark, orders_path, order_rows, label="Bronze")

            if args.lesson == "orders":
                return {
                    "lesson": args.lesson,
                    "ok": True,
                    "results": [{"stage": "ingest2bronze", **bronze_counts}],
                    "output": bronze_details,
                    "no_change_watermark": watermark,
                }

            silver_counts = require_terminal_result(
                driver.run(
                    dataflows=_dataflows_for(
                        metadata, root, "bronze2silver", "orders_to_silver"
                    )
                ),
                lesson="orders_to_silver",
            )
            silver_details = _validate_silver_output(spark, silver_path, order_rows)
            return {
                "lesson": args.lesson,
                "ok": True,
                "results": [
                    {"stage": "ingest2bronze", **bronze_counts},
                    {"stage": "bronze2silver", **silver_counts},
                ],
                "bronze": bronze_details,
                "silver": silver_details,
                "no_change_watermark": watermark,
            }
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        payload = _run_lesson(args)
    except GuardError as exc:
        print(json.dumps({"lesson": args.lesson, "ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - keeps CLI failures machine-readable
        print(
            json.dumps(
                {"lesson": args.lesson, "ok": False, "error": f"{type(exc).__name__}: {exc}"}
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
