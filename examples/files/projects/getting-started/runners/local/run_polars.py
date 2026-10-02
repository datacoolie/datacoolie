"""Run one lesson from the canonical local Polars onboarding project."""

from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
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
        description="Run the canonical DataCoolie getting-started lesson with Polars."
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


def _require_columns(frame: pl.DataFrame, required: dict[str, str], *, label: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise GuardError(f"{label} is missing columns: {', '.join(missing)}")
    for column, kind in required.items():
        dtype = str(frame.schema[column]).lower()
        if kind not in dtype:
            raise GuardError(
                f"{label}.{column} has type {frame.schema[column]!s}; expected a {kind} type"
            )


def _require_system_columns(frame: pl.DataFrame, *, label: str) -> None:
    required = {"__created_at", "__updated_at", "__updated_by"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise GuardError(f"{label} is missing framework columns: {', '.join(missing)}")


def _read_delta(path: Path, *, label: str) -> pl.DataFrame:
    require_delta_path(path, label=label)
    try:
        return pl.read_delta(str(path))
    except Exception as exc:
        raise GuardError(f"Cannot read {label} Delta output: {path}") from exc


def _validate_orders_output(path: Path, rows: list[dict[str, str]], *, label: str) -> dict[str, Any]:
    frame = _read_delta(path, label=label)
    _require_columns(
        frame,
        {
            "order_id": "int",
            "customer_id": "int",
            "amount": "decimal",
            "updated_at": "datetime",
        },
        label=label,
    )
    _require_system_columns(frame, label=label)
    expected = latest_orders(rows)
    actual_ids = {int(value) for value in frame.get_column("order_id").to_list()}
    expected_ids = set(expected)
    if actual_ids != expected_ids or frame.height != len(expected_ids):
        raise GuardError(
            f"{label} IDs/row count mismatch: actual_ids={sorted(actual_ids)} "
            f"rows={frame.height}, expected_ids={sorted(expected_ids)}"
        )
    actual = {
        int(row["order_id"]): row
        for row in frame.select("order_id", "customer_id", "amount").to_dicts()
    }
    for order_id, source in expected.items():
        output = actual[order_id]
        if int(output["customer_id"]) != parse_int(source["customer_id"], column="customer_id"):
            raise GuardError(f"{label} customer_id mismatch for order_id {order_id}")
        if Decimal(str(output["amount"])).quantize(Decimal("0.01")) != parse_amount(source["amount"]):
            raise GuardError(f"{label} amount mismatch for order_id {order_id}")
    return {"rows": frame.height, "order_ids": sorted(actual_ids), "columns": frame.columns}


def _validate_customers_output(path: Path, rows: list[dict[str, str]]) -> dict[str, Any]:
    frame = _read_delta(path, label="Customers")
    _require_columns(frame, {"customer_id": "int", "name": "string"}, label="Customers")
    _require_system_columns(frame, label="Customers")
    expected = {
        parse_int(row["customer_id"], column="customer_id"): row["name"]
        for row in rows
    }
    actual = {
        int(row["customer_id"]): str(row["name"])
        for row in frame.select("customer_id", "name").to_dicts()
    }
    if actual != expected:
        raise GuardError(f"Customers output mismatch: actual={actual}, expected={expected}")
    return {"rows": frame.height, "customer_ids": sorted(actual)}


def _validate_silver_output(path: Path, rows: list[dict[str, str]]) -> dict[str, Any]:
    frame = _read_delta(path, label="Silver")
    _require_columns(
        frame,
        {
            "order_id": "int",
            "customer_id": "int",
            "amount": "decimal",
            "updated_at": "datetime",
            "order_date": "date",
        },
        label="Silver",
    )
    result = _validate_orders_output(path, rows, label="Silver")
    actual_dates = {
        int(row["order_id"]): str(row["order_date"])
        for row in frame.select("order_id", "order_date").to_dicts()
    }
    expected_dates = {
        order_id: source["updated_at"][:10]
        for order_id, source in latest_orders(rows).items()
    }
    if actual_dates != expected_dates:
        raise GuardError(f"Silver order_date mismatch: actual={actual_dates}, expected={expected_dates}")
    partition_dirs = sorted(path.rglob("order_date=*") if path.is_dir() else ())
    if not partition_dirs:
        raise GuardError(f"Silver output has no order_date partitions: {path}")
    result["partitions"] = [item.name for item in partition_dirs]
    result["order_dates"] = actual_dates
    return result


def _result_payload(
    lesson: str,
    stage: str,
    counts: dict[str, Any],
    **details: Any,
) -> dict[str, Any]:
    return {"lesson": lesson, "ok": True, "results": [{"stage": stage, **counts}], **details}


def _run_lesson(args: argparse.Namespace) -> dict[str, Any]:
    root = _project_root()
    os.chdir(root)
    state_root = resolve_root(root, args.state_base_path).resolve()
    platform = LocalPlatform()
    metadata = FileProvider(
        metadata_base_path=str(root / "metadata"),
        platform=platform,
        watermark_base_path=str(state_root / "watermarks"),
    )
    config = DataCoolieRunConfig(
        job_id="getting-started-local-polars",
        max_workers=1,
        stop_on_error=True,
        allowed_function_prefixes=[],
    )
    engine = PolarsEngine(platform=platform)
    orders_path = root / "data" / "output" / "bronze" / "orders"
    silver_path = root / "data" / "output" / "silver" / "orders"
    customers_path = root / "data" / "output" / "customers" / "customers"

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
            selected = require_named_dataflows(
                driver.load_dataflows(stage="customers_full_refresh", active_only=True),
                expected_name="customers_full_refresh",
                stage="customers_full_refresh",
            )
            counts = require_terminal_result(
                driver.run(dataflows=selected),
                lesson="customers_full_refresh",
            )
            details = _validate_customers_output(customers_path, customer_rows)
            return _result_payload(
                args.lesson,
                "customers_full_refresh",
                counts,
                output=details,
            )

        _, order_rows = _orders_input(root)
        bronze = require_named_dataflows(
            driver.load_dataflows(stage="ingest2bronze", active_only=True),
            expected_name="orders_to_bronze",
            stage="ingest2bronze",
        )
        bronze_counts = require_terminal_result(
            driver.run(dataflows=bronze),
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
        bronze_details = _validate_orders_output(orders_path, order_rows, label="Bronze")

        if args.lesson == "orders":
            return _result_payload(
                args.lesson,
                "ingest2bronze",
                bronze_counts,
                output=bronze_details,
                no_change_watermark=watermark,
            )

        silver = require_named_dataflows(
            driver.load_dataflows(stage="bronze2silver", active_only=True),
            expected_name="orders_to_silver",
            stage="bronze2silver",
        )
        silver_counts = require_terminal_result(
            driver.run(dataflows=silver),
            lesson="orders_to_silver",
        )
        silver_details = _validate_silver_output(silver_path, order_rows)
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
