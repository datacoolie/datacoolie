"""Local subprocess journey for the canonical getting-started project."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest


ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "getting-started"


def _copy_project(tmp_path: Path) -> Path:
    destination = tmp_path / "getting-started"
    shutil.copytree(
        PROJECT,
        destination,
        ignore=shutil.ignore_patterns(".runtime", ".builds", "output", "__pycache__"),
    )
    return destination


def _run(project: Path, lesson: str) -> tuple[subprocess.CompletedProcess[str], dict]:
    completed = subprocess.run(
        [
            sys.executable,
            "runners/local/run_polars.py",
            "--lesson",
            lesson,
            "--state-base-path",
            ".runtime",
        ],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    error_lines = [line for line in completed.stderr.splitlines() if line.startswith("{")]
    payload = json.loads((lines or error_lines)[-1]) if (lines or error_lines) else {}
    return completed, payload


def _orders(project: Path) -> pl.DataFrame:
    return (
        pl.read_delta(project / "data" / "output" / "bronze" / "orders")
        .select("order_id", "customer_id", "amount")
        .sort("order_id")
    )


def _customers(project: Path) -> pl.DataFrame:
    return (
        pl.read_delta(project / "data" / "output" / "customers" / "customers")
        .select("customer_id", "name")
        .sort("customer_id")
    )


def _silver(project: Path) -> pl.DataFrame:
    return (
        pl.read_delta(project / "data" / "output" / "silver" / "orders")
        .select("order_id", "customer_id", "amount", "order_date")
        .sort("order_id")
    )


def test_polars_first_no_change_append_customers_and_multistage(tmp_path: Path) -> None:
    project = _copy_project(tmp_path)

    first, first_payload = _run(project, "orders")
    assert first.returncode == 0, first.stderr
    assert first_payload["output"]["rows"] == 3
    assert first_payload["results"][0]["succeeded"] == 1
    first_rows = _orders(project)
    assert first_rows.select("order_id", "customer_id").rows() == [(1, 42), (2, 42), (3, 17)]
    assert first_rows["amount"].to_list() == [Decimal("19.99"), Decimal("29.00"), Decimal("5.50")]

    no_change, no_change_payload = _run(project, "orders")
    assert no_change.returncode == 0, no_change.stderr
    assert no_change_payload["output"]["rows"] == 3
    assert no_change_payload["results"][0]["skipped"] == 1
    assert _orders(project).equals(first_rows)

    with (project / "data" / "input" / "orders" / "orders.csv").open(
        "a", encoding="utf-8", newline=""
    ) as handle:
        handle.write("4,99,12.00,2026-04-04T08:00:00\n")
    appended, appended_payload = _run(project, "orders")
    assert appended.returncode == 0, appended.stderr
    assert appended_payload["output"]["rows"] == 4
    assert appended_payload["output"]["order_ids"] == [1, 2, 3, 4]
    appended_rows = _orders(project)
    assert appended_rows["order_id"].to_list() == [1, 2, 3, 4]
    assert appended_rows["amount"].sum() == Decimal("66.49")

    customers, customers_payload = _run(project, "customers")
    assert customers.returncode == 0, customers.stderr
    assert customers_payload["output"]["rows"] == 2
    assert customers_payload["output"]["customer_ids"] == [17, 42]
    assert _customers(project).rows() == [(17, "Bob"), (42, "Alice")]

    multi, multi_payload = _run(project, "multi-stage")
    assert multi.returncode == 0, multi.stderr
    assert multi_payload["bronze"]["rows"] == 4
    assert multi_payload["silver"]["rows"] == 4
    assert multi_payload["results"][0]["skipped"] == 1
    assert multi_payload["results"][1]["succeeded"] == 1
    silver_rows = _silver(project)
    assert silver_rows["order_id"].to_list() == [1, 2, 3, 4]
    assert silver_rows["order_date"].to_list() == [
        date(2026, 4, 1),
        date(2026, 4, 2),
        date(2026, 4, 3),
        date(2026, 4, 4),
    ]


def test_missing_input_after_success_cannot_promote_stale_output(tmp_path: Path) -> None:
    project = _copy_project(tmp_path)
    completed, _ = _run(project, "multi-stage")
    assert completed.returncode == 0, completed.stderr
    silver = project / "data" / "output" / "silver" / "orders"
    before = sorted(path.relative_to(silver).as_posix() for path in silver.rglob("*") if path.is_file())

    input_path = project / "data" / "input" / "orders" / "orders.csv"
    missing_path = input_path.with_suffix(".csv.missing")
    input_path.rename(missing_path)
    try:
        failed, payload = _run(project, "multi-stage")
    finally:
        missing_path.rename(input_path)

    assert failed.returncode != 0
    assert payload.get("ok") is False or "missing" in failed.stderr.lower()
    after = sorted(path.relative_to(silver).as_posix() for path in silver.rglob("*") if path.is_file())
    assert after == before


def test_unexpected_single_flow_name_fails_before_execution(tmp_path: Path) -> None:
    project = _copy_project(tmp_path)
    metadata_path = project / "metadata" / "dataflows.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["dataflows"][0]["name"] = "unexpected_orders_flow"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    failed, payload = _run(project, "orders")

    assert failed.returncode != 0
    assert payload.get("ok") is False
    assert "expected" in failed.stderr.lower()
    assert not (project / "data" / "output" / "bronze").exists()


@pytest.mark.parametrize("runner", ["run_polars.py", "run_spark.py"])
def test_project_runner_is_publicly_discoverable(runner: str) -> None:
    path = PROJECT / "runners" / "local" / runner
    assert path.is_file()
    assert "--lesson" in path.read_text(encoding="utf-8")
