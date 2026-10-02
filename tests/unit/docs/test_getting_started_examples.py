"""Contract checks for the canonical getting-started project."""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "getting-started"
RUNNERS = PROJECT / "runners" / "local"


def _load_checks():
    import importlib.util

    path = RUNNERS / "checks.py"
    spec = importlib.util.spec_from_file_location("getting_started_checks", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_project_contains_one_canonical_local_layout() -> None:
    config = (PROJECT / "datacoolie.yml").read_text(encoding="utf-8")
    assert "name: getting-started" in config
    assert "local:" in config

    connections = json.loads((PROJECT / "metadata" / "connections.json").read_text())
    assert {item["name"] for item in connections["connections"]} == {
        "local_input",
        "local_bronze",
        "local_silver",
        "local_customers",
    }
    flows = json.loads((PROJECT / "metadata" / "dataflows.json").read_text())["dataflows"]
    assert {item["name"] for item in flows} == {
        "orders_to_bronze",
        "customers_full_refresh",
        "orders_to_silver",
    }
    assert {item["stage"] for item in flows} == {
        "ingest2bronze",
        "customers_full_refresh",
        "bronze2silver",
    }

    with (PROJECT / "data" / "input" / "orders" / "orders.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 4
    assert [row["order_id"] for row in rows] == ["1", "2", "2", "3"]


def test_polars_and_spark_runners_have_explicit_lesson_and_runtime_contract() -> None:
    for name in ("run_polars.py", "run_spark.py"):
        source = (RUNNERS / name).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=name)
        assert '"multi-stage"' in source
        assert '".runtime"' in source
        assert "stop_on_error=True" in source
        assert "spark.stop()" in source if name == "run_spark.py" else "require_no_newer_rows" in source
        assert any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            for node in ast.walk(tree)
        )


def test_fixture_guards_reject_missing_input_and_unexpected_empty_selection(tmp_path: Path) -> None:
    checks = _load_checks()
    with pytest.raises(checks.GuardError, match="missing"):
        checks.read_fixture(tmp_path / "missing.csv", ("id",))

    empty = tmp_path / "empty.csv"
    empty.write_text("id\n", encoding="utf-8")
    with pytest.raises(checks.GuardError, match="no business rows"):
        checks.read_fixture(empty, ("id",))

    with pytest.raises(checks.GuardError, match="exactly one"):
        checks.require_terminal_result(
            SimpleNamespace(total=0, succeeded=0, failed=0, skipped=0, pending=0),
            lesson="orders_to_bronze",
        )


def test_terminal_guard_allows_only_an_explicit_incremental_skip() -> None:
    checks = _load_checks()
    result = SimpleNamespace(total=1, succeeded=0, failed=0, skipped=1, pending=0, errors={})
    with pytest.raises(checks.GuardError, match="skipped unexpectedly"):
        checks.require_terminal_result(result, lesson="orders_to_bronze")
    assert checks.require_terminal_result(result, lesson="orders_to_bronze", allow_skip=True)["skipped"] == 1


def test_flow_guard_rejects_a_single_unexpected_name() -> None:
    checks = _load_checks()
    with pytest.raises(checks.GuardError, match="expected"):
        checks.require_named_dataflows(
            [SimpleNamespace(name="unexpected_orders_flow")],
            expected_name="orders_to_bronze",
            stage="ingest2bronze",
        )
    selected = [SimpleNamespace(name="orders_to_bronze")]
    assert checks.require_named_dataflows(
        selected,
        expected_name="orders_to_bronze",
        stage="ingest2bronze",
    ) == selected
