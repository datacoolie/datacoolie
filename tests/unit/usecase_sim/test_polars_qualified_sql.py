"""Contract tests for the focused Polars qualified-SQL usecase fixture."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform


REPO_ROOT = Path(__file__).resolve().parents[3]
PREPARE_PATH = REPO_ROOT / "usecase-sim" / "scripts" / "prepare_polars_qualified_sql.py"
METADATA_PATH = (
    REPO_ROOT / "usecase-sim" / "metadata" / "file" / "polars_qualified_sql.json"
)
VALIDATOR_PATH = (
    REPO_ROOT / "usecase-sim" / "scripts" / "validate_polars_qualified_sql.py"
)
SCENARIOS_PATH = REPO_ROOT / "usecase-sim" / "scenarios" / "scenarios.json"
REGISTRATION_PATH = REPO_ROOT / "usecase-sim" / "runner" / "qualified_sql_setup.py"
EXPECTED_CASE_IDS = {
    "delta_name_4",
    "delta_name_3",
    "delta_name_2",
    "delta_name_1",
    "delta_include",
    "delta_exclude",
    "delta_lazy_reuse",
    "delta_ambiguity",
    "iceberg_default_root",
    "iceberg_logical_prefix",
    "iceberg_name_1",
    "iceberg_include",
    "iceberg_exclude",
    "iceberg_lazy_reuse",
    "iceberg_ambiguity",
}

SPEC = importlib.util.spec_from_file_location(
    "prepare_polars_qualified_sql", PREPARE_PATH
)
assert SPEC and SPEC.loader
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)

VALIDATOR_SPEC = importlib.util.spec_from_file_location(
    "validate_polars_qualified_sql", VALIDATOR_PATH
)
assert VALIDATOR_SPEC and VALIDATOR_SPEC.loader
validator = importlib.util.module_from_spec(VALIDATOR_SPEC)
VALIDATOR_SPEC.loader.exec_module(validator)

REGISTRATION_SPEC = importlib.util.spec_from_file_location(
    "qualified_sql_setup", REGISTRATION_PATH
)
assert REGISTRATION_SPEC and REGISTRATION_SPEC.loader
registration = importlib.util.module_from_spec(REGISTRATION_SPEC)
REGISTRATION_SPEC.loader.exec_module(registration)


def _write_result(
    output_root: Path,
    table_name: str,
    *,
    matched_rows: int = 3,
    total_amount: float = 60.0,
) -> None:
    path = output_root / table_name
    path.mkdir(parents=True)
    table = pa.table(
        {
            "matched_rows": pa.array([matched_rows], type=pa.uint32()),
            "total_amount": pa.array([total_amount], type=pa.float64()),
        }
    )
    pq.write_table(table, path / f"{table_name}.parquet")


def test_delta_fixture_setup_is_idempotent(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixtures"

    prepare.prepare_delta_suite("delta-positive", fixture_root=fixture_root)
    prepare.prepare_delta_suite("delta-positive", fixture_root=fixture_root)

    markers = sorted(fixture_root.rglob("_delta_log"))
    assert len(markers) == len(prepare.DELTA_TABLES["delta-positive"])


def test_delta_fixture_guard_rejects_fixture_root_itself(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be a child"):
        prepare._assert_scoped_delta_root(tmp_path, tmp_path)


@pytest.mark.parametrize(
    "identifier",
    [("default", "orders"), ("qsql",), ("prod", "schema", "orders")],
)
def test_iceberg_fixture_guard_rejects_non_test_namespace(
    identifier: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match=r"qsql_\*"):
        prepare._assert_scoped_iceberg_identifier(identifier)


def test_iceberg_fixture_cleanup_does_not_hide_catalog_failures() -> None:
    class _UnavailableCatalog:
        def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
            raise RuntimeError(f"catalog unavailable for {namespace}")

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        prepare._purge_tables(
            _UnavailableCatalog(),
            [("qsql_positive", "default_root", "orders")],
        )


def test_metadata_fixture_declares_one_primary_case_per_dataflow() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    dataflows = metadata["dataflows"]

    outputs = [dataflow["destination"]["table"] for dataflow in dataflows]

    assert len(dataflows) == 15
    assert set(outputs) == EXPECTED_CASE_IDS
    assert len(outputs) == len(set(outputs))
    assert all(dataflow["transform"] == {} for dataflow in dataflows)
    assert all(dataflow["source"].get("query") for dataflow in dataflows)
    assert all("python_function" not in dataflow["source"] for dataflow in dataflows)
    assert all("configure" not in dataflow["source"] for dataflow in dataflows)
    source_formats = {
        connection["name"]: connection["format"]
        for connection in metadata["connections"]
    }
    assert {
        source_formats[dataflow["source"]["connection_name"]] for dataflow in dataflows
    } == {"delta", "iceberg"}


def test_metadata_fixture_parses_through_file_provider() -> None:
    provider = FileProvider(config_path=str(METADATA_PATH), platform=LocalPlatform())

    dataflows = provider.get_dataflows()
    assert len(dataflows) == 15
    assert all(dataflow.source.query for dataflow in dataflows)
    assert all(dataflow.source.python_function is None for dataflow in dataflows)


def test_delta_positive_flows_share_one_parallel_execution_order_bucket() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    dataflows = [
        dataflow
        for dataflow in metadata["dataflows"]
        if dataflow["stage"] == "polars_qualified_sql_delta"
    ]

    assert len(dataflows) == 7
    assert {dataflow["group_number"] for dataflow in dataflows} == {3}
    assert {dataflow["execution_order"] for dataflow in dataflows} == {10}


def test_ambiguity_stages_are_isolated() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    negative_stages = {
        "polars_qualified_sql_delta_ambiguity",
        "polars_qualified_sql_iceberg_ambiguity",
    }

    counts = {
        stage: sum(dataflow["stage"] == stage for dataflow in metadata["dataflows"])
        for stage in negative_stages
    }

    assert counts == {stage: 1 for stage in negative_stages}


def test_qualified_sql_scenarios_are_focused_and_self_preparing() -> None:
    scenarios = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    expected = {
        "local_polars_qualified_sql_delta": ("delta-positive", None, 8),
        "local_polars_qualified_sql_delta_ambiguity": ("delta-ambiguity", None, 1),
        "local_polars_qualified_sql_iceberg": (
            "iceberg-positive",
            {"minio", "iceberg-rest"},
            1,
        ),
        "local_polars_qualified_sql_iceberg_ambiguity": (
            "iceberg-ambiguity",
            {"minio", "iceberg-rest"},
            1,
        ),
    }

    for scenario_name, (suite, services, max_workers) in expected.items():
        scenario = scenarios[scenario_name]
        assert scenario["engine"] == "polars"
        assert scenario["metadata_path"].endswith("polars_qualified_sql.json")
        assert scenario["max_workers"] == max_workers
        assert scenario["setup"]["script"].endswith("prepare_polars_qualified_sql.py")
        assert scenario["setup"]["args"] == ["--suite", suite]
        assert scenario["engine_setup"] == {
            "python_function": "runner.qualified_sql_setup.register_tables",
            "args": ["--suite", suite],
        }
        if services is None:
            assert "services" not in scenario
        else:
            assert set(scenario["services"]) == services
        if "ambiguity" not in scenario_name:
            assert scenario["validation"]["script"].endswith(
                "validate_polars_qualified_sql.py"
            )


def test_validator_accepts_reconciled_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(validator.EXPECTED_OUTPUTS, "delta", ("delta_case",))
    _write_result(tmp_path, "delta_case")

    validator.validate_suite("delta", output_root=tmp_path)


def test_validator_rejects_missing_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(validator.EXPECTED_OUTPUTS, "delta", ("missing",))

    with pytest.raises(AssertionError, match="no Parquet output"):
        validator.validate_suite("delta", output_root=tmp_path)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"matched_rows": 2}, "matched_rows=3"),
        ({"total_amount": 59.0}, "total_amount=60.0"),
    ],
)
def test_validator_rejects_incorrect_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, object],
    message: str,
) -> None:
    monkeypatch.setitem(validator.EXPECTED_OUTPUTS, "delta", ("delta_case",))
    _write_result(tmp_path, "delta_case", **overrides)

    with pytest.raises(AssertionError, match=message):
        validator.validate_suite("delta", output_root=tmp_path)


class _DeltaRegistrationEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def register_delta_tables(self, path: str, **options: Any) -> list[str]:
        self.calls.append((Path(path).name, options))
        names = {
            "name_4": "catalog_A.database_B.schema_C.orders_l4",
            "name_3": "catalog_A.database_B.schema_C.orders_l3",
            "name_2": "catalog_A.database_B.schema_C.orders_l2",
            "name_1": "catalog_A.database_B.schema_C.orders_l1",
            "include": "catalog_A.database_B.schema_C.d_daily",
            "exclude": "catalog_A.database_B.schema_C.orders_keep",
            "reuse": "catalog_A.database_B.schema_C.orders_reuse",
        }
        return [names[Path(path).name]]

    def execute_sql(self, query: str) -> None:
        raise AssertionError(f"registration setup must not execute SQL: {query}")


def test_registration_profile_only_registers_tables() -> None:
    engine = _DeltaRegistrationEngine()

    registration.register_tables(
        engine=engine,
        args=["--suite", "delta-positive"],
    )

    assert [name for name, _ in engine.calls] == [
        "name_4",
        "name_3",
        "name_2",
        "name_1",
        "include",
        "exclude",
        "reuse",
    ]
    assert engine.calls[4][1]["include"] == "database_B.**.d_*"
    assert engine.calls[5][1]["exclude"] == "**.*_tmp"
