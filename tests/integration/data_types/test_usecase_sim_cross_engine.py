"""Opt-in equal-metadata Spark/Polars qualification through usecase-sim."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest

from tests.support.data_types import (
    assert_observation_matches,
    compare_observations,
    observe_delta_table,
    observe_iceberg_table,
    observe_parquet_dataset,
)
from tests.support.datatype_config import require_docker_services


pytestmark = [
    pytest.mark.integration,
    pytest.mark.datatype_qualification,
    pytest.mark.spark,
    pytest.mark.xdist_group("spark"),
]


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SCENARIOS_SOURCE = PRODUCT_ROOT / "usecase-sim" / "scenarios" / "scenarios.json"

EXPECTED_BUSINESS = json.loads(
    (
        PRODUCT_ROOT
        / "tests"
        / "fixtures"
        / "data_types"
        / "persisted_contract.json"
    ).read_text(encoding="utf-8")
)

CANONICAL_METADATA = json.loads(
    (
        PRODUCT_ROOT
        / "usecase-sim"
        / "metadata"
        / "file"
        / "datatype_qualification.json"
    ).read_text(encoding="utf-8")
)

QUALIFICATION_TABLES = (
    "datatype_decimal",
    "datatype_unhinted_csv",
    "datatype_unhinted_parquet",
    "datatype_unhinted_json",
    "datatype_unhinted_jsonl",
    "datatype_matrix_postgresql",
    "datatype_matrix_mysql",
    "datatype_matrix_mssql",
    "datatype_matrix_oracle",
    "datatype_matrix_sqlite",
    "datatype_matrix_spark_sql",
)


def test_independent_contract_covers_every_canonical_dataflow() -> None:
    """Do not let a newly added matrix flow escape persisted assertions."""

    canonical_tables = tuple(
        dataflow["destination"]["table"]
        for dataflow in CANONICAL_METADATA["dataflows"]
        if dataflow.get("stage") == "datatype_qualification"
    )
    assert canonical_tables == QUALIFICATION_TABLES
    assert set(canonical_tables) == set(EXPECTED_BUSINESS)


def _iceberg_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "datacoolie-datatype-qualification",
        type="rest",
        uri="http://localhost:8181",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def _run_scenario(name: str, scenarios_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "usecase-sim/runner/run_scenario.py",
            "--scenarios-path",
            str(scenarios_path),
            "--scenario",
            name,
        ],
        cwd=PRODUCT_ROOT,
        capture_output=True,
        text=True,
        timeout=450,
        check=False,
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, output[-16000:]


def _input_digest(input_root: Path) -> str:
    """Return a stable digest for the generated input files.

    The simulator prepares the fixtures once per engine invocation.  Hashing
    the isolated input root before and after the pair makes that implicit
    contract explicit without putting comparison logic in usecase-sim.
    """

    digest = hashlib.sha256()
    files = sorted(path for path in input_root.rglob("*") if path.is_file())
    if not files:
        raise AssertionError(f"No qualification input files found under {input_root}")
    for path in files:
        relative = path.relative_to(input_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _build_run_scenarios(
    *, run_root: Path, output_format: str, table_suffix: str, scenario_path: Path
) -> dict[str, str]:
    """Materialize only the paired simulator scenarios for one isolated run."""

    source = json.loads(SCENARIOS_SOURCE.read_text(encoding="utf-8"))
    run_root.mkdir(parents=True, exist_ok=True)
    run_root_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    output_relative = f"{run_root_relative}/output/{output_format}"
    selected: dict[str, dict] = {}
    names: dict[str, str] = {}
    for engine in ("polars", "spark"):
        name = f"local_{engine}_datatype_qualification"
        if output_format != "parquet":
            name += f"_{output_format}"
        scenario = deepcopy(source[name])
        scenario["metadata_path"] = (
            f"{run_root_relative}/metadata/{engine}_{output_format}.json"
        )
        scenario["pre_clean_paths"] = [f"{output_relative}/{engine}"]
        scenario["setup"]["args"] = [
            "--run-root",
            run_root_relative,
            "--formats",
            output_format,
            "--iceberg-table-suffix",
            table_suffix,
        ]
        selected[name] = scenario
        names[engine] = name
    scenario_path.write_text(json.dumps(selected, indent=2) + "\n", encoding="utf-8")
    return names


def test_shared_metadata_contract_matches_between_polars_and_spark() -> None:
    """Run each engine, then compare persisted observations for every format.

    This is deliberately not part of the default suite.  The Spark coordinate
    must be prepared explicitly by usecase-sim/Docker before this test runs;
    pytest never starts or mutates those services.
    """

    require_docker_services(("spark", "minio", "iceberg-rest"))
    run_token = uuid4().hex[:12]
    run_root = (
        PRODUCT_ROOT
        / "usecase-sim"
        / ".runtime"
        / "data"
        / "datatype_qualification"
        / "runs"
        / run_token
    )
    scenario_path = run_root / "scenarios.json"
    table_suffix = f"_{run_token}"
    iceberg_catalog = None
    iceberg_tables: list[str] = []
    try:
        for output_format in ("parquet", "delta", "iceberg"):
            names = _build_run_scenarios(
                run_root=run_root,
                output_format=output_format,
                table_suffix=table_suffix,
                scenario_path=scenario_path,
            )
            _run_scenario(names["polars"], scenario_path)
            input_digest = _input_digest(run_root / "input")
            _run_scenario(names["spark"], scenario_path)
            assert _input_digest(run_root / "input") == input_digest

            catalog = _iceberg_catalog() if output_format == "iceberg" else None
            if catalog is not None:
                iceberg_catalog = catalog
            output_root = run_root / "output"
            for table_name in QUALIFICATION_TABLES:
                if output_format == "iceberg":
                    assert catalog is not None
                    polars_table = f"default.{table_name}_polars_iceberg{table_suffix}"
                    spark_table = f"default.{table_name}_spark_iceberg{table_suffix}"
                    iceberg_tables.extend((polars_table, spark_table))
                    polars_observation = observe_iceberg_table(
                        catalog,
                        polars_table,
                        case_id=table_name,
                        engine="polars",
                    )
                    spark_observation = observe_iceberg_table(
                        catalog,
                        spark_table,
                        case_id=table_name,
                        engine="spark",
                    )
                else:
                    observer = (
                        observe_delta_table
                        if output_format == "delta"
                        else observe_parquet_dataset
                    )
                    polars_observation = observer(
                        output_root / output_format / "polars",
                        table_name=table_name,
                        case_id=table_name,
                        engine="polars",
                        output_format=output_format,
                    )
                    spark_observation = observer(
                        output_root / output_format / "spark",
                        table_name=table_name,
                        case_id=table_name,
                        engine="spark",
                        output_format=output_format,
                    )
                assert_observation_matches(polars_observation, EXPECTED_BUSINESS[table_name])
                assert_observation_matches(spark_observation, EXPECTED_BUSINESS[table_name])
                compare_observations(polars_observation, spark_observation)
    finally:
        cleanup_errors: list[str] = []
        if iceberg_catalog is not None:
            for table_name in iceberg_tables:
                try:
                    iceberg_catalog.drop_table(table_name)
                except Exception as exc:  # noqa: BLE001 - report cleanup status
                    cleanup_errors.append(f"drop_table {table_name}: {exc}")
        if cleanup_errors:
            if sys.exc_info()[0] is None:
                raise AssertionError("Iceberg cleanup failed: " + "; ".join(cleanup_errors))
            print("Iceberg cleanup failed: " + "; ".join(cleanup_errors))
        if run_root.exists():
            shutil.rmtree(run_root)
