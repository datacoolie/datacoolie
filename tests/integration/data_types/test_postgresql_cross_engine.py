"""Opt-in paired PostgreSQL extraction qualification.

The file-source matrix proves persisted parity after a reader has already
materialised typed values.  This gate exercises the real PostgreSQL reader in
both engines against one run-owned source table, then compares each persisted
format to an independent contract and to its peer engine.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import date
from decimal import Decimal
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
CANONICAL_METADATA = (
    PRODUCT_ROOT
    / "usecase-sim"
    / "metadata"
    / "file"
    / "datatype_qualification.json"
)
EXPECTED_CONTRACT = json.loads(
    (
        PRODUCT_ROOT
        / "tests"
        / "fixtures"
        / "data_types"
        / "database_postgresql_contract.json"
    ).read_text(encoding="utf-8")
)

DEFAULT_SQLALCHEMY_URL = (
    "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie"
)
DEFAULT_POLARS_URL = "postgresql://datacoolie:datacoolie@localhost:5432/datacoolie"
DEFAULT_SPARK_URL = (
    "jdbc:postgresql://host.docker.internal:5432/datacoolie"
)


def _iceberg_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "datacoolie-postgresql-qualification",
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
        timeout=600,
        check=False,
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, output[-20000:]


def _metadata_for_engine(
    *,
    run_root: Path,
    engine: str,
    output_format: str,
    source_table: str,
    table_suffix: str,
) -> tuple[Path, str]:
    """Address canonical metadata without changing its semantic definition."""

    metadata = json.loads(CANONICAL_METADATA.read_text(encoding="utf-8"))
    run_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    source_url = DEFAULT_POLARS_URL if engine == "polars" else DEFAULT_SPARK_URL
    for connection in metadata["connections"]:
        if connection["name"] == "datatype_qualification_postgresql_db_source":
            configure = connection["configure"]
            configure["url"] = source_url
            configure["username"] = "datacoolie"
            configure["password"] = "datacoolie"
            if engine == "polars":
                # ConnectorX currently widens PostgreSQL NUMERIC values to a
                # generic Decimal(38, 10).  The native DB-API route preserves
                # the source result semantics for this qualification cell.
                configure["database_read_engine"] = "native"
        if connection["name"] != "datatype_qualification_destination":
            continue
        configure = connection["configure"]
        configure["base_path"] = (
            f"./{run_relative}/output/{output_format}/{engine}"
        )
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

    destination_table = "datatype_database_postgresql"
    if output_format == "iceberg":
        destination_table = (
            f"{destination_table}_{engine}_iceberg{table_suffix}"
        )
    for dataflow in metadata["dataflows"]:
        if dataflow["name"] != "datatype_database_postgresql":
            continue
        dataflow["source"]["table"] = source_table
        dataflow["destination"]["table"] = destination_table
    # The canonical fixture also contains the other live-database cells.  A
    # paired gate owns one source snapshot at a time; isolate this addressed
    # copy so the runner cannot execute unrelated run-owned placeholders.
    metadata["dataflows"] = [
        dataflow
        for dataflow in metadata["dataflows"]
        if dataflow["name"] == "datatype_database_postgresql"
    ]

    metadata_root = run_root / "metadata"
    metadata_root.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_root / f"{engine}_{output_format}.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata_path, destination_table


def _scenario_for(
    *,
    run_root: Path,
    metadata_path: Path,
    engine: str,
    output_format: str,
) -> dict:
    run_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    scenario = {
        "engine": engine,
        "metadata_type": "file",
        "platform": "local",
        "metadata_path": f"./{run_relative}/metadata/{metadata_path.name}",
        "stage": "datatype_database_qualification",
        "column_name_mode": "lower",
        "priority": "P3",
        "skip_api_sources": True,
        "timeout_seconds": 600,
        "pre_clean_paths": [
            f"{run_relative}/output/{output_format}/{engine}"
        ],
    }
    if engine == "spark":
        scenario["max_workers"] = 1
    if output_format == "iceberg":
        scenario["needs_iceberg"] = True
    return scenario


def _seed_source_table(table: str):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = sqlalchemy.create_engine(
        sqlalchemy.engine.url.make_url(
            os.environ.get(
                "DATACOOLIE_QUALIFICATION_POSTGRES_URL",
                DEFAULT_SQLALCHEMY_URL,
            )
        )
    )
    text = sqlalchemy.text
    with database.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                "id BIGINT NOT NULL PRIMARY KEY, "
                "amount NUMERIC(18,2), "
                "event_date DATE, "
                "active BOOLEAN, "
                "label TEXT)"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} "
                "(id, amount, event_date, active, label) VALUES "
                "(:id, :amount, :event_date, :active, :label)"
            ),
            [
                {
                    "id": 1,
                    "amount": Decimal("12.30"),
                    "event_date": date(2024, 1, 15),
                    "active": True,
                    "label": "alpha",
                },
                {
                    "id": 2,
                    "amount": None,
                    "event_date": None,
                    "active": None,
                    "label": None,
                },
            ],
        )
    return database


@pytest.mark.parametrize("output_format", ("parquet", "delta", "iceberg"))
def test_postgresql_source_matches_between_polars_and_spark(
    output_format: str,
) -> None:
    """Run the same live PostgreSQL source through both framework readers."""

    require_docker_services(("postgres", "spark", "minio", "iceberg-rest"))
    run_token = uuid4().hex[:12]
    run_root = (
        PRODUCT_ROOT
        / "usecase-sim"
        / ".runtime"
        / "data"
        / "datatype_qualification"
        / "database_runs"
        / run_token
    )
    source_table = f"dc_db_dtype_{run_token}"
    table_suffix = f"_{run_token}"
    database = _seed_source_table(source_table)
    catalog = None
    iceberg_tables: list[str] = []
    try:
        scenarios: dict[str, dict] = {}
        destination_tables: dict[str, str] = {}
        for engine in ("polars", "spark"):
            metadata_path, destination_table = _metadata_for_engine(
                run_root=run_root,
                engine=engine,
                output_format=output_format,
                source_table=source_table,
                table_suffix=table_suffix,
            )
            name = f"local_{engine}_postgresql_datatype_qualification"
            scenarios[name] = _scenario_for(
                run_root=run_root,
                metadata_path=metadata_path,
                engine=engine,
                output_format=output_format,
            )
            destination_tables[engine] = destination_table

        scenario_path = run_root / "scenarios.json"
        scenario_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_path.write_text(
            json.dumps(scenarios, indent=2) + "\n", encoding="utf-8"
        )
        _run_scenario("local_polars_postgresql_datatype_qualification", scenario_path)
        _run_scenario("local_spark_postgresql_datatype_qualification", scenario_path)

        output_root = run_root / "output" / output_format
        if output_format == "iceberg":
            catalog = _iceberg_catalog()
            observations = {}
            for engine in ("polars", "spark"):
                table_name = f"default.{destination_tables[engine]}"
                iceberg_tables.append(table_name)
                observations[engine] = observe_iceberg_table(
                    catalog,
                    table_name,
                    case_id="datatype_database_postgresql",
                    engine=engine,
                )
        else:
            observer = (
                observe_delta_table
                if output_format == "delta"
                else observe_parquet_dataset
            )
            missing_outputs = {
                engine: [path.as_posix() for path in (output_root / engine).rglob("*")]
                for engine in ("polars", "spark")
                if not (output_root / engine).exists()
            }
            if missing_outputs:
                raise AssertionError(
                    f"No addressed output roots for {output_format}: {missing_outputs}"
                )
            for engine in ("polars", "spark"):
                root = output_root / engine
                if output_format == "parquet":
                    files = [path.as_posix() for path in root.rglob("*.parquet")]
                else:
                    files = [path.as_posix() for path in root.rglob("*")]
                if not files:
                    raise AssertionError(
                        f"No {output_format} files for {engine} under {root}; "
                        f"tree={[path.as_posix() for path in root.rglob('*')]}"
                    )
            observations = {
                engine: observer(
                    output_root / engine,
                    table_name="datatype_database_postgresql",
                    case_id="datatype_database_postgresql",
                    engine=engine,
                    output_format=output_format,
                )
                for engine in ("polars", "spark")
            }
        for observation in observations.values():
            try:
                assert_observation_matches(observation, EXPECTED_CONTRACT)
            except AssertionError as exc:
                raise AssertionError(
                    f"{output_format}/{observation.engine}: {exc}"
                ) from exc
        compare_observations(observations["polars"], observations["spark"])
    finally:
        if catalog is not None:
            for table_name in iceberg_tables:
                try:
                    catalog.drop_table(table_name)
                except Exception:  # noqa: BLE001 - preserve source cleanup
                    pass
        try:
            from sqlalchemy import text as sql_text

            with database.begin() as connection:
                connection.execute(
                    sql_text(
                        f"DROP TABLE IF EXISTS {source_table}"
                    )
                )
        finally:
            database.dispose()
        if run_root.exists():
            shutil.rmtree(run_root)
