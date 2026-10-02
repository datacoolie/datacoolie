"""Opt-in paired database extraction gates for non-PostgreSQL sources.

The file matrix and the PostgreSQL gate establish the persisted comparison
boundary.  These cases extend the same black-box metadata execution to the
other local database transports without importing production reader helpers.
Each parameter owns one source snapshot and one addressed metadata copy.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
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
POSTGRES_EXPECTED = json.loads(
    (
        PRODUCT_ROOT
        / "tests"
        / "fixtures"
        / "data_types"
        / "database_postgresql_contract.json"
    ).read_text(encoding="utf-8")
)
MYSQL_EXPECTED = json.loads(
    (
        PRODUCT_ROOT
        / "tests"
        / "fixtures"
        / "data_types"
        / "database_mysql_contract.json"
    ).read_text(encoding="utf-8")
)
TEXT_EXPECTED = json.loads(
    (
        PRODUCT_ROOT
        / "tests"
        / "fixtures"
        / "data_types"
        / "database_text_contract.json"
    ).read_text(encoding="utf-8")
)


def _iceberg_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "datacoolie-database-qualification",
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
    dialect: str,
    run_root: Path,
    engine: str,
    output_format: str,
    source_table: str,
    table_suffix: str,
    sqlite_path: Path | None = None,
) -> tuple[Path, str]:
    """Address one canonical DB flow without changing its semantic definition."""

    metadata = json.loads(CANONICAL_METADATA.read_text(encoding="utf-8"))
    run_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    connection_name = f"datatype_qualification_{dialect}_db_source"
    flow_name = f"datatype_database_{dialect}"

    if dialect == "mysql":
        source_url = (
            "mysql://datacoolie:datacoolie@localhost:3306/datacoolie"
            if engine == "polars"
            else "jdbc:mysql://host.docker.internal:3306/datacoolie"
        )
    elif dialect == "mssql":
        source_url = (
            "mssql://sa:Datacoolie%401@localhost:1433/datacoolie"
            if engine == "polars"
            else "jdbc:sqlserver://host.docker.internal:1433;databaseName=datacoolie;trustServerCertificate=true"
        )
    elif dialect == "oracle":
        source_url = (
            "oracle://datacoolie:datacoolie@localhost:1521/FREEPDB1"
            if engine == "polars"
            else "jdbc:oracle:thin:@host.docker.internal:1521/FREEPDB1"
        )
    elif dialect == "sqlite":
        assert sqlite_path is not None
        source_url = (
            f"sqlite:///{sqlite_path.as_posix()}"
            if engine == "polars"
            else f"jdbc:sqlite:/datacoolie/{sqlite_path.relative_to(PRODUCT_ROOT).as_posix()}"
        )
    else:  # pragma: no cover - guarded by the parameter list below
        raise AssertionError(f"Unsupported paired dialect: {dialect}")

    for connection in metadata["connections"]:
        if connection["name"] == connection_name:
            configure = connection["configure"]
            configure["url"] = source_url
            if dialect == "mysql":
                configure["username"] = "datacoolie"
                configure["password"] = "datacoolie"
                if engine == "polars":
                    # Keep the transport explicit: this gate qualifies the
                    # exact DB-API values, not ConnectorX's separate policy.
                    configure["database_read_engine"] = "native"
            elif dialect == "mssql":
                configure["username"] = "sa"
                configure["password"] = "Datacoolie@1"
                if engine == "polars":
                    configure["database_read_engine"] = "native"
            elif dialect == "oracle":
                configure["username"] = "datacoolie"
                configure["password"] = "datacoolie"
                if engine == "polars":
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

    destination_table = flow_name
    if output_format == "iceberg":
        destination_table = f"{flow_name}_{engine}_iceberg{table_suffix}"
    for dataflow in metadata["dataflows"]:
        if dataflow["name"] != flow_name:
            continue
        dataflow["source"]["table"] = source_table
        if dialect == "sqlite":
            dataflow["source"]["query"] = (
                f"SELECT label FROM {source_table}"
            )
        dataflow["destination"]["table"] = destination_table

    # The canonical file contains multiple live DB placeholders.  Address only
    # the selected source snapshot in this process.
    metadata["dataflows"] = [
        dataflow
        for dataflow in metadata["dataflows"]
        if dataflow["name"] == flow_name
    ]
    metadata_root = run_root / "metadata"
    metadata_root.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_root / f"{engine}_{dialect}_{output_format}.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata_path, destination_table


def _scenario_for(
    *, run_root: Path, metadata_path: Path, engine: str, output_format: str
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
        "pre_clean_paths": [f"{run_relative}/output/{output_format}/{engine}"],
    }
    if engine == "spark":
        scenario["max_workers"] = 1
    if output_format == "iceberg":
        scenario["needs_iceberg"] = True
    return scenario


def _seed_mysql(table: str):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = sqlalchemy.create_engine(
        "mysql+pymysql://datacoolie:datacoolie@localhost:3306/datacoolie"
    )
    text = sqlalchemy.text
    with database.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                "id BIGINT NOT NULL PRIMARY KEY, "
                "amount DECIMAL(18,2), "
                "event_date DATE, "
                "active BOOLEAN, "
                "label TEXT)"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} "
                "(id, amount, event_date, active, label) VALUES "
                "(1, 12.30, '2024-01-15', 1, 'alpha'), "
                "(2, NULL, NULL, NULL, NULL)"
            )
        )
    return database


def _seed_sqlite(path: Path, table: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        f"CREATE TABLE {table} ("
        "id INTEGER NOT NULL PRIMARY KEY, "
        "label TEXT)"
    )
    connection.executemany(
        f"INSERT INTO {table} (id, label) VALUES (?, ?)",
        [(1, "alpha"), (2, None)],
    )
    connection.commit()
    return connection


def _seed_mssql(table: str):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = sqlalchemy.create_engine(
        "mssql+pymssql://sa:Datacoolie%401@localhost:1433/datacoolie"
    )
    text = sqlalchemy.text
    with database.begin() as connection:
        connection.execute(text(f"CREATE TABLE {table} (label NVARCHAR(50))"))
        connection.execute(
            text(f"INSERT INTO {table} (label) VALUES ('alpha'), (NULL)")
        )
    return database


def _seed_oracle(table: str):
    oracledb = pytest.importorskip("oracledb")
    database = oracledb.connect(
        user="datacoolie",
        password="datacoolie",
        dsn="localhost:1521/FREEPDB1",
    )
    with database.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table} (label VARCHAR2(50))")
        cursor.executemany(
            f"INSERT INTO {table} (label) VALUES (:1)",
            [("alpha",), (None,)],
        )
    database.commit()
    return database


@pytest.mark.parametrize(
    ("dialect", "service"),
    [
        ("mysql", "mysql"),
        ("mssql", "mssql"),
        ("oracle", "oracle"),
        ("sqlite", None),
    ],
)
@pytest.mark.parametrize("output_format", ("parquet", "delta", "iceberg"))
def test_database_source_matches_between_polars_and_spark(
    dialect: str,
    service: str | None,
    output_format: str,
) -> None:
    """Run one live source snapshot through both framework readers."""

    required = ["spark", "minio", "iceberg-rest"]
    if service:
        required.insert(0, service)
    require_docker_services(required)
    run_token = uuid4().hex[:12]
    run_root = (
        PRODUCT_ROOT
        / "usecase-sim"
        / ".runtime"
        / "data"
        / "datatype_qualification"
        / "database_runs"
        / f"{dialect}_{run_token}"
    )
    source_table = f"dc_db_dtype_{dialect}_{run_token}"
    table_suffix = f"_{run_token}"
    sqlite_path = run_root / "source.sqlite" if dialect == "sqlite" else None
    run_root.mkdir(parents=True, exist_ok=True)
    database = (
        _seed_mysql(source_table)
        if dialect == "mysql"
        else _seed_mssql(source_table)
        if dialect == "mssql"
        else _seed_oracle(source_table)
        if dialect == "oracle"
        else _seed_sqlite(sqlite_path, source_table)
    )
    catalog = None
    iceberg_tables: list[str] = []
    try:
        scenarios: dict[str, dict] = {}
        destination_tables: dict[str, str] = {}
        for engine in ("polars", "spark"):
            metadata_path, destination_table = _metadata_for_engine(
                dialect=dialect,
                run_root=run_root,
                engine=engine,
                output_format=output_format,
                source_table=source_table,
                table_suffix=table_suffix,
                sqlite_path=sqlite_path,
            )
            name = f"local_{engine}_{dialect}_datatype_qualification"
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
        for engine in ("polars", "spark"):
            _run_scenario(
                f"local_{engine}_{dialect}_datatype_qualification", scenario_path
            )

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
                    case_id=f"datatype_database_{dialect}",
                    engine=engine,
                )
        else:
            observer = (
                observe_delta_table
                if output_format == "delta"
                else observe_parquet_dataset
            )
            observations = {
                engine: observer(
                    output_root / engine,
                    table_name=f"datatype_database_{dialect}",
                    case_id=f"datatype_database_{dialect}",
                    engine=engine,
                    output_format=output_format,
                )
                for engine in ("polars", "spark")
            }

        expected = MYSQL_EXPECTED if dialect == "mysql" else TEXT_EXPECTED
        for observation in observations.values():
            try:
                assert_observation_matches(observation, expected)
            except AssertionError as exc:
                raise AssertionError(
                    f"{dialect}/{output_format}/{observation.engine}: {exc}"
                ) from exc
        compare_observations(observations["polars"], observations["spark"])
    finally:
        if catalog is not None:
            for table_name in iceberg_tables:
                try:
                    catalog.drop_table(table_name)
                except Exception:  # noqa: BLE001 - preserve source cleanup
                    pass
        if dialect == "mysql":
            from sqlalchemy import text as sql_text

            with database.begin() as connection:
                connection.execute(sql_text(f"DROP TABLE IF EXISTS {source_table}"))
            database.dispose()
        elif dialect == "mssql":
            from sqlalchemy import text as sql_text

            with database.begin() as connection:
                connection.execute(sql_text(f"DROP TABLE IF EXISTS {source_table}"))
            database.dispose()
        elif dialect == "oracle":
            with database.cursor() as connection:
                connection.execute(f"DROP TABLE {source_table} PURGE")
            database.commit()
            database.close()
        else:
            database.close()
        if run_root.exists():
            shutil.rmtree(run_root)
