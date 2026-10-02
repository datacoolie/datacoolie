"""Test-only helpers for live vendor datatype matrix qualification.

The canonical matrix metadata remains under ``usecase-sim/metadata``.  This
module only creates an owned source snapshot, addresses that metadata to the
snapshot and dispatches the black-box simulator runner.  No production reader
or resolver is imported here.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
CANONICAL_METADATA = (
    PRODUCT_ROOT
    / "usecase-sim"
    / "metadata"
    / "file"
    / "datatype_qualification.json"
)


@dataclass(slots=True)
class SeededMatrixSource:
    """Owned source state and cleanup callback for one dialect."""

    table: str
    database: Any
    cleanup: Callable[[], None]
    sqlite_path: Path | None = None


def _safe_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe test identifier: {value!r}")
    return value


def _seed_mysql(table: str) -> SeededMatrixSource:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = sqlalchemy.create_engine(
        os.getenv(
            "DATACOOLIE_QUALIFICATION_MYSQL_URL",
            "mysql+pymysql://datacoolie:datacoolie@localhost:3306/datacoolie",
        )
    )
    sql = sqlalchemy.text
    table = _safe_identifier(table)
    with database.begin() as connection:
        connection.execute(
            sql(
                f"CREATE TABLE {table} ("
                "my_row_id BIGINT, "
                "my_tinyint_unsigned TINYINT UNSIGNED, "
                "my_smallint_unsigned SMALLINT UNSIGNED, "
                "my_mediumint MEDIUMINT, "
                "my_bigint_unsigned BIGINT UNSIGNED, "
                "my_float FLOAT, my_double DOUBLE, "
                "my_decimal DECIMAL(18,2), my_text TEXT, my_binary BINARY(2), "
                "my_date DATE, my_datetime DATETIME(6), "
                "my_timestamp TIMESTAMP(6), my_year YEAR)"
            )
        )
        connection.execute(
            sql(
                f"INSERT INTO {table} VALUES ("
                ":row_id, :tiny, :small, :medium, :big, :float, :double, "
                ":decimal, :text, :binary, :date, :datetime, :timestamp, :year)"
            ),
            {
                "row_id": 1,
                "tiny": 255,
                "small": 65535,
                "medium": 8388607,
                "big": Decimal("18446744073709551615"),
                "float": 1.25,
                "double": 2.5,
                "decimal": Decimal("12.30"),
                "text": "mysql",
                "binary": b"my",
                "date": date(2024, 1, 15),
                "datetime": datetime(2024, 1, 15, 10, 30, 45, 123456),
                "timestamp": datetime(2024, 1, 15, 3, 30, 45, 123456),
                "year": "2024",
            },
        )
        connection.execute(
            sql(f"INSERT INTO {table} VALUES (" + ",".join([":row_id"] + [":null"] * 13) + ")"),
            {"row_id": 2, "null": None},
        )

    def cleanup() -> None:
        try:
            with database.begin() as connection:
                connection.execute(sql(f"DROP TABLE IF EXISTS {table}"))
        finally:
            database.dispose()

    return SeededMatrixSource(table=table, database=database, cleanup=cleanup)


def _seed_mssql(table: str) -> SeededMatrixSource:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = sqlalchemy.create_engine(
        os.getenv(
            "DATACOOLIE_QUALIFICATION_MSSQL_URL",
            "mssql+pymssql://sa:Datacoolie%401@localhost:1433/datacoolie",
        )
    )
    sql = sqlalchemy.text
    table = _safe_identifier(table)
    with database.begin() as connection:
        connection.execute(
            sql(
                f"CREATE TABLE {table} ("
                "ms_row_id INT, ms_bit BIT, ms_tinyint TINYINT, "
                "ms_smallint SMALLINT, ms_int INT, ms_bigint BIGINT, "
                "ms_decimal DECIMAL(18,2), ms_money MONEY, "
                "ms_smallmoney SMALLMONEY, ms_real REAL, ms_float FLOAT, "
                "ms_string NVARCHAR(50), ms_binary VARBINARY(2), "
                "ms_date DATE, ms_datetime DATETIME2(6), "
                "ms_datetimeoffset DATETIMEOFFSET(6))"
            )
        )
        connection.execute(
            sql(
                f"INSERT INTO {table} VALUES ("
                ":row_id, :bit, :tiny, :small, :int, :big, :decimal, :money, "
                ":smallmoney, :real, :float, :string, "
                "CONVERT(VARBINARY(2), :binary_hex, 2), :date, "
                "CAST('2024-01-15T10:30:45.123456' AS DATETIME2(6)), "
                "CAST('2024-01-15T03:30:45.123456+00:00' AS DATETIMEOFFSET(6)))"
            ),
            {
                "row_id": 1,
                "bit": True,
                "tiny": 255,
                "small": 32767,
                "int": 2147483647,
                "big": 9223372036854775807,
                "decimal": Decimal("12.30"),
                "money": Decimal("1234.5678"),
                "smallmoney": Decimal("12.3400"),
                "real": 1.25,
                "float": 2.5,
                "string": "mssql",
                # pymssql does not bind bytes to VARBINARY reliably through
                # the SQLAlchemy text path; use SQL Server's explicit hex
                # conversion so the fixture remains a real VARBINARY value.
                "binary_hex": "6D73",
                "date": date(2024, 1, 15),
            },
        )
        connection.execute(
            sql(f"INSERT INTO {table} VALUES (" + ",".join([":row_id"] + [":null"] * 15) + ")"),
            {"row_id": 2, "null": None},
        )

    def cleanup() -> None:
        try:
            with database.begin() as connection:
                connection.execute(sql(f"DROP TABLE IF EXISTS {table}"))
        finally:
            database.dispose()

    return SeededMatrixSource(table=table, database=database, cleanup=cleanup)


def _seed_oracle(table: str) -> SeededMatrixSource:
    oracledb = pytest.importorskip("oracledb")
    table = _safe_identifier(table).upper()
    connection = oracledb.connect(
        user=os.getenv("DATACOOLIE_QUALIFICATION_ORACLE_USER", "datacoolie"),
        password=os.getenv("DATACOOLIE_QUALIFICATION_ORACLE_PASSWORD", "datacoolie"),
        dsn=os.getenv(
            "DATACOOLIE_QUALIFICATION_ORACLE_DSN", "localhost:1521/FREEPDB1"
        ),
    )
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table} ("
            "ora_row_id NUMBER(10,0), ora_number NUMBER(18,2), "
            "ora_binary_float BINARY_FLOAT, ora_binary_double BINARY_DOUBLE, "
            "ora_float FLOAT(24), ora_string VARCHAR2(50), ora_binary RAW(2), "
            "ora_date DATE, ora_timestamp TIMESTAMP(6), "
            "ora_timestamptz TIMESTAMP(6) WITH TIME ZONE)"
        )
        cursor.execute(
            f"INSERT INTO {table} VALUES (:row_id, :number_value, :binary_float, "
            ":binary_double, :float_value, :string_value, :binary_value, "
            "TO_DATE('2024-01-15 10:30:45', 'YYYY-MM-DD HH24:MI:SS'), "
            "TO_TIMESTAMP('2024-01-15 10:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF6'), "
            "TO_TIMESTAMP_TZ('2024-01-15 03:30:45.123456 +00:00', "
            "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'))",
            {
                "row_id": 1,
                "number_value": Decimal("12.30"),
                "binary_float": 1.25,
                "binary_double": 2.5,
                "float_value": 3.5,
                "string_value": "oracle",
                "binary_value": b"or",
            },
        )
        cursor.execute(f"INSERT INTO {table} VALUES (2,{','.join(['NULL'] * 9)})")
    connection.commit()

    def cleanup() -> None:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE {table} PURGE")
            connection.commit()
        finally:
            connection.close()

    return SeededMatrixSource(table=table, database=connection, cleanup=cleanup)


def _seed_sqlite(path: Path, table: str) -> SeededMatrixSource:
    table = _safe_identifier(table)
    connection = sqlite3.connect(path)
    connection.execute(
        f"CREATE TABLE {table} ("
        "sqlite_row_id INTEGER, sqlite_integer INTEGER, sqlite_real REAL, "
        "sqlite_numeric NUMERIC, sqlite_text TEXT, sqlite_blob BLOB, "
        "sqlite_date DATE, sqlite_datetime DATETIME, sqlite_timestamp TIMESTAMP)"
    )
    connection.execute(
        f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            1,
            9223372036854775807,
            2.5,
            "12.30",
            "sqlite",
            b"sqlite",
            # SQLite stores DATE values as text.  Keep the canonical
            # date-time representation accepted by the xerial JDBC driver's
            # DATE getter; the metadata hint still projects it to a logical
            # date in both engines.
            "2024-01-15 00:00:00.000000",
            "2024-01-15 10:30:45.123456",
            "2024-01-15 10:30:45.123456",
        ),
    )
    connection.execute(
        f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?)",
        (2,) + (None,) * 8,
    )
    connection.commit()

    def cleanup() -> None:
        connection.close()

    return SeededMatrixSource(
        table=table,
        database=connection,
        cleanup=cleanup,
        sqlite_path=path,
    )


def seed_matrix_source(
    dialect: str, *, table: str, sqlite_path: Path | None = None
) -> SeededMatrixSource:
    """Create one run-owned table containing the full dialect matrix."""

    if dialect == "mysql":
        return _seed_mysql(table)
    if dialect == "mssql":
        return _seed_mssql(table)
    if dialect == "oracle":
        return _seed_oracle(table)
    if dialect == "sqlite":
        if sqlite_path is None:
            raise ValueError("sqlite_path is required for SQLite qualification")
        return _seed_sqlite(sqlite_path, table)
    raise ValueError(f"Unsupported matrix dialect: {dialect}")


def _source_url(
    dialect: str,
    *,
    engine: str,
    sqlite_path: Path | None,
) -> str:
    if dialect == "mysql":
        return (
            "mysql://datacoolie:datacoolie@localhost:3306/datacoolie"
            if engine == "polars"
            else "jdbc:mysql://host.docker.internal:3306/datacoolie"
        )
    if dialect == "mssql":
        return (
            "mssql://sa:Datacoolie%401@localhost:1433/datacoolie"
            if engine == "polars"
            else "jdbc:sqlserver://host.docker.internal:1433;databaseName=datacoolie;trustServerCertificate=true"
        )
    if dialect == "oracle":
        return (
            "oracle://datacoolie:datacoolie@localhost:1521/FREEPDB1"
            if engine == "polars"
            else "jdbc:oracle:thin:@host.docker.internal:1521/FREEPDB1"
        )
    if dialect == "sqlite":
        if sqlite_path is None:
            raise ValueError("sqlite_path is required for SQLite qualification")
        if engine == "polars":
            return f"sqlite:///{sqlite_path.as_posix()}"
        return f"jdbc:sqlite:/datacoolie/{sqlite_path.relative_to(PRODUCT_ROOT).as_posix()}"
    raise ValueError(f"Unsupported matrix dialect: {dialect}")


def address_matrix_metadata(
    *,
    dialect: str,
    engine: str,
    output_format: str,
    run_root: Path,
    source_table: str,
    table_suffix: str,
    sqlite_path: Path | None,
) -> tuple[Path, str]:
    """Address one canonical matrix flow to a real source snapshot."""

    metadata = json.loads(CANONICAL_METADATA.read_text(encoding="utf-8"))
    run_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    source_name = f"datatype_qualification_{dialect}_db_source"
    flow_name = f"datatype_matrix_{dialect}"
    for connection in metadata["connections"]:
        if connection["name"] == source_name:
            configure = connection["configure"]
            configure["url"] = _source_url(
                dialect, engine=engine, sqlite_path=sqlite_path
            )
            configure["use_schema_hint"] = True
            configure["schema_hint_type_system"] = dialect
            if engine == "polars":
                configure["database_read_engine"] = "native"
            elif dialect == "sqlite":
                # Xerial reports SQLite's weakly typed DATE/TIMESTAMP and
                # INTEGER affinities through JDBC getters that cannot
                # preserve the source text/int64 range.  An explicit Spark
                # custom schema keeps extraction lossless; SchemaConverter
                # then applies the authored SQLite hints identically to the
                # Polars native route.
                configure["read_options"] = {
                    "customSchema": (
                        "sqlite_row_id BIGINT,sqlite_integer BIGINT,"
                        "sqlite_real DOUBLE,sqlite_numeric DECIMAL(18,2),"
                        "sqlite_text STRING,sqlite_blob BINARY,"
                        "sqlite_date STRING,sqlite_datetime STRING,"
                        "sqlite_timestamp STRING"
                    )
                }
        if connection["name"] != "datatype_qualification_destination":
            continue
        configure = connection["configure"]
        configure["base_path"] = f"./{run_relative}/output/{output_format}/{engine}"
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
    selected = []
    for dataflow in metadata["dataflows"]:
        if dataflow["name"] != flow_name:
            continue
        dataflow["source"]["connection_name"] = source_name
        dataflow["source"]["table"] = source_table
        dataflow["destination"]["table"] = destination_table
        selected.append(dataflow)
    if len(selected) != 1:
        raise AssertionError(f"Canonical matrix flow missing: {flow_name}")
    metadata["dataflows"] = selected
    metadata_root = run_root / "metadata"
    metadata_root.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_root / f"{engine}_{dialect}_{output_format}.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata_path, destination_table


def build_matrix_scenario(
    *, run_root: Path, metadata_path: Path, engine: str, output_format: str
) -> dict[str, Any]:
    run_relative = run_root.relative_to(PRODUCT_ROOT).as_posix()
    scenario: dict[str, Any] = {
        "engine": engine,
        "metadata_type": "file",
        "platform": "local",
        "metadata_path": f"./{run_relative}/metadata/{metadata_path.name}",
        "stage": "datatype_qualification",
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


def run_matrix_scenario(name: str, scenarios_path: Path) -> None:
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
    if completed.returncode != 0:
        raise AssertionError(output[-20000:])
