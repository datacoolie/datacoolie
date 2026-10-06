"""Isolated normal-Driver table/query/password and incremental database proofs."""

from __future__ import annotations
import argparse
import copy
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import (  # noqa: E402
    USECASE_SIM_DIR,
    assert_exact_rows,
    assert_successful_flow,
    business_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)

PROFILES = {
    "postgres": (
        "postgresql",
        5432,
        "datacoolie",
        "DATACOOLIE_PG_USER",
        "DATACOOLIE_PG_PASS",
        "datacoolie",
        "datacoolie",
    ),
    "mysql": (
        "mysql",
        3306,
        "datacoolie",
        "DATACOOLIE_MYSQL_USER",
        "DATACOOLIE_MYSQL_PASS",
        "datacoolie",
        "datacoolie",
    ),
    "mssql": (
        "mssql",
        1433,
        "datacoolie",
        "DATACOOLIE_MSSQL_USER",
        "DATACOOLIE_MSSQL_PASS",
        "sa",
        "Datacoolie@1",
    ),
    "oracle": (
        "oracle",
        1521,
        "FREEPDB1",
        "DATACOOLIE_ORA_USER",
        "DATACOOLIE_ORA_PASS",
        "datacoolie",
        "datacoolie",
    ),
}


def assert_typed_checkpoint(value, column):
    expected = {column: {"__datetime__": "2024-01-02T10:00:00"}}
    assert value == expected, f"Unexpected database checkpoint: {value!r}"


def _connection(profile, password=None):
    _, port, database, user_var, password_var, default_user, default_password = (
        PROFILES[profile]
    )
    user = os.environ.get(user_var, default_user)
    password = (
        os.environ.get(password_var, default_password) if password is None else password
    )
    if profile == "postgres":
        import psycopg2

        return psycopg2.connect(
            host="127.0.0.1",
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=5,
        )
    if profile == "mysql":
        import pymysql

        return pymysql.connect(
            host="127.0.0.1",
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5,
        )
    if profile == "mssql":
        import pymssql

        return pymssql.connect(
            server="127.0.0.1",
            port=port,
            database=database,
            user=user,
            password=password,
            login_timeout=5,
            encryption="off",
        )
    import oracledb

    return oracledb.connect(
        user=user, password=password, dsn=f"127.0.0.1:{port}/{database}"
    )


def _write(db, sql):
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    finally:
        cursor.close()


def _native_rows(path):
    import polars as pl

    files = sorted(path.glob("*.parquet"))
    assert files, "No persisted database output"
    frame = pl.concat([pl.read_parquet(p) for p in files])
    rows = business_rows(frame.to_dicts())
    schema = {k: str(v) for k, v in frame.schema.items() if not k.startswith("__")}
    assert schema == {
        "id": "Int64",
        "value": "String",
        "modified_at": "Datetime(time_unit='us', time_zone=None)",
    }, schema
    return rows


def validate(root=None, profile="postgres"):
    case = resolve_case_root("read", root)
    source_doc = json.loads(
        (USECASE_SIM_DIR / "metadata/file/read/local.json").read_text()
    )
    shared = json.loads(
        (USECASE_SIM_DIR / "metadata/file/shared/local.json").read_text()
    )
    authored = next(
        f for f in source_doc["dataflows"] if f["name"] == f"read_db__{profile}"
    )
    source_name = authored["source"]["connection_name"]
    connection = copy.deepcopy(
        next(c for c in shared["connections"] if c["name"] == source_name)
    )
    connection["configure"]["database_read_engine"] = "native"
    connection["configure"]["schema_hint_type_system"] = "spark"
    database_type, _, _, user_var, password_var, user_default, password_default = (
        PROFILES[profile]
    )
    connection["configure"]["database_type"] = database_type
    os.environ.setdefault(user_var, user_default)
    os.environ.setdefault(password_var, password_default)
    runtime_name = f"{authored['name']}-{uuid.uuid4().hex[:12]}"
    table = "a9_" + uuid.uuid4().hex[:16]
    flow = copy.deepcopy(authored)
    stage = "database_profile_" + table
    flow.update(name=runtime_name, stage=stage, group_number=1, execution_order=1)
    flow["transform"] = {"schema_hints": [{"column_name": "id", "data_type": "BIGINT"}]}
    flow["source"]["table"] = table
    flow["destination"]["table"] = "records"
    destination = {
        "name": flow["destination"]["connection_name"],
        "connection_type": "file",
        "format": "parquet",
        "configure": {"base_path": case.output.as_posix()},
    }
    document = {"connections": [connection, destination], "dataflows": [flow]}
    case.metadata.write_text(json.dumps(document, indent=2))
    db = _connection(profile)
    rows = [
        {"id": 1, "value": "first", "modified_at": datetime(2024, 1, 1, 10)},
        {"id": 2, "value": "newer", "modified_at": datetime(2024, 1, 2, 10)},
    ]
    types = (
        ("NUMBER(19)", "VARCHAR2(100)", "TIMESTAMP")
        if profile == "oracle"
        else (
            "BIGINT",
            "VARCHAR(100)",
            "DATETIME2"
            if profile == "mssql"
            else "TIMESTAMP"
            if profile == "postgres"
            else "DATETIME(6)",
        )
    )
    _write(
        db,
        f"CREATE TABLE {table} (id {types[0]}, value {types[1]}, modified_at {types[2]})",
    )
    timestamp = (
        "TIMESTAMP '2024-01-01 10:00:00'"
        if profile == "oracle"
        else "'2024-01-01 10:00:00'"
    )
    _write(db, f"INSERT INTO {table} VALUES (1,'first',{timestamp})")
    runs = []
    for iteration, query in [(1, None), (2, f"SELECT * FROM {table} WHERE id=1")]:
        if query:
            document["dataflows"][0]["source"]["query"] = query
        case.metadata.write_text(json.dumps(document, indent=2))
        evidence = run_driver(case, stage=stage, iteration=iteration)
        assert_successful_flow(evidence, runtime_name)
        assert_exact_rows(_native_rows(case.output / "records"), rows[:1], key="id")
        runs.append(evidence)
    # Admit a watermark branch on the same authored primary flow, then add a newer row.
    document["dataflows"][0]["source"].pop("query", None)
    document["dataflows"][0]["source"]["watermark_columns"] = [
        "MODIFIED_AT" if profile == "oracle" else "modified_at"
    ]
    case.metadata.write_text(json.dumps(document, indent=2))
    initial = run_driver(case, stage=stage, iteration=3)
    assert_successful_flow(initial, runtime_name)
    runs.append(initial)
    timestamp = (
        "TIMESTAMP '2024-01-02 10:00:00'"
        if profile == "oracle"
        else "'2024-01-02 10:00:00'"
    )
    _write(db, f"INSERT INTO {table} VALUES (2,'newer',{timestamp})")
    latest = run_driver(case, stage=stage, iteration=4)
    assert_successful_flow(latest, runtime_name)
    assert_exact_rows(_native_rows(case.output / "records"), rows[1:], key="id")
    runs.append(latest)
    checkpoints = sorted(case.watermarks.rglob("watermark_value.json"))
    assert len(checkpoints) == 1, f"Expected one incremental checkpoint, got {checkpoints}"
    snapshot = {str(p): p.read_bytes() for p in checkpoints}
    assert_typed_checkpoint(
        json.loads(checkpoints[0].read_text()),
        "MODIFIED_AT" if profile == "oracle" else "modified_at",
    )
    # Direct auth probe verifies the category independently of wrapped pipeline errors.
    wrong = "invalid-a9-development-password"
    try:
        unexpected = _connection(profile, password=wrong)
    except Exception as exc:
        markers = {
            "postgres": ("password authentication failed", "28P01"),
            "mysql": ("1045", "Access denied"),
            "mssql": ("18456", "Login failed"),
            "oracle": ("ORA-01017", "invalid username/password"),
        }
        assert any(marker.lower() in str(exc).lower() for marker in markers[profile]), (
            type(exc).__name__
        )
        auth_category = type(exc).__name__
    else:
        unexpected.close()
        raise AssertionError("Wrong password unexpectedly authenticated")
    original_password = os.environ[password_var]
    os.environ[password_var] = wrong
    try:
        failed = run_driver(case, stage=stage, iteration=5)
    finally:
        os.environ[password_var] = original_password
    assert {k: failed.result[k] for k in ("succeeded", "failed", "skipped")} == {
        "succeeded": 0,
        "failed": 1,
        "skipped": 0,
    }
    assert {str(p): p.read_bytes() for p in checkpoints} == snapshot
    assert_exact_rows(_native_rows(case.output / "records"), rows[1:], key="id")
    runs.append(failed)
    document["dataflows"][0]["source"]["query"] = (
        f"SELECT nonexistent_column FROM {table}"
    )
    cursor = db.cursor()
    try:
        cursor.execute(f"SELECT nonexistent_column FROM {table}")
    except Exception as exc:
        markers = {
            "postgres": ("42703", "does not exist"),
            "mysql": ("1054", "Unknown column"),
            "mssql": ("207", "Invalid column"),
            "oracle": ("ORA-00904", "invalid identifier"),
        }
        assert any(marker.lower() in str(exc).lower() for marker in markers[profile]), (
            type(exc).__name__
        )
        query_category = type(exc).__name__
        db.rollback()
    else:
        raise AssertionError("Invalid column query unexpectedly succeeded")
    finally:
        cursor.close()
    case.metadata.write_text(json.dumps(document, indent=2))
    failed_query = run_driver(case, stage=stage, iteration=6)
    assert {k: failed_query.result[k] for k in ("succeeded", "failed", "skipped")} == {
        "succeeded": 0,
        "failed": 1,
        "skipped": 0,
    }
    assert {str(p): p.read_bytes() for p in checkpoints} == snapshot
    assert_exact_rows(_native_rows(case.output / "records"), rows[1:], key="id")
    runs.append(failed_query)
    db.close()
    payload = receipt_payload(
        case,
        runs=runs,
        script=Path(__file__),
        details={
            "profile": profile,
            "source_table": table,
            "identity_mapping": {authored["name"]: runtime_name},
            "phase_contract": [
                "table",
                "query",
                "watermark-seed",
                "newer-selection",
                "wrong-password",
                "invalid-query",
            ],
            "exact_rows": rows,
            "native_auth_failure_category": auth_category,
            "query_failure_category": query_category,
            "checkpoint_unchanged_on_failures": True,
            "compiled_schema_hints": "explicit spark type system: id BIGINT",
            "compiled_watermark_column": "MODIFIED_AT"
            if profile == "oracle"
            else "modified_at",
        },
        qualification_gaps=[
            "Password-auth local service only; cloud identities remain unqualified."
        ],
    )
    payload["profile"].update(
        destination_format="parquet",
        qualification_scope=f"host-polars-docker-{profile}",
    )
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--profile", choices=PROFILES, default="postgres")
    args = parser.parse_args()
    print(validate(args.root, args.profile))
