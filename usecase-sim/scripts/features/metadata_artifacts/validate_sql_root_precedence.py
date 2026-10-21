"""Qualify shorthand SQL resolution against explicit SQL and artifact roots."""

from __future__ import annotations

import argparse
import dataclasses
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    CasePaths,
    DriverEvidence,
    PolarsEngine,
    assert_exact_rows,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    write_receipt,
)
from datacoolie.core.models.run_config import DataCoolieRunConfig  # noqa: E402
from datacoolie.orchestration.driver import DataCoolieDriver  # noqa: E402
from datacoolie.platforms.local_platform import LocalPlatform  # noqa: E402
from runner.runtime_logs import job_sessions, records  # noqa: E402
from runner.runtime_paths import LOG_DIR  # noqa: E402
from runner.metadata_sources import make_case_provider  # noqa: E402


EXPECTED_SQL_ROWS = [
    {
        "order_id": 101,
        "amount": 10.0,
        "marker_id": 7101,
        "root_marker": "sql-root",
    },
    {
        "order_id": 102,
        "amount": 20.0,
        "marker_id": 7102,
        "root_marker": "sql-root",
    },
]
ARTIFACT_ROOT_ROWS = [
    {
        "order_id": 999,
        "amount": 99.0,
        "marker_id": 9701,
        "root_marker": "artifact-root",
    }
]
SOURCE_TABLE = "catalog_A.database_B.schema_C.orders_l1"

SQL_ROOT_QUERY = f"""\
SELECT order_id, amount, marker_id, 'sql-root' AS root_marker
FROM {SOURCE_TABLE}
WHERE order_id IN (101, 102)
ORDER BY order_id
"""
ARTIFACT_ROOT_QUERY = f"""\
SELECT order_id, amount, marker_id, 'artifact-root' AS root_marker
FROM {SOURCE_TABLE}
WHERE order_id = 999
ORDER BY order_id
"""


def assert_sql_root_precedence(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] | None = None,
    *,
    label: str = "sql-root-precedence",
) -> None:
    """Require the complete business output produced by the SQL root."""

    actual_business = business_rows(actual)
    expected_business = [
        dict(row) for row in (EXPECTED_SQL_ROWS if expected is None else expected)
    ]
    actual_columns = (
        set().union(*(row.keys() for row in actual_business))
        if actual_business
        else set()
    )
    expected_columns = (
        set().union(*(row.keys() for row in expected_business))
        if expected_business
        else set()
    )
    if actual_columns != expected_columns:
        raise AssertionError(
            f"{label} columns mismatch: actual={sorted(actual_columns)!r}, "
            f"expected={sorted(expected_columns)!r}"
        )
    assert_exact_rows(
        actual_business,
        expected_business,
        key="order_id",
        label=label,
    )


def _write_query_roots(case_root: Path) -> tuple[Path, Path]:
    sql_root = case_root / "sql"
    artifact_root = case_root / "artifact"
    sql_root.mkdir()
    artifact_root.mkdir()
    (sql_root / "orders.sql").write_text(SQL_ROOT_QUERY, encoding="utf-8")
    (artifact_root / "orders.sql").write_text(ARTIFACT_ROOT_QUERY, encoding="utf-8")
    return sql_root, artifact_root


def _registered_source(engine: PolarsEngine, case_root: Path) -> list[str]:
    registered_root = case_root / "registered"
    registered_root.mkdir()
    table_root = registered_root / "orders_l1"
    engine.write_to_path(
        engine.create_dataframe(
            [
                {"order_id": 101, "amount": 10.0, "marker_id": 7101},
                {"order_id": 102, "amount": 20.0, "marker_id": 7102},
                {"order_id": 999, "amount": 99.0, "marker_id": 9701},
            ]
        ),
        str(table_root),
        mode="overwrite",
        fmt="delta",
    )
    return engine.register_delta_tables(
        str(registered_root),
        logical_prefix=("catalog_A", "database_B", "schema_C"),
        recursive=True,
    )


def _run_registered_driver(
    case: CasePaths,
    *,
    sql_root: Path,
    artifact_root: Path,
    engine: PolarsEngine,
) -> DriverEvidence:
    """Run the authored flow through one normal Driver and registered engine."""

    job_id = f"focused-{case.group}-sql_root_precedence-1"
    previous = job_sessions(LOG_DIR, job_id)
    platform = engine.platform
    if platform is None:
        raise AssertionError("focused SQL case requires a bound local platform")
    provider = make_case_provider(
        case,
        platform=platform,
        watermark_base_path=str(case.watermarks / "provider"),
    )
    from runner.metadata_sources import case_metadata_provenance

    provenance = case_metadata_provenance(case, stage="sql_root_precedence")
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=provider,
        artifact_base_path=str(artifact_root),
        sql_base_path=str(sql_root),
        state_base_path=str(case.state),
        log_base_path=str(LOG_DIR),
        config=DataCoolieRunConfig(
            job_id=job_id,
            max_workers=1,
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        result = driver.run(stage="sql_root_precedence")

    sessions = job_sessions(LOG_DIR, job_id) - previous
    if len(sessions) != 1:
        raise AssertionError(f"expected one current framework session for {job_id}, got {sessions}")
    session_id = next(iter(sessions))
    evidence_records: list[dict[str, Any]] = []
    for path, value in records(LOG_DIR, job_id, "execution_logs", session_id):
        evidence_records.append(
            {
                "path": str(path.resolve().relative_to(LOG_DIR.resolve())).replace("\\", "/"),
                "record_type": value.get("_type"),
                "dataflow_id": value.get("dataflow_id"),
                "status": value.get("status"),
                "destination": value.get("destination"),
            }
        )
    return DriverEvidence(
        job_id=job_id,
        stage="sql_root_precedence",
        session_id=session_id,
        result=dataclasses.asdict(result),
        records=tuple(evidence_records),
        metadata=provenance,
    )


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("metadata_artifacts", root)
    materialize_fixture(case, "sql_root_precedence.json")
    sql_root, artifact_root = _write_query_roots(case.root)
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    registered_tables = _registered_source(engine, case.root)
    if SOURCE_TABLE not in registered_tables:
        raise AssertionError(
            f"registered source missing {SOURCE_TABLE!r}: {registered_tables!r}"
        )

    run = _run_registered_driver(
        case,
        sql_root=sql_root,
        artifact_root=artifact_root,
        engine=engine,
    )
    if run.result["succeeded"] != 1:
        raise AssertionError(f"SQL root precedence flow did not succeed: {run.result!r}")
    actual_rows = read_delta_rows(engine, case.output / "orders")
    assert_sql_root_precedence(actual_rows)

    payload = receipt_payload(
        case,
        runs=[run],
        details={
            "primary_flow": "sql-root-precedence",
            "flow_roles": {
                "sql-root-precedence": "primary",
                "registered-source-seed": "support",
                "persisted-output-readback": "support",
            },
            "sql_base_path": str(sql_root),
            "artifact_base_path": str(artifact_root),
            "logical_query_filename": "orders.sql",
            "registered_tables": registered_tables,
            "expected_sql_root_rows": EXPECTED_SQL_ROWS,
            "artifact_root_rows": ARTIFACT_ROOT_ROWS,
            "output_rows": business_rows(actual_rows),
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies only local native Polars plus Delta-rs query registration; Spark, Iceberg, and remote profiles remain unqualified.",
            "The focused flow proves SQL-root precedence over artifact fallback for a shorthand orders.sql reference; it does not claim priority among multiple SQL roots.",
        ),
        references=(
            "src/datacoolie/orchestration/preparation/query.py",
            "src/datacoolie/engines/_polars/registration.py",
            "src/datacoolie/engines/_polars/sql.py",
        ),
    )
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    payload = validate(args.root)
    print(
        {
            "status": payload["status"],
            "receipt": payload["receipt"],
            "root": payload["root"],
            "profile": payload["profile"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
