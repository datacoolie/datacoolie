"""Paired live-database qualification for vendor-specific datatype families."""

from __future__ import annotations

import json
import os
import shutil
from urllib.request import Request, urlopen
from uuid import uuid4

import pytest

from tests.support.data_types import (
    assert_observation_matches,
    compare_observations,
    observe_delta_table,
    observe_iceberg_table,
    observe_parquet_dataset,
)
from tests.support.data_types.database_matrix import (
    PRODUCT_ROOT,
    address_matrix_metadata,
    build_matrix_scenario,
    require_matrix_drivers,
    run_matrix_scenario,
    seed_matrix_source,
)
from tests.support.datatype_config import require_docker_services
from tests.support.s3_config import host_s3_config, host_s3_properties


pytestmark = [
    pytest.mark.integration,
    pytest.mark.datatype_qualification,
    pytest.mark.spark,
    pytest.mark.xdist_group("spark"),
]


EXPECTED_CONTRACTS = {
    dialect: json.loads(
        (
            PRODUCT_ROOT
            / "tests"
            / "fixtures"
            / "data_types"
            / "database_vendor_contract.json"
        ).read_text(encoding="utf-8")
    )[f"datatype_matrix_{dialect}"]
    for dialect in ("mysql", "mssql", "oracle", "sqlite")
}


def _iceberg_catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "datacoolie-database-matrix-qualification",
        type="rest",
        uri=os.getenv("DATACOOLIE_QUALIFICATION_ICEBERG_URI", "http://localhost:8181"),
        **host_s3_properties(),
    )


def _cleanup_iceberg_objects(table_names: list[str]) -> None:
    """Delete only the warehouse prefixes owned by this matrix run."""

    if not table_names:
        return
    import boto3

    settings = host_s3_config()
    client = boto3.client(
        "s3",
        endpoint_url=settings.endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
    )
    for qualified_name in table_names:
        namespace, table = qualified_name.split(".", 1)
        prefix = f"iceberg-warehouse/{namespace}/{table}/"
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=settings.bucket, Prefix=prefix):
            keys = [item["Key"] for item in page.get("Contents", [])]
            if not keys:
                continue
            response = client.delete_objects(
                Bucket=settings.bucket,
                Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
            )
            errors = response.get("Errors", [])
            if errors:
                raise AssertionError(
                    f"RustFS cleanup failed for {qualified_name}: {errors}"
                )


def _trino_query(statement: str) -> list[list[object]]:
    """Execute one bounded Trino statement and follow its result cursor."""

    base_url = os.getenv(
        "DATACOOLIE_QUALIFICATION_TRINO_URI",
        "http://trino:8080"
        if os.getenv("DATACOOLIE_QUALIFICATION_CONTAINER") == "1"
        else "http://localhost:8080",
    ).rstrip("/")
    request = Request(
        f"{base_url}/v1/statement",
        data=statement.encode("utf-8"),
        headers={"X-Trino-User": "datacoolie"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - scoped local URI
        payload = json.load(response)
    rows: list[list[object]] = list(payload.get("data", []))
    while payload.get("nextUri"):
        with urlopen(payload["nextUri"], timeout=30) as response:  # noqa: S310
            payload = json.load(response)
        rows.extend(payload.get("data", []))
    if payload.get("error"):
        raise AssertionError(f"Trino query failed: {payload['error']}")
    return rows


def _assert_trino_table(table_name: str, expected: dict) -> None:
    """Check exact row-key cardinality and aggregates through Trino."""

    row_key = next(
        name for name in expected["rows"][0] if name.endswith("_row_id")
    )
    qualified = f'iceberg.default."{table_name}"'
    quoted_key = f'"{row_key}"'
    rows = _trino_query(
        "SELECT count(*), sum(CAST({key} AS DECIMAL(38,0))), "
        "min(CAST({key} AS DECIMAL(38,0))), max(CAST({key} AS DECIMAL(38,0))) "
        "FROM {table}".format(key=quoted_key, table=qualified)
    )
    if len(rows) != 1 or len(rows[0]) != 4:
        raise AssertionError(f"Unexpected Trino result for {table_name}: {rows}")
    actual = rows[0]
    expected_values = [
        int(row[row_key]["decimal"] if isinstance(row[row_key], dict) else row[row_key])
        for row in expected["rows"]
    ]
    assert int(actual[0]) == expected["row_count"]
    assert int(str(actual[1])) == sum(expected_values)
    assert int(str(actual[2])) == min(expected_values)
    assert int(str(actual[3])) == max(expected_values)


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
def test_vendor_matrix_source_matches_between_polars_and_spark(
    dialect: str,
    service: str | None,
    output_format: str,
) -> None:
    """Run one dialect's full vendor matrix through both framework readers."""

    required = ["spark", "rustfs", "iceberg-rest"]
    if output_format == "iceberg":
        required.append("trino")
    if service:
        required.insert(0, service)
    require_docker_services(required)
    require_matrix_drivers((dialect,))
    run_token = uuid4().hex[:12]
    run_root = (
        PRODUCT_ROOT
        / "usecase-sim"
        / ".runtime"
        / "data"
        / "datatype_qualification"
        / "vendor_matrix_runs"
        / f"{dialect}_{run_token}"
    )
    source_table = f"dc_dtype_matrix_{dialect}_{run_token}"
    sqlite_path = run_root / "source.sqlite" if dialect == "sqlite" else None
    run_root.mkdir(parents=True, exist_ok=True)
    source = None
    catalog = None
    iceberg_tables: list[str] = []
    try:
        # Keep the cleanup guard active across driver import/seed failures too;
        # an optional native dependency must not leave an empty run directory.
        source = seed_matrix_source(
            dialect, table=source_table, sqlite_path=sqlite_path
        )
        scenarios: dict[str, dict] = {}
        destination_tables: dict[str, str] = {}
        for engine in ("polars", "spark"):
            metadata_path, destination_table = address_matrix_metadata(
                dialect=dialect,
                engine=engine,
                output_format=output_format,
                run_root=run_root,
                source_table=source.table,
                table_suffix=f"_{run_token}",
                sqlite_path=source.sqlite_path,
            )
            # Include format and run token in scenario/receipt identity so a
            # repeated matrix run cannot overwrite another cell's evidence.
            name = f"local_{engine}_{dialect}_{output_format}_{run_token}_vendor_matrix"
            scenarios[name] = build_matrix_scenario(
                run_root=run_root,
                metadata_path=metadata_path,
                engine=engine,
                output_format=output_format,
            )
            destination_tables[engine] = destination_table

        scenario_path = run_root / "scenarios.json"
        scenario_path.write_text(
            json.dumps(scenarios, indent=2) + "\n", encoding="utf-8"
        )
        for engine in ("polars", "spark"):
            run_matrix_scenario(
                f"local_{engine}_{dialect}_{output_format}_{run_token}_vendor_matrix",
                scenario_path,
            )

        if output_format == "iceberg":
            catalog = _iceberg_catalog()
            observations = {}
            for engine in ("polars", "spark"):
                table_name = f"default.{destination_tables[engine]}"
                iceberg_tables.append(table_name)
                observations[engine] = observe_iceberg_table(
                    catalog,
                    table_name,
                    case_id=f"datatype_matrix_{dialect}",
                    engine=engine,
                )
        else:
            observer = (
                observe_delta_table
                if output_format == "delta"
                else observe_parquet_dataset
            )
            output_root = run_root / "output" / output_format
            observations = {
                engine: observer(
                    output_root / engine,
                    table_name=f"datatype_matrix_{dialect}",
                    case_id=f"datatype_matrix_{dialect}",
                    engine=engine,
                    output_format=output_format,
                )
                for engine in ("polars", "spark")
            }

        expected = EXPECTED_CONTRACTS[dialect]
        for observation in observations.values():
            try:
                assert_observation_matches(observation, expected)
            except AssertionError as exc:
                raise AssertionError(
                    f"{dialect}/{output_format}/{observation.engine}: {exc}"
                ) from exc
        compare_observations(observations["polars"], observations["spark"])
        if output_format == "iceberg":
            for engine in ("polars", "spark"):
                _assert_trino_table(
                    destination_tables[engine],
                    expected,
                )
    finally:
        cleanup_errors: list[str] = []
        if catalog is not None:
            for table_name in iceberg_tables:
                try:
                    catalog.drop_table(table_name)
                except Exception as exc:  # noqa: BLE001 - report cleanup failure
                    cleanup_errors.append(f"drop_table {table_name}: {exc}")
            try:
                _cleanup_iceberg_objects(iceberg_tables)
            except Exception as exc:  # noqa: BLE001 - report cleanup failure
                cleanup_errors.append(f"RustFS object cleanup: {exc}")
        if source is not None:
            try:
                source.cleanup()
            except Exception as exc:  # noqa: BLE001 - report cleanup failure
                cleanup_errors.append(f"source cleanup: {exc}")
        if run_root.exists():
            try:
                shutil.rmtree(run_root)
            except Exception as exc:  # noqa: BLE001 - report cleanup failure
                cleanup_errors.append(f"run-root cleanup: {exc}")
        if cleanup_errors:
            raise AssertionError("; ".join(cleanup_errors))
