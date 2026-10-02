"""Paired live-database qualification for vendor-specific datatype families."""

from __future__ import annotations

import json
import shutil
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
    run_matrix_scenario,
    seed_matrix_source,
)
from tests.support.datatype_config import require_docker_services


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
        uri="http://localhost:8181",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


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
        / "vendor_matrix_runs"
        / f"{dialect}_{run_token}"
    )
    source_table = f"dc_dtype_matrix_{dialect}_{run_token}"
    sqlite_path = run_root / "source.sqlite" if dialect == "sqlite" else None
    run_root.mkdir(parents=True, exist_ok=True)
    source = seed_matrix_source(
        dialect, table=source_table, sqlite_path=sqlite_path
    )
    catalog = None
    iceberg_tables: list[str] = []
    try:
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
            name = f"local_{engine}_{dialect}_vendor_matrix"
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
                f"local_{engine}_{dialect}_vendor_matrix", scenario_path
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
    finally:
        if catalog is not None:
            for table_name in iceberg_tables:
                try:
                    catalog.drop_table(table_name)
                except Exception:  # noqa: BLE001 - preserve source cleanup
                    pass
        source.cleanup()
        if run_root.exists():
            shutil.rmtree(run_root)
