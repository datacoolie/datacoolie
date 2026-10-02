"""Assert the bounded native Spark/Polars Delta and Iceberg observations."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from tests.support.datatype_config import require_docker_services


pytestmark = [
    pytest.mark.integration,
    pytest.mark.datatype_qualification,
    pytest.mark.spark,
    pytest.mark.xdist_group("spark"),
]


PRODUCT_ROOT = Path(__file__).resolve().parents[3]


EXPECTED_POLARS = {
    "schema": {
        "id": "Int32",
        "amount": "Decimal(precision=10, scale=2)",
        "instant": "Datetime(time_unit='us', time_zone='UTC')",
        "wall_clock": "Datetime(time_unit='us', time_zone=None)",
    },
    "rows": [
        {
            "id": 1,
            "amount": {"decimal": "12.30"},
            "instant": {"datetime": "2024-01-15T03:30:45.123456+00:00"},
            "wall_clock": {"datetime": "2024-01-15T10:30:45.123456"},
        }
    ],
}
EXPECTED_SPARK = {
    "schema": "struct<id:bigint,amount:decimal(10,2),instant:timestamp,wall_clock:timestamp_ntz>",
    "rows": [
        {
            "id": 1,
            "amount": "12.30",
            "instant_us": 1705289445123456,
            "wall_clock": "2024-01-15 10:30:45.123456",
        }
    ],
}


def run_spark_container_cross_engine(
    *, container_name: str, service_name: str, expected_spark_major: int
) -> None:
    """Keep execution in one simulator container and assert its runtime line."""

    require_docker_services((service_name, "minio", "iceberg-rest"))
    docker = shutil.which("docker")
    if docker is None:  # pragma: no cover - preflight gives the diagnostic
        raise pytest.UsageError("Docker CLI is required for Spark qualification")
    run_token = uuid4().hex[:12]
    run_root = (
        PRODUCT_ROOT
        / "usecase-sim"
        / ".runtime"
        / "data"
        / "datatype-observation"
        / "runs"
        / run_token
    )
    result_path = run_root / "result.json"
    container_run_root = (
        f"/datacoolie/usecase-sim/.runtime/data/datatype-observation/runs/{run_token}"
    )
    container_result = f"{container_run_root}/result.json"
    run_root.mkdir(parents=True)
    try:
        worker = "/datacoolie/tests/support/data_types/container_worker.py"
        completed = subprocess.run(
            [
                docker,
                "exec",
                container_name,
                "python3",
                worker,
                "--result",
                container_result,
                "--run-root",
                container_run_root,
            ],
            cwd=PRODUCT_ROOT,
            capture_output=True,
            text=True,
            timeout=450,
            check=False,
        )
        assert result_path.is_file(), (
            f"container worker did not write {result_path}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
        result = json.loads(result_path.read_text(encoding="utf-8"))
        assert result.get("status") == "succeeded", result
        assert result.get("cleanup_errors") == [], result
        assert completed.returncode == 0, (
            f"worker exit={completed.returncode}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}\nresult={result}"
        )

        runtime = result.get("runtime", {})
        actual_major = int(str(runtime["pyspark"]).split(".")[0])
        assert actual_major == expected_spark_major, runtime

        observations = result["observations"]
        assert observations["delta_spark_to_polars"] == EXPECTED_POLARS
        assert observations["iceberg_spark_to_polars"] == EXPECTED_POLARS
        assert observations["delta_polars_to_spark"] == EXPECTED_SPARK
        assert observations["iceberg_polars_to_spark"] == EXPECTED_SPARK
    finally:
        shutil.rmtree(run_root, ignore_errors=True)


def test_spark_container_cross_engine_delta_and_iceberg() -> None:
    run_spark_container_cross_engine(
        container_name="datacoolie-spark",
        service_name="spark",
        expected_spark_major=3,
    )
