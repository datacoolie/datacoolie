"""Opt-in persisted-format qualification against the Spark 4.1 container."""

from __future__ import annotations

import pytest

from tests.integration.data_types.test_spark_container_cross_engine import (
    run_spark_container_cross_engine,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.datatype_qualification,
    pytest.mark.spark4_qualification,
    pytest.mark.spark,
    pytest.mark.xdist_group("spark4"),
]


def test_spark4_container_cross_engine_delta_and_iceberg() -> None:
    """Keep Spark 4.1 evidence separate from the default Spark 3.5 gate."""

    run_spark_container_cross_engine(
        container_name="datacoolie-spark4",
        service_name="spark4",
        expected_spark_major=4,
    )
