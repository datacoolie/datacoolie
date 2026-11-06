from __future__ import annotations

import sys
from pathlib import Path

SIM_ROOT = Path(__file__).resolve().parents[3] / "usecase-sim"
if str(SIM_ROOT) not in sys.path:
    sys.path.insert(0, str(SIM_ROOT))

from runner.s3_config import (
    DEFAULT_S3_ENDPOINT,
    DEFAULT_S3_REGION,
    get_s3_config,
)


def test_s3_config_uses_local_fixture_defaults_without_legacy_minio_fallback() -> None:
    config = get_s3_config({"DATACOOLIE_MINIO_ENDPOINT": "http://old-store:9000"})

    assert config.endpoint == DEFAULT_S3_ENDPOINT
    assert config.region == DEFAULT_S3_REGION
    assert config.access_key == "minioadmin"
    assert config.bucket == "datacoolie-test"


def test_s3_config_resolves_all_values_from_the_child_environment() -> None:
    config = get_s3_config(
        {
            "DATACOOLIE_SIM_S3_ENDPOINT": "http://rustfs:9000",
            "DATACOOLIE_SIM_S3_ACCESS_KEY": "qualification-access",
            "DATACOOLIE_SIM_S3_SECRET_KEY": "qualification-secret",
            "DATACOOLIE_SIM_S3_REGION": "eu-test-1",
            "DATACOOLIE_SIM_S3_BUCKET": "qualification-bucket",
        }
    )

    assert config.storage_options() == {
        "aws_access_key_id": "qualification-access",
        "aws_secret_access_key": "qualification-secret",
        "aws_endpoint_url": "http://rustfs:9000",
        "aws_region": "eu-test-1",
        "aws_allow_http": "true",
    }
    assert config.spark_hadoop_config()["spark.hadoop.fs.s3a.endpoint"] == (
        "http://rustfs:9000"
    )
    assert config.iceberg_properties("spark.sql.catalog.local_catalog.") == {
        "spark.sql.catalog.local_catalog.s3.endpoint": "http://rustfs:9000",
        "spark.sql.catalog.local_catalog.s3.access-key-id": "qualification-access",
        "spark.sql.catalog.local_catalog.s3.secret-access-key": "qualification-secret",
        "spark.sql.catalog.local_catalog.s3.path-style-access": "true",
        "spark.sql.catalog.local_catalog.s3.region": "eu-test-1",
    }
