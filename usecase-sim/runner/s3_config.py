"""Import-light S3 settings shared by the simulator's object-store clients.

The simulator uses an S3-compatible endpoint for local qualification.  Keep
the provider-neutral settings in one module so boto3, Polars, PyIceberg,
Spark and Compose-facing child processes cannot silently drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from collections.abc import Mapping


S3_ENDPOINT_ENV = "DATACOOLIE_SIM_S3_ENDPOINT"
S3_ACCESS_KEY_ENV = "DATACOOLIE_SIM_S3_ACCESS_KEY"
S3_SECRET_KEY_ENV = "DATACOOLIE_SIM_S3_SECRET_KEY"
S3_REGION_ENV = "DATACOOLIE_SIM_S3_REGION"
S3_BUCKET_ENV = "DATACOOLIE_SIM_S3_BUCKET"

DEFAULT_S3_ENDPOINT = "http://localhost:9000"
DEFAULT_S3_ACCESS_KEY = "minioadmin"
DEFAULT_S3_SECRET_KEY = "minioadmin"
DEFAULT_S3_REGION = "us-east-1"
DEFAULT_S3_BUCKET = "datacoolie-test"


def _configured(environ: Mapping[str, str], name: str, default: str) -> str:
    """Return a non-empty explicit value, or the local fixture default."""

    value = environ.get(name)
    return value if value else default


@dataclass(frozen=True, slots=True)
class S3Config:
    """Resolved S3 settings for one simulator invocation."""

    endpoint: str
    access_key: str
    secret_key: str
    region: str
    bucket: str

    def storage_options(self) -> dict[str, str]:
        """Return object-store options used by Polars and PyIceberg."""

        return {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_endpoint_url": self.endpoint,
            "aws_region": self.region,
            "aws_allow_http": "true",
        }

    def spark_hadoop_config(self) -> dict[str, str]:
        """Return Hadoop S3A settings used by Spark and Delta."""

        return {
            "spark.hadoop.fs.s3a.endpoint": self.endpoint,
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # Bare ``s3://`` paths returned by AWSPlatform are handled by S3A.
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }

    def iceberg_properties(self, prefix: str = "") -> dict[str, str]:
        """Return S3FileIO properties for an Iceberg catalog.

        ``prefix`` is empty for PyIceberg and includes the Spark catalog
        prefix (for example ``spark.sql.catalog.local_catalog.``) for Spark.
        """

        return {
            f"{prefix}s3.endpoint": self.endpoint,
            f"{prefix}s3.access-key-id": self.access_key,
            f"{prefix}s3.secret-access-key": self.secret_key,
            f"{prefix}s3.path-style-access": "true",
            f"{prefix}s3.region": self.region,
        }


def get_s3_config(environ: Mapping[str, str] | None = None) -> S3Config:
    """Resolve S3 settings at call time so child environments can override them."""

    values = os.environ if environ is None else environ
    return S3Config(
        endpoint=_configured(values, S3_ENDPOINT_ENV, DEFAULT_S3_ENDPOINT),
        access_key=_configured(values, S3_ACCESS_KEY_ENV, DEFAULT_S3_ACCESS_KEY),
        secret_key=_configured(values, S3_SECRET_KEY_ENV, DEFAULT_S3_SECRET_KEY),
        region=_configured(values, S3_REGION_ENV, DEFAULT_S3_REGION),
        bucket=_configured(values, S3_BUCKET_ENV, DEFAULT_S3_BUCKET),
    )


__all__ = [
    "DEFAULT_S3_ACCESS_KEY",
    "DEFAULT_S3_BUCKET",
    "DEFAULT_S3_ENDPOINT",
    "DEFAULT_S3_REGION",
    "DEFAULT_S3_SECRET_KEY",
    "S3_ACCESS_KEY_ENV",
    "S3_BUCKET_ENV",
    "S3_ENDPOINT_ENV",
    "S3_REGION_ENV",
    "S3_SECRET_KEY_ENV",
    "S3Config",
    "get_s3_config",
]
