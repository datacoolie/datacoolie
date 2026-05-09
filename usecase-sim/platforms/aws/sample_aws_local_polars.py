"""
sample_aws_local_polars.py — DataCoolie usecase-sim runner for AWS / Polars (local execution)

Runs the Polars pipeline locally against real AWS resources using the local
AWS CLI credentials configured with ``aws configure``.  No Glue runtime or
IAM execution role is required.

Usage
-----
    # Run all stages using the default AWS profile
    python sample_aws_local_polars.py

    # Run specific stages
    python sample_aws_local_polars.py --STAGE read_file,load_delta

    # Use a named AWS CLI profile
    python sample_aws_local_polars.py --PROFILE my-sso-profile

    # Override region or bucket
    python sample_aws_local_polars.py --REGION ap-southeast-1 --BUCKET de-dev-0007

Parameters (all optional — defaults shown below)
-------------------------------------------------
  --REGION           AWS region                         default: ap-southeast-1
  --BUCKET           S3 bucket name                     default: de-dev-0007
  --STAGE            Comma-separated stage names        default: "" (all stages)
  --PROFILE          AWS CLI named profile              default: "" (default profile)
  --METADATA_PATH    Full S3 path to the metadata JSON  default: s3://{BUCKET}/metadata/aws_glue_use_cases.json
  --LOG_BASE_PATH    S3 base path for DataCoolie logs   default: s3://{BUCKET}/logs/datacoolie
  --ICEBERG_CATALOG  pyiceberg catalog alias            default: glue_catalog
  --MAX_WORKERS      Parallel dataflow worker count     default: 4

Prerequisites
-------------
    pip install "datacoolie[aws-polars]"   # polars + deltalake + pyiceberg + boto3
    aws configure                           # or: aws configure --profile <name>

Required AWS permissions for the local IAM user / role
-------------------------------------------------------
  s3:GetObject / s3:PutObject / s3:DeleteObject on s3://de-dev-0007/*
  s3:ListBucket on s3://de-dev-0007
  secretsmanager:GetSecretValue for any secrets referenced in connections
  glue:GetDatabase / glue:GetTable / glue:GetTables / glue:CreateTable /
  glue:UpdateTable / glue:DeleteTable on the target Glue catalog database

Notes on Iceberg with PolarsEngine
-----------------------------------
  - A Glue-backed pyiceberg catalog is initialised using the local AWS credentials.
  - If pyiceberg is not installed, Delta and file stages still run; only Iceberg
    stages will fail.
"""

import sys
import logging

import boto3

from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.core import DataCoolieRunConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing — --NAME VALUE pairs
# ---------------------------------------------------------------------------
def _get_arg(name: str, default: str = "") -> str:
    """Read a --NAME VALUE pair from sys.argv."""
    try:
        idx = sys.argv.index(f"--{name}")
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return default


REGION: str = _get_arg("REGION", "ap-southeast-1")
BUCKET: str = _get_arg("BUCKET", "de-dev-0007")
STAGE: str = _get_arg("STAGE", "")           # empty = run all stages
PROFILE: str = _get_arg("PROFILE", "")       # empty = use the default profile
ICEBERG_CATALOG: str = _get_arg("ICEBERG_CATALOG", "glue_catalog")
MAX_WORKERS: int = int(_get_arg("MAX_WORKERS", "4"))

METADATA_PATH: str = _get_arg(
    "METADATA_PATH",
    f"s3://{BUCKET}/metadata/aws_glue_use_cases.json",
)
LOG_BASE_PATH: str = _get_arg(
    "LOG_BASE_PATH",
    f"s3://{BUCKET}/logs/datacoolie",
)
# Warehouse root — pyiceberg derives the table location as:
#   {ICEBERG_WAREHOUSE}/{database}.db/{table_name}
# This matches the Hive-style path that Glue Spark uses when the same
# warehouse is set in the Spark catalog conf.
ICEBERG_WAREHOUSE: str = _get_arg(
    "ICEBERG_WAREHOUSE",
    f"s3://{BUCKET}/output/iceberg/",
)

logger.info("Region       : %s", REGION)
logger.info("Bucket       : %s", BUCKET)
logger.info("Profile      : %s", PROFILE or "<default>")
logger.info("Stage(s)     : %s", STAGE or "<all>")
logger.info("Metadata     : %s", METADATA_PATH)
logger.info("Iceberg WH   : %s", ICEBERG_WAREHOUSE)
logger.info("Max workers  : %d", MAX_WORKERS)

# ---------------------------------------------------------------------------
# AWS credentials — resolved from the local AWS CLI configuration.
# boto3 reads ~/.aws/credentials and ~/.aws/config automatically.
# A named profile can be specified via --PROFILE.
# ---------------------------------------------------------------------------
session = boto3.Session(
    region_name=REGION,
    profile_name=PROFILE if PROFILE else None,
)
creds = session.get_credentials().get_frozen_credentials()

storage_options: dict[str, str] = {
    "aws_access_key_id":     creds.access_key,
    "aws_secret_access_key": creds.secret_key,
    "aws_region":            REGION,
}
# Session token is present when using SSO, assumed roles, or temporary credentials.
if creds.token:
    storage_options["aws_session_token"] = creds.token

logger.info(
    "Resolved AWS credentials (key prefix: %s...)",
    creds.access_key[:8] if creds.access_key else "<none>",
)


# ---------------------------------------------------------------------------
# pyiceberg Glue catalog — optional; graceful fallback when not installed.
# ---------------------------------------------------------------------------
def _build_iceberg_catalog(region: str, catalog_name: str, warehouse: str):
    """Initialise a Glue-backed pyiceberg catalog from local credentials.

    The ``warehouse`` URI is the S3 root used to derive table locations:
        {warehouse}/{database}.db/{table_name}
    This matches the Hive-style layout that Glue Spark uses when the same
    warehouse root is configured in the Spark catalog conf, ensuring both
    engines write to consistent S3 paths.
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        logger.info(
            "pyiceberg not installed — Iceberg stages will be skipped. "
            "Install with: pip install pyiceberg[glue]"
        )
        return None

    try:
        catalog = load_catalog(
            catalog_name,
            type="glue",
            **{
                "client.region": region,
                "warehouse": warehouse,
            },
        )
        logger.info(
            "Initialised pyiceberg Glue catalog '%s' in region %s (warehouse=%s)",
            catalog_name,
            region,
            warehouse,
        )
        return catalog
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Unable to initialise pyiceberg Glue catalog '%s': %s. "
            "Iceberg stages will fail if attempted.",
            catalog_name,
            exc,
        )
        return None


iceberg_catalog = _build_iceberg_catalog(REGION, ICEBERG_CATALOG, ICEBERG_WAREHOUSE)

# ---------------------------------------------------------------------------
# DataCoolie setup
# ---------------------------------------------------------------------------
platform = AWSPlatform(region=REGION, profile=PROFILE if PROFILE else None)

engine = PolarsEngine(
    platform=platform,
    storage_options=storage_options,
    iceberg_catalog=iceberg_catalog,
)

metadata = FileProvider(METADATA_PATH, platform=platform)

config = DataCoolieRunConfig(max_workers=MAX_WORKERS)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
with DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    config=config,
    base_log_path=LOG_BASE_PATH,
) as driver:
    result = driver.run(stage=STAGE if STAGE else None)

logger.info("DataCoolie run complete. result=%s", result)
