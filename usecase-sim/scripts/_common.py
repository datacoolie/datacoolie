"""Shared helpers and constants for usecase-sim scripts/.

Kept intentionally minimal — import-light, no heavy deps at module load.
Deps like boto3 / pyiceberg / SQLAlchemy are imported lazily inside helpers
so that scripts needing only a subset of functionality don't pay for unused
imports.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("usecase_sim")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
USECASE_SIM = SCRIPTS_DIR.parent
DATACOOLIE = USECASE_SIM.parent

DATA_DIR = USECASE_SIM / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
PERF_DIR = DATA_DIR / "perf"
PERF_INPUT_DIR = PERF_DIR / "input"
PERF_OUTPUT_DIR = PERF_DIR / "output"

METADATA_DIR = USECASE_SIM / "metadata"
METADATA_FILE_DIR = METADATA_DIR / "file"
METADATA_DB_DIR = METADATA_DIR / "database"
METADATA_API_DIR = METADATA_DIR / "api"
FILE_WATERMARKS_DIR = METADATA_FILE_DIR / "watermarks"
API_WATERMARKS_DIR = METADATA_API_DIR / "watermarks"

DOCKER_DIR = USECASE_SIM / "docker"
COMPOSE_FILE = DOCKER_DIR / "docker-compose.yml"

LOCAL_USE_CASES_JSON = METADATA_FILE_DIR / "local_use_cases.json"
AWS_USE_CASES_JSON = METADATA_FILE_DIR / "aws_use_cases.json"
PERF_USE_CASES_JSON = METADATA_FILE_DIR / "perf_test.json"

# ---------------------------------------------------------------------------
# MinIO / Iceberg defaults (match docker-compose.yml + _runner_utils.py)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"
MINIO_BUCKET = "datacoolie-test"

ICEBERG_REST_URI = "http://localhost:8181"

# ---------------------------------------------------------------------------
# Default database connection strings per dialect
# ---------------------------------------------------------------------------
DB_URLS: dict[str, str] = {
    "sqlite":     f"sqlite:///{METADATA_DB_DIR / 'datacoolie_metadata.db'}",
    "postgresql": "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie",
    "mysql":      "mysql+pymysql://datacoolie:datacoolie@localhost:3306/datacoolie",
    "mssql":      "mssql+pymssql://sa:Datacoolie%401@localhost:1433/datacoolie",
    "oracle":     "oracle+oracledb://datacoolie:datacoolie@localhost:1521/?service_name=FREEPDB1",
}

# ---------------------------------------------------------------------------
# Canonical sample dataset (30 rows) — used by generate_data and mock_api
# ---------------------------------------------------------------------------
SAMPLE_COLUMNS = [
    "order_id", "order_date", "amount", "quantity",
    "region", "modified_at", "customer_name", "status",
]

SAMPLE_ROWS: list[tuple] = [
    (1001, "2024-01-15", "199.99",  2, "US-East", "2024-01-15T10:00:00", "Alice Johnson", "completed"),
    (1002, "2024-01-15",  "49.50",  1, "US-West", "2024-01-15T10:05:00", "Bob Smith",     "completed"),
    (1003, "2024-01-16", "325.00",  5, "EU-West", "2024-01-16T08:30:00", "Clara Müller",  "shipped"),
    (1004, "2024-01-16",  "15.75",  3, "US-East", "2024-01-16T09:00:00", "David Lee",     "pending"),
    (1005, "2024-01-17",  "89.00",  1, "APAC",    "2024-01-17T12:00:00", "Emily Chen",    "completed"),
    (1006, "2024-01-17", "210.50",  4, "US-West", "2024-01-17T12:30:00", "Frank Garcia",  "cancelled"),
    (1007, "2024-01-18", "750.00", 10, "EU-West", "2024-01-18T07:00:00", "Greta Berg",    "completed"),
    (1008, "2024-01-18",  "34.99",  2, "US-East", "2024-01-18T08:15:00", "Henry Park",    "shipped"),
    (1009, "2024-01-19", "120.00",  3, "APAC",    "2024-01-19T14:00:00", "Isha Patel",    "completed"),
    (1010, "2024-01-19",  "55.25",  1, "US-West", "2024-01-19T14:30:00", "James Brown",   "pending"),
    (1011, "2024-01-20", "430.00",  6, "EU-West", "2024-01-20T09:00:00", "Katrin Johansson", "completed"),
    (1012, "2024-01-20",  "22.50",  1, "US-East", "2024-01-20T09:30:00", "Liam Wilson",   "shipped"),
    (1013, "2024-01-21", "675.00",  8, "APAC",    "2024-01-21T11:00:00", "Mei Tanaka",    "completed"),
    (1014, "2024-01-21",  "95.00",  2, "US-West", "2024-01-21T11:30:00", "Noah Martinez", "completed"),
    (1015, "2024-01-22", "180.00",  3, "EU-West", "2024-01-22T06:45:00", "Olivia Fischer","pending"),
    (1016, "2024-01-22",  "42.00",  1, "US-East", "2024-01-22T07:00:00", "Paul Kim",      "completed"),
    (1017, "2024-01-23", "560.00",  7, "APAC",    "2024-01-23T13:00:00", "Qian Li",       "shipped"),
    (1018, "2024-01-23",  "28.75",  1, "US-West", "2024-01-23T13:15:00", "Rosa Alvarez",  "completed"),
    (1019, "2024-01-24", "305.00",  4, "EU-West", "2024-01-24T08:00:00", "Stefan Novak",  "completed"),
    (1020, "2024-01-24",  "67.50",  2, "US-East", "2024-01-24T08:20:00", "Tara Singh",    "pending"),
    (1021, "2024-01-25", "410.00",  5, "APAC",    "2024-01-25T15:00:00", "Umar Hassan",   "completed"),
    (1022, "2024-01-25",  "19.99",  1, "US-West", "2024-01-25T15:10:00", "Violet Chang",  "cancelled"),
    (1023, "2024-01-26", "880.00", 12, "EU-West", "2024-01-26T07:30:00", "Wolfgang Braun","completed"),
    (1024, "2024-01-26", "130.00",  3, "US-East", "2024-01-26T07:45:00", "Xena Petrov",   "shipped"),
    (1025, "2024-01-27", "245.50",  4, "APAC",    "2024-01-27T10:00:00", "Yuto Nakamura", "completed"),
    # Duplicate order_ids — exercise deduplicator
    (1001, "2024-01-15", "199.99",  2, "US-East", "2024-01-28T10:00:00", "Alice Johnson", "completed"),
    (1003, "2024-01-16", "340.00",  5, "EU-West", "2024-01-28T08:30:00", "Clara Müller",  "shipped"),
    (1010, "2024-01-19",  "55.25",  1, "US-West", "2024-01-28T14:30:00", "James Brown",   "completed"),
    (1026, "2024-02-01",  "72.00",  2, "US-East", "2024-02-01T09:00:00", "Zara Okafor",   "pending"),
    (1027, "2024-02-01", "510.00",  6, "EU-West", "2024-02-01T09:30:00", "Aarav Sharma",  "completed"),
]

# ---------------------------------------------------------------------------
# Canonical source_orders (10 rows) — inserted into each RDBMS dialect
# ---------------------------------------------------------------------------
SOURCE_ORDERS_ROWS: list[tuple] = [
    ("P001", "2024-01-10", 250.00, 2, "north", "2024-01-10 09:00:00"),
    ("P002", "2024-02-14",  99.50, 1, "south", "2024-02-14 12:00:00"),
    ("P003", "2024-03-20", 480.75, 6, "east",  "2024-03-20 15:00:00"),
    ("P004", "2024-04-05",  33.25, 1, "west",  "2024-04-05 10:30:00"),
    ("P005", "2024-05-18", 310.00, 3, "north", "2024-05-18 08:45:00"),
    ("P006", "2024-06-02", 125.00, 2, "south", "2024-06-02 11:15:00"),
    ("P007", "2024-07-19",  77.80, 1, "east",  "2024-07-19 14:00:00"),
    ("P008", "2024-08-08", 560.00, 7, "north", "2024-08-08 09:30:00"),
    ("P009", "2024-09-25", 198.50, 3, "west",  "2024-09-25 16:45:00"),
    ("P010", "2024-10-30", 402.00, 5, "south", "2024-10-30 13:00:00"),
]


# ===========================================================================
# Helpers
# ===========================================================================

def setup_logging(name: str = "usecase_sim") -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def wipe_dir_children(directory: Path, dry_run: bool = False, label: str | None = None) -> int:
    """Remove every child (file or dir) of *directory*. Returns count removed."""
    import shutil
    tag = label or directory.name
    if not directory.exists():
        logger.info("No %s directory: %s", tag, directory)
        return 0
    removed = 0
    for child in sorted(directory.iterdir()):
        if child.name.startswith("."):
            continue
        if dry_run:
            logger.info("[DRY-RUN] Would remove: %s", child)
        else:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            logger.info("Removed: %s", child)
        removed += 1
    return removed


# ---------------------------------------------------------------------------
# MinIO client (lazy import)
# ---------------------------------------------------------------------------

def minio_client():
    """Return a boto3 S3 client pointing at local MinIO, or None if boto3 missing."""
    try:
        import boto3  # noqa: PLC0415
        from botocore.config import Config  # noqa: PLC0415
    except ImportError:
        logger.warning("boto3 not installed — MinIO operations disabled")
        return None
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=MINIO_REGION,
    )


def ensure_bucket(client=None, bucket: str = MINIO_BUCKET) -> bool:
    """Create *bucket* in MinIO if missing. Returns True if reachable."""
    client = client or minio_client()
    if client is None:
        return False
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)
    else:
        logger.info("Bucket exists: %s", bucket)
    return True


def delete_minio_prefix(prefix: str, dry_run: bool = False, bucket: str = MINIO_BUCKET) -> int:
    """Delete all objects under *prefix*. Returns count."""
    client = minio_client()
    if client is None:
        return 0
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [obj["Key"] for obj in page.get("Contents", [])]
        if not keys:
            continue
        if dry_run:
            for k in keys:
                logger.info("[DRY-RUN] Would delete s3://%s/%s", bucket, k)
        else:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
            )
        deleted += len(keys)
    return deleted


def upload_tree(local_root: Path, prefix: str, bucket: str = MINIO_BUCKET) -> int:
    """Upload every file under *local_root* → s3://bucket/<prefix>/<rel>. Returns count."""
    client = minio_client()
    if client is None:
        return 0
    if not local_root.exists():
        logger.warning("No such directory: %s", local_root)
        return 0
    uploaded = 0
    prefix = prefix.rstrip("/")
    for file_path in sorted(local_root.rglob("*")):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(local_root)
        key = f"{prefix}/{rel.as_posix()}" if prefix else rel.as_posix()
        client.upload_file(str(file_path), bucket, key)
        uploaded += 1
    logger.info("Uploaded %d files → s3://%s/%s/", uploaded, bucket, prefix)
    return uploaded


# ---------------------------------------------------------------------------
# Iceberg REST catalog (lazy import)
# ---------------------------------------------------------------------------

def iceberg_catalog():
    """Return a pyiceberg REST catalog pointing at local Iceberg REST + MinIO."""
    try:
        from pyiceberg.catalog import load_catalog  # noqa: PLC0415
    except ImportError:
        logger.warning("pyiceberg not installed — Iceberg operations disabled")
        return None
    return load_catalog(
        "datacoolie",
        **{
            "type": "rest",
            "uri": ICEBERG_REST_URI,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": MINIO_REGION,
        },
    )


def drop_iceberg_namespace(namespace: str, dry_run: bool = False, drop_namespace: bool = True) -> int:
    """Purge all tables in *namespace*, then optionally drop the namespace."""
    catalog = iceberg_catalog()
    if catalog is None:
        return 0
    try:
        tables = list(catalog.list_tables(namespace))
    except Exception:
        logger.info("Iceberg namespace '%s' does not exist", namespace)
        return 0
    dropped = 0
    for table_id in tables:
        fqn = f"{table_id[0]}.{table_id[1]}" if isinstance(table_id, tuple) else str(table_id)
        if dry_run:
            logger.info("[DRY-RUN] Would purge: %s", fqn)
        else:
            try:
                catalog.purge_table(fqn)
                logger.info("Purged Iceberg table: %s", fqn)
            except Exception as exc:
                logger.warning("Failed to purge %s: %s", fqn, exc)
        dropped += 1
    if drop_namespace and not dry_run:
        try:
            catalog.drop_namespace(namespace)
            logger.info("Dropped namespace: %s", namespace)
        except Exception as exc:
            logger.warning("drop_namespace(%s): %s", namespace, exc)
    return dropped


# ---------------------------------------------------------------------------
# TCP port check — used to skip unreachable services silently
# ---------------------------------------------------------------------------

def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
