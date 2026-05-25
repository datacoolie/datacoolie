"""Quick test script for real cloud and Docker platform introspection.

Run:
    python _test_cloud.py

Always-run tests (try/except on failure):
    ADLS Gen2, OneLake, S3, Hive (Docker), Iceberg (Docker)

Env-gated tests (skipped unless env var set):
    Databricks Volumes  — set DATABRICKS_HOST
    Databricks Unity    — set DATABRICKS_HOST
    Fabric Lakehouse    — set FABRIC_WORKSPACE_NAME or FABRIC_WORKSPACE_ID
    AWS Glue            — set AWS_GLUE_DATABASE
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "datacoolie-discover", "scripts"))

# ===========================================================================
# CONFIGURATION — edit defaults here; all values can be overridden via env vars
# ===========================================================================

# --- Cloud file sources (always run) ---
ADLS_PATH    = "abfss://dev-container@azdatalakesungroup.dfs.core.windows.net/input/"
ONELAKE_PATH = "abfss://DataCoolie_DEV_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/datacoolie/input"
S3_PATH      = "s3://de-dev-0007/input/"

# --- Databricks (env-gated: requires DATABRICKS_HOST) ---
DATABRICKS_HOST         = os.environ.get("DATABRICKS_HOST")
DATABRICKS_VOLUMES_PATH = os.environ.get("DATABRICKS_VOLUMES_PATH",
                              "/Volumes/workspace/default/datacoolie_sim/input/")
DATABRICKS_CATALOG      = os.environ.get("DATABRICKS_CATALOG", "workspace")
DATABRICKS_SCHEMA       = os.environ.get("DATABRICKS_SCHEMA",  "default")

# --- Fabric (env-gated: requires FABRIC_WORKSPACE_NAME or FABRIC_WORKSPACE_ID in env) ---
FABRIC_WORKSPACE_NAME = os.environ.get("FABRIC_WORKSPACE_NAME", "DataCoolie_DEV_WS")
FABRIC_LAKEHOUSE_NAME = os.environ.get("FABRIC_LAKEHOUSE_NAME", "lh_bronze")
FABRIC_WORKSPACE_ID   = os.environ.get("FABRIC_WORKSPACE_ID")   # GUID; auto-resolved from name if blank
FABRIC_LAKEHOUSE_ID   = os.environ.get("FABRIC_LAKEHOUSE_ID")   # GUID; auto-resolved from name if blank

# --- AWS Glue (env-gated: requires AWS_GLUE_DATABASE) ---
GLUE_DATABASE = os.environ.get("AWS_GLUE_DATABASE",   "datacoolie")
GLUE_REGION   = os.environ.get("AWS_REGION",          "ap-southeast-1")

# --- Hive Metastore — Docker (skills-test-hive, port 10000; always run) ---
HIVE_URI      = os.environ.get("HIVE_METASTORE_URI", "thrift://localhost:10000")
HIVE_DATABASE = os.environ.get("HIVE_DATABASE",      "datacoolie_test")

# --- Iceberg REST Catalog — Docker (skills-test-iceberg-rest, port 8182; always run) ---
ICEBERG_URI      = os.environ.get("ICEBERG_URI",      "http://localhost:8182")
ICEBERG_DATABASE = os.environ.get("ICEBERG_DATABASE", "sales")

# ===========================================================================
# IMPORTS & HELPERS
# ===========================================================================
from _file_introspect import introspect_files
from _lakehouse_introspect import introspect_lakehouse
from _assets import format_csv


def _header(title: str) -> None:
    print("=" * 60)
    print(f"  TEST: {title}")
    print("=" * 60)


def _show(result, limit: int = 2000) -> None:
    print(f"  Tables: {len(result.tables)}  Columns: {result.total_columns}")
    if result.tables:
        print(format_csv(result)[:limit])


def _passed(name: str) -> None:
    print(f"  ✓ {name} PASSED\n")


def _failed(name: str, e: Exception) -> None:
    print(f"  ✗ {name} FAILED: {e}\n")


def _skip(reason: str) -> None:
    print(f"  SKIP: {reason}\n")


# ===========================================================================
# ALWAYS-RUN: cloud file sources
# ===========================================================================

_header("ADLS Gen2 (DefaultAzureCredential)")
try:
    result = introspect_files(ADLS_PATH, storage_options={"credential": "default"})
    _show(result)
    _passed("ADLS Gen2")
except Exception as e:
    _failed("ADLS Gen2", e)

_header("OneLake (DefaultAzureCredential)")
try:
    result = introspect_files(ONELAKE_PATH, storage_options={"credential": "default"})
    _show(result)
    _passed("OneLake")
except (Exception, SystemExit) as e:
    _failed("OneLake", e)

_header("AWS S3 (ambient credentials)")
try:
    result = introspect_files(S3_PATH)
    _show(result)
    _passed("S3")
except Exception as e:
    _failed("S3", e)

# ===========================================================================
# ALWAYS-RUN: Docker lakehouse backends
# ===========================================================================

_header(f"Hive Metastore — Docker ({HIVE_URI})")
try:
    result = introspect_lakehouse(catalog="hive", database=HIVE_DATABASE, catalog_uri=HIVE_URI)
    _show(result)
    _passed("Hive")
except Exception as e:
    _failed("Hive", e)

_header(f"Iceberg REST — Docker ({ICEBERG_URI})")
try:
    result = introspect_lakehouse(catalog="iceberg", database=ICEBERG_DATABASE, catalog_uri=ICEBERG_URI)
    _show(result)
    _passed("Iceberg")
except Exception as e:
    _failed("Iceberg", e)

# ===========================================================================
# ENV-GATED: Databricks (requires DATABRICKS_HOST)
# ===========================================================================

if not DATABRICKS_HOST:
    _skip("Databricks — set DATABRICKS_HOST + DATABRICKS_TOKEN to enable")
else:
    _header(f"Databricks Volumes — {DATABRICKS_VOLUMES_PATH}")
    try:
        result = introspect_files(DATABRICKS_VOLUMES_PATH)
        _show(result)
        _passed("Databricks Volumes")
    except Exception as e:
        _failed("Databricks Volumes", e)

    _header(f"Databricks Unity Catalog — {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}")
    try:
        result = introspect_lakehouse(
            catalog="unity",
            database=DATABRICKS_CATALOG,
            schema_filter=DATABRICKS_SCHEMA,
        )
        _show(result)
        _passed("Databricks Unity")
    except Exception as e:
        _failed("Databricks Unity", e)

# ===========================================================================
# ENV-GATED: Microsoft Fabric (requires FABRIC_WORKSPACE_NAME or FABRIC_WORKSPACE_ID)
# ===========================================================================

_fabric_enabled = bool(os.environ.get("FABRIC_WORKSPACE_NAME") or os.environ.get("FABRIC_WORKSPACE_ID"))
if not _fabric_enabled:
    _skip("Fabric Lakehouse — set FABRIC_WORKSPACE_NAME (or FABRIC_WORKSPACE_ID) to enable")
else:
    _header(f"Fabric Lakehouse — {FABRIC_WORKSPACE_NAME} / {FABRIC_LAKEHOUSE_NAME}")
    try:
        result = introspect_lakehouse(
            catalog="fabric",
            database=FABRIC_LAKEHOUSE_ID or FABRIC_LAKEHOUSE_NAME,
            workspace_id=FABRIC_WORKSPACE_ID,
            workspace_name=FABRIC_WORKSPACE_NAME,
            lakehouse_name=FABRIC_LAKEHOUSE_NAME,
        )
        _show(result)
        _passed("Fabric Lakehouse")
    except Exception as e:
        _failed("Fabric Lakehouse", e)

# ===========================================================================
# ENV-GATED: AWS Glue (requires AWS_GLUE_DATABASE)
# ===========================================================================

if not os.environ.get("AWS_GLUE_DATABASE"):
    _skip("AWS Glue — set AWS_GLUE_DATABASE to enable")
else:
    _header(f"AWS Glue — {GLUE_DATABASE} ({GLUE_REGION})")
    try:
        result = introspect_lakehouse(catalog="glue", database=GLUE_DATABASE, region=GLUE_REGION)
        _show(result)
        _passed("Glue")
    except Exception as e:
        _failed("Glue", e)
