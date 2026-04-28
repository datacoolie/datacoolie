"""Spark session and Iceberg catalog factory for usecase-sim runners.

Centralises all session-creation concerns so every runner stays thin:
  - warehouse / metastore paths (pointed at datacoolie/ root)
  - Delta Lake JAR auto-injection
  - S3A / MinIO config for Hadoop filesystem
  - Iceberg REST catalog config for Spark (and matching pyiceberg catalog for Polars)

Catalog presets
---------------
"local"
    Delta only.  Iceberg REST catalog points to tabulario/iceberg-rest on
    http://localhost:8181 (docker-compose.yml), warehouse on MinIO.

"unity_catalog"
    Iceberg REST catalog points to Unity Catalog REST endpoint.
    Works for both OSS Unity Catalog and Databricks UC — only the URI
    (and optional token/credential) differs.

    OSS UC:   http://<host>:8080/api/2.1/unity-catalog/iceberg
    Databricks UC:
              https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg

    In both cases Delta is still served by ``spark_catalog`` (DeltaCatalog);
    you do NOT need a second catalog entry for Delta under UC.

Production swap table
---------------------
| Environment            | --catalog-preset   | --iceberg-catalog-uri                                          | --uc-token or credential   |
|------------------------|--------------------|----------------------------------------------------------------|----------------------------|
| Local (docker-compose) | local              | http://localhost:8181 (default)                                | not required               |
| OSS Unity Catalog      | unity_catalog      | http://<host>:8080/api/2.1/unity-catalog/iceberg               | --uc-credential client:sec |
| Databricks UC          | unity_catalog      | https://<ws>.azuredatabricks.net/api/2.1/unity-catalog/iceberg | --uc-token <pat>           |
"""

from __future__ import annotations

import logging
import os
import pathlib
import sys
from typing import Any

# ---------------------------------------------------------------------------
# Windows console encoding — pyiceberg uses ✅/❌ emojis in schema
# compatibility output that cp1252 cannot encode.  Switching the error
# handler to "replace" lets the logger write safely without crashing.
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(errors="replace")

from pyspark.sql import SparkSession

from datacoolie.engines.spark_session_builder import get_or_create_spark_session
from datacoolie.platforms import LocalPlatform
try:  # boto3 is optional
    from datacoolie.platforms import AWSPlatform  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    AWSPlatform = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths — warehouse and Derby metastore live at the datacoolie/ root so they
# are shared between tests/ and usecase-sim/ without polluting the repo root.
# ---------------------------------------------------------------------------
_DATACOOLIE_ROOT = pathlib.Path(__file__).parent.parent.parent.resolve()
_SPARK_WAREHOUSE = str(_DATACOOLIE_ROOT / "spark-warehouse")
_METASTORE_DIR = str(_DATACOOLIE_ROOT / "metastore_db")
_SPARK_IVY_DIR = str(pathlib.Path(__file__).resolve().parent / "spark-jars")

# ---------------------------------------------------------------------------
# MinIO defaults (docker-compose.yml)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"

DEFAULT_LOCAL_ICEBERG_URI = "http://localhost:8181"

# ---------------------------------------------------------------------------
# Local dev env defaults — values are set only when the variable is not
# already present, so real environment secrets always take precedence.
# Import _runner_utils to activate these in any runner script.
# ---------------------------------------------------------------------------
_LOCAL_ENV_DEFAULTS: dict[str, str] = {
    "DATACOOLIE_SQLITE_DB": "./usecase-sim/data/input/sqlite/orders.db",
    "DATACOOLIE_PG_USER": "datacoolie",
    "DATACOOLIE_PG_PASS": "datacoolie",
    "DATACOOLIE_MYSQL_USER": "datacoolie",
    "DATACOOLIE_MYSQL_PASS": "datacoolie",
    "DATACOOLIE_MSSQL_USER": "sa",
    "DATACOOLIE_MSSQL_PASS": "Datacoolie@1",
    "DATACOOLIE_ORA_USER": "datacoolie",
    "DATACOOLIE_ORA_PASS": "datacoolie",
    "DATACOOLIE_API_BEARER_TOKEN": "test-bearer-token-123",
    "DATACOOLIE_API_KEY_VALUE": "test-api-key-456",
    "DATACOOLIE_API_BASIC_USER": "datacoolie",
    "DATACOOLIE_API_BASIC_PASS": "password123",
    "DATACOOLIE_OAUTH2_CLIENT_ID": "datacoolie-client",
    "DATACOOLIE_OAUTH2_CLIENT_SECRET": "supersecret",
}
for _key, _val in _LOCAL_ENV_DEFAULTS.items():
    os.environ.setdefault(_key, _val)

# ---------------------------------------------------------------------------
# S3A config injected when the metadata path references AWS/MinIO paths.
# Uses Hadoop S3A filesystem (required by Spark reads/writes).
# ---------------------------------------------------------------------------
_S3A_SPARK_CONFIG: dict[str, str] = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # Alias the bare ``s3://`` scheme to S3A so paths returned by
    # AWSPlatform.list_files (which uses ``s3://bucket/key``) are readable
    # by Spark without needing the caller to rewrite to ``s3a://``.
    # Note: only register the FileSystem API alias. We intentionally do NOT
    # alias ``fs.AbstractFileSystem.s3.impl`` to ``S3A`` because that class
    # hardcodes its scheme to ``"s3a"`` and Hadoop's checkScheme() would
    # then emit a warning on every FileContext operation (e.g. Delta's
    # ChecksumHook.WriteChecksum).
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

# Polars / pyiceberg storage options (object-store, not Hadoop FS)
MINIO_STORAGE_OPTIONS: dict[str, str] = {
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_endpoint_url": MINIO_ENDPOINT,
    "aws_region": MINIO_REGION,
    "aws_allow_http": "true",
}


def setup_platform(
    is_aws: bool,
    storage_opts: dict[str, str],
    logger_: logging.Logger,
) -> Any:
    """Return a platform instance for the given runner mode.

    When ``is_aws`` is ``True``, seed ``storage_opts`` (in place) and the
    ``AWS_*`` environment variables with the MinIO defaults and return an
    :class:`AWSPlatform`.  Otherwise return :class:`LocalPlatform`.

    The env-var injection is required because :class:`AWSPlatform` resolves
    credentials through the standard boto3 chain (env vars, ``~/.aws/``,
    IAM role) — not from constructor arguments — so runners that drive
    MinIO must export the creds before the boto3 session is built.

    Caller-provided keys in ``storage_opts`` win over the MinIO defaults.
    """
    if not is_aws:
        return LocalPlatform()
    if AWSPlatform is None:
        raise SystemExit("AWSPlatform requires boto3 (pip install boto3)")
    for k, v in MINIO_STORAGE_OPTIONS.items():
        storage_opts.setdefault(k, v)
    os.environ.setdefault("AWS_ENDPOINT_URL", storage_opts.get("aws_endpoint_url", ""))
    os.environ.setdefault("AWS_ACCESS_KEY_ID", storage_opts.get("aws_access_key_id", ""))
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", storage_opts.get("aws_secret_access_key", ""))
    os.environ.setdefault("AWS_REGION", storage_opts.get("aws_region", "us-east-1"))
    os.environ.setdefault("AWS_ALLOW_HTTP", storage_opts.get("aws_allow_http", "true"))
    platform = AWSPlatform(
        endpoint_url=storage_opts.get("aws_endpoint_url"),
        region=storage_opts.get("aws_region", "us-east-1"),
    )
    logger_.info("Using AWSPlatform (endpoint=%s)", platform._endpoint_url)
    return platform


# ---------------------------------------------------------------------------
# Iceberg REST catalog Spark configs
# ---------------------------------------------------------------------------

def _iceberg_rest_spark_config(
    uri: str,
    token: str = "",
    credential: str = "",
    catalog_name: str = "local_catalog",
) -> dict[str, str]:
    """Return Spark configs for a named Iceberg REST catalog.

    Registers ``spark.sql.catalog.<catalog_name>`` as a SparkCatalog and
    configures S3FileIO so Iceberg writes bypass Hadoop FS entirely and
    speak directly to the object store.
    """
    c = catalog_name
    cfg: dict[str, str] = {
        f"spark.sql.catalog.{c}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{c}.type": "rest",
        f"spark.sql.catalog.{c}.uri": uri,
        f"spark.sql.catalog.{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{c}.s3.endpoint": MINIO_ENDPOINT,
        f"spark.sql.catalog.{c}.s3.access-key-id": MINIO_ACCESS_KEY,
        f"spark.sql.catalog.{c}.s3.secret-access-key": MINIO_SECRET_KEY,
        f"spark.sql.catalog.{c}.s3.path-style-access": "true",
        f"spark.sql.catalog.{c}.s3.region": MINIO_REGION,
    }
    if token:
        cfg[f"spark.sql.catalog.{c}.token"] = token
    if credential:
        cfg[f"spark.sql.catalog.{c}.credential"] = credential
    return cfg


# ---------------------------------------------------------------------------
# JAR auto-injection
# ---------------------------------------------------------------------------

def _resolve_packages(
    needs_s3: bool = False,
    needs_iceberg: bool = False,
    catalog_preset: str = "local",
) -> str | None:
    """Build a comma-separated ``spark.jars.packages`` value.

    Detects installed ``delta-spark`` version and optionally appends
    ``hadoop-aws`` + ``aws-java-sdk-bundle`` for S3A support, and
    ``iceberg-spark-runtime`` + AWS SDK v2 for Iceberg support.
    """
    packages: list[str] = []

    try:
        import importlib_metadata
        import pyspark
        delta_version = importlib_metadata.version("delta_spark")
        # PySpark 3.x ships with Scala 2.12; PySpark 4.x ships with Scala 2.13
        spark_major = int(pyspark.__version__.split(".")[0])
        scala_suffix = "2.13" if spark_major >= 4 else "2.12"
        pkg = f"io.delta:delta-spark_{scala_suffix}:{delta_version}"
        packages.append(pkg)
        logger.info("Resolved Delta Lake package: %s", pkg)

        # Avro: external module since Spark 2.4
        avro_pkg = f"org.apache.spark:spark-avro_{scala_suffix}:{pyspark.__version__}"
        packages.append(avro_pkg)
        logger.info("Added Avro package: %s", avro_pkg)
    except Exception:
        pass

    # JDBC drivers
    jdbc_drivers = [
        "org.xerial:sqlite-jdbc:3.51.3.0",
        "org.postgresql:postgresql:42.7.5",
        "com.mysql:mysql-connector-j:9.2.0",
        "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
        "com.oracle.database.jdbc:ojdbc11:23.7.0.25.01",
    ]
    for drv in jdbc_drivers:
        packages.append(drv)
    logger.info("Added JDBC drivers: %s", ", ".join(jdbc_drivers))

    if needs_iceberg:
        try:
            import pyspark
            spark_major_minor = ".".join(pyspark.__version__.split(".")[:2])
            # Iceberg artifact: iceberg-spark-runtime-<spark_major.minor>_<scala_version>
            iceberg_spark = f"org.apache.iceberg:iceberg-spark-runtime-{spark_major_minor}_2.12:1.10.1"
            packages.append(iceberg_spark)
            packages.append("software.amazon.awssdk:bundle:2.29.51")
            logger.info("Added Iceberg Spark runtime: %s", iceberg_spark)
        except Exception:
            pass

    if needs_s3:
        packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        packages.append("com.amazonaws:aws-java-sdk-bundle:1.12.780")
        logger.info("Added hadoop-aws + aws-java-sdk-bundle for S3A")

    return ",".join(packages) if packages else None


# ---------------------------------------------------------------------------
# Public API — Spark session builder
# ---------------------------------------------------------------------------

def build_spark_session(
    app_name: str = "DataCoolie-UseCase",
    catalog_preset: str = "local",
    iceberg_catalog_uri: str | None = None,
    uc_token: str = "",
    uc_credential: str = "",
    needs_s3: bool = False,
    needs_iceberg: bool = False,
    extra_config: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a SparkSession for usecase-sim runners.

    Args:
        app_name: Spark application name.
        catalog_preset: ``"local"`` (tabulario/iceberg-rest) or
            ``"unity_catalog"`` (OSS UC / Databricks UC).
        iceberg_catalog_uri: Explicitly override the Iceberg REST URI.
            Defaults to ``http://localhost:8181`` for ``"local"`` preset.
        uc_token: PAT for Databricks UC (``spark.sql.catalog.iceberg.token``).
        uc_credential: OAuth credential for OSS UC, ``client_id:secret``
            (``spark.sql.catalog.iceberg.credential``).
        needs_s3: When ``True``, inject S3A Hadoop configs and hadoop-aws JARs.
        extra_config: Any additional Spark configs that take final precedence.

    Returns:
        Configured ``SparkSession``.
    """
    # Base: warehouse + Derby metastore location
    spark_config: dict[str, str] = {
        # Cluster mode — single local machine, all available cores
        "spark.master": f"local[{os.cpu_count()//2}]",
        # 32 GB driver heap (local mode has no separate executor JVM)
        "spark.driver.memory": "32g",
        "spark.driver.maxResultSize": "8g",
        "spark.sql.warehouse.dir": _SPARK_WAREHOUSE,
        "spark.driver.extraJavaOptions": (
            f"-Dderby.system.home={_METASTORE_DIR}"
            " -Dsun.zip.disableMemoryMapping=true"
            # Silence AWS SDK for Java 1.x maintenance-mode banner
            # (hadoop-aws 3.3.4 still ships aws-java-sdk-bundle 1.x).
            " -Daws.java.v1.disableDeprecationAnnouncement=true"
        ),
        # -----------------------------------------------------------------
        # Iceberg optimizations
        "spark.sql.iceberg.merge-schema": "true",
        # Delta Lake catalog + optimizations
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.properties.defaults.targetFileSize": "128m", # Default 1g. Like delta.targetFileSize: effects OPTIMIZE, Auto Compaction, and Optimized Writes
        "spark.databricks.delta.optimizeWrite.enabled": "true", # Default true
        "spark.databricks.delta.optimizeWrite.binSize": "128m",
        "spark.databricks.delta.optimizeWrite.partitioned.enabled": "true", # Default unset
        "spark.databricks.delta.optimize.minFileSize": f"{64*1024*1024}",   # For Optimize command
        "spark.databricks.delta.optimize.maxFileSize": f"{128*1024*1024}",  # For Optimize command
        "spark.databricks.delta.merge.enableLowShuffle": "true",
        "spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",   # Auto compact small files during writes
        # "spark.databricks.delta.autoCompact.minFileSize": f"{64*1024*1024}",    # Default unset, calculated as 1/2 of the maxFileSize
        "spark.databricks.delta.autoCompact.maxFileSize": f"{128*1024*1024}",   # Default 128MB
        "spark.databricks.delta.autoCompact.minNumFiles": "50", # Default 50
        "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled": "false",
        # Spark configs
        "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        
        "spark.sql.shuffle.partitions": "64",  # Lower for small data
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": f"{32*1024*1024}", # 32MB
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{128*1024*1024}",  # 128MB
        "spark.sql.files.maxPartitionBytes": f"{128*1024*1024}",  # 128MB for init reads
        "spark.sql.files.openCostInBytes": f"{4*1024*1024}",  # 4MB
        "spark.sql.autoBroadcastJoinThreshold": f"{32*1024*1024}",  # 32MB
    }

    # Iceberg REST catalog
    if catalog_preset == "unity_catalog":
        uri = iceberg_catalog_uri or ""
        if not uri:
            raise ValueError(
                "--iceberg-catalog-uri is required for the 'unity_catalog' preset. "
                "Example: http://<uc-host>:8080/api/2.1/unity-catalog/iceberg"
            )
        logger.info("Catalog preset: unity_catalog  uri=%s", uri)
    else:
        uri = iceberg_catalog_uri or DEFAULT_LOCAL_ICEBERG_URI
        logger.info("Catalog preset: local  uri=%s", uri)

    # Catalog name must match the metadata JSON "catalog" field.
    catalog_name = "local_catalog" if catalog_preset != "unity_catalog" else "iceberg"
    spark_config.update(_iceberg_rest_spark_config(
        uri, token=uc_token, credential=uc_credential, catalog_name=catalog_name,
    ))

    # S3A Hadoop filesystem config (for s3a:// paths in Spark reads/writes)
    if needs_s3:
        spark_config.update(_S3A_SPARK_CONFIG)
        logger.info("Injected S3A Hadoop FS config for MinIO")

    # JAR packages — Ivy cache lives next to this file (spark-jars/);
    # first run downloads from Maven, subsequent runs reuse the cache.
    spark_config["spark.jars.ivy"] = _SPARK_IVY_DIR
    packages = _resolve_packages(
        needs_s3=needs_s3, needs_iceberg=needs_iceberg, catalog_preset=catalog_preset,
    )
    if packages:
        spark_config.setdefault("spark.jars.packages", packages)

    # Extensions — always include both Delta + Iceberg since the runner
    # always resolves Iceberg JARs via _resolve_packages.
    spark_config.setdefault(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # Caller overrides win
    if extra_config:
        spark_config.update(extra_config)

    logger.debug("Spark config: %s", spark_config)
    spark = get_or_create_spark_session(app_name=app_name, config=spark_config)

    # Silence the benign Windows-only shutdown warning from SparkEnv that
    # appears because the JVM classloader still holds a lock on jars under
    # userFiles-* when Spark's shutdown hook tries to rm -rf the temp dir:
    #   WARN SparkEnv: Exception while deleting Spark temp dir: ... userFiles-*
    # Job results are unaffected; the OS reclaims the temp dir on next boot.
    try:
        jvm = spark.sparkContext._jvm
        log4j_level = jvm.org.apache.logging.log4j.Level.ERROR
        configurator = jvm.org.apache.logging.log4j.core.config.Configurator
        for logger_name in (
            # Windows-only shutdown noise (see comment above)
            "org.apache.spark.SparkEnv",
            # Benign on S3: Delta's CheckpointFileManager tries Hadoop
            # FileContext first, then falls back to FileSystem API — which
            # is what Delta on S3 uses by design (S3 has no atomic rename
            # regardless of API). Silence the per-table fallback notice:
            #   WARN CheckpointFileManager: Could not use FileContext API
            #   for managing Structured Streaming checkpoint files at s3://...
            "org.apache.spark.sql.execution.streaming.CheckpointFileManager",
        ):
            configurator.setLevel(logger_name, log4j_level)
    except Exception:  # pragma: no cover — best-effort log tweak
        pass
    return spark


# ---------------------------------------------------------------------------
# Public API — pyiceberg REST catalog (Polars runners)
# ---------------------------------------------------------------------------

def build_iceberg_rest_catalog(
    catalog_preset: str = "local",
    iceberg_catalog_uri: str | None = None,
    uc_token: str = "",
    uc_credential: str = "",
    storage_opts: dict[str, str] | None = None,
) -> Any | None:
    """Return a ``pyiceberg.catalog.Catalog`` backed by the REST catalog.

    Mirrors the Spark catalog preset logic so Polars and Spark always talk
    to the same catalog endpoint.  Returns ``None`` gracefully when
    ``pyiceberg`` is not installed.

    Args:
        catalog_preset: ``"local"`` or ``"unity_catalog"``.
        iceberg_catalog_uri: Explicit URI override (same as Spark arg).
        uc_token: Bearer token for Databricks UC.
        uc_credential: ``client_id:secret`` for OSS UC.
        storage_opts: Polars/boto3-style S3 credentials
            (``aws_endpoint_url``, etc.). Defaults to MinIO when preset
            is ``"local"``.
    """
    try:
        from pyiceberg.catalog import load_catalog  # noqa: PLC0415
    except ImportError:
        logger.warning(
            "pyiceberg not installed — Iceberg catalog unavailable. "
            "Install with: pip install 'pyiceberg[s3fs,pyarrow]'"
        )
        return None

    if catalog_preset == "unity_catalog":
        uri = iceberg_catalog_uri or ""
        if not uri:
            raise ValueError("iceberg_catalog_uri is required for the 'unity_catalog' preset")
    else:
        uri = iceberg_catalog_uri or DEFAULT_LOCAL_ICEBERG_URI

    opts = dict(storage_opts or {})
    if not opts and catalog_preset == "local":
        opts = dict(MINIO_STORAGE_OPTIONS)

    # pyiceberg REST catalog uses s3.* keys, not aws_* boto3-style keys
    props: dict[str, str] = {"type": "rest", "uri": uri}
    if opts.get("aws_endpoint_url"):
        props["s3.endpoint"] = opts["aws_endpoint_url"]
        props["s3.access-key-id"] = opts.get("aws_access_key_id", "")
        props["s3.secret-access-key"] = opts.get("aws_secret_access_key", "")
        props["s3.path-style-access"] = "true"
        props["s3.region"] = opts.get("aws_region", MINIO_REGION)
    if uc_token:
        props["token"] = uc_token
    if uc_credential:
        props["credential"] = uc_credential

    logger.info("Building pyiceberg REST catalog  preset=%s  uri=%s", catalog_preset, uri)
    return load_catalog("datacoolie", **props)


# ---------------------------------------------------------------------------
# Shared runner execution helper
# ---------------------------------------------------------------------------

def run_and_report(
    driver,
    stage: str,
    column_name_mode,
    logger: logging.Logger,
    cleanup_fn=None,
    skip_api_sources: bool = False,
) -> None:
    """Execute ``driver.run()``, log results, and ``sys.exit`` appropriately.

    Args:
        driver: A ``DataCoolieDriver`` instance.
        stage: Stage name(s) forwarded to ``driver.run()``.
        column_name_mode: ``ColumnCaseMode`` value.
        logger: Logger for result output.
        cleanup_fn: Optional callable invoked in the ``finally`` block
            (e.g. ``spark.stop``).  ``driver.close()`` is always called.
        skip_api_sources: When True, exclude any dataflow whose source
            connection_type is ``"api"`` before execution.
    """
    from datacoolie.core import ColumnCaseMode

    try:
        dataflows = None
        if skip_api_sources:
            all_dfs = driver.load_dataflows(stage=stage)
            dataflows = [
                df for df in all_dfs
                if (df.source.connection.connection_type or "").lower() != "api"
            ]
            excluded = len(all_dfs) - len(dataflows)
            logger.info(
                "Skip-api-sources enabled: excluded %d dataflow(s) with API source; %d remain",
                excluded, len(dataflows),
            )

        result = driver.run(
            stage=stage,
            dataflows=dataflows,
            column_name_mode=ColumnCaseMode(column_name_mode),
        )
        logger.info(
            "Result — total: %d, succeeded: %d, failed: %d, skipped: %d (%.1fs)",
            result.total, result.succeeded, result.failed, result.skipped, result.duration_seconds,
        )
        if result.errors:
            for name, err in result.errors.items():
                logger.error("  %s: %s", name, err)
        sys.exit(2 if result.has_failures else 0)
    except Exception:
        logger.exception("Runtime error")
        sys.exit(2)
    finally:
        driver.close()
        if cleanup_fn:
            cleanup_fn()

