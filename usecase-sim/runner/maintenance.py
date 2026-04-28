"""Unified maintenance runner — compact + cleanup lakehouse tables.

Replaces:
    polars_maintenance.py
    spark_maintenance.py

Usage:
    python usecase-sim/runner/maintenance.py --engine polars \
        --metadata-path usecase-sim/metadata/file/local_use_cases.json \
        --connection local_delta_dest

    python usecase-sim/runner/maintenance.py --engine spark \
        --metadata-path usecase-sim/metadata/file/aws_use_cases.json \
        --connection aws_iceberg_dest --platform aws
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Make usecase-sim/ importable for functions.* resolution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datacoolie.core import DataCoolieRunConfig
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform
from datacoolie.watermark import WatermarkManager
from _runner_utils import (
    MINIO_STORAGE_OPTIONS,
    build_iceberg_rest_catalog,
    build_spark_session,
    setup_platform,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("maintenance")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataCoolie maintenance runner (compact / cleanup)")

    parser.add_argument("--engine", required=True, choices=["polars", "spark"])
    parser.add_argument("--platform", default="local", choices=["local", "aws"],
                        help="Storage platform: 'local' filesystem or 'aws' (S3/MinIO)")
    parser.add_argument("--metadata-path", required=True, help="Path to metadata file")
    parser.add_argument("--connection", default=None, help="Optional connection name filter")

    parser.add_argument("--do-compact", action="store_true", default=True)
    parser.add_argument("--no-compact", dest="do_compact", action="store_false")
    parser.add_argument("--do-cleanup", action="store_true", default=True)
    parser.add_argument("--no-cleanup", dest="do_cleanup", action="store_false")
    parser.add_argument("--retention-hours", type=int, default=168)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--storage-options", action="append", default=[], metavar="KEY=VALUE")

    parser.add_argument("--catalog-preset", default="local", choices=["local", "unity_catalog"])
    parser.add_argument("--iceberg-catalog-uri", default=None)
    parser.add_argument("--uc-token", default="")
    parser.add_argument("--uc-credential", default="")
    parser.add_argument("--log-path", default=None)
    parser.add_argument("--skip-api-sources", action="store_true",
                        help="Skip any dataflow whose source connection_type is 'api'")

    # Spark-only
    parser.add_argument("--app-name", default="DataCoolie-Maintenance")
    parser.add_argument("--spark-config", action="append", default=[], metavar="KEY=VALUE")

    return parser.parse_args()


def _parse_kv_list(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kv in pairs:
        key, _, value = kv.partition("=")
        out[key] = value
    return out


def main() -> None:
    args = parse_args()

    is_aws = args.platform == "aws"
    is_spark = args.engine == "spark"

    storage_opts = _parse_kv_list(args.storage_options)
    extra_config = _parse_kv_list(args.spark_config)

    if not is_spark:
        if is_aws:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for MinIO")
        elif args.catalog_preset == "local" and not storage_opts:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for local Iceberg catalog")

    logger.info(
        "Maintenance — engine: %s | platform: %s | connection: %s | compact: %s | cleanup: %s | retention: %dh | preset: %s",
        args.engine, args.platform, args.connection, args.do_compact, args.do_cleanup,
        args.retention_hours, args.catalog_preset,
    )

    platform = setup_platform(is_aws, storage_opts, logger)

    cleanup_fn = None
    if is_spark:
        # Local catalog preset targets MinIO (s3://), so S3A JARs are always needed.
        needs_s3 = is_aws or args.catalog_preset == "local"
        spark = build_spark_session(
            app_name=args.app_name,
            catalog_preset=args.catalog_preset,
            iceberg_catalog_uri=args.iceberg_catalog_uri,
            uc_token=args.uc_token,
            uc_credential=args.uc_credential,
            needs_s3=needs_s3,
            needs_iceberg=True,
            extra_config=extra_config or None,
        )
        from datacoolie.engines import SparkEngine
        engine = SparkEngine(spark_session=spark, platform=platform)
        cleanup_fn = spark.stop
    else:
        iceberg_catalog = build_iceberg_rest_catalog(
            catalog_preset=args.catalog_preset,
            iceberg_catalog_uri=args.iceberg_catalog_uri,
            uc_token=args.uc_token,
            uc_credential=args.uc_credential,
            storage_opts=storage_opts or None,
        )
        from datacoolie.engines import PolarsEngine
        engine = PolarsEngine(
            platform=platform,
            storage_options=storage_opts or None,
            iceberg_catalog=iceberg_catalog,
        )

    metadata = FileProvider(config_path=args.metadata_path, platform=LocalPlatform(), eager_prefetch=True)
    watermark = WatermarkManager(metadata_provider=metadata)
    config = DataCoolieRunConfig(
        dry_run=args.dry_run,
        retention_hours=args.retention_hours,
    )

    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=config,
        base_log_path=args.log_path,
    )

    try:
        maintenance_dfs = driver.load_maintenance_dataflows(connection=args.connection)
        if args.skip_api_sources:
            original = len(maintenance_dfs)
            maintenance_dfs = [
                df for df in maintenance_dfs
                if (df.source.connection.connection_type or "").lower() != "api"
            ]
            logger.info(
                "Skip-api-sources enabled: excluded %d dataflow(s) with API source; %d remain",
                original - len(maintenance_dfs), len(maintenance_dfs),
            )
        result = driver.run_maintenance(
            dataflows=maintenance_dfs,
            do_compact=args.do_compact,
            do_cleanup=args.do_cleanup,
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
        if cleanup_fn is not None:
            try:
                cleanup_fn()
            except Exception:
                pass


if __name__ == "__main__":
    main()
