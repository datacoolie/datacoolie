"""Unified maintenance runner — compact + cleanup lakehouse tables.

Replaces:
    polars_maintenance.py
    spark_maintenance.py

Usage:
    python usecase-sim/runner/maintenance.py --engine polars \
        --metadata-path usecase-sim/metadata/file/local_use_cases.json \
        --connection local_delta_dest

    python usecase-sim/runner/maintenance.py --engine spark \
        --metadata-path s3://datacoolie-test/metadata/aws_use_cases.json \
        --connection aws_iceberg_dest --platform aws
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

# Make usecase-sim/ importable for functions.* resolution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datacoolie.core import DataCoolieRunConfig
from datacoolie.logging import LogConfig
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from _runner_utils import (
    MINIO_STORAGE_OPTIONS,
    build_iceberg_rest_catalog,
    build_spark_session,
    install_graceful_shutdown,
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
    parser.add_argument("--metadata-path", default=None, help="Path to metadata file")
    parser.add_argument("--metadata-base-path", default=None,
                        help="Directory containing metadata documents")
    parser.add_argument("--artifact-base-path", default=None,
                        help="Deployed artifact root; metadata defaults to <root>/metadata")
    parser.add_argument("--sql-base-path", action="append", default=None,
                        metavar="PATH",
                        help="Optional SQL root used to resolve relative SQL files; repeat for multiple roots")
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
    parser.add_argument("--job-id", default=None,
                        help="Stable Driver session/job identifier for reproducible scenarios")
    parser.add_argument("--state-base-path", default=None,
                        help="Framework runtime state root")
    parser.add_argument("--run-attributes", default=None,
                        help="JSON object with caller-owned correlation attributes")
    parser.add_argument("--log-persistence-mode", choices=["snapshot", "batch"], default=None)
    parser.add_argument("--log-flush-interval-seconds", type=float, default=None)
    parser.add_argument("--log-flush-batch-bytes", type=int, default=None)
    parser.add_argument("--log-console-color", choices=["auto", "always", "never"], default=None)
    parser.add_argument("--skip-api-sources", action="store_true",
                        help="Skip any dataflow whose source connection_type is 'api'")

    # Spark-only
    parser.add_argument("--app-name", default="DataCoolie-Maintenance")
    parser.add_argument("--spark-config", action="append", default=[], metavar="KEY=VALUE")

    args = parser.parse_args()
    if not any((args.metadata_path, args.metadata_base_path, args.artifact_base_path)):
        parser.error(
            "maintenance requires --metadata-path, --metadata-base-path, "
            "or --artifact-base-path"
        )
    if args.metadata_path and args.metadata_base_path:
        parser.error("--metadata-path and --metadata-base-path are mutually exclusive")
    return args


def _parse_kv_list(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kv in pairs:
        key, _, value = kv.partition("=")
        out[key] = value
    return out


def _parse_run_attributes(value: str | None) -> dict | None:
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"--run-attributes must be valid JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("--run-attributes must decode to a JSON object")
    return parsed


def _build_log_config(args: argparse.Namespace) -> LogConfig | None:
    values = {
        "persistence_mode": args.log_persistence_mode,
        "flush_interval_seconds": args.log_flush_interval_seconds,
        "flush_batch_bytes": args.log_flush_batch_bytes,
        "console_color": args.log_console_color,
    }
    values = {key: value for key, value in values.items() if value is not None}
    return LogConfig(**values) if values else None


def main() -> None:
    args = parse_args()

    is_aws = args.platform == "aws"
    is_spark = args.engine == "spark"

    # Validate caller-owned run/log configuration before starting an engine
    # session so malformed input cannot leak a Spark/JVM resource.
    config_kwargs = {
        "dry_run": args.dry_run,
        "retention_hours": args.retention_hours,
        "run_attributes": _parse_run_attributes(args.run_attributes),
    }
    if args.job_id is not None:
        config_kwargs["job_id"] = args.job_id
    config = DataCoolieRunConfig(**config_kwargs)
    log_config = _build_log_config(args)

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
            verify_local_file_checksums=is_aws,
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

    metadata = (
        FileProvider(
            config_path=args.metadata_path,
            sql_base_path=args.sql_base_path or None,
        )
        if args.metadata_path
        else None
    )
    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        config=config,
        artifact_base_path=args.artifact_base_path,
        metadata_base_path=args.metadata_base_path,
        sql_base_path=args.sql_base_path,
        state_base_path=args.state_base_path,
        log_base_path=args.log_path,
        log_config=log_config,
    )

    install_graceful_shutdown(driver, logger)

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
