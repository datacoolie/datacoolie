"""Unified runner — one entrypoint for every (engine × metadata source) combo.

Replaces:
    polars_file.py, polars_database.py, polars_api.py
    spark_file.py,  spark_database.py,  spark_api.py

Usage:
    python usecase-sim/runner/run.py --engine polars --metadata-source file \
        --metadata-path usecase-sim/metadata/file/local_use_cases.json --stage read__csv

    python usecase-sim/runner/run.py --engine spark --metadata-source database \
        --metadata-db-connection-string "sqlite:///..." \
        --metadata-workspace-id local-workspace --stage ""

    python usecase-sim/runner/run.py --engine polars --metadata-source api \
        --metadata-api-url http://localhost:8000 \
        --metadata-workspace-id local-workspace --stage ""
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Make usecase-sim/ importable so that ``functions.*`` modules can be resolved
# by PythonFunctionReader when running this script directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datacoolie.core import DataCoolieRunConfig
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms import LocalPlatform
from datacoolie.watermark import WatermarkManager
from _runner_utils import (
    MINIO_STORAGE_OPTIONS,
    build_iceberg_rest_catalog,
    build_spark_session,
    run_and_report,
    setup_platform,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataCoolie unified runner")

    parser.add_argument("--engine", required=True, choices=["polars", "spark"])
    parser.add_argument("--metadata-source", required=True,
                        choices=["file", "database", "api"],
                        help="Where to load metadata from")
    parser.add_argument("--platform", default="local", choices=["local", "aws"],
                        help="Storage platform: 'local' filesystem or 'aws' (S3/MinIO)")

    # File source
    parser.add_argument("--metadata-path", default=None,
                        help="Path to metadata file (.json|.yaml|.xlsx)")
    # Database source
    parser.add_argument("--metadata-db-connection-string", default=None,
                        help="SQLAlchemy connection string for metadata DB")
    # API source
    parser.add_argument("--metadata-api-url", default=None,
                        help="Base URL of metadata API")
    parser.add_argument("--metadata-api-key", default="",
                        help="Optional API key")
    # Database + API share this
    parser.add_argument("--metadata-workspace-id", default=None,
                        help="Workspace ID (database + api sources)")

    # Common
    parser.add_argument("--stage", required=True,
                        help="Stage name(s) — passed raw to driver.run()")
    parser.add_argument("--column-name-mode", default="lower", choices=["lower", "snake"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--storage-options", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--iceberg-catalog-uri", default=None)
    parser.add_argument("--catalog-preset", default="local", choices=["local", "unity_catalog"])
    parser.add_argument("--uc-token", default="")
    parser.add_argument("--uc-credential", default="")
    parser.add_argument("--log-path", default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--skip-api-sources", action="store_true",
                        help="Skip any dataflow whose source connection_type is 'api'")

    # Spark-only (ignored when --engine polars)
    parser.add_argument("--app-name", default="DataCoolie-UseCase")
    parser.add_argument("--spark-config", action="append", default=[], metavar="KEY=VALUE")

    args = parser.parse_args()
    _validate_source_args(parser, args)
    return args


def _validate_source_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    src = args.metadata_source
    if src == "file" and not args.metadata_path:
        parser.error("--metadata-source file requires --metadata-path")
    if src == "database":
        if not args.metadata_db_connection_string or not args.metadata_workspace_id:
            parser.error("--metadata-source database requires --metadata-db-connection-string and --metadata-workspace-id")
    if src == "api":
        if not args.metadata_api_url or not args.metadata_workspace_id:
            parser.error("--metadata-source api requires --metadata-api-url and --metadata-workspace-id")


def _build_metadata(source: str, args: argparse.Namespace):
    """Instantiate the correct MetadataProvider for the requested source."""
    if source == "file":
        from datacoolie.metadata import FileProvider
        return FileProvider(
            config_path=args.metadata_path,
            platform=LocalPlatform(),
            eager_prefetch=True,
        )
    if source == "database":
        from datacoolie.metadata import DatabaseProvider
        return DatabaseProvider(
            connection_string=args.metadata_db_connection_string,
            workspace_id=args.metadata_workspace_id,
            eager_prefetch=True,
        )
    if source == "api":
        from datacoolie.metadata import APIClient
        return APIClient(
            base_url=args.metadata_api_url,
            api_key=args.metadata_api_key,
            workspace_id=args.metadata_workspace_id,
            eager_prefetch=True,
        )
    raise ValueError(f"Unknown metadata source: {source}")


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

    # Polars path: inject MinIO storage opts for local Iceberg writes too.
    # Spark path: storage opts start empty (S3A config handled inside build_spark_session).
    if not is_spark:
        if is_aws:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for MinIO")
        elif args.catalog_preset == "local" and not storage_opts:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for local Iceberg catalog")

    needs_iceberg = not args.stage or "iceberg" in args.stage.lower()
    if not args.stage and is_spark:
        logger.warning("--stage '' was passed; ALL dataflows will be executed.")

    logger.info(
        "Engine: %s | Source: %s | Platform: %s | Stage: %s | Mode: %s | DryRun: %s",
        args.engine, args.metadata_source, args.platform, args.stage,
        args.column_name_mode, args.dry_run,
    )

    platform = setup_platform(is_aws, storage_opts, logger)

    # Build engine
    cleanup_fn = None
    if is_spark:
        spark = build_spark_session(
            app_name=args.app_name,
            catalog_preset=args.catalog_preset,
            iceberg_catalog_uri=args.iceberg_catalog_uri,
            uc_token=args.uc_token,
            uc_credential=args.uc_credential,
            needs_s3=is_aws,
            needs_iceberg=needs_iceberg,
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

    metadata = _build_metadata(args.metadata_source, args)
    watermark = WatermarkManager(metadata_provider=metadata)

    config_kwargs: dict = dict(dry_run=args.dry_run)
    if args.max_workers is not None:
        config_kwargs["max_workers"] = args.max_workers
    config = DataCoolieRunConfig(**config_kwargs)

    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=config,
        base_log_path=args.log_path,
    )

    run_and_report(
        driver, args.stage, args.column_name_mode, logger,
        cleanup_fn=cleanup_fn,
        skip_api_sources=args.skip_api_sources,
    )


if __name__ == "__main__":
    main()
