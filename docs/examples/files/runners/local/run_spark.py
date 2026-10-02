"""Canonical local Spark runner with the same path contract as Polars."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.logging import LogConfig
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def parse_args() -> argparse.Namespace:
    """Parse runner-owned host and DataCoolie configuration."""
    parser = argparse.ArgumentParser()
    metadata = parser.add_mutually_exclusive_group()
    metadata.add_argument(
        "--metadata-path",
        help="Exact metadata file, or a metadata directory",
    )
    metadata.add_argument(
        "--metadata-base-path",
        help="Directory containing wrapped metadata documents",
    )
    parser.add_argument(
        "--artifact-base-path",
        help="Built artifact root; metadata defaults to <artifact>/metadata",
    )
    parser.add_argument(
        "--sql-base-path",
        action="append",
        default=[],
        help="SQL root; repeat for multiple named roots",
    )
    parser.add_argument("--connections-path")
    parser.add_argument("--schema-hints-path")
    parser.add_argument(
        "--state-base-path",
        help="Runtime state root used to derive logs when no log root is given",
    )
    parser.add_argument(
        "--watermark-base-path",
        help="Explicit watermark root for a standalone FileProvider",
    )
    parser.add_argument("--log-base-path")
    parser.add_argument(
        "--log-persistence-mode",
        choices=("snapshot", "batch"),
        default="snapshot",
        help="Structured log persistence mode",
    )
    parser.add_argument(
        "--log-flush-interval-seconds",
        type=float,
        default=300.0,
        help="Periodic flush interval for batch dataflow logs",
    )
    parser.add_argument(
        "--log-flush-batch-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="Batch dataflow log size threshold",
    )
    parser.add_argument(
        "--log-console-color",
        choices=("auto", "always", "never"),
        default="auto",
        help="Console color policy",
    )
    parser.add_argument(
        "--working-directory",
        help="Directory used to resolve relative connection and SQL paths",
    )
    parser.add_argument("--run-attributes-json", type=json.loads, default={})
    parser.add_argument("--stage")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    args = parser.parse_args()
    if not (args.metadata_path or args.metadata_base_path or args.artifact_base_path):
        parser.error(
            "one of --metadata-path, --metadata-base-path, or --artifact-base-path "
            "is required"
        )
    return args


def main() -> int:
    """Start one local Spark session and return a scheduler-friendly status."""
    args = parse_args()
    if args.working_directory:
        working_directory = Path(args.working_directory).expanduser().resolve()
        if not working_directory.is_dir():
            raise FileNotFoundError(f"Working directory not found: {working_directory}")
        os.chdir(working_directory)

    platform = LocalPlatform()
    spark = (
        SparkSession.builder.appName("datacoolie-example")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    try:
        engine = SparkEngine(spark_session=spark, platform=platform)
        metadata = None
        if args.metadata_path or args.metadata_base_path:
            metadata_kwargs = {
                "metadata_base_path": args.metadata_base_path,
                "connections_path": args.connections_path,
                "schema_hints_path": args.schema_hints_path,
                "platform": platform,
                "watermark_base_path": args.watermark_base_path,
                "sql_base_path": args.sql_base_path or None,
            }
            if args.metadata_path:
                metadata_path = Path(args.metadata_path).expanduser()
                if metadata_path.is_dir():
                    metadata_kwargs["metadata_base_path"] = args.metadata_path
                else:
                    metadata_kwargs["config_path"] = args.metadata_path
            metadata = FileProvider(**metadata_kwargs)
        elif args.artifact_base_path and any(
            (args.connections_path, args.schema_hints_path, args.watermark_base_path)
        ):
            # Driver artifact mode can infer a FileProvider, but component
            # overrides belong to the explicitly constructed FileProvider.
            metadata = FileProvider(
                metadata_base_path=str(
                    Path(args.artifact_base_path).expanduser() / "metadata"
                ),
                connections_path=args.connections_path,
                schema_hints_path=args.schema_hints_path,
                platform=platform,
                watermark_base_path=args.watermark_base_path,
                sql_base_path=args.sql_base_path or None,
            )
        config = DataCoolieRunConfig(
            dry_run=args.dry_run,
            max_workers=args.max_workers,
            job_num=args.job_num,
            job_index=args.job_index,
            run_attributes=args.run_attributes_json,
            stop_on_error=True,
            allowed_function_prefixes=[],
        )
        log_config = LogConfig(
            persistence_mode=args.log_persistence_mode,
            flush_interval_seconds=args.log_flush_interval_seconds,
            flush_batch_bytes=args.log_flush_batch_bytes,
            console_color=args.log_console_color,
        )

        with DataCoolieDriver(
            engine=engine,
            metadata_provider=metadata,
            artifact_base_path=args.artifact_base_path,
            sql_base_path=args.sql_base_path or None,
            state_base_path=args.state_base_path,
            log_base_path=args.log_base_path,
            log_config=log_config,
            config=config,
        ) as driver:
            result = driver.run(stage=args.stage)
        return 1 if result.failed else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
