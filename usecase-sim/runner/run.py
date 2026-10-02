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
import importlib
import json
import logging
import os
import sys
from pathlib import Path

# Make usecase-sim/ importable so that ``functions.*`` modules can be resolved
# by PythonFunctionReader when running this script directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datacoolie.core import DataCoolieRunConfig
from datacoolie.logging import LogConfig
from datacoolie.orchestration import DataCoolieDriver
from _runner_utils import (
    MINIO_STORAGE_OPTIONS,
    build_iceberg_rest_catalog,
    build_spark_session,
    replay_and_report,
    run_and_report,
    setup_platform,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run")
USECASE_SIM_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataCoolie unified runner")

    parser.add_argument("--engine", required=True, choices=["polars", "spark"])
    parser.add_argument(
        "--metadata-source",
        required=True,
        choices=["file", "database", "api"],
        help="Where to load metadata from",
    )
    parser.add_argument(
        "--platform",
        default="local",
        choices=["local", "aws"],
        help="Storage platform: 'local' filesystem or 'aws' (S3/MinIO)",
    )

    # File source
    parser.add_argument(
        "--metadata-path",
        default=None,
        help="Path to metadata file (.json|.yaml|.xlsx)",
    )
    parser.add_argument(
        "--metadata-base-path",
        default=None,
        help="Directory containing metadata documents (artifact mode)",
    )
    parser.add_argument(
        "--artifact-base-path",
        default=None,
        help="Deployed artifact root; metadata defaults to <root>/metadata",
    )
    parser.add_argument(
        "--sql-base-path",
        action="append",
        default=None,
        metavar="PATH",
        help="Optional SQL root used to resolve relative SQL files; repeat for multiple roots",
    )
    # Database source
    parser.add_argument(
        "--metadata-db-connection-string",
        default=None,
        help="SQLAlchemy connection string for metadata DB",
    )
    # API source
    parser.add_argument(
        "--metadata-api-url", default=None, help="Base URL of metadata API"
    )
    parser.add_argument("--metadata-api-key", default="", help="Optional API key")
    # Database + API share this
    parser.add_argument(
        "--metadata-workspace-id",
        default=None,
        help="Workspace ID (database + api sources)",
    )

    # Common
    parser.add_argument(
        "--stage", required=True, help="Stage name(s) — passed raw to driver.run()"
    )
    parser.add_argument(
        "--column-name-mode", default="lower", choices=["lower", "snake"]
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--storage-options", action="append", default=[], metavar="KEY=VALUE"
    )
    parser.add_argument("--iceberg-catalog-uri", default=None)
    parser.add_argument(
        "--needs-iceberg",
        action="store_true",
        help="Initialize the Iceberg catalog even when the stage name is neutral",
    )
    parser.add_argument(
        "--catalog-preset", default="local", choices=["local", "unity_catalog"]
    )
    parser.add_argument("--uc-token", default="")
    parser.add_argument("--uc-credential", default="")
    parser.add_argument("--log-path", default=None)
    parser.add_argument(
        "--job-id",
        default=None,
        help="Stable Driver session/job identifier for reproducible scenarios",
    )
    parser.add_argument(
        "--state-base-path",
        default=None,
        help="Framework runtime state root; derives logs and file watermarks",
    )
    parser.add_argument(
        "--run-attributes",
        default=None,
        help="JSON object with caller-owned correlation attributes",
    )
    parser.add_argument(
        "--log-persistence-mode",
        choices=["snapshot", "batch"],
        default=None,
        help="Structured log persistence mode",
    )
    parser.add_argument(
        "--log-flush-interval-seconds",
        type=float,
        default=None,
        help="Periodic log flush interval (seconds)",
    )
    parser.add_argument(
        "--log-flush-batch-bytes",
        type=int,
        default=None,
        help="Batch log size threshold in bytes",
    )
    parser.add_argument(
        "--log-console-color",
        choices=["auto", "always", "never"],
        default=None,
        help="Console color policy",
    )
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument(
        "--skip-api-sources",
        action="store_true",
        help="Skip any dataflow whose source connection_type is 'api'",
    )
    parser.add_argument(
        "--engine-setup-function",
        default=None,
        help="Repository-local callable invoked with the active engine before metadata execution",
    )
    parser.add_argument(
        "--engine-setup-arg",
        action="append",
        default=[],
        help="Argument forwarded to --engine-setup-function (repeatable)",
    )

    # Replay mode (mutually exclusive with normal run)
    parser.add_argument(
        "--replay-start",
        default=None,
        help="Inclusive replay range start (ISO date/datetime or int)",
    )
    parser.add_argument(
        "--replay-end",
        default=None,
        help="Exclusive replay range end (ISO date/datetime or int)",
    )
    parser.add_argument(
        "--replay-chunk-interval",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Chunk interval, e.g. days=1  (repeatable)",
    )
    parser.add_argument(
        "--replay-save-watermark",
        action="store_true",
        help="Persist the source watermark observation after each successful chunk; replay always reruns its requested range",
    )
    parser.add_argument(
        "--replay-chunk-column",
        default=None,
        help="Override auto-resolved chunk column",
    )

    # Spark-only (ignored when --engine polars)
    parser.add_argument("--app-name", default="DataCoolie-UseCase")
    parser.add_argument(
        "--spark-config", action="append", default=[], metavar="KEY=VALUE"
    )

    args = parser.parse_args()
    _validate_source_args(parser, args)
    return args


def _validate_source_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    src = args.metadata_source
    if args.metadata_path and args.metadata_base_path:
        parser.error("--metadata-path and --metadata-base-path are mutually exclusive")
    if src == "file" and not any(
        (args.metadata_path, args.metadata_base_path, args.artifact_base_path)
    ):
        parser.error(
            "--metadata-source file requires --metadata-path, "
            "--metadata-base-path, or --artifact-base-path"
        )
    if src == "database":
        if not args.metadata_db_connection_string or not args.metadata_workspace_id:
            parser.error(
                "--metadata-source database requires --metadata-db-connection-string and --metadata-workspace-id"
            )
    if src == "api":
        if not args.metadata_api_url or not args.metadata_workspace_id:
            parser.error(
                "--metadata-source api requires --metadata-api-url and --metadata-workspace-id"
            )


def _build_metadata(source: str, args: argparse.Namespace):
    """Instantiate the correct MetadataProvider for the requested source."""
    if source == "file":
        from datacoolie.metadata import FileProvider

        # Directory and artifact modes are assembled by Driver so the same
        # provider/platform binding contract is used by every provider type.
        if args.metadata_path:
            return FileProvider(
                config_path=args.metadata_path,
                sql_base_path=args.sql_base_path or None,
            )
        return None
    if source == "database":
        from datacoolie.metadata import DatabaseProvider

        return DatabaseProvider(
            connection_string=args.metadata_db_connection_string,
            workspace_id=args.metadata_workspace_id,
            sql_base_path=args.sql_base_path or None,
        )
    if source == "api":
        from datacoolie.metadata import APIProvider

        return APIProvider(
            base_url=args.metadata_api_url,
            api_key=args.metadata_api_key,
            workspace_id=args.metadata_workspace_id,
            sql_base_path=args.sql_base_path or None,
        )
    raise ValueError(f"Unknown metadata source: {source}")


def _parse_kv_list(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kv in pairs:
        key, _, value = kv.partition("=")
        out[key] = value
    return out


def _parse_run_attributes(value: str | None) -> dict | None:
    """Parse the caller-owned correlation object passed to DataCoolieRunConfig."""
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
    """Build one shared logging config only when logging overrides were given."""
    values = {
        "persistence_mode": args.log_persistence_mode,
        "flush_interval_seconds": args.log_flush_interval_seconds,
        "flush_batch_bytes": args.log_flush_batch_bytes,
        "console_color": args.log_console_color,
    }
    values = {key: value for key, value in values.items() if value is not None}
    return LogConfig(**values) if values else None


def _run_engine_setup(
    function_path: str | None,
    setup_args: list[str],
    engine: object,
) -> None:
    """Invoke an optional usecase-local setup hook in the active engine process."""

    if not function_path:
        return
    module_name, separator, function_name = function_path.rpartition(".")
    if not separator:
        raise ValueError(
            "--engine-setup-function must be a dotted module.function path"
        )
    module_spec = importlib.util.find_spec(module_name)
    module_file = module_spec.origin if module_spec is not None else None
    if not module_file:
        raise ValueError(f"Engine setup module has no local file: {module_name}")
    resolved_module = Path(module_file).resolve()
    if not resolved_module.is_relative_to(USECASE_SIM_ROOT):
        raise ValueError(
            f"Engine setup module must resolve inside usecase-sim: {resolved_module}"
        )
    module = importlib.import_module(module_name)
    setup_function = getattr(module, function_name, None)
    if not callable(setup_function):
        raise ValueError(f"Engine setup function is not callable: {function_path}")
    logger.info("Running engine setup: %s", function_path)
    setup_function(engine=engine, args=list(setup_args))


def main() -> None:
    args = parse_args()

    is_aws = args.platform == "aws"
    is_spark = args.engine == "spark"
    if is_spark and args.engine_setup_function:
        raise ValueError("--engine-setup-function is supported only for Polars")

    # Validate caller-owned run/log configuration before constructing an
    # engine or platform session.  A malformed JSON payload or logging
    # override must fail without leaking a Spark/JVM resource.
    config_kwargs: dict = dict(
        dry_run=args.dry_run,
        run_attributes=_parse_run_attributes(args.run_attributes),
    )
    if args.job_id is not None:
        config_kwargs["job_id"] = args.job_id
    if args.max_workers is not None:
        config_kwargs["max_workers"] = args.max_workers
    config = DataCoolieRunConfig(**config_kwargs)
    log_config = _build_log_config(args)

    needs_iceberg = args.needs_iceberg or not args.stage or "iceberg" in args.stage.lower()

    storage_opts = _parse_kv_list(args.storage_options)
    extra_config = _parse_kv_list(args.spark_config)

    # Polars path: inject MinIO storage opts for local Iceberg writes too.
    # Spark path: storage opts start empty (S3A config handled inside build_spark_session).
    if not is_spark:
        if is_aws:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for MinIO")
        elif needs_iceberg and args.catalog_preset == "local" and not storage_opts:
            for k, v in MINIO_STORAGE_OPTIONS.items():
                storage_opts.setdefault(k, v)
            logger.info("Injected S3 storage options for local Iceberg catalog")

    if not args.stage and is_spark:
        logger.warning("--stage '' was passed; ALL dataflows will be executed.")

    logger.info(
        "Engine: %s | Source: %s | Platform: %s | Stage: %s | Mode: %s | DryRun: %s",
        args.engine,
        args.metadata_source,
        args.platform,
        args.stage,
        args.column_name_mode,
        args.dry_run,
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
            verify_local_file_checksums=is_aws,
        )
        from datacoolie.engines import SparkEngine

        engine = SparkEngine(spark_session=spark, platform=platform)
        cleanup_fn = spark.stop
    else:
        iceberg_catalog = None
        if needs_iceberg:
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

    _run_engine_setup(args.engine_setup_function, args.engine_setup_arg, engine)

    metadata = _build_metadata(args.metadata_source, args)

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

    if args.replay_start:
        replay_and_report(
            driver,
            stage=args.stage,
            column_name_mode=args.column_name_mode,
            logger=logger,
            replay_start=args.replay_start,
            replay_end=args.replay_end,
            replay_chunk_interval=args.replay_chunk_interval,
            replay_save_watermark=args.replay_save_watermark,
            replay_chunk_column=args.replay_chunk_column,
            skip_api_sources=args.skip_api_sources,
            cleanup_fn=cleanup_fn,
        )
    else:
        run_and_report(
            driver,
            args.stage,
            args.column_name_mode,
            logger,
            cleanup_fn=cleanup_fn,
            skip_api_sources=args.skip_api_sources,
        )


if __name__ == "__main__":
    main()
