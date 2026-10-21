"""Unified maintenance runner — compact + cleanup lakehouse tables.

Replaces:
    polars_maintenance.py
    spark_maintenance.py

Usage:
    python usecase-sim/runner/maintenance.py --engine polars \
        --metadata-path usecase-sim/metadata/file/read.json \
        --connections-path usecase-sim/metadata/file/connections.json \
        --schema-hints-path usecase-sim/metadata/file/schema_hints.json \
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
from pathlib import Path

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
from runner.runtime_paths import DATACOOLIE_ROOT, apply_runtime_defaults
from runner.runtime_timing import phase, runtime_entrypoint
from runner.runtime_profile import optimized, metadata_capabilities

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
    parser.add_argument(
        "--metadata-path",
        action="append",
        default=None,
        help="Path to metadata file; repeat for an ordered exact file list",
    )
    parser.add_argument(
        "--metadata-document",
        default=None,
        help="Inline JSON metadata object for a generated scenario projection",
    )
    parser.add_argument(
        "--metadata-bindings",
        default=None,
        help="JSON object with per-invocation bindings (exact file source only)",
    )
    parser.add_argument(
        "--dataflow",
        action="append",
        default=None,
        metavar="NAME",
        help="Select one dataflow by authored name; repeat to preserve execution order",
    )
    parser.add_argument(
        "--connections-path",
        default=None,
        help="Optional exact file containing the connections section",
    )
    parser.add_argument(
        "--schema-hints-path",
        default=None,
        help="Optional exact file containing the schema_hints section",
    )
    parser.add_argument("--metadata-base-path", default=None,
                        help="Directory containing metadata documents")
    parser.add_argument("--artifact-base-path", default=None,
                        help="Deployed artifact root; metadata defaults to <root>/metadata")
    parser.add_argument("--sql-base-path", action="append", default=None,
                        metavar="PATH",
                        help="Optional SQL root used to resolve relative SQL files; repeat for multiple roots")
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--connection", default=None, help="Optional connection name filter")

    parser.add_argument("--do-compact", action="store_true", default=True)
    parser.add_argument("--no-compact", dest="do_compact", action="store_false")
    parser.add_argument("--do-cleanup", action="store_true", default=True)
    parser.add_argument("--no-cleanup", dest="do_cleanup", action="store_false")
    parser.add_argument("--retention-hours", type=int, default=168)
    parser.add_argument("--compact-options", help="JSON boolean action mapping")
    parser.add_argument("--cleanup-options", help="JSON boolean action mapping")
    parser.add_argument("--maintenance-deployment", help="Operator-attested REST deployment JSON file")
    parser.add_argument("--maintenance-guard-manifest", help="Prebuilt native Spark maintenance manifest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--storage-options", action="append", default=[], metavar="KEY=VALUE")

    parser.add_argument("--catalog-preset", default="local", choices=["local", "unity_catalog"])
    parser.add_argument("--iceberg-catalog-uri", default=None)
    parser.add_argument("--uc-token", default="")
    parser.add_argument("--uc-credential", default="")
    parser.add_argument(
        "--log-path",
        default=None,
        help="Structured log root (default: local .runtime/logs or AWS logs URI)",
    )
    parser.add_argument("--job-id", default=None,
                        help="Stable Driver session/job identifier for reproducible scenarios")
    parser.add_argument("--state-base-path", default=None,
                        help="Framework runtime state root")
    parser.add_argument(
        "--watermark-path",
        default=None,
        help="Explicit FileProvider watermark root (default: local .runtime/watermarks or AWS URI)",
    )
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
    _normalize_metadata_path_arg(parser, args)
    for option_name in ("connections_path", "schema_hints_path"):
        value = getattr(args, option_name)
        if value is not None and not value.strip():
            parser.error(f"--{option_name.replace('_', '-')} must be a non-empty path")
    if args.metadata_document is not None:
        try:
            document = json.loads(args.metadata_document)
        except json.JSONDecodeError as exc:
            parser.error(f"--metadata-document must be valid JSON: {exc.msg}")
        if not isinstance(document, dict):
            parser.error("--metadata-document must decode to a JSON object")
        if any((args.metadata_path, args.metadata_base_path, args.artifact_base_path)):
            parser.error(
                "--metadata-document is mutually exclusive with --metadata-path, "
                "--metadata-base-path, and --artifact-base-path"
            )
        if args.connections_path or args.schema_hints_path:
            parser.error("--metadata-document cannot be combined with section override paths")
    if args.metadata_bindings is not None or args.dataflow:
        if not args.metadata_path or args.metadata_base_path or args.artifact_base_path:
            parser.error(
                "--metadata-bindings/--dataflow require exact --metadata-path file inputs"
            )
        if args.metadata_bindings is not None:
            try:
                from runner.metadata_bindings import parse_metadata_bindings
                parse_metadata_bindings(args.metadata_bindings)
            except Exception as exc:
                parser.error(str(exc))
    if not any((args.metadata_path, args.metadata_base_path, args.artifact_base_path,
                args.metadata_document)):
        parser.error(
            "maintenance requires --metadata-path, --metadata-base-path, "
            "--artifact-base-path, or --metadata-document"
        )
    if args.metadata_path and args.metadata_base_path:
        parser.error("--metadata-path and --metadata-base-path are mutually exclusive")
    _apply_runtime_defaults(args)
    return args


def _parse_kv_list(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kv in pairs:
        key, _, value = kv.partition("=")
        out[key] = value
    return out


_apply_runtime_defaults = apply_runtime_defaults


def _normalize_metadata_path_arg(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Keep one metadata path scalar while retaining repeated path order."""
    values = args.metadata_path
    if values is None:
        return
    if not values or any(not isinstance(value, str) or not value.strip() for value in values):
        parser.error("--metadata-path values must be non-empty paths")
    args.metadata_path = values[0] if len(values) == 1 else values


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


def _maintenance_request(args):
    """Validate selection and optional REST evidence before creating resources."""
    from datacoolie.engines.contracts.maintenance import RestMaintenanceDeployment, selected_actions
    options = {}
    for name in ("compact_options", "cleanup_options"):
        value = getattr(args, name, None)
        options[name] = None if value is None else json.loads(value)
    selected_actions(do_compact=args.do_compact, do_cleanup=args.do_cleanup, **options)
    deployment = None
    evidence = getattr(args, "maintenance_deployment", None)
    if evidence:
        values = json.loads(Path(evidence).read_text(encoding="utf-8"))
        for name in ("deployment_identity", "routes", "sentinel_identifier"):
            values[name] = tuple(values[name])
        deployment = RestMaintenanceDeployment(**values)
    manifest = getattr(args, "maintenance_guard_manifest", None)
    if manifest:
        from datacoolie.engines._spark.iceberg.native_build import load_manifest, SPARK_VERSION
        manifest = load_manifest(Path(manifest))
        if args.engine == "spark":
            import pyspark
            if pyspark.__version__ != SPARK_VERSION:
                raise ValueError(f"Native REST maintenance requires Spark {SPARK_VERSION}; installed {pyspark.__version__}")
    return options, deployment, manifest


@runtime_entrypoint
def main() -> None:
    # Keep metadata, SQL, and relative fixture paths anchored at the product
    # root regardless of the shell directory used to invoke this script.
    os.chdir(DATACOOLIE_ROOT)
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
    if getattr(args, "max_workers", None) is not None:
        config_kwargs["max_workers"] = args.max_workers
    elif is_spark and optimized():
        config_kwargs["max_workers"] = 3
    config = DataCoolieRunConfig(**config_kwargs)
    log_config = _build_log_config(args)
    action_options, maintenance_deployment, guard_manifest = _maintenance_request(args)

    capabilities = metadata_capabilities(args.metadata_path, connection=args.connection,
        maintenance=True, skip_api_sources=args.skip_api_sources,
        connections_path=getattr(args, "connections_path", None),
        schema_hints_path=getattr(args, "schema_hints_path", None),
        metadata_bindings=getattr(args, "metadata_bindings", None),
        dataflow_names=getattr(args, "dataflow", None)) if optimized() and args.metadata_path else None

    storage_opts = _parse_kv_list(args.storage_options)
    extra_config = _parse_kv_list(args.spark_config)
    if maintenance_deployment is not None:
        # Configure the qualified REST catalog before creating a session;
        # opaque cache wrappers are outside the native preparation contract.
        extra_config.setdefault(f"spark.sql.catalog.{maintenance_deployment.catalog_label}.cache-enabled", "false")
    if guard_manifest is not None:
        supplied_jars = extra_config.get("spark.jars", "")
        guard_jar = guard_manifest.manifest_path.with_name(guard_manifest.helper_jar_name)
        extra_config["spark.jars"] = ",".join(value for value in
            (supplied_jars, str(guard_jar)) if value)

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
    driver = None
    try:
        with phase("engine_creation_s"):
            if is_spark:
                # Local catalog preset targets MinIO (s3://), so S3A JARs are always needed.
                needs_iceberg = (maintenance_deployment is not None or
                    capabilities["needs_iceberg"]) if capabilities else True
                needs_s3 = is_aws or (capabilities["needs_s3"] if capabilities
                    else args.catalog_preset == "local")
                spark = build_spark_session(
                    app_name=args.app_name,
                    catalog_preset=args.catalog_preset,
                    iceberg_catalog_uri=args.iceberg_catalog_uri,
                    uc_token=args.uc_token,
                    uc_credential=args.uc_credential,
                    needs_s3=needs_s3,
                    needs_iceberg=needs_iceberg,
                    needs_jdbc=False if capabilities else True,
                    extra_config=extra_config or None,
                    verify_local_file_checksums=is_aws,
                )
                cleanup_fn = spark.stop
                from datacoolie.engines import SparkEngine
                engine = SparkEngine(spark_session=spark, platform=platform,
                    maintenance_deployment=maintenance_deployment, maintenance_guard_manifest=guard_manifest)
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
                    maintenance_deployment=maintenance_deployment,
                )

        # Directory and artifact modes still need a provider instance so Driver
        # can bind metadata/artifact context during startup.  Keep config_path
        # absent in those modes to avoid the provider's path conflict contract.
        provider_kwargs = {
            "config_path": args.metadata_path,
            "watermark_base_path": args.watermark_path,
            "sql_base_path": args.sql_base_path or None,
        }
        for option_name in ("connections_path", "schema_hints_path"):
            value = getattr(args, option_name, None)
            if value is not None:
                provider_kwargs[option_name] = value
        if args.metadata_document is not None:
            from runner.metadata_bindings import MemoryFileProvider
            metadata = MemoryFileProvider(
                document=json.loads(args.metadata_document),
                watermark_base_path=args.watermark_path,
                sql_base_path=args.sql_base_path or None,
            )
        elif args.metadata_bindings is not None or args.dataflow:
            from runner.metadata_bindings import build_file_provider
            metadata = build_file_provider(
                **provider_kwargs,
                metadata_bindings=args.metadata_bindings,
                dataflows=args.dataflow,
            )
        else:
            metadata = FileProvider(**provider_kwargs)
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
        with phase("execution_s"):
            result = driver.run_maintenance(
                dataflows=maintenance_dfs,
                do_compact=args.do_compact,
                do_cleanup=args.do_cleanup,
                **{key: value for key, value in action_options.items() if value is not None},
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
        with phase("shutdown_s"):
            try:
                if driver is not None:
                    driver.close()
            finally:
                if cleanup_fn is not None:
                    cleanup_fn()


if __name__ == "__main__":
    main()
