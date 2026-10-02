"""DataCoolie external AWS S3 Polars runner reference.

This process runs outside Glue and uses :class:`AWSPlatform` for metadata,
watermark, log and data paths. Every path consumed by the platform must be a
qualified ``s3://`` or ``s3a://`` URI; rejecting local paths here prevents a
cloud run from accidentally reading a checkout on the launch host. The
optional endpoint is useful for an explicitly configured S3-compatible test
service. Credentials come from the standard AWS credential chain; no
credentials belong in this file.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.aws_platform import AWSPlatform


def json_object(value: str) -> dict[str, Any]:
    """Decode one external context object without accepting arrays/scalars."""
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON object: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("run attributes JSON must be an object")
    return parsed


def require_s3_uri(value: str, option: str) -> str:
    """Reject local paths at the runner boundary before Driver construction."""
    if not value or not value.startswith(("s3://", "s3a://")):
        raise argparse.ArgumentTypeError(
            f"{option} must be an s3:// or s3a:// URI; upload the input first"
        )
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    metadata = parser.add_mutually_exclusive_group()
    metadata.add_argument("--metadata-path")
    metadata.add_argument("--metadata-base-path")
    metadata.add_argument("--artifact-base-path")
    parser.add_argument("--connections-path")
    parser.add_argument("--schema-hints-path")
    parser.add_argument("--sql-base-path", action="append", default=[])
    parser.add_argument("--state-base-path")
    parser.add_argument("--watermark-base-path")
    parser.add_argument("--log-base-path", required=True)
    parser.add_argument("--endpoint-url")
    parser.add_argument("--stage")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-attributes-json", type=json_object, default={})
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    args = parser.parse_args()
    if not (
        args.metadata_path
        or args.metadata_base_path
        or args.artifact_base_path
    ):
        parser.error(
            "one of --metadata-path, --metadata-base-path, or "
            "--artifact-base-path is required"
        )
    for option in (
        "metadata_path",
        "metadata_base_path",
        "artifact_base_path",
        "connections_path",
        "schema_hints_path",
        "state_base_path",
        "watermark_base_path",
        "log_base_path",
    ):
        value = getattr(args, option)
        if value is not None:
            try:
                require_s3_uri(value, f"--{option.replace('_', '-')}")
            except argparse.ArgumentTypeError as exc:
                parser.error(str(exc))
    for value in args.sql_base_path:
        try:
            require_s3_uri(value, "--sql-base-path")
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))
    return args


def main() -> int:
    args = parse_args()
    platform = AWSPlatform(
        bucket=args.bucket,
        region=args.region,
        endpoint_url=args.endpoint_url,
    )
    engine = PolarsEngine(platform=platform)
    metadata_kwargs: dict[str, Any] = {
        "connections_path": args.connections_path,
        "schema_hints_path": args.schema_hints_path,
        "platform": platform,
        "watermark_base_path": args.watermark_base_path,
        "sql_base_path": args.sql_base_path or None,
    }
    if args.metadata_path:
        metadata_kwargs["config_path"] = args.metadata_path
    elif args.metadata_base_path:
        metadata_kwargs["metadata_base_path"] = args.metadata_base_path
    # Artifact-only mode can let Driver construct and context-bind FileProvider.
    # An explicit provider is needed when a provider-owned override is supplied.
    metadata = (
        FileProvider(**metadata_kwargs)
        if any(
            metadata_kwargs.get(name) is not None
            for name in (
                "config_path",
                "metadata_base_path",
                "connections_path",
                "schema_hints_path",
                "watermark_base_path",
            )
        )
        else None
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

    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        artifact_base_path=args.artifact_base_path,
        state_base_path=args.state_base_path,
        sql_base_path=args.sql_base_path or None,
        log_base_path=args.log_base_path,
        config=config,
    ) as driver:
        result = driver.run(stage=args.stage)

    print(
        json.dumps(
            {
                "job_id": config.job_id,
                "total": result.total,
                "succeeded": result.succeeded,
                "failed": result.failed,
            },
            sort_keys=True,
        )
    )
    return 1 if result.failed else 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    raise SystemExit(main())
