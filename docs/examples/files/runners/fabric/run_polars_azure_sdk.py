"""DataCoolie external Fabric/ADLS Polars runner reference.

Install the verified project environment with ``datacoolie[fabric-external]``
plus the selected Polars source/format profiles before starting the process.
Paths passed to the platform must be qualified ABFS(S) or HTTPS URIs.
"""

from __future__ import annotations

import argparse
import logging
from urllib.parse import urlsplit

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.fabric_platform import FabricPlatform


def require_azure_path(value: str, option: str) -> str:
    """Reject local paths before an external Fabric session starts."""
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{option} must be a qualified Azure URI") from exc
    if parsed.scheme.lower() not in {"abfs", "abfss", "https"} or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"{option} must be an abfs://, abfss://, or https:// URI"
        )
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--metadata-path",
        required=True,
        type=lambda value: require_azure_path(value, "--metadata-path"),
    )
    parser.add_argument(
        "--connections-path",
        type=lambda value: require_azure_path(value, "--connections-path"),
    )
    parser.add_argument(
        "--schema-hints-path",
        type=lambda value: require_azure_path(value, "--schema-hints-path"),
    )
    parser.add_argument(
        "--watermark-base-path",
        required=True,
        type=lambda value: require_azure_path(value, "--watermark-base-path"),
    )
    parser.add_argument(
        "--log-base-path",
        required=True,
        type=lambda value: require_azure_path(value, "--log-base-path"),
    )
    parser.add_argument("--stage")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    # DefaultAzureCredential is created lazily; inject a credential only when
    # the host application intentionally owns identity selection.
    platform = FabricPlatform(runtime="external")
    engine = PolarsEngine(platform=platform)
    metadata = FileProvider(
        config_path=args.metadata_path,
        connections_path=args.connections_path,
        schema_hints_path=args.schema_hints_path,
        platform=platform,
        watermark_base_path=args.watermark_base_path,
    )
    config = DataCoolieRunConfig(
        dry_run=args.dry_run,
        max_workers=args.max_workers,
        job_num=args.job_num,
        job_index=args.job_index,
        stop_on_error=True,
        allowed_function_prefixes=[],
    )

    failed = 0
    with DataCoolieDriver(
        engine=engine,
        metadata_provider=metadata,
        log_base_path=args.log_base_path,
        config=config,
    ) as driver:
        result = driver.run(stage=args.stage)
        failed = result.failed

    return 1 if failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
