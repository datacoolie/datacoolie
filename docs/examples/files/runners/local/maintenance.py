"""DataCoolie local Polars maintenance runner reference."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata-path", required=True)
    parser.add_argument("--connections-path")
    parser.add_argument("--schema-hints-path")
    parser.add_argument("--watermark-base-path", required=True)
    parser.add_argument("--log-base-path", required=True)
    parser.add_argument(
        "--working-directory",
        help="Directory used to resolve relative connection paths",
    )
    parser.add_argument("--connection")
    parser.add_argument("--no-compact", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--retention-hours", type=int, default=168)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    parser.add_argument("--confirm-maintenance", action="store_true")
    args = parser.parse_args()
    if args.no_compact and args.no_cleanup:
        parser.error("maintenance must enable compact, cleanup, or both")
    if not args.confirm_maintenance:
        parser.error("maintenance mutation requires --confirm-maintenance")
    return args


def main() -> int:
    args = parse_args()
    if args.working_directory:
        working_directory = Path(args.working_directory).expanduser().resolve()
        if not working_directory.is_dir():
            raise FileNotFoundError(f"Working directory not found: {working_directory}")
        os.chdir(working_directory)
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    metadata_path = Path(args.metadata_path).expanduser()
    metadata_kwargs = {
        "connections_path": args.connections_path,
        "schema_hints_path": args.schema_hints_path,
        "platform": platform,
        "watermark_base_path": args.watermark_base_path,
    }
    if metadata_path.is_dir():
        metadata_kwargs["metadata_base_path"] = args.metadata_path
    else:
        metadata_kwargs["config_path"] = args.metadata_path
    metadata = FileProvider(**metadata_kwargs)
    config = DataCoolieRunConfig(
        retention_hours=args.retention_hours,
        max_workers=args.max_workers,
        job_num=args.job_num,
        job_index=args.job_index,
        stop_on_error=True,
        allowed_function_prefixes=[],
    )

    with DataCoolieDriver(
        engine=engine,
        metadata_provider=metadata,
        log_base_path=args.log_base_path,
        config=config,
    ) as driver:
        result = driver.run_maintenance(
            connection=args.connection,
            do_compact=not args.no_compact,
            do_cleanup=not args.no_cleanup,
        )
        return 1 if result.failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
