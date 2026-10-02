"""DataCoolie local Polars replay runner reference."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import re

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def decode_boundary(value: str) -> str | int:
    return int(value) if re.fullmatch(r"[+-]?\d+", value) else value


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
    parser.add_argument("--stage")
    parser.add_argument("--start", required=True, type=decode_boundary)
    parser.add_argument("--end", required=True, type=decode_boundary)
    parser.add_argument("--chunk-interval-json", dest="chunk_interval", type=json.loads)
    parser.add_argument("--chunk-column")
    parser.add_argument("--save-watermark", action="store_true")
    parser.add_argument("--confirm-save-watermark", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    args = parser.parse_args()
    if args.save_watermark and not args.confirm_save_watermark:
        parser.error("--save-watermark requires --confirm-save-watermark")
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
        max_workers=args.max_workers,
        job_num=args.job_num,
        job_index=args.job_index,
        stop_on_error=True,
        allowed_function_prefixes=[],
    )
    replay = ReplayConfig(
        start=args.start,
        end=args.end,
        chunk_interval=args.chunk_interval,
        save_watermark=args.save_watermark,
        chunk_column=args.chunk_column,
    )

    failed = 0
    with DataCoolieDriver(
        engine=engine,
        metadata_provider=metadata,
        log_base_path=args.log_base_path,
        config=config,
    ) as driver:
        dataflows = driver.load_dataflows(stage=args.stage)
        result = driver.run_replay(dataflows=dataflows, replay=replay)
        failed = result.failed

    return 1 if failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
