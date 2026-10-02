"""Run a bounded replay that can be repeated safely.

This example deliberately keeps the retry boundary in project code. A later
invocation can use a new ``job_id`` and the same watermark root, but it still
runs every requested chunk. When ``save_watermark`` is enabled, the reader's
observations are persisted after successful writes; that state is not replay
progress tracking.

The framework writes a destination before persisting the watermark.  A hard
process termination in that gap can therefore leave committed output to be
written again. This script does not pretend to provide exactly-once
delivery; choose a keyed load strategy (for example ``merge_upsert``) when
retries must reconcile business rows.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import re
from typing import Any

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def decode_boundary(value: str) -> str | int:
    """Decode integer ranges while retaining date/datetime strings."""
    return int(value) if re.fullmatch(r"[+-]?\d+", value) else value


def run_once(
    *,
    metadata_path: str,
    watermark_base_path: str,
    log_base_path: str,
    start: str | int,
    end: str | int,
    chunk_interval: dict[str, int] | None = None,
    chunk_column: str | None = None,
    working_directory: str | None = None,
    job_id: str | None = None,
    save_watermark: bool = False,
    max_workers: int = 4,
) -> dict[str, Any]:
    """Execute one repeatable replay session and return a JSON summary.

    ``metadata_path`` may be a section-wrapped file or a metadata directory.
    The caller owns the input snapshot and must use an isolated watermark and
    output root while testing recovery.
    """
    if working_directory:
        root = Path(working_directory).expanduser().resolve()
        if not root.is_dir():
            raise FileNotFoundError(f"Working directory not found: {root}")
        os.chdir(root)

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    metadata = FileProvider(
        config_path=metadata_path if not Path(metadata_path).is_dir() else None,
        metadata_base_path=metadata_path if Path(metadata_path).is_dir() else None,
        platform=platform,
        watermark_base_path=watermark_base_path,
    )
    config_kwargs: dict[str, Any] = {
        "max_workers": max_workers,
        "stop_on_error": True,
        "allowed_function_prefixes": [],
    }
    if job_id is not None:
        config_kwargs["job_id"] = job_id
    config = DataCoolieRunConfig(**config_kwargs)
    replay = ReplayConfig(
        start=start,
        end=end,
        chunk_interval=chunk_interval,
        chunk_column=chunk_column,
        save_watermark=save_watermark,
    )

    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        log_base_path=log_base_path,
        config=config,
    ) as driver:
        dataflows = driver.load_dataflows()
        result = driver.run_replay(dataflows=dataflows, replay=replay)

    return {
        "job_id": config.job_id,
        "total": result.total,
        "succeeded": result.succeeded,
        "failed": result.failed,
        "errors": result.errors,
        "save_watermark": save_watermark,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-path", required=True)
    parser.add_argument("--watermark-base-path", required=True)
    parser.add_argument("--log-base-path", required=True)
    parser.add_argument("--working-directory")
    parser.add_argument("--job-id")
    parser.add_argument("--start", required=True, type=decode_boundary)
    parser.add_argument("--end", required=True, type=decode_boundary)
    parser.add_argument("--chunk-interval-json", type=json.loads)
    parser.add_argument("--chunk-column")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--save-watermark",
        action="store_true",
        help="Persist each successful chunk's reader watermark observation",
    )
    parser.add_argument("--confirm-save-watermark", action="store_true")
    args = parser.parse_args()
    if args.chunk_interval_json is not None and not isinstance(
        args.chunk_interval_json, dict
    ):
        parser.error("--chunk-interval-json must decode to an object")
    if args.save_watermark and not args.confirm_save_watermark:
        parser.error("--save-watermark requires --confirm-save-watermark")
    return args


def main() -> int:
    args = parse_args()
    summary = run_once(
        metadata_path=args.metadata_path,
        watermark_base_path=args.watermark_base_path,
        log_base_path=args.log_base_path,
        start=args.start,
        end=args.end,
        chunk_interval=args.chunk_interval_json,
        chunk_column=args.chunk_column,
        working_directory=args.working_directory,
        job_id=args.job_id,
        save_watermark=args.save_watermark,
        max_workers=args.max_workers,
    )
    print(json.dumps(summary, sort_keys=True, default=str))
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    raise SystemExit(main())
