"""Reset perf benchmark artifacts.

Default (output only):
    - Wipe data/perf/output/*
    - Drop Iceberg 'perf_dst' namespace
    - Delete RustFS/S3 perf/output/ and iceberg-warehouse/perf_dst/

--all additionally:
    - Wipe data/perf/input/*
    - Delete RustFS/S3 perf/input/
    - Drop Iceberg 'perf_src' namespace

Usage:
    python usecase-sim/scripts/features/performance/reset_perf_data.py
    python usecase-sim/scripts/features/performance/reset_perf_data.py --all
    python usecase-sim/scripts/features/performance/reset_perf_data.py --dry-run
"""
from __future__ import annotations

import os

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


from _common import (  # noqa: E402
    PERF_INPUT_DIR,
    PERF_OUTPUT_DIR,
    delete_s3_prefix,
    drop_iceberg_namespace,
    setup_logging,
    wipe_dir_children,
)

logger = setup_logging("reset_perf_data")


def _wipe_rustfs(prefix: str, label: str, dry_run: bool) -> None:
    try:
        n = delete_s3_prefix(prefix, dry_run=dry_run)
        logger.info("RustFS/S3 %s: %d object(s)", label, n)
    except Exception as exc:
        logger.warning("RustFS/S3 %s skipped: %s", label, exc)


def _drop_ns(ns: str, dry_run: bool) -> None:
    try:
        drop_iceberg_namespace(ns, dry_run=dry_run)
    except Exception as exc:
        logger.warning("Iceberg namespace %s skipped: %s", ns, exc)


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description="Reset perf benchmark data")
    parser.add_argument("--all", action="store_true",
                        help="Also reset inputs (perf/input, iceberg perf_src)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger.info("=== reset_perf_data %s===", "(DRY RUN) " if args.dry_run else "")

    # Outputs
    logger.info("--- local perf output ---")
    wipe_dir_children(PERF_OUTPUT_DIR, dry_run=args.dry_run, label="perf output")

    logger.info("--- RustFS/S3 perf outputs ---")
    _wipe_rustfs("perf/output/",                 "perf/output",              args.dry_run)
    _wipe_rustfs("iceberg-warehouse/perf_dst/",  "iceberg-warehouse/perf_dst", args.dry_run)

    logger.info("--- Iceberg perf_dst ---")
    _drop_ns("perf_dst", args.dry_run)

    if args.all:
        logger.info("--- local perf input ---")
        wipe_dir_children(PERF_INPUT_DIR, dry_run=args.dry_run, label="perf input")

        logger.info("--- RustFS/S3 perf inputs ---")
        _wipe_rustfs("perf/input/",                 "perf/input",              args.dry_run)
        _wipe_rustfs("iceberg-warehouse/perf_src/", "iceberg-warehouse/perf_src", args.dry_run)

        logger.info("--- Iceberg perf_src ---")
        _drop_ns("perf_src", args.dry_run)

    logger.info("=== reset_perf_data complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
