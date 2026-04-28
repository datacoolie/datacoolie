"""Reset all output data (including watermarks).

Performs, in order:
    1. reset_watermarks (local folders, MinIO watermarks/, DB watermark tables)
    2. wipe local data/output/*
    3. delete MinIO output/ and iceberg-warehouse/ prefixes
    4. purge Iceberg REST catalog 'default' namespace

Usage:
    python usecase-sim/scripts/reset_data.py
    python usecase-sim/scripts/reset_data.py --dry-run
    python usecase-sim/scripts/reset_data.py --local-only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    OUTPUT_DIR,
    delete_minio_prefix,
    drop_iceberg_namespace,
    setup_logging,
    wipe_dir_children,
)
from reset_watermarks import (  # noqa: E402
    ALL_DIALECTS,
    reset_db,
    reset_local as reset_local_watermarks,
    reset_minio as reset_minio_watermarks,
)
from _common import DB_URLS  # noqa: E402

logger = setup_logging("reset_data")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset usecase-sim output + watermarks")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-only", action="store_true",
                        help="Skip MinIO + Iceberg + DB cleanup")
    parser.add_argument("--dialects", nargs="+", default=ALL_DIALECTS)
    args = parser.parse_args()

    logger.info("=== reset_data %s===", "(DRY RUN) " if args.dry_run else "")

    # 1. watermarks (local always; others only when not --local-only)
    reset_local_watermarks(args.dry_run)
    if not args.local_only:
        reset_minio_watermarks(args.dry_run)
        for d in args.dialects:
            if d in DB_URLS:
                logger.info("--- db watermarks: %s ---", d)
                reset_db(d, DB_URLS[d], args.dry_run)

    # 2. local output
    logger.info("--- local output ---")
    wipe_dir_children(OUTPUT_DIR, dry_run=args.dry_run, label="output")

    if args.local_only:
        return 0

    # 3. MinIO output + iceberg-warehouse
    for prefix, label in [("output/", "output"),
                          ("iceberg-warehouse/", "iceberg warehouse")]:
        logger.info("--- MinIO %s ---", label)
        try:
            n = delete_minio_prefix(prefix, dry_run=args.dry_run)
            logger.info("%s: %d object(s)", label, n)
        except Exception as exc:
            logger.warning("%s skipped: %s", label, exc)

    # 4. Iceberg catalog default namespace
    logger.info("--- Iceberg namespace 'default' ---")
    try:
        drop_iceberg_namespace("default", dry_run=args.dry_run)
    except Exception as exc:
        logger.warning("Iceberg cleanup skipped: %s", exc)

    logger.info("=== reset_data complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
