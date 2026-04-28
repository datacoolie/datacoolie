"""Reset watermarks across every storage location.

Removes:
    - Local watermark folders (metadata/file/watermarks, metadata/api/watermarks)
    - MinIO watermarks/ prefix in the datacoolie-test bucket
    - dc_framework_watermarks rows in every reachable metadata DB

Usage:
    python usecase-sim/scripts/reset_watermarks.py
    python usecase-sim/scripts/reset_watermarks.py --dry-run
    python usecase-sim/scripts/reset_watermarks.py --skip-minio --dialects postgresql
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    API_WATERMARKS_DIR,
    DB_URLS,
    FILE_WATERMARKS_DIR,
    delete_minio_prefix,
    setup_logging,
    wipe_dir_children,
)

logger = setup_logging("reset_watermarks")

ALL_DIALECTS = ["sqlite", "postgresql", "mysql", "mssql", "oracle"]


def reset_local(dry_run: bool) -> None:
    logger.info("--- local watermarks ---")
    wipe_dir_children(FILE_WATERMARKS_DIR, dry_run=dry_run, label="file watermarks")
    wipe_dir_children(API_WATERMARKS_DIR,  dry_run=dry_run, label="api watermarks")


def reset_minio(dry_run: bool) -> None:
    logger.info("--- MinIO watermarks/ ---")
    try:
        count = delete_minio_prefix("watermarks/", dry_run=dry_run)
        logger.info("MinIO watermarks: %d object(s)", count)
    except Exception as exc:
        logger.warning("MinIO cleanup skipped: %s", exc)


def reset_db(dialect: str, url: str, dry_run: bool) -> None:
    try:
        from sqlalchemy import create_engine, inspect, text  # noqa: PLC0415
    except ImportError:
        logger.warning("sqlalchemy not installed — skipping %s", dialect)
        return
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            if not inspect(conn).has_table("dc_framework_watermarks"):
                logger.info("%s: watermarks table not found — skipping", dialect)
                return
            if dry_run:
                n = conn.execute(text("SELECT count(*) FROM dc_framework_watermarks")).scalar()
                logger.info("[DRY-RUN] %s: would delete %d row(s)", dialect, n)
                return
            result = conn.execute(text("DELETE FROM dc_framework_watermarks"))
            conn.commit()
            logger.info("%s: deleted %d row(s)", dialect, result.rowcount)
    except Exception as exc:
        logger.warning("%s skipped: %s", dialect, exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset watermarks everywhere")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-local", action="store_true")
    parser.add_argument("--skip-minio", action="store_true")
    parser.add_argument("--skip-db",    action="store_true")
    parser.add_argument("--dialects", nargs="+", default=ALL_DIALECTS,
                        help="DB dialects to target (default: all)")
    parser.add_argument("--db-url", action="append", default=[],
                        metavar="DIALECT=URL",
                        help="Override a dialect's connection string")
    args = parser.parse_args()

    db_urls = dict(DB_URLS)
    for kv in args.db_url:
        k, v = kv.split("=", 1)
        db_urls[k] = v

    logger.info("=== reset watermarks %s===", "(DRY RUN) " if args.dry_run else "")

    if not args.skip_local:
        reset_local(args.dry_run)
    if not args.skip_minio:
        reset_minio(args.dry_run)
    if not args.skip_db:
        for d in args.dialects:
            if d not in db_urls:
                logger.warning("Unknown dialect: %s", d)
                continue
            logger.info("--- db: %s ---", d)
            reset_db(d, db_urls[d], args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
