"""Generate performance benchmark data at various row counts.

Writes to local filesystem, MinIO (parity mirror), and Iceberg REST catalog.

Usage (from the datacoolie/ directory):
    python usecase-sim/scripts/generate_perf_data.py
    python usecase-sim/scripts/generate_perf_data.py --sizes 10k,50k
    python usecase-sim/scripts/generate_perf_data.py --formats parquet,delta
    python usecase-sim/scripts/generate_perf_data.py --targets local,iceberg
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    ICEBERG_REST_URI,
    MINIO_BUCKET,
    PERF_INPUT_DIR,
    ensure_bucket,
    iceberg_catalog,
    port_open,
    setup_logging,
    upload_tree,
)

logger = setup_logging("generate_perf_data")

NUM_SHARDS = 12
JSONL_MAX_ROWS = 1_000_000

SIZES = {
    "10k": 10_000, "50k": 50_000, "100k": 100_000, "500k": 500_000,
    "1m": 1_000_000, "5m": 5_000_000, "10m": 10_000_000, "50m": 50_000_000,
}

REGIONS = ["US-East", "US-West", "EU-West", "APAC", "EU-North"]
STATUSES = ["completed", "shipped", "pending", "cancelled"]
FIRST_NAMES = [
    "Alice", "Bob", "Clara", "David", "Emily", "Frank", "Greta", "Henry",
    "Isha", "James", "Katrin", "Liam", "Mei", "Noah", "Olivia", "Paul",
    "Qian", "Rosa", "Stefan", "Tara", "Umar", "Violet", "Wolfgang", "Xena",
    "Yuto", "Zara", "Aarav", "Beatrice", "Carlos", "Diana",
]
LAST_NAMES = [
    "Johnson", "Smith", "Müller", "Lee", "Chen", "Garcia", "Berg", "Park",
    "Patel", "Brown", "Johansson", "Wilson", "Tanaka", "Martinez", "Fischer",
    "Kim", "Li", "Alvarez", "Novak", "Singh", "Hassan", "Chang", "Braun",
    "Petrov", "Nakamura", "Okafor", "Sharma", "Laurent", "Santos", "Nguyen",
]

ALL_TARGETS  = ["local", "minio", "iceberg"]
ALL_FORMATS  = ["jsonl", "parquet", "delta", "iceberg"]


def _generate_table(num_rows: int, seed: int = 42):
    import numpy as np  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415

    rng = np.random.default_rng(seed)
    order_ids = np.arange(1, num_rows + 1, dtype=np.int64)
    rng.shuffle(order_ids)

    date_origin = np.datetime64("2023-01-01", "D")
    day_offsets = rng.integers(0, 730, size=num_rows)
    dates = (date_origin + day_offsets.astype("timedelta64[D]")).astype(str).tolist()

    amounts = [f"{v:.2f}" for v in rng.uniform(0.01, 9999.99, size=num_rows)]
    quantities = rng.integers(1, 100, size=num_rows, dtype=np.int64)
    regions = [REGIONS[i] for i in rng.integers(0, len(REGIONS), size=num_rows)]
    statuses = [STATUSES[i] for i in rng.integers(0, len(STATUSES), size=num_rows)]
    hours = rng.integers(0, 24, size=num_rows)
    minutes = rng.integers(0, 60, size=num_rows)
    modified = [f"{d}T{h:02d}:{m:02d}:00" for d, h, m in zip(dates, hours, minutes)]
    fi = rng.integers(0, len(FIRST_NAMES), size=num_rows)
    li = rng.integers(0, len(LAST_NAMES),  size=num_rows)
    names = [f"{FIRST_NAMES[f]} {LAST_NAMES[l]}" for f, l in zip(fi, li)]

    return pa.table({
        "order_id":      pa.array(order_ids, type=pa.int64()),
        "order_date":    pa.array(dates, type=pa.string()),
        "amount":        pa.array(amounts, type=pa.string()),
        "quantity":      pa.array(quantities, type=pa.int64()),
        "region":        pa.array(regions, type=pa.string()),
        "modified_at":   pa.array(modified, type=pa.string()),
        "customer_name": pa.array(names, type=pa.string()),
        "status":        pa.array(statuses, type=pa.string()),
    })


def _write_parquet_shards(table, label: str) -> Path:
    import pyarrow.parquet as pq  # noqa: PLC0415
    out = PERF_INPUT_DIR / "parquet" / f"orders_{label}"
    out.mkdir(parents=True, exist_ok=True)
    n = table.num_rows
    shard = n // NUM_SHARDS
    for i in range(NUM_SHARDS):
        start = i * shard
        end = n if i == NUM_SHARDS - 1 else (i + 1) * shard
        pq.write_table(table.slice(start, end - start),
                       out / f"part-{i:02d}.parquet", compression="snappy")
    logger.info("Parquet → %s (%d rows, %d shards)", out, n, NUM_SHARDS)
    return out


def _write_jsonl_shards(table, label: str) -> Path:
    out = PERF_INPUT_DIR / "jsonl" / f"orders_{label}"
    out.mkdir(parents=True, exist_ok=True)
    n = table.num_rows
    shard = n // NUM_SHARDS
    for i in range(NUM_SHARDS):
        start = i * shard
        end = n if i == NUM_SHARDS - 1 else (i + 1) * shard
        rows = table.slice(start, end - start).to_pylist()
        path = out / f"part-{i:02d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    logger.info("JSONL → %s (%d rows, %d shards)", out, n, NUM_SHARDS)
    return out


def _write_delta(table, label: str) -> Path:
    import deltalake  # noqa: PLC0415
    out = PERF_INPUT_DIR / "delta" / f"orders_{label}"
    deltalake.write_deltalake(str(out), table, mode="overwrite")
    logger.info("Delta → %s (%d rows)", out, table.num_rows)
    return out


def _write_iceberg(table, label: str, catalog) -> None:
    import pyarrow as pa  # noqa: PLC0415
    ns = "perf_src"
    try:
        catalog.create_namespace(ns)
    except Exception:
        pass
    fqn = f"{ns}.orders_{label}"
    try:
        catalog.drop_table(fqn)
    except Exception:
        pass
    schema = pa.schema([
        ("order_id", pa.int64()), ("order_date", pa.string()),
        ("amount", pa.string()), ("quantity", pa.int64()),
        ("region", pa.string()), ("modified_at", pa.string()),
        ("customer_name", pa.string()), ("status", pa.string()),
    ])
    t = catalog.create_table(fqn, schema=schema)
    t.append(table)
    logger.info("Iceberg → %s (%d rows)", fqn, table.num_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate perf benchmark data")
    parser.add_argument("--sizes", default=",".join(SIZES),
                        help="Comma-separated size labels (default: all)")
    parser.add_argument("--formats", default=",".join(ALL_FORMATS),
                        help="Comma-separated formats (default: all)")
    parser.add_argument("--targets", default=",".join(ALL_TARGETS),
                        help="Comma-separated: " + ", ".join(ALL_TARGETS))
    parser.add_argument("--iceberg-uri", default=ICEBERG_REST_URI)
    args = parser.parse_args()

    sizes   = [s.strip() for s in args.sizes.split(",") if s.strip()]
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    targets = [t.strip().lower() for t in args.targets.split(",") if t.strip()]

    for s in sizes:
        if s not in SIZES:
            raise ValueError(f"Unknown size: {s}. Valid: {list(SIZES)}")
    for f in formats:
        if f not in ALL_FORMATS:
            raise ValueError(f"Unknown format: {f}. Valid: {ALL_FORMATS}")
    for t in targets:
        if t not in ALL_TARGETS:
            raise ValueError(f"Unknown target: {t}. Valid: {ALL_TARGETS}")

    do_local   = "local"   in targets
    do_minio   = "minio"   in targets
    do_iceberg = "iceberg" in targets and "iceberg" in formats

    catalog = None
    if do_iceberg:
        catalog = iceberg_catalog()
        if catalog:
            try:
                catalog.create_namespace("perf_dst")
            except Exception:
                pass
        else:
            do_iceberg = False

    if do_minio and not port_open("localhost", 9000):
        logger.warning("MinIO not reachable — disabling minio target")
        do_minio = False
    if do_minio:
        ensure_bucket(bucket=MINIO_BUCKET)

    logger.info("=== generate_perf_data ===")
    logger.info("sizes=%s formats=%s targets=%s", sizes, formats, targets)

    for label in sizes:
        num_rows = SIZES[label]
        logger.info("--- %s (%s rows) ---", label, f"{num_rows:,}")
        t0 = time.monotonic()
        table = _generate_table(num_rows)
        logger.info("generated table in %.1fs", time.monotonic() - t0)

        written_dirs: list[tuple[Path, str]] = []  # (local_dir, s3_sub_prefix)

        if do_local:
            if "jsonl" in formats and num_rows <= JSONL_MAX_ROWS:
                d = _write_jsonl_shards(table, label)
                written_dirs.append((d, f"perf/input/jsonl/orders_{label}"))
            if "parquet" in formats:
                d = _write_parquet_shards(table, label)
                written_dirs.append((d, f"perf/input/parquet/orders_{label}"))
            if "delta" in formats:
                d = _write_delta(table, label)
                written_dirs.append((d, f"perf/input/delta/orders_{label}"))

        if do_iceberg and catalog is not None:
            _write_iceberg(table, label, catalog)

        if do_minio:
            for local_dir, s3_prefix in written_dirs:
                upload_tree(local_dir, s3_prefix, bucket=MINIO_BUCKET)

        del table

    logger.info("=== generation complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
