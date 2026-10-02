"""Validate Iceberg replay output and same-range replacement on MinIO."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import boto3
from botocore.config import Config
from pyiceberg.catalog import load_catalog


BUCKET = "datacoolie-test"
ENDPOINT = os.environ.get("DATACOOLIE_MINIO_ENDPOINT", "http://localhost:9000")
ICEBERG_URI = os.environ.get("DATACOOLIE_ICEBERG_URI", "http://localhost:8181")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def _catalog():
    return load_catalog(
        "datacoolie",
        type="rest",
        uri=ICEBERG_URI,
        **{
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )


def _run_again(engine: str) -> None:
    metadata = f"s3://{BUCKET}/metadata/replay_iceberg_validation_{engine}.json"
    common = [
        "--engine", engine,
        "--platform", "aws",
        "--metadata-source", "file",
        "--metadata-path", metadata,
        "--stage", "iceberg_replay_validation",
        "--needs-iceberg",
        "--state-base-path", f"s3://{BUCKET}/state/replay_validation/iceberg_{engine}",
        "--replay-start", "2024-01-15",
        "--replay-end", "2024-01-18",
        "--replay-chunk-interval", "days=1",
        "--replay-save-watermark",
    ]
    if engine == "spark":
        command = [
            "docker", "exec", "datacoolie-spark", "python3",
            "/datacoolie/usecase-sim/runner/run.py",
            *common,
        ]
    else:
        command = [sys.executable, "usecase-sim/runner/run.py", *common]
    completed = subprocess.run(
        command,
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[2]),
        capture_output=True,
        text=True,
        timeout=360,
    )
    if completed.returncode:
        raise AssertionError(
            "Same-range Iceberg replay failed on replacement pass:\n"
            f"{completed.stdout}\n{completed.stderr}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()

    table_id = f"default.iceberg_replay_validation_{args.engine}"
    catalog = _catalog()
    table = catalog.load_table(table_id)
    first = table.scan().to_arrow()
    first_ids = sorted(first.column("order_id").to_pylist())
    if first_ids != [1001, 1002, 1003, 1004, 1005, 1006]:
        raise AssertionError(f"Unexpected first-pass Iceberg rows: {first_ids}")
    first_snapshots = len(list(table.snapshots() or []))

    _run_again(args.engine)

    table = catalog.load_table(table_id)
    second = table.scan().to_arrow()
    second_ids = sorted(second.column("order_id").to_pylist())
    if second_ids != first_ids:
        raise AssertionError(
            "Same-range replacement changed the selected rows: "
            f"before={first_ids}, after={second_ids}"
        )
    second_snapshots = len(list(table.snapshots() or []))
    if second_snapshots <= first_snapshots:
        raise AssertionError("Replacement pass did not commit a new Iceberg snapshot")

    client = _client()
    state_prefix = f"state/replay_validation/iceberg_{args.engine}/"
    saved = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=state_prefix
    ):
        for item in page.get("Contents", []):
            if item["Key"].endswith("watermark_value.json"):
                body = client.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read()
                saved.append(json.loads(body.decode("utf-8")))
    if not any(
        value.get("order_date", {}).get("__date__") == "2024-01-17"
        for value in saved
    ):
        raise AssertionError("Saved Iceberg replay watermark order_date=2024-01-17 not found")

    print(
        "validated MinIO Iceberg replay/replacement: "
        f"engine={args.engine} rows={len(second_ids)} ids={second_ids} "
        f"snapshots={first_snapshots}->{second_snapshots} "
        "range=[2024-01-15,2024-01-18)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
