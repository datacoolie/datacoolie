"""Validate a MinIO-backed replay output and saved watermark."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

import boto3
from deltalake import DeltaTable


BUCKET = "datacoolie-test"
ENDPOINT = os.environ.get("DATACOOLIE_MINIO_ENDPOINT", "http://localhost:9000")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    storage_options = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_ENDPOINT_URL": ENDPOINT,
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
    }
    output_uri = f"s3://{BUCKET}/output/delta/aws_replay_validation_{args.engine}"
    table = DeltaTable(output_uri, storage_options=storage_options).to_pyarrow_table()
    if table.num_rows != 6:
        raise AssertionError(f"Expected six replay rows, got {table.num_rows}")
    ids = set(table.column("order_id").to_pylist())
    if ids != {1001, 1002, 1003, 1004, 1005, 1006}:
        raise AssertionError(f"Unexpected replay IDs: {sorted(ids)}")
    lower = datetime.fromisoformat(args.start)
    upper = datetime.fromisoformat(args.end)
    modified = [value.replace(tzinfo=None) for value in table.column("modified_at").to_pylist()]
    if any(value < lower or value >= upper for value in modified):
        raise AssertionError(f"Replay rows escaped [{args.start}, {args.end})")

    client = _client()
    state_prefix = f"state/replay_validation/aws_{args.engine}/"
    values: list[dict[str, object]] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=state_prefix):
        for item in page.get("Contents", []):
            if not item["Key"].endswith("watermark_value.json"):
                continue
            body = client.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read()
            values.append(json.loads(body.decode("utf-8")))
    if not any(value.get("order_date") == "2024-01-17" for value in values):
        raise AssertionError(f"Saved order_date watermark not found below {state_prefix}")
    print(
        f"validated MinIO replay: engine={args.engine} rows={table.num_rows} "
        f"ids={sorted(ids)} state={state_prefix} watermark=2024-01-17"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
