"""Prepare the MinIO-backed AWS replay qualification profile."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3


ROOT = Path(__file__).resolve().parents[1]
BUCKET = "datacoolie-test"
METADATA_FILE = ROOT / "metadata" / "file" / "replay_aws_validation.json"
ENDPOINT = os.environ.get("DATACOOLIE_MINIO_ENDPOINT", "http://localhost:9000")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _delete_prefix(client, prefix: str) -> int:
    paginator = client.get_paginator("list_objects_v2")
    keys: list[dict[str, str]] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        keys.extend({"Key": item["Key"]} for item in page.get("Contents", []))
    for start in range(0, len(keys), 1000):
        client.delete_objects(Bucket=BUCKET, Delete={"Objects": keys[start : start + 1000]})
    return len(keys)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()

    if not METADATA_FILE.is_file():
        raise SystemExit(f"Missing checked-in metadata fixture: {METADATA_FILE}")

    # Re-seed the standard AWS/MinIO input fixture through the supported setup
    # path, then publish the replay-only metadata file beside it.
    generate = ROOT / "scripts" / "generate_data.py"
    completed = subprocess.run(
        [sys.executable, str(generate), "--targets", "minio"],
        cwd=str(ROOT.parent),
        check=False,
        text=True,
    )
    if completed.returncode:
        raise SystemExit(completed.returncode)

    client = _client()
    metadata_key = f"metadata/replay_aws_validation_{args.engine}.json"
    output_table = f"aws_replay_validation_{args.engine}"
    metadata = json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    metadata["dataflows"][0]["destination"]["table"] = output_table
    client.put_object(
        Bucket=BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    removed_output = _delete_prefix(client, f"output/delta/{output_table}/")
    removed_state = _delete_prefix(client, f"state/replay_validation/aws_{args.engine}/")
    print(
        f"prepared AWS-compatible MinIO replay: endpoint={ENDPOINT} "
        f"metadata=s3://{BUCKET}/{metadata_key} "
        f"output=s3://{BUCKET}/output/delta/{output_table} "
        f"removed_output={removed_output} removed_state={removed_state}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
