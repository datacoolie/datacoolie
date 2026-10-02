"""Prepare an Iceberg replay + replacement profile on MinIO/REST."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import boto3
import pyarrow as pa
from botocore.config import Config
from pyiceberg.catalog import load_catalog


ROOT = Path(__file__).resolve().parents[1]
BASE_METADATA = ROOT / "metadata" / "file" / "replay_iceberg_validation.json"
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


def _delete_prefix(client, prefix: str) -> int:
    keys: list[dict[str, str]] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix
    ):
        keys.extend({"Key": item["Key"]} for item in page.get("Contents", []))
    for start in range(0, len(keys), 1000):
        client.delete_objects(
            Bucket=BUCKET, Delete={"Objects": keys[start : start + 1000]}
        )
    return len(keys)


def _source_table(catalog) -> None:
    source_id = "default.replay_iceberg_source"
    if catalog.table_exists(source_id):
        catalog.drop_table(source_id)

    # Keep the fixture to the six rows in the replay range and omit the later
    # duplicate sample rows. This makes replacement assertions unambiguous.
    from _common import SAMPLE_ROWS  # noqa: PLC0415

    cols = list(zip(*SAMPLE_ROWS[:25]))
    table = pa.table(
        {
            "order_id": pa.array(cols[0], type=pa.int64()),
            "order_date": pa.array(
                [date.fromisoformat(value) for value in cols[1]], type=pa.date32()
            ),
            "amount": pa.array(
                [Decimal(value) for value in cols[2]], type=pa.decimal128(18, 2)
            ),
            "quantity": pa.array(cols[3], type=pa.int32()),
            "region": pa.array(cols[4], type=pa.string()),
            "modified_at": pa.array(cols[5], type=pa.string()),
            "customer_name": pa.array(cols[6], type=pa.string()),
            "status": pa.array(cols[7], type=pa.string()),
        }
    )
    catalog.create_namespace_if_not_exists("default")
    target = catalog.create_table(source_id, schema=table.schema)
    target.append(table)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()

    if not BASE_METADATA.is_file():
        raise SystemExit(f"Missing checked-in metadata fixture: {BASE_METADATA}")

    catalog = _catalog()
    output_table = f"iceberg_replay_validation_{args.engine}"
    output_id = f"default.{output_table}"
    if catalog.table_exists(output_id):
        catalog.drop_table(output_id)
    _source_table(catalog)

    client = _client()
    metadata_key = f"metadata/replay_iceberg_validation_{args.engine}.json"
    state_prefix = f"state/replay_validation/iceberg_{args.engine}/"
    metadata = json.loads(BASE_METADATA.read_text(encoding="utf-8"))
    metadata["dataflows"][0]["destination"]["table"] = output_table
    client.put_object(
        Bucket=BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    removed_state = _delete_prefix(client, state_prefix)
    print(
        "prepared MinIO Iceberg replay: "
        f"engine={args.engine} source=default.replay_iceberg_source "
        f"metadata=s3://{BUCKET}/{metadata_key} output={output_id} "
        f"removed_state={removed_state}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
