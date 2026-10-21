"""Prepare one isolated REST Iceberg maintenance target for a focused case.

The setup owns only a UUID-suffixed namespace and writes the caller's
deployment descriptor below the simulator data root.  It deliberately does
not reset a catalog, delete an old namespace, or infer deployment identity
from the REST URI.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


SCRIPT = Path(__file__).resolve()
USECASE_SIM_DIR = SCRIPT.parents[3]
PRODUCT_ROOT = USECASE_SIM_DIR.parent
if str(USECASE_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(USECASE_SIM_DIR))
if str(PRODUCT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PRODUCT_ROOT / "src"))

from runner.runtime_paths import DATA_DIR  # noqa: E402


ALLOWED_ROUTES = frozenset(("http://localhost:8181", "http://iceberg-rest:8181"))
EXPECTED_WAREHOUSE = "s3://datacoolie-test/iceberg-warehouse"
AWS_METADATA_PREFIX = "metadata/maintenance_t5/"
ALLOWED_S3_ENDPOINTS = frozenset(("http://localhost:9000", "http://minio:9000"))


def _path(value: str, *, option: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = PRODUCT_ROOT / candidate
    candidate = candidate.resolve()
    data_root = DATA_DIR.resolve()
    if candidate == data_root or not candidate.is_relative_to(data_root):
        raise ValueError(
            f"{option} must stay strictly below usecase-sim/.runtime/data"
        )
    return candidate


def _catalog(uri: str, s3_endpoint: str):
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "datacoolie",
        uri=uri,
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.getenv(
                "AWS_SECRET_ACCESS_KEY", "minioadmin"
            ),
            "s3.region": os.getenv("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def _validate_deployment_inputs(
    catalog_uri: str, alternate_route: str, warehouse: str, s3_endpoint: str
) -> tuple[str, ...]:
    routes = tuple(
        route
        for route in (catalog_uri, alternate_route)
        if route and route not in ()
    )
    if not routes or any(route not in ALLOWED_ROUTES for route in routes):
        raise ValueError(
            "REST qualification accepts only the verified localhost and "
            "iceberg-rest routes"
        )
    if warehouse != EXPECTED_WAREHOUSE:
        raise ValueError(
            f"REST qualification requires warehouse {EXPECTED_WAREHOUSE!r}"
        )
    if s3_endpoint not in ALLOWED_S3_ENDPOINTS:
        raise ValueError(
            "REST qualification accepts only the checked-in MinIO endpoints"
        )
    return tuple(dict.fromkeys(routes))


def _inspect_container_candidates(service: str) -> list[dict[str, object]]:
    """Return running containers for one compose service without mutating Docker."""

    try:
        listed = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "qualified Docker deployment inspection is required before REST preparation"
        ) from exc
    ids = [value.strip() for value in listed.stdout.splitlines() if value.strip()]
    if len(ids) != 1:
        raise RuntimeError(
            f"expected exactly one running Docker {service!r} container, got {ids!r}"
        )
    try:
        inspected = subprocess.run(
            ["docker", "inspect", ids[0]],
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(inspected.stdout)
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not inspect qualified Docker {service!r}") from exc
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise RuntimeError(f"unexpected Docker inspect payload for {service!r}")
    container = payload[0]
    if not container.get("Id") or not container.get("State", {}).get("Running"):
        raise RuntimeError(f"qualified Docker {service!r} container is not running")
    return [container]


def _docker_attestation(routes: tuple[str, ...], warehouse: str) -> tuple[tuple[str, ...], dict[str, object]]:
    """Capture actual REST/catalog backend IDs for the operator descriptor.

    The descriptor deliberately contains only the opaque identity tuple.  The
    richer Docker evidence lives in a separate receipt so the product's
    descriptor contract remains closed and no connection label is treated as
    a deployment identity.
    """

    rest = _inspect_container_candidates("iceberg-rest")[0]
    postgres = _inspect_container_candidates("postgres")[0]
    rest_env = {
        str(value).split("=", 1)[0]: str(value).split("=", 1)[1]
        for value in rest.get("Config", {}).get("Env", [])
        if isinstance(value, str) and "=" in value
    }
    if rest_env.get("CATALOG_WAREHOUSE") != warehouse:
        raise RuntimeError("inspected REST warehouse does not match qualification warehouse")
    if rest_env.get("CATALOG_URI") != "jdbc:postgresql://postgres:5432/iceberg_catalog":
        raise RuntimeError("inspected REST JDBC backend is not the qualified Postgres catalog")
    rest_networks = rest.get("NetworkSettings", {}).get("Networks", {})
    postgres_networks = postgres.get("NetworkSettings", {}).get("Networks", {})
    shared_networks = set(rest_networks).intersection(postgres_networks)
    if not shared_networks:
        raise RuntimeError("inspected REST and Postgres containers do not share a Docker network")
    if not any(
        "iceberg-rest" in rest_networks[network].get("Aliases", [])
        and "postgres" in postgres_networks[network].get("Aliases", [])
        for network in shared_networks
    ):
        raise RuntimeError("inspected REST container has no iceberg-rest network alias")
    published_rest = rest.get("NetworkSettings", {}).get("Ports", {}).get("8181/tcp", [])
    if not any(str(item.get("HostPort")) == "8181" for item in published_rest if isinstance(item, dict)):
        raise RuntimeError("inspected REST container does not publish host port 8181")
    published_postgres = postgres.get("NetworkSettings", {}).get("Ports", {}).get("5432/tcp", [])
    if not any(str(item.get("HostPort")) == "5432" for item in published_postgres if isinstance(item, dict)):
        raise RuntimeError("inspected Postgres container does not publish host port 5432")
    postgres_mounts = postgres.get("Mounts") or []
    volumes = sorted(
        str(item.get("Name") or item.get("Source"))
        for item in postgres_mounts
        if isinstance(item, dict)
        and item.get("Type") == "volume"
        and (item.get("Name") or item.get("Source"))
    )
    if not volumes:
        raise RuntimeError("qualified Postgres container has no inspected volume identity")
    identity = (
        "docker",
        f"iceberg-rest:{rest['Id']}",
        f"postgres:{postgres['Id']}",
        *(f"postgres-volume:{value}" for value in volumes),
    )
    receipt = {
        "format": "iceberg-rest-deployment-attestation-v1",
        "method": "docker_inspect",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "identity": list(identity),
        "routes": list(routes),
        "warehouse": warehouse,
        "containers": {
            "iceberg_rest": {
                "id": rest["Id"],
                "name": rest.get("Name"),
                "image": rest.get("Config", {}).get("Image"),
            },
            "postgres": {
                "id": postgres["Id"],
                "name": postgres.get("Name"),
                "image": postgres.get("Config", {}).get("Image"),
                "volumes": volumes,
            },
        },
    }
    return identity, receipt


def _validate_metadata_key(value: str) -> str:
    key = value.replace("\\", "/").lstrip("/")
    parts = key.split("/")
    if (
        not key
        or any(part in ("", ".", "..") for part in parts)
        or not key.startswith(AWS_METADATA_PREFIX)
        or not key.endswith(".json")
    ):
        raise ValueError(
            "--aws-metadata-key must be one case-owned JSON key below "
            f"{AWS_METADATA_PREFIX!r}"
        )
    return key


def _aws_client(endpoint: str):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def _upload_metadata(
    metadata: Path,
    *,
    bucket: str,
    key: str,
    endpoint: str,
) -> str:
    if bucket != "datacoolie-test":
        raise ValueError("maintenance AWS metadata is restricted to datacoolie-test")
    key = _validate_metadata_key(key)
    client = _aws_client(endpoint)
    try:
        client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        error = getattr(exc, "response", {}).get("Error", {})
        if str(error.get("Code", "")) not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        raise FileExistsError(f"case-owned AWS metadata key already exists: s3://{bucket}/{key}")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=metadata.read_bytes(),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def _schema_signature(metadata_json: str) -> dict[str, object]:
    payload = json.loads(metadata_json)
    return {
        key: payload.get(key)
        for key in (
            "schemas",
            "current-schema-id",
            "partition-specs",
            "default-spec-id",
            "sort-orders",
            "default-sort-order-id",
            "properties",
        )
        if key in payload
    }


def prepare(
    descriptor: Path,
    *,
    metadata: Path,
    expected_state: Path,
    catalog_uri: str,
    alternate_route: str,
    warehouse: str,
    s3_endpoint: str,
    aws_metadata_key: str | None = None,
    aws_bucket: str = "datacoolie-test",
    deployment_receipt: Path | None = None,
) -> dict[str, object]:
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    for path, label in (
        (descriptor, "descriptor"),
        (metadata, "metadata"),
        (expected_state, "expected state"),
    ):
        if path.exists():
            raise FileExistsError(f"{label} already exists: {path}")
    descriptor.parent.mkdir(parents=True, exist_ok=True)
    metadata.parent.mkdir(parents=True, exist_ok=True)
    expected_state.parent.mkdir(parents=True, exist_ok=True)

    routes = _validate_deployment_inputs(
        catalog_uri, alternate_route, warehouse, s3_endpoint
    )
    identity, attestation = _docker_attestation(routes, warehouse)
    if deployment_receipt is not None:
        if deployment_receipt.exists():
            raise FileExistsError(f"deployment receipt already exists: {deployment_receipt}")
        deployment_receipt.parent.mkdir(parents=True, exist_ok=True)
    catalog = _catalog(catalog_uri, s3_endpoint)
    namespace = f"maintenance_t5_{uuid4().hex}"
    catalog.create_namespace(namespace)
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    sentinel = catalog.create_table((namespace, "sentinel"), schema=schema)
    target = catalog.create_table((namespace, "Orders"), schema=schema)
    sentinel.append(pa.table({"id": pa.array([909], type=pa.int64())}))
    for value in (101, 102, 103, 104):
        target.append(pa.table({"id": pa.array([value], type=pa.int64())}))
    snapshots = [item.snapshot_id for item in target.snapshots()]
    if len(snapshots) < 2:
        raise AssertionError(f"expected multiple seed snapshots, got {snapshots!r}")
    target.manage_snapshots().create_tag(snapshots[0], "protected").commit()
    target.manage_snapshots().create_branch(snapshots[1], "protected_branch").commit()
    # Give the expiration cutoff a stable millisecond boundary.  The current
    # REST implementation uses a strict timestamp comparison for retention=0.
    time.sleep(0.02)

    payload = {
        "deployment_identity": list(identity),
        "routes": list(routes),
        "warehouse": warehouse,
        "sentinel_identifier": [namespace, "sentinel"],
        "sentinel_uuid": str(sentinel.metadata.table_uuid),
        "catalog_label": "local_catalog",
        "prefix": "",
    }
    rows = sorted(
        (dict(row) for row in target.scan().to_arrow().to_pylist()),
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )
    sentinel_metadata = sentinel.metadata.model_dump_json()
    sentinel_rows = sorted(
        (dict(row) for row in sentinel.scan().to_arrow().to_pylist()),
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )
    refs = {
        str(name): str(ref.snapshot_id)
        for name, ref in target.refs().items()
    }
    metadata_document = {
        "connections": [
            {
                "name": "maintenance_source",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": str(descriptor.parent / "input")},
            },
            {
                "name": "maintenance_iceberg_dest",
                "connection_type": "lakehouse",
                "format": "iceberg",
                "catalog": "local_catalog",
                "database": namespace,
                "configure": {},
            },
        ],
        "dataflows": [
            {
                "name": "iceberg-rest-maintenance",
                "stage": "iceberg_rest_maintenance",
                "source": {
                    "connection_name": "maintenance_source",
                    "table": "noop",
                },
                "destination": {
                    "connection_name": "maintenance_iceberg_dest",
                    "table": "Orders",
                    "load_type": "append",
                },
            }
        ],
    }
    expected_document = {
        "namespace": namespace,
        "target": [namespace, "Orders"],
        "snapshot_ids": [str(value) for value in snapshots],
        "refs": refs,
        "rows": rows,
        "metadata": target.metadata.model_dump_json(),
        "sentinel_identifier": [namespace, "sentinel"],
        "sentinel_uuid": str(sentinel.metadata.table_uuid),
        "sentinel_metadata": sentinel_metadata,
        "sentinel_rows": sentinel_rows,
        "sentinel_schema_signature": _schema_signature(sentinel_metadata),
    }
    with descriptor.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    with metadata.open("x", encoding="utf-8") as handle:
        json.dump(metadata_document, handle, indent=2, sort_keys=True)
        handle.write("\n")
    with expected_state.open("x", encoding="utf-8") as handle:
        json.dump(expected_document, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    if deployment_receipt is not None:
        with deployment_receipt.open("x", encoding="utf-8") as handle:
            json.dump(attestation, handle, indent=2, sort_keys=True)
            handle.write("\n")
    evidence: dict[str, object] = {
        "descriptor": str(descriptor),
        "metadata": str(metadata),
        "expected_state": str(expected_state),
        "namespace": namespace,
        "target": f"{namespace}.Orders",
        "sentinel_uuid": str(sentinel.metadata.table_uuid),
        "seed_snapshot_ids": snapshots,
        "deployment_identity": list(identity),
        "deployment_receipt": str(deployment_receipt) if deployment_receipt else None,
    }
    if aws_metadata_key is not None:
        evidence["aws_metadata"] = _upload_metadata(
            metadata,
            bucket=aws_bucket,
            key=aws_metadata_key,
            endpoint=s3_endpoint,
        )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--descriptor",
        required=True,
        type=lambda value: _path(value, option="--descriptor"),
    )
    parser.add_argument(
        "--metadata",
        required=True,
        type=lambda value: _path(value, option="--metadata"),
    )
    parser.add_argument(
        "--expected-state",
        required=True,
        type=lambda value: _path(value, option="--expected-state"),
    )
    parser.add_argument(
        "--catalog-uri",
        default=os.getenv("DATACOOLIE_QUALIFICATION_ICEBERG_URI", "http://localhost:8181"),
    )
    parser.add_argument(
        "--alternate-route",
        default="http://iceberg-rest:8181",
        help="container-visible alias retained as a permitted route",
    )
    parser.add_argument(
        "--warehouse",
        default="s3://datacoolie-test/iceberg-warehouse",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv("DATACOOLIE_QUALIFICATION_MINIO_ENDPOINT", "http://localhost:9000"),
    )
    parser.add_argument(
        "--aws-metadata-key",
        help="Upload generated metadata once to a case-owned maintenance_t5 S3 key",
    )
    parser.add_argument("--aws-bucket", default="datacoolie-test")
    parser.add_argument(
        "--deployment-receipt",
        type=lambda value: _path(value, option="--deployment-receipt"),
        help="Separate Docker inspection receipt; never embedded in the descriptor",
    )
    args = parser.parse_args()
    evidence = prepare(
        args.descriptor,
        metadata=args.metadata,
        expected_state=args.expected_state,
        catalog_uri=args.catalog_uri,
        alternate_route=args.alternate_route,
        warehouse=args.warehouse,
        s3_endpoint=args.s3_endpoint,
        aws_metadata_key=args.aws_metadata_key,
        aws_bucket=args.aws_bucket,
        deployment_receipt=args.deployment_receipt,
    )
    print("ICEBERG_REST_MAINTENANCE_PREPARED=" + json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
