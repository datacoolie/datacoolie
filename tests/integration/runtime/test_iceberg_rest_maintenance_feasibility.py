"""Installed native API probes for T1; passes do not close M1/M2 acceptance."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

import pytest

from tests.support.datatype_config import require_docker_services

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


def _catalog(name, **options):
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(name, **{
        "uri": os.getenv("DATACOOLIE_QUALIFICATION_ICEBERG_URI", "http://localhost:8181"),
        "s3.endpoint": os.getenv("DATACOOLIE_QUALIFICATION_MINIO_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("DATACOOLIE_QUALIFICATION_MINIO_ACCESS_KEY", "minioadmin"),
        "s3.secret-access-key": os.getenv("DATACOOLIE_QUALIFICATION_MINIO_SECRET_KEY", "minioadmin"),
        "s3.region": "us-east-1", "s3.path-style-access": "true",
        **options,
    })


def test_pyiceberg_rest_refresh_commit_uuid_and_protected_expiration():
    """Prove native UUID guards and expiration effects on unique synthetic tables."""
    require_docker_services(("minio", "iceberg-rest"))
    import pyarrow as pa
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    catalog = _catalog("probe")
    namespace = f"maintenance_probe_{uuid4().hex}"
    catalog.create_namespace(namespace)
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = catalog.create_table((namespace, "Orders"), schema=schema)
    for value in (101, 102, 103):
        table.append(pa.table({"id": pa.array([value], type=pa.int64())}))
    snapshots = [item.snapshot_id for item in table.snapshots()]
    table.manage_snapshots().create_tag(snapshots[0], "protected").commit()
    table.maintenance.expire_snapshots().older_than(datetime.now(timezone.utc) + timedelta(seconds=1)).commit()
    table.refresh()
    assert {s.snapshot_id for s in table.snapshots()} == {snapshots[0], snapshots[-1]}
    assert sorted(row["id"] for row in table.scan().to_arrow().to_pylist()) == [101, 102, 103]
    assert table.refs()["protected"].snapshot_id == snapshots[0]
    alias = _catalog("another-authored-alias")
    assert alias.load_table((namespace, "Orders")).metadata.table_uuid == table.metadata.table_uuid
    lower = catalog.create_table((namespace, "orders"), schema=schema)
    assert lower.metadata.table_uuid != table.metadata.table_uuid

    # Stage a real expiration before replacement, then prove the native commit
    # UUID requirement rejects it. Expiration has no snapshot producers, so
    # Transaction.commit_transaction does not retry this failed commit.
    table.append(pa.table({"id": pa.array([104], type=pa.int64())}))
    original_uuid = table.metadata.table_uuid
    staged = table.maintenance.expire_snapshots().older_than(datetime.now(timezone.utc) + timedelta(seconds=1))
    catalog.drop_table((namespace, "Orders"))
    replacement = catalog.create_table((namespace, "Orders"), schema=schema)
    replacement.append(pa.table({"id": pa.array([999], type=pa.int64())}))
    before = replacement.metadata.model_dump_json()
    assert replacement.metadata.table_uuid != original_uuid
    with pytest.raises(ValueError, match="UUID"):
        table.refresh()
    with pytest.raises(CommitFailedException, match="UUID|uuid"):
        staged.commit()
    assert catalog.load_table((namespace, "Orders")).metadata.model_dump_json() == before
    assert replacement.scan().to_arrow().to_pylist() == [{"id": 999}]
    print("PYICEBERG_REST_PROBE=" + json.dumps({
        "namespace": namespace, "original_uuid": str(original_uuid),
        "replacement_uuid": str(replacement.metadata.table_uuid),
        "seed_snapshot_ids": snapshots,
        "retained_snapshot_ids": [snapshots[0], snapshots[-1]],
        "protected_tag_snapshot_id": snapshots[0],
        "refresh_error": "ValueError", "commit_error": "CommitFailedException",
        "replacement_unchanged": True, "driver_maintenance_qualified": False,
    }, sort_keys=True))
    # Retain this unique namespace for review; no data-file reclamation claim.


def test_rest_server_warehouse_parameter_does_not_establish_scope():
    """Current server ignores requested warehouse; config is not physical identity."""
    require_docker_services(("minio", "iceberg-rest"))
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    import requests

    first = _catalog("one", warehouse="s3://datacoolie-test/probe-one")
    second = _catalog("two", warehouse="s3://datacoolie-test/probe-two")
    namespace = f"maintenance_scope_probe_{uuid4().hex}"
    first.create_namespace(namespace)
    table = first.create_table((namespace, "scope"), schema=Schema(NestedField(1, "id", LongType(), required=False)))
    assert second.load_table((namespace, "scope")).metadata.table_uuid == table.metadata.table_uuid
    assert table.location().startswith("s3://datacoolie-test/iceberg-warehouse/")
    response = requests.get(first.uri.rstrip("/") + "/v1/config", timeout=10)
    response.raise_for_status()
    assert response.json() == {"defaults": {}, "overrides": {}}
    print("REST_SCOPE_PROBE=" + json.dumps({
        "namespace": namespace, "uuid": str(table.metadata.table_uuid),
        "location": table.location(), "config": response.json(),
        "different_requested_warehouses_same_table": True,
    }, sort_keys=True))


def test_pyiceberg_rest_empty_expiration_is_server_error_without_effects():
    """The installed server rejects an empty update; adapter must detect no-work."""
    require_docker_services(("minio", "iceberg-rest"))
    from pyiceberg.exceptions import CommitStateUnknownException
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    catalog = _catalog("empty-probe")
    namespace = f"maintenance_empty_probe_{uuid4().hex}"
    catalog.create_namespace(namespace)
    table = catalog.create_table((namespace, "empty"), schema=Schema(NestedField(1, "id", LongType(), required=False)))
    before = table.metadata.model_dump_json()
    with pytest.raises(CommitStateUnknownException, match="Invalid set of snapshot ids to remove"):
        table.maintenance.expire_snapshots().older_than(datetime.now(timezone.utc)).commit()
    assert catalog.load_table((namespace, "empty")).metadata.model_dump_json() == before


def test_pyiceberg_rest_native_multilevel_namespace_requires_tuple_boundaries():
    """Native tuple succeeds where the current SQL-name helper strips a level."""
    require_docker_services(("minio", "iceberg-rest"))
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    from datacoolie.engines._polars.iceberg.schema import table_id

    catalog = _catalog("multipart-probe")
    namespace = (f"maintenance_multipart_probe_{uuid4().hex}", "nested")
    catalog.create_namespace(namespace)
    identifier = (*namespace, "Orders")
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "id", LongType(), required=False)))
    assert catalog.load_table(identifier).metadata.table_uuid == table.metadata.table_uuid
    assert table_id(".".join(identifier)) == "nested.Orders"
    assert not catalog.table_exists(table_id(".".join(identifier)))


@pytest.mark.spark
def test_spark35_rest_table_refresh_adopts_replacement_native_probe():
    """Record the unguarded API limitation; this explicitly does not assert A2."""
    require_docker_services(("spark", "minio", "iceberg-rest"))
    product = Path(__file__).resolve().parents[3]
    result = subprocess.run([
        "docker", "exec", "-w", "/datacoolie", "datacoolie-spark", "python3",
        "/datacoolie/tests/support/spark_rest_maintenance_probe.py",
    ], cwd=product, capture_output=True, text=True, timeout=240)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = [line.removeprefix("REST_MAINTENANCE_PROBE=") for line in result.stdout.splitlines()
             if line.startswith("REST_MAINTENANCE_PROBE=")]
    assert len(lines) == 1, result.stdout
    evidence = json.loads(lines[0])
    host_table = _catalog("host-alias").load_table((evidence["namespace"], "Orders"))
    assert str(host_table.metadata.table_uuid) == evidence["replacement_uuid"]
    assert evidence["driver_maintenance_qualified"] is False
    assert evidence["original_uuid"] != evidence["replacement_uuid"] == evidence["refreshed_uuid"]
    assert evidence["rewritten_data_files"] == 2
    assert evidence["replacement_snapshots"][0] != evidence["replacement_snapshots"][1]
    print("REST_MAINTENANCE_PROBE=" + lines[0])
