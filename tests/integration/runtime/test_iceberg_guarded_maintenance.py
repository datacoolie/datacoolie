"""Opt-in strict-A2 native guard and verified deployment-scope qualification."""
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

import pytest

from tests.support.datatype_config import require_docker_services

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


def test_native_rest_distinct_backend_and_host_container_scope_binding():
    require_docker_services(("spark", "minio", "iceberg-rest"))
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    import pyarrow as pa

    manifest_path = os.getenv("DATACOOLIE_REST_SCOPE_RECEIPT")
    assert manifest_path, "Explicitly prepare an isolated scope service and pass DATACOOLIE_REST_SCOPE_RECEIPT"
    manifest = json.loads(Path(manifest_path).read_text())
    inspected = json.loads(subprocess.check_output([
        "docker", "inspect", "--format", "{{json .}}", manifest["container"]], text=True))
    assert inspected["Id"] == manifest["container_id"]
    assert inspected["Image"] == manifest["image"]
    assert inspected["State"]["Running"]
    assert manifest["warehouse"].startswith("s3://datacoolie-test/rest-scope-qualification/")
    properties = {"s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1"}
    catalogs = [RestCatalog("shared", uri="http://localhost:8181", **properties),
                RestCatalog("isolated", uri=manifest["host_uri"], **properties)]
    namespace = f"maintenance_scope_{uuid4().hex}"
    uuids = []
    locations = []
    for catalog, value in zip(catalogs, (111, 222)):
        catalog.create_namespace(namespace)
        table = catalog.create_table((namespace, "scope"), schema=Schema(NestedField(1, "id", LongType(), required=False)))
        table.append(pa.table({"id": pa.array([value], type=pa.int64())}))
        assert table.scan().to_arrow().to_pylist() == [{"id": value}]
        uuids.append(str(table.metadata.table_uuid))
        locations.append(table.location())
    assert uuids[0] != uuids[1]
    assert locations[0].startswith("s3://datacoolie-test/iceberg-warehouse/")
    assert locations[1].startswith(manifest["warehouse"] + "/")
    child = subprocess.run(["docker", "exec", "-w", "/datacoolie", "datacoolie-spark", "python3",
        "tests/support/rest_scope_worker.py", json.dumps({**manifest, "namespace": namespace, "uuids": uuids})],
        capture_output=True, text=True, timeout=60)
    assert child.returncode == 0, child.stdout + child.stderr
    assert "container_aliases_verified" in child.stdout
    print("VERIFIED_REST_SCOPE=" + json.dumps({
        "namespace": namespace, "uuids": uuids, "locations": locations,
        "routes": ["http://localhost:8181", "http://iceberg-rest:8181", manifest["host_uri"], manifest["container_uri"]],
        "scope_evidence": "Checked service deployment + independent table UUID/row sentinels through operative host/container routes",
        "tenant_or_arbitrary_endpoint_qualified": False,
    }, sort_keys=True))


@pytest.mark.spark
def test_spark_guarded_native_actions_reject_replacement_at_native_boundaries():
    require_docker_services(("spark", "minio", "iceberg-rest"))
    manifest = os.getenv("DATACOOLIE_GUARD_MANIFEST")
    assert manifest, "Pass a container-visible canonical guard manifest for shipping-helper qualification"
    result = subprocess.run(["docker", "exec", "-e", f"DATACOOLIE_GUARD_MANIFEST={manifest}",
        "-w", "/datacoolie", "datacoolie-spark", "python3",
        "tests/support/spark_guarded_maintenance_worker.py"], capture_output=True, text=True, timeout=360)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = [line.split("GUARDED_MAINTENANCE_PROBE=", 1)[1] for line in result.stdout.splitlines()
             if "GUARDED_MAINTENANCE_PROBE=" in line]
    assert len(lines) == 1, result.stdout
    evidence = json.loads(lines[0])
    assert evidence["canonical_helper"]
    assert len(evidence["cases"]) == 18
    assert len(evidence["reused_root_cases"]) == 18
    assert len(evidence["positive"]) == 5
    assert evidence["immutable_paths_and_sticky_latch"] and evidence["same_uuid_native_retry"]
    assert all(case["replacement_metadata_unchanged"] and case["replacement_objects_unchanged"]
               and case["cleanup_callback_deletions"] == 0
               for case in evidence["cases"] + evidence["reused_root_cases"])
    assert any(case["old_uncommitted_outputs_deleted"] > 0 for case in evidence["reused_root_cases"])
    print("GUARDED_MAINTENANCE_PROBE=" + lines[0])
