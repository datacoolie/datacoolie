"""Explicit isolated REST scope service; never called by pytest/audit/collection."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from uuid import uuid4


def _docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main() -> None:
    product = Path(__file__).resolve().parents[2]
    output = product / "usecase-sim/.runtime/dependencies/rest-scope-qualification"
    output.mkdir(parents=True, exist_ok=True)
    image = _docker("inspect", "--format", "{{.Image}}", "datacoolie-iceberg-rest")
    networks = json.loads(_docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", "datacoolie-iceberg-rest"))
    assert "docker_default" in networks, "Qualification requires the checked local simulation network"
    token = uuid4().hex
    name = f"datacoolie-rest-scope-{token[:12]}"
    warehouse = f"s3://datacoolie-test/rest-scope-qualification/{token}"
    args = ["run", "-d", "--name", name, "--network", "docker_default", "-p", "127.0.0.1::8181"]
    options = {
        "CATALOG_URI": "jdbc:sqlite:/tmp/qualification.db",
        "CATALOG_WAREHOUSE": warehouse,
        "CATALOG_IO__IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
        "CATALOG_S3_ENDPOINT": "http://minio:9000",
        "CATALOG_S3_ACCESS__KEY__ID": "minioadmin",
        "CATALOG_S3_SECRET__ACCESS__KEY": "minioadmin",
        "CATALOG_S3_PATH__STYLE__ACCESS": "true", "CATALOG_S3_REGION": "us-east-1",
        "AWS_REGION": "us-east-1",
    }
    for key, value in options.items():
        args.extend(("--env", f"{key}={value}"))
    args.append(image)
    container = _docker(*args)
    port = _docker("port", name, "8181/tcp").rsplit(":", 1)[1]
    receipt = {"container": name, "container_id": container, "image": image,
        "host_uri": f"http://127.0.0.1:{port}", "container_uri": f"http://{name}:8181",
        "warehouse": warehouse, "scope": "isolated container-local SQLite database",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "cleanup": "Retained for inspection; no shared reset or service replacement"}
    path = output / f"{token}.json"
    path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"receipt": str(path), **receipt}))


if __name__ == "__main__":
    main()
