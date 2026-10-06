"""Prove actual APIReader SigV4 with a private MinIO object and denial controls."""

from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    read_delta_schema,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)


EXPECTED_SCHEMA = {
    "id": "Int64",
    "value": "String",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}


@contextmanager
def driver_http_trace(endpoint):
    """Observe actual Driver HTTP responses without logging signing headers."""
    import httpx
    from unittest.mock import patch

    original = httpx.Client.__init__
    trace = []

    def capture(response):
        if response.request.url.path != endpoint:
            return
        response.read()
        code = None
        if response.status_code >= 400:
            try:
                code = ElementTree.fromstring(response.content).findtext("Code")
            except ElementTree.ParseError:
                code = "non-xml-error"
        trace.append(
            {
                "method": response.request.method,
                "path": endpoint,
                "status": response.status_code,
                "error_code": code,
            }
        )

    def traced_init(client, *args, **kwargs):
        hooks = {
            key: list(value) for key, value in kwargs.pop("event_hooks", {}).items()
        }
        hooks.setdefault("response", []).append(capture)
        original(client, *args, event_hooks=hooks, **kwargs)

    with patch.object(httpx.Client, "__init__", traced_init):
        yield trace


def assert_denial(evidence, trace, error_code):
    counts = {key: evidence.result[key] for key in ("succeeded", "failed", "skipped")}
    assert counts == {"succeeded": 0, "failed": 1, "skipped": 0}, counts
    assert trace, "Driver failed before a backend response"
    assert all(
        row["status"] == 403 and row["error_code"] == error_code for row in trace
    ), trace


def assert_output(engine, target, rows):
    actual = read_delta_rows(engine, target)
    assert_exact_rows(business_rows(actual), rows, key="id")
    assert_schema(read_delta_schema(engine, target), EXPECTED_SCHEMA)
    for row in actual:
        for column in ("__created_at", "__updated_at"):
            value = row.get(column)
            assert isinstance(value, datetime) and value.utcoffset() == timedelta(0), (
                f"Invalid {column}"
            )
        assert row["__created_at"] == row["__updated_at"], (
            "New overwrite audit timestamps differ"
        )
        assert row.get("__updated_by") == "DataCoolie", "Wrong audit updater"
        run_id = row.get("__dataflow_run_id")
        assert isinstance(run_id, str) and str(uuid.UUID(run_id)) == run_id, (
            "Invalid dataflow run ID"
        )
    assert len({row["__dataflow_run_id"] for row in actual}) == 1, "Mixed execution IDs"


def assert_denied_artifacts(target, watermarks):
    assert not (target / "records").exists(), "Denied request produced business output"
    assert not list(watermarks.rglob("watermark_value.json")), (
        "Denied request advanced a checkpoint"
    )


def _isolated_environment(case, access: str, secret: str):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("AWS_") and key not in {"BOTO_CONFIG"}
    }
    for filename in ("empty-credentials", "empty-config"):
        (case.root / filename).touch(exist_ok=False)
    env.update(
        AWS_ACCESS_KEY_ID=access,
        AWS_SECRET_ACCESS_KEY=secret,
        A9_MINIO_ACCESS_KEY=access,
        A9_MINIO_SECRET_KEY=secret,
        AWS_REGION="us-east-1",
        AWS_DEFAULT_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true",
        AWS_SHARED_CREDENTIALS_FILE=str(case.root / "empty-credentials"),
        AWS_CONFIG_FILE=str(case.root / "empty-config"),
        BOTO_CONFIG=str(case.root / "empty-config"),
    )
    return env


def _phases(root):
    """Called only in the child with an isolated development credential chain."""
    import boto3
    import httpx
    from dataclasses import replace

    case = resolve_case_root("read", root)
    materialize_fixture(case, "minio_sigv4.json")
    access, secret = (
        os.environ["A9_MINIO_ACCESS_KEY"],
        os.environ["A9_MINIO_SECRET_KEY"],
    )
    bucket = "a9-sigv4-" + uuid.uuid4().hex
    key = "private/records.json"
    rows = [{"id": 1, "value": "signed-only"}, {"id": 2, "value": "Unicode Việt"}]
    s3 = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        region_name="us-east-1",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
    )
    s3.create_bucket(Bucket=bucket)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(rows).encode(),
        ContentType="application/json",
    )
    endpoint = f"/{bucket}/{key}"
    unsigned = httpx.get("http://127.0.0.1:9000" + endpoint)
    assert unsigned.status_code == 403 and "AccessDenied" in unsigned.text
    document = json.loads(
        case.metadata.read_text().replace("__PRIVATE_OBJECT__", endpoint)
    )
    authored_flow = document["dataflows"][0]["name"]
    runtime_flow = f"{authored_flow}-{case.root.name}"
    runtime_stage = f"minio_sigv4-{case.root.name}"
    document["dataflows"][0].update(name=runtime_flow, stage=runtime_stage)
    document["connections"][0]["configure"].pop("aws_session_token", None)
    case.metadata.write_text(json.dumps(document, indent=2))
    with driver_http_trace(endpoint) as positive_trace:
        run = run_driver(case, stage=runtime_stage)
    assert_successful_flow(run, runtime_flow)
    assert positive_trace and all(row["status"] == 200 for row in positive_trace), (
        positive_trace
    )
    engine = PolarsEngine()
    assert_output(engine, case.output / "records", rows)
    runs = [run]
    failures = []
    for iteration, mode in enumerate(("unsigned", "wrong-secret"), 2):
        negative = json.loads(case.metadata.read_text())
        target = case.output / mode
        negative["connections"][1]["configure"]["base_path"] = target.as_posix()
        original_secret = os.environ["A9_MINIO_SECRET_KEY"]
        if mode == "unsigned":
            negative["connections"][0]["configure"]["auth_type"] = "none"
        else:
            os.environ["A9_MINIO_SECRET_KEY"] = "invalid-development-signature"
        phase_path = case.root / f"metadata-{mode}.json"
        phase_path.write_text(json.dumps(negative, indent=2))
        try:
            with driver_http_trace(endpoint) as negative_trace:
                evidence = run_driver(
                    replace(case, metadata=phase_path),
                    stage=runtime_stage,
                    iteration=iteration,
                )
        finally:
            os.environ["A9_MINIO_SECRET_KEY"] = original_secret
        code = "AccessDenied" if mode == "unsigned" else "SignatureDoesNotMatch"
        assert_denial(evidence, negative_trace, code)
        from datacoolie.utils.identity import name_to_uuid

        failed_flows = [
            record
            for record in evidence.records
            if record.get("record_type") == "dataflow_run_log"
            and record.get("dataflow_id") == name_to_uuid(runtime_flow)
        ]
        assert len(failed_flows) == 1 and failed_flows[0].get("status") == "failed", (
            failed_flows
        )
        assert_denied_artifacts(target, case.watermarks)
        assert_output(engine, case.output / "records", rows)
        failures.append({"mode": mode, "trace": negative_trace})
        runs.append(evidence)
    assert_output(engine, case.output / "records", rows)
    payload = receipt_payload(
        case,
        runs=runs,
        script=Path(__file__),
        details={
            "bucket": bucket,
            "key": key,
            "unsigned_status": unsigned.status_code,
            "positive_driver_trace": positive_trace,
            "identity_mapping": {
                "authored_flow": authored_flow,
                "runtime_flow": runtime_flow,
                "runtime_stage": runtime_stage,
            },
            "negative_controls": failures,
            "rows": rows,
            "exact_schema": EXPECTED_SCHEMA,
            "credential_chain": "child development env only; empty config files; EC2 discovery disabled",
            "signer": "actual APIReader botocore SigV4; MinIO verifier",
        },
        qualification_gaps=["Real IAM, STS and API Gateway remain unqualified."],
    )
    payload["profile"]["qualification_scope"] = "host-polars-docker-minio-api-sigv4"
    return write_receipt(case, payload)


def validate(root=None):
    # Allocate a guard directory only for environment files; the child owns its data root.
    from types import SimpleNamespace
    from runner.runtime_paths import DATA_DIR

    env_root = DATA_DIR / "behavior_cases" / ("sigv4_env_" + uuid.uuid4().hex)
    env_root.mkdir(parents=True, exist_ok=False)
    env = _isolated_environment(
        SimpleNamespace(root=env_root),
        os.environ.get("DATACOOLIE_MINIO_DEV_ACCESS_KEY", "minioadmin"),
        os.environ.get("DATACOOLIE_MINIO_DEV_SECRET_KEY", "minioadmin"),
    )
    command = [sys.executable, "-B", str(Path(__file__).resolve()), "--isolated-child"]
    if root is not None:
        command += ["--root", str(root)]
    result = subprocess.run(command, env=env, check=False)
    if result.returncode:
        raise AssertionError(
            f"SigV4 isolated child failed with exit {result.returncode}; retain diagnostics"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--isolated-child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.isolated_child:
        print(_phases(args.root))
    else:
        validate(args.root)
