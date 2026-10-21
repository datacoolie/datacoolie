"""Validate focused REST Iceberg maintenance through normal Driver records.

The scenario path observes one or two normal ``maintenance.py`` child sessions,
then checks nested v6 action details against a fresh REST read.  Direct Driver
execution remains available for isolated troubleshooting.  Neither path uses
process exit status as an action oracle.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import hashlib
import json
import os
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
USECASE_SIM_DIR = SCRIPT.parents[3]
PRODUCT_ROOT = USECASE_SIM_DIR.parent
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))
if str(USECASE_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(USECASE_SIM_DIR))
if str(PRODUCT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PRODUCT_ROOT / "src"))

from runner.runtime_logs import job_sessions, records  # noqa: E402
from runner.runtime_paths import DATA_DIR, LOG_DIR  # noqa: E402
from _stateful_support import (  # noqa: E402
    CasePaths,
    DriverEvidence,
    receipt_payload,
    resolve_case_root,
    write_receipt,
)
from runner.metadata_sources import make_case_provider, recipe_document  # noqa: E402


MODES = (
    "polars_expiration",
    "spark_expiration",
    "polars_unsupported_compact",
    "polars_unsupported_orphan_files",
)
AWS_BUCKET = "datacoolie-test"
AWS_ENDPOINTS = frozenset(("http://localhost:9000", "http://minio:9000"))


def _resolve(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PRODUCT_ROOT / path
    return path.resolve()


def _catalog_uri(value: str | None, *, engine: str) -> str:
    if value:
        return value
    configured = os.getenv("DATACOOLIE_QUALIFICATION_ICEBERG_URI")
    if configured:
        return configured
    # Spark qualification normally runs in the pinned container.  Host
    # Polars uses the published REST port; the descriptor permits both aliases.
    if engine == "spark" and Path("/datacoolie").exists():
        return "http://iceberg-rest:8181"
    return "http://localhost:8181"


def _s3_endpoint(value: str | None) -> str:
    if value:
        return value
    configured = os.getenv("DATACOOLIE_QUALIFICATION_MINIO_ENDPOINT")
    if configured:
        return configured
    return "http://minio:9000" if Path("/datacoolie").exists() else "http://localhost:9000"


def _aws_client(endpoint: str):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


@contextlib.contextmanager
def _aws_log_root(
    *, bucket: str, prefix: str, endpoint: str
):
    """Download only one case-owned AWS log prefix into a disposable root."""

    normalized = prefix.replace("\\", "/").strip("/")
    if not normalized or any(part in ("", ".", "..") for part in normalized.split("/")):
        raise ValueError("AWS log prefix must be a non-empty relative case prefix")
    root_context = tempfile.TemporaryDirectory(prefix="iceberg_rest_aws_logs_")
    root = Path(root_context.name)
    try:
        client = _aws_client(endpoint)
        paginator = client.get_paginator("list_objects_v2")
        object_count = 0
        object_prefix = normalized.rstrip("/") + "/"
        for page in paginator.paginate(Bucket=bucket, Prefix=object_prefix):
            for item in page.get("Contents", []) or []:
                if not isinstance(item, Mapping):
                    continue
                key = str(item.get("Key", ""))
                if not key.startswith(object_prefix) or not key.endswith(".json"):
                    continue
                relative = key[len(object_prefix) :]
                destination = (root / relative).resolve()
                if not destination.is_relative_to(root.resolve()):
                    raise ValueError(f"AWS log key escapes temporary root: {key!r}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(destination))
                object_count += 1
        if object_count == 0:
            raise AssertionError(
                f"no JSON execution logs found below s3://{bucket}/{object_prefix}"
            )
        yield root
    finally:
        root_context.cleanup()


def _storage_options(endpoint: str) -> dict[str, str]:
    return {
        "aws_endpoint_url": endpoint,
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "aws_allow_http": "true",
    }


def _load_deployment(path: Path):
    from datacoolie.engines.contracts.maintenance import RestMaintenanceDeployment

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("maintenance deployment descriptor must be an object")
    required = {
        "deployment_identity",
        "routes",
        "warehouse",
        "sentinel_identifier",
        "sentinel_uuid",
        "catalog_label",
    }
    if set(payload) - {*required, "prefix"} or not required <= set(payload):
        raise ValueError("maintenance deployment descriptor has an unexpected shape")
    return RestMaintenanceDeployment(
        deployment_identity=tuple(payload["deployment_identity"]),
        routes=tuple(payload["routes"]),
        warehouse=str(payload["warehouse"]),
        sentinel_identifier=tuple(payload["sentinel_identifier"]),
        sentinel_uuid=str(payload["sentinel_uuid"]),
        catalog_label=str(payload["catalog_label"]),
        prefix=str(payload.get("prefix", "")),
    )


def _deployment_receipt_path(descriptor: Path) -> Path:
    return descriptor.with_name("deployment_receipt.json")


def _validate_deployment_receipt(descriptor: Path, deployment: Any) -> Path | None:
    """Bind an optional preparation receipt to the closed descriptor identity."""

    receipt_path = _deployment_receipt_path(descriptor)
    if not receipt_path.is_file():
        return None
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or payload.get("method") != "docker_inspect":
        raise ValueError("deployment receipt is not a Docker inspection attestation")
    if tuple(payload.get("identity", ())) != deployment.deployment_identity:
        raise ValueError("deployment receipt identity differs from descriptor")
    if tuple(payload.get("routes", ())) != deployment.routes:
        raise ValueError("deployment receipt routes differ from descriptor")
    if payload.get("warehouse") != deployment.warehouse:
        raise ValueError("deployment receipt warehouse differs from descriptor")
    containers = payload.get("containers")
    if not isinstance(containers, Mapping):
        raise ValueError("deployment receipt lacks inspected containers")
    for name in ("iceberg_rest", "postgres"):
        value = containers.get(name)
        if not isinstance(value, Mapping) or not str(value.get("id", "")).strip():
            raise ValueError(f"deployment receipt lacks {name} container identity")
    return receipt_path


def _materialize_fixture(case: CasePaths, namespace: str) -> None:
    document = recipe_document(
        "maintenance",
        "iceberg_rest_maintenance",
        tokens={
            "__CASE_ROOT__": case.root.as_posix(),
            "__ICEBERG_NAMESPACE__": namespace,
        },
    )
    if not isinstance(document, dict) or not document.get("connections"):
        raise ValueError("maintenance recipe is not a FileProvider document")
    case.document = document
    case.sources = (
        str((USECASE_SIM_DIR / "metadata" / "file" / "maintenance.json").resolve()),
    )


def _rest_catalog(uri: str, endpoint: str):
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "datacoolie",
        uri=uri,
        **{
            "s3.endpoint": endpoint,
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.region": os.getenv("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def _target_identifier(deployment) -> tuple[str, ...]:
    return (*deployment.sentinel_identifier[:-1], "Orders")


def _table_state(uri: str, endpoint: str, identifier: tuple[str, ...]) -> dict[str, Any]:
    table = _rest_catalog(uri, endpoint).load_table(identifier)
    snapshots = [str(item.snapshot_id) for item in table.snapshots()]
    refs = {
        str(name): str(ref.snapshot_id)
        for name, ref in table.refs().items()
    }
    rows = sorted(
        (dict(row) for row in table.scan().to_arrow().to_pylist()),
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )
    metadata_json = table.metadata.model_dump_json()
    metadata = json.loads(metadata_json)
    return {
        "metadata": metadata_json,
        "schema_signature": _schema_signature(metadata),
        "snapshot_ids": snapshots,
        "refs": refs,
        "rows": rows,
    }


def _schema_signature(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """Keep schema/partition semantics while ignoring changing snapshot fields."""

    return {
        key: metadata.get(key)
        for key in (
            "schemas",
            "current-schema-id",
            "partition-specs",
            "default-spec-id",
            "sort-orders",
            "default-sort-order-id",
            "properties",
        )
        if key in metadata
    }


def _expected_schema_signature(expected: Mapping[str, Any]) -> dict[str, Any] | None:
    value = expected.get("schema_signature")
    if isinstance(value, Mapping):
        return {str(key): value[key] for key in value}
    raw = expected.get("metadata")
    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return _schema_signature(parsed) if isinstance(parsed, Mapping) else None


def _assert_state_continuity(
    before: Mapping[str, Any], after: Mapping[str, Any], *, context: str
) -> None:
    for key in ("refs", "rows"):
        if before.get(key) != after.get(key):
            raise AssertionError(
                f"{context} changed target {key}: "
                f"before={before.get(key)!r}, after={after.get(key)!r}"
            )
    expected_schema = _expected_schema_signature(before)
    actual_schema = after.get("schema_signature")
    if expected_schema is not None and expected_schema != actual_schema:
        raise AssertionError(
            f"{context} changed table schema/partition semantics: "
            f"before={expected_schema!r}, after={actual_schema!r}"
        )


def _assert_sentinel(
    uri: str,
    endpoint: str,
    deployment: Any,
    expected: Mapping[str, Any],
) -> None:
    identifier = tuple(str(value) for value in deployment.sentinel_identifier)
    sentinel = _rest_catalog(uri, endpoint).load_table(identifier)
    expected_uuid = str(expected.get("sentinel_uuid") or deployment.sentinel_uuid)
    actual_uuid = str(sentinel.metadata.table_uuid)
    if actual_uuid != expected_uuid or actual_uuid != deployment.sentinel_uuid:
        raise AssertionError(
            f"REST continuity sentinel changed: expected={expected_uuid}, "
            f"descriptor={deployment.sentinel_uuid}, actual={actual_uuid}"
        )
    expected_rows = expected.get("sentinel_rows")
    expected_schema = expected.get("sentinel_schema_signature")
    expected_metadata = expected.get("sentinel_metadata")
    actual_metadata = sentinel.metadata.model_dump_json()
    if expected_metadata is not None and actual_metadata != expected_metadata:
        raise AssertionError(
            "REST continuity sentinel metadata changed: "
            f"expected={expected_metadata!r}, actual={actual_metadata!r}"
        )
    if expected_rows is not None or expected_schema is not None:
        state = _table_state(uri, endpoint, identifier)
        if expected_rows is not None and state.get("rows") != expected_rows:
            raise AssertionError(
                "REST continuity sentinel rows changed: "
                f"expected={expected_rows!r}, actual={state.get('rows')!r}"
            )
        if expected_schema is not None and state.get("schema_signature") != expected_schema:
            raise AssertionError(
                "REST continuity sentinel schema changed: "
                f"expected={expected_schema!r}, actual={state.get('schema_signature')!r}"
            )


def _plain(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {key: _plain(item) for key, item in dataclasses.asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _find_operation_details(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, Mapping):
        candidate = value.get("operation_details")
        if isinstance(candidate, list) and all(isinstance(item, dict) for item in candidate):
            return candidate
        for nested in value.values():
            found = _find_operation_details(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_operation_details(nested)
            if found is not None:
                return found
    return None


def _run_driver(
    case: CasePaths,
    *,
    engine_name: str,
    deployment: Any,
    manifest: Path | None,
    catalog_uri: str,
    endpoint: str,
    stage: str,
    do_compact: bool,
    do_cleanup: bool,
    compact_options: Mapping[str, bool] | None = None,
    cleanup_options: Mapping[str, bool] | None = None,
    retention_hours: int = 0,
) -> DriverEvidence:
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.orchestration.driver import DataCoolieDriver
    from datacoolie.platforms.local_platform import LocalPlatform

    platform = LocalPlatform()
    cleanup = None
    if engine_name == "polars":
        from runner._runner_utils import build_iceberg_rest_catalog

        storage = _storage_options(endpoint)
        catalog = build_iceberg_rest_catalog(
            catalog_preset="local",
            iceberg_catalog_uri=catalog_uri,
            storage_opts=storage,
        )
        engine = PolarsEngine(
            platform=platform,
            storage_options=storage,
            iceberg_catalog=catalog,
            maintenance_deployment=deployment,
        )
    else:
        from runner._runner_utils import build_spark_session
        from datacoolie.engines.spark_engine import SparkEngine
        from datacoolie.engines._spark.iceberg.native_build import load_manifest

        if manifest is None:
            raise ValueError("Spark REST qualification requires --maintenance-guard-manifest")
        manifest_value = load_manifest(manifest)
        # Keep the validator on the same caller-owned manifest/JAR boundary as
        # the maintenance runner.  A missing or stale manifest is a setup
        # blocker, never a reason to run an unguarded native action.
        helper_jar = manifest_value.manifest_path.with_name(
            manifest_value.helper_jar_name
        )
        spark = build_spark_session(
            app_name=f"T5-{stage}-{case.root.name}",
            catalog_preset="local",
            iceberg_catalog_uri=catalog_uri,
            needs_s3=True,
            needs_iceberg=True,
            extra_config={"spark.jars": str(helper_jar)},
            verify_local_file_checksums=False,
        )
        engine = SparkEngine(
            spark_session=spark,
            platform=platform,
            maintenance_deployment=deployment,
            maintenance_guard_manifest=manifest_value,
        )
        cleanup = spark.stop

    job_id = f"focused-{stage}-{case.root.name}"
    before_sessions = job_sessions(LOG_DIR, job_id)
    provider = make_case_provider(
        case,
        platform=platform,
        watermark_base_path=str(case.watermarks / "provider"),
    )
    result: Any = None
    try:
        with DataCoolieDriver(
            engine=engine,
            platform=platform,
            metadata_provider=provider,
            state_base_path=str(case.state),
            log_base_path=str(LOG_DIR),
            config=DataCoolieRunConfig(
                job_id=job_id,
                max_workers=1,
                retry_count=0,
                retry_delay=0,
                retention_hours=retention_hours,
            ),
        ) as driver:
            dataflows = driver.load_maintenance_dataflows(
                connection="maintenance_iceberg_dest"
            )
            result = driver.run_maintenance(
                dataflows=dataflows,
                do_compact=do_compact,
                do_cleanup=do_cleanup,
                compact_options=compact_options,
                cleanup_options=cleanup_options,
            )
    finally:
        if cleanup is not None:
            cleanup()

    sessions = job_sessions(LOG_DIR, job_id) - before_sessions
    if len(sessions) != 1:
        raise AssertionError(
            f"expected one current framework session for {job_id}, got {sessions}"
        )
    session_id = next(iter(sessions))
    observed: list[dict[str, Any]] = []
    for path, value in records(LOG_DIR, job_id, "execution_logs", session_id):
        observed.append(
            {
                "path": str(path.resolve().relative_to(LOG_DIR.resolve())).replace("\\", "/"),
                "record_type": value.get("_type"),
                "status": value.get("status"),
                "error": value.get("error"),
                "operation_details": _find_operation_details(value),
            }
        )
    result_value = _plain(result)
    if not isinstance(result_value, dict):
        raise AssertionError(f"unexpected maintenance result: {result_value!r}")
    return DriverEvidence(
        job_id=job_id,
        stage=stage,
        session_id=session_id,
        result=result_value,
        records=tuple(observed),
    )


def _load_expected_state(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("expected REST maintenance state must be an object")
    required = {"target", "snapshot_ids", "refs", "rows"}
    if not required <= set(payload):
        raise ValueError(
            "expected REST maintenance state is missing "
            + ", ".join(sorted(required - set(payload)))
        )
    if not isinstance(payload["target"], list) or not all(
        isinstance(value, str) for value in payload["target"]
    ):
        raise ValueError("expected REST maintenance target must be a string list")
    if not isinstance(payload["snapshot_ids"], list):
        raise ValueError("expected REST maintenance snapshots must be a list")
    if not isinstance(payload["refs"], dict) or not isinstance(payload["rows"], list):
        raise ValueError("expected REST maintenance refs/rows have invalid shapes")
    return payload


def _observed_evidence(
    job_id: str,
    session_id: str,
    *,
    stage: str,
    log_root: Path = LOG_DIR,
) -> DriverEvidence:
    """Read one normal maintenance child session from the v6 log streams."""

    job_values = list(
        records(log_root, job_id, "execution_logs/job_run_log", session_id)
    )
    if not job_values:
        raise AssertionError(
            f"no job snapshot for normal maintenance session {job_id}/{session_id}"
        )
    job = job_values[-1][1]
    observed: list[dict[str, Any]] = []
    for path, value in records(log_root, job_id, "execution_logs", session_id):
        try:
            relative = str(path.resolve().relative_to(log_root.resolve())).replace(
                "\\", "/"
            )
        except ValueError:
            relative = str(path)
        observed.append(
            {
                "path": relative,
                "record_type": value.get("_type"),
                "status": value.get("status"),
                "error": value.get("error"),
                "message": value.get("message"),
                "operation_details": _find_operation_details(value),
            }
        )
    return DriverEvidence(
        job_id=job_id,
        stage=stage,
        session_id=session_id,
        result={
            "total": int(job.get("total_dataflows") or 0),
            "succeeded": int(job.get("total_succeeded") or 0),
            "failed": int(job.get("total_failed") or 0),
            "skipped": int(job.get("total_skipped") or 0),
            "status": job.get("status"),
            "errors": {},
        },
        records=tuple(observed),
    )


def _all_execution_records(log_root: Path = LOG_DIR) -> list[tuple[Path, dict[str, Any]]]:
    result: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((log_root / "execution_logs").rglob("*.json")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                result.append((path, value))
    return result


def _observed_runs(
    job_id: str | None,
    *,
    target: tuple[str, ...],
    stage: str,
    log_root: Path = LOG_DIR,
) -> list[DriverEvidence]:
    if job_id:
        sessions = [(job_id, session_id) for session_id in job_sessions(log_root, job_id)]
    else:
        # Setup allocates a UUID namespace, so destination identity is a
        # collision-resistant correlation key.  This avoids the runner's
        # pre-clean of a stable job ID deleting prior qualification evidence.
        namespace, table_name = target[-2], target[-1]

        def target_record(value: Mapping[str, Any]) -> bool:
            if (
                value.get("destination_database") == namespace
                and value.get("destination_table") == table_name
            ):
                return True
            full_name = str(value.get("destination_full_table") or "")
            return full_name in {
                f"{namespace}.{table_name}",
                f"local_catalog.{namespace}.{table_name}",
                f"`{namespace}`.`{table_name}`",
                f"`local_catalog`.`{namespace}`.`{table_name}`",
            }

        sessions = sorted(
            {
                (str(value.get("job_id")), str(value.get("log_session_id")))
                for _, value in _all_execution_records(log_root)
                if value.get("_type") == "dataflow_run_log"
                and target_record(value)
                and isinstance(value.get("job_id"), str)
                and isinstance(value.get("log_session_id"), str)
            }
        )
    if not sessions:
        correlation = job_id or ".".join(target)
        raise AssertionError(f"normal maintenance produced no sessions for {correlation}")

    def sort_key(pair: tuple[str, str]) -> tuple[str, str, str]:
        current_job_id, session_id = pair
        values = list(
            records(log_root, current_job_id, "execution_logs/job_run_log", session_id)
        )
        start = str(values[-1][1].get("start_time", "")) if values else ""
        return start, current_job_id, session_id

    return [
        _observed_evidence(
            current_job_id,
            session_id,
            stage=f"{stage}_{index}",
            log_root=log_root,
        )
        for index, (current_job_id, session_id) in enumerate(
            sorted(sessions, key=sort_key), start=1
        )
    ]


def _details(evidence: DriverEvidence) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for record in evidence.records:
        value = record.get("operation_details")
        if isinstance(value, list):
            result.extend(item for item in value if isinstance(item, dict))
    return result


def _group(evidence: DriverEvidence, name: str) -> dict[str, Any]:
    groups = [item for item in _details(evidence) if item.get("operation") == name]
    if not groups:
        raise AssertionError(f"no persisted {name} operation detail: {evidence!r}")
    return groups[-1]


def _assert_nullable_file_metrics(group: Mapping[str, Any]) -> None:
    metrics = group.get("metrics")
    if not isinstance(metrics, Mapping):
        return
    for key in ("files_added", "files_removed", "bytes_added", "bytes_removed"):
        if key in metrics and metrics[key] is not None:
            raise AssertionError(f"expiration-only group fabricated {key}={metrics[key]!r}")


def _assert_expiration(
    evidence: DriverEvidence,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    require_effect: bool,
    engine_name: str,
) -> None:
    result = evidence.result
    if require_effect:
        if result.get("failed") != 0 or result.get("succeeded") != 1:
            raise AssertionError(f"expiration run did not succeed: {result!r}")
    else:
        if result.get("failed") != 0 or result.get("succeeded") != 1:
            raise AssertionError(f"expiration no-work run did not succeed: {result!r}")

    group = _group(evidence, "cleanup")
    if group.get("status") != "succeeded":
        raise AssertionError(f"cleanup group is not successful: {group!r}")
    actions = group.get("actions")
    if not isinstance(actions, list) or len(actions) != 1:
        raise AssertionError(f"expiration-only group selected unexpected actions: {group!r}")
    action = actions[0]
    if action.get("action") != "expire_snapshots" or action.get("status") != "succeeded":
        raise AssertionError(f"expiration action detail mismatch: {action!r}")
    metrics = action.get("metrics")
    changed = set(before["snapshot_ids"]) - set(after["snapshot_ids"])
    protected = set(before.get("refs", {}).values())
    if protected & changed:
        raise AssertionError(f"protected refs expired: {protected & changed!r}")
    eligible = set(before["snapshot_ids"]) - protected
    if engine_name == "polars":
        _assert_state_continuity(before, after, context="expiration")
        if not isinstance(metrics, Mapping) or type(metrics.get("snapshots_expired")) is not int:
            raise AssertionError(f"Polars expiration lacks snapshot effect metric: {action!r}")
        expired_ids = {str(value) for value in action.get("expired_snapshot_ids", [])}
        if require_effect:
            if (
                changed != eligible
                or metrics["snapshots_expired"] != len(eligible)
                or expired_ids != changed
            ):
                raise AssertionError(
                    f"Polars expiration effect mismatch: before={before!r}, after={after!r}, action={action!r}"
                )
        elif changed or metrics["snapshots_expired"] != 0 or expired_ids:
            raise AssertionError(f"Polars no-work expiration mutated snapshots: {action!r}")
        _assert_nullable_file_metrics(group)
        if action.get("file_reclamation") is not False:
            raise AssertionError(
                f"Polars expiration made a file-reclamation claim: {action!r}"
            )
    else:
        if not isinstance(metrics, Mapping) or type(metrics.get("files_removed")) is not int:
            raise AssertionError(f"Spark expiration lacks native file effect metric: {action!r}")
        callback_count = action.get("delete_callback_success_count")
        if type(callback_count) is not int:
            raise AssertionError(f"Spark expiration lacks callback effect count: {action!r}")
        if require_effect:
            if changed != eligible or callback_count <= 0 or metrics["files_removed"] != callback_count:
                raise AssertionError(
                    f"Spark expiration effect mismatch from fresh snapshot state: "
                    f"before={before!r}, after={after!r}, action={action!r}"
                )
        elif changed or callback_count != 0 or metrics["files_removed"] != 0:
            raise AssertionError(f"Spark no-work expiration mutated state or files: {action!r}")
        _assert_state_continuity(before, after, context="expiration")


def _assert_unsupported(
    evidence: DriverEvidence,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    action_name: str,
) -> None:
    result = evidence.result
    if result.get("failed", 0) != 1 or result.get("succeeded", 0) != 0:
        raise AssertionError(f"unsupported request was not a failed Driver result: {result!r}")
    for key in ("snapshot_ids", "refs", "rows"):
        if before.get(key) != after.get(key):
            raise AssertionError(
                f"unsupported maintenance changed target {key}: "
                f"before={before.get(key)!r}, after={after.get(key)!r}"
            )
    _assert_state_continuity(before, after, context="unsupported maintenance")
    details = _details(evidence)
    reasons: list[str] = []
    for value in (
        result.get("errors", {}).values()
        if isinstance(result.get("errors"), Mapping)
        else ()
    ):
        reasons.append(str(value))
    for record in evidence.records:
        if record.get("error"):
            reasons.append(str(record["error"]))
        if record.get("message"):
            reasons.append(str(record["message"]))
    for item in details:
        if item.get("reason"):
            reasons.append(str(item["reason"]))
        for action in item.get("actions", []):
            if isinstance(action, Mapping) and action.get("reason"):
                reasons.append(str(action["reason"]))
    if not any("unsupported" in reason.lower() or "support" in reason.lower() for reason in reasons):
        raise AssertionError(f"unsupported failure has no explicit reason: {evidence!r}")
    successful = [
        item
        for item in details
        if item.get("status") == "succeeded"
        and (
            item.get("operation") == action_name
            or any(
                isinstance(action, Mapping)
                and action.get("action") == action_name
                and action.get("status") == "succeeded"
                for action in item.get("actions", [])
            )
        )
    ]
    if successful:
        raise AssertionError(f"unsupported action was reported successful: {successful!r}")


def _receipt(
    case: CasePaths,
    *,
    mode: str,
    engine: str,
    deployment: Any,
    descriptor: Path,
    manifest: Path | None,
    runs: Sequence[DriverEvidence],
    details: Mapping[str, Any],
    status: str,
    actual_exit_code: int,
    platform: str = "local",
    aws_log_prefix: str | None = None,
    deployment_receipt: Path | None = None,
    gaps: Sequence[str] = (),
) -> Path:
    payload = receipt_payload(
        case,
        runs=runs,
        details=details,
        script=SCRIPT,
        status=status,
        actual_exit_code=actual_exit_code,
        qualification_gaps=gaps,
        references=(
            "usecase-sim/metadata/file/maintenance.json",
            "usecase-sim/scenarios/metadata_recipes.json#maintenance/iceberg_rest_maintenance",
            "src/datacoolie/engines/contracts/maintenance.py",
        ),
    )
    payload["case"] = mode
    payload["profile"] = {
        "engine": engine,
        "platform": platform,
        "destination_format": "iceberg",
        "qualification_scope": "isolated-rest-native",
    }
    payload["details"] = {
        **dict(payload.get("details", {})),
        "deployment_scope": deployment.scope,
        "deployment_identity_attested": True,
        "descriptor_sha256": hashlib.sha256(descriptor.read_bytes()).hexdigest(),
        "descriptor_path": descriptor.name,
        "deployment_receipt_path": (
            deployment_receipt.name if deployment_receipt is not None else None
        ),
        "aws_log_prefix": aws_log_prefix,
        "guard_manifest_sha256": (
            hashlib.sha256(manifest.read_bytes()).hexdigest()
            if manifest is not None and manifest.is_file()
            else None
        ),
    }
    return write_receipt(case, payload)


def _validate_observed(
    args: argparse.Namespace,
    *,
    engine_name: str,
    deployment: Any,
    descriptor: Path,
    manifest: Path | None,
    catalog_uri: str,
    endpoint: str,
) -> int:
    if args.platform == "aws":
        if not args.aws_log_prefix:
            raise ValueError("AWS observation requires --aws-log-prefix")
        manager = _aws_log_root(
            bucket=args.aws_bucket,
            prefix=args.aws_log_prefix,
            endpoint=endpoint,
        )
    else:
        manager = contextlib.nullcontext(LOG_DIR)
    with manager as log_root:
        return _validate_observed_records(
            args,
            engine_name=engine_name,
            deployment=deployment,
            descriptor=descriptor,
            manifest=manifest,
            catalog_uri=catalog_uri,
            endpoint=endpoint,
            log_root=log_root,
        )


def _capture_path(value: str) -> Path:
    path = _resolve(value)
    if path == DATA_DIR.resolve() or not path.is_relative_to(DATA_DIR.resolve()):
        raise ValueError("--capture-state must stay below usecase-sim/.runtime/data")
    return path


def _capture_observed_state(
    args: argparse.Namespace,
    *,
    deployment: Any,
    catalog_uri: str,
    endpoint: str,
) -> int:
    if not args.expected_state:
        raise ValueError("state capture requires --expected-state")
    expected_path = _resolve(args.expected_state)
    expected = _load_expected_state(expected_path)
    identifier = tuple(str(value) for value in expected["target"])
    if identifier != _target_identifier(deployment):
        raise AssertionError(
            f"expected target {identifier!r} does not match deployment sentinel "
            f"{_target_identifier(deployment)!r}"
        )
    state = _table_state(catalog_uri, endpoint, identifier)
    _assert_sentinel(catalog_uri, endpoint, deployment, expected)
    destination = _capture_path(args.capture_state)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": args.mode,
        "platform": args.platform,
        "target": list(identifier),
        "state": state,
        "sentinel": {
            "uuid": deployment.sentinel_uuid,
            "metadata": _table_state(
                catalog_uri, endpoint, tuple(deployment.sentinel_identifier)
            )["metadata"],
            "rows": expected.get("sentinel_rows"),
            "schema_signature": expected.get("sentinel_schema_signature"),
        },
        "sentinel_uuid": deployment.sentinel_uuid,
    }
    with destination.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    print("ICEBERG_REST_MAINTENANCE_CAPTURE=" + json.dumps({
        "capture_state": str(destination),
        "target": list(identifier),
    }, sort_keys=True))
    return 0


def _load_capture(value: str) -> dict[str, Any]:
    path = _capture_path(value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or not isinstance(payload.get("state"), Mapping):
        raise ValueError(f"invalid maintenance state capture: {path}")
    return dict(payload)


def _assert_capture_pair(
    effect_capture: Mapping[str, Any],
    no_work_capture: Mapping[str, Any],
    final: Mapping[str, Any],
    *,
    expected_target: Sequence[str],
    expected_sentinel_uuid: str,
    expected_sentinel_metadata: str | None = None,
    expected_sentinel_rows: Sequence[Any] | None = None,
    expected_sentinel_schema: Mapping[str, Any] | None = None,
) -> None:
    for capture in (effect_capture, no_work_capture):
        if tuple(capture.get("target", ())) != tuple(expected_target):
            raise AssertionError("state capture target differs from expected target")
        if capture.get("sentinel_uuid") != expected_sentinel_uuid:
            raise AssertionError("state capture is not bound to the descriptor sentinel")
        sentinel = capture.get("sentinel")
        if not isinstance(sentinel, Mapping):
            raise AssertionError("state capture lacks complete continuity sentinel state")
        if sentinel.get("uuid") != expected_sentinel_uuid:
            raise AssertionError("state capture sentinel metadata is not descriptor-bound")
        if (
            expected_sentinel_metadata is not None
            and sentinel.get("metadata") != expected_sentinel_metadata
        ):
            raise AssertionError("state capture sentinel metadata differs from setup")
        if expected_sentinel_rows is not None and sentinel.get("rows") != expected_sentinel_rows:
            raise AssertionError("state capture sentinel rows differ from setup")
        if (
            expected_sentinel_schema is not None
            and sentinel.get("schema_signature") != expected_sentinel_schema
        ):
            raise AssertionError("state capture sentinel schema differs from setup")
    if effect_capture.get("sentinel_uuid") != no_work_capture.get("sentinel_uuid"):
        raise AssertionError("effect/no-work captures changed the continuity sentinel")
    effect_sentinel = effect_capture["sentinel"]
    no_work_sentinel = no_work_capture["sentinel"]
    if effect_sentinel != no_work_sentinel:
        raise AssertionError("no-work invocation changed captured sentinel metadata/state")
    effect = effect_capture["state"]
    no_work = no_work_capture["state"]
    if not isinstance(effect, Mapping) or not isinstance(no_work, Mapping):
        raise AssertionError("effect/no-work captures have invalid state mappings")
    for key in ("metadata", "snapshot_ids", "refs", "rows", "schema_signature"):
        if effect.get(key) != no_work.get(key):
            raise AssertionError(
                f"no-work invocation changed captured {key}: "
                f"effect={effect.get(key)!r}, no_work={no_work.get(key)!r}"
            )
        if final.get(key) != no_work.get(key):
            raise AssertionError(
                f"final REST state differs from no-work capture for {key}: "
                f"final={final.get(key)!r}, capture={no_work.get(key)!r}"
            )
def _validate_observed_records(
    args: argparse.Namespace,
    *,
    engine_name: str,
    deployment: Any,
    descriptor: Path,
    manifest: Path | None,
    catalog_uri: str,
    endpoint: str,
    log_root: Path,
) -> int:
    deployment_receipt = _validate_deployment_receipt(descriptor, deployment)
    if not args.expected_state:
        raise ValueError("normal runner validation requires --expected-state")
    expected_path = _resolve(args.expected_state)
    if not expected_path.is_file():
        raise FileNotFoundError(f"expected REST state not found: {expected_path}")
    expected = _load_expected_state(expected_path)
    identifier = tuple(str(value) for value in expected["target"])
    if identifier != _target_identifier(deployment):
        raise AssertionError(
            f"expected target {identifier!r} does not match deployment sentinel "
            f"{_target_identifier(deployment)!r}"
        )
    case = resolve_case_root("maintenance")
    runs = _observed_runs(
        args.job_id,
        target=identifier,
        stage="iceberg_rest_maintenance",
        log_root=log_root,
    )
    after = _table_state(catalog_uri, endpoint, identifier)
    _assert_sentinel(catalog_uri, endpoint, deployment, expected)
    if bool(args.effect_state) != bool(args.no_work_state):
        raise ValueError("--effect-state and --no-work-state must be supplied together")
    if args.effect_state:
        _assert_capture_pair(
            _load_capture(args.effect_state),
            _load_capture(args.no_work_state),
            after,
            expected_target=identifier,
            expected_sentinel_uuid=deployment.sentinel_uuid,
            expected_sentinel_metadata=expected.get("sentinel_metadata"),
            expected_sentinel_rows=expected.get("sentinel_rows"),
            expected_sentinel_schema=expected.get("sentinel_schema_signature"),
        )
    details: dict[str, Any] = {
        "mode": args.mode,
        "target": list(identifier),
        "observed_job_id": args.job_id,
        "expected_state_path": expected_path.name,
        "observed_sessions": [run.session_id for run in runs],
        "state_captures": {
            "effect": str(_capture_path(args.effect_state))
            if args.effect_state
            else None,
            "no_work": str(_capture_path(args.no_work_state))
            if args.no_work_state
            else None,
        },
    }
    for label, value in (("effect", args.effect_state), ("no_work", args.no_work_state)):
        if value:
            capture_path = _capture_path(value)
            details["state_captures"][f"{label}_sha256"] = hashlib.sha256(
                capture_path.read_bytes()
            ).hexdigest()
    if args.mode.endswith("expiration"):
        if len(runs) != 2:
            raise AssertionError(
                "expiration qualification requires exactly two normal child "
                f"sessions (effect + no-work), got {len(runs)}"
            )
        effect_runs = []
        noop_runs = []
        for run in runs:
            actions = [
                action
                for group in _details(run)
                if isinstance(group.get("actions"), list)
                for action in group["actions"]
                if isinstance(action, Mapping)
                and action.get("action") == "expire_snapshots"
            ]
            if engine_name == "polars":
                has_effect = any(
                    isinstance(action.get("metrics"), Mapping)
                    and action["metrics"].get("snapshots_expired", 0) > 0
                    for action in actions
                )
                has_noop = any(
                    isinstance(action.get("metrics"), Mapping)
                    and action["metrics"].get("snapshots_expired") == 0
                    for action in actions
                )
            else:
                has_effect = any(
                    type(action.get("delete_callback_success_count")) is int
                    and action["delete_callback_success_count"] > 0
                    for action in actions
                )
                has_noop = any(
                    type(action.get("delete_callback_success_count")) is int
                    and action["delete_callback_success_count"] == 0
                    for action in actions
                )
            if has_effect:
                effect_runs.append(run)
            elif has_noop:
                noop_runs.append(run)
        if len(effect_runs) != 1 or len(noop_runs) != 1:
            raise AssertionError(
                "normal expiration sessions did not separate one positive "
                f"effect and one known no-work action: {runs!r}"
            )
        effect = effect_runs[0]
        noop = noop_runs[0]
        _assert_expiration(
            effect,
            expected,
            after,
            require_effect=True,
            engine_name=engine_name,
        )
        # The second child ran after the first child committed.  The final
        # REST read must match the first action's explicit effect state too.
        effect_actions = [
            action
            for group in _details(effect)
            if isinstance(group.get("actions"), list)
            for action in group["actions"]
            if isinstance(action, Mapping)
            and action.get("action") == "expire_snapshots"
        ]
        effect_expired = {
            str(value)
            for action in effect_actions
            for value in action.get("expired_snapshot_ids", [])
        }
        noop_before = dict(expected)
        if effect_expired:
            noop_before["snapshot_ids"] = [
                value
                for value in expected["snapshot_ids"]
                if str(value) not in effect_expired
            ]
        else:
            noop_before["snapshot_ids"] = list(after["snapshot_ids"])
        _assert_expiration(
            noop,
            noop_before,
            after,
            require_effect=False,
            engine_name=engine_name,
        )
        _assert_sentinel(catalog_uri, endpoint, deployment, expected)
        details.update({"before": expected, "after": after})
    else:
        if len(runs) != 1:
            raise AssertionError(
                "unsupported qualification requires one normal child session, "
                f"got {len(runs)}"
            )
        if args.mode.endswith("compact"):
            unsupported_action = "rewrite_data_files"
        else:
            unsupported_action = "remove_orphan_files"
        _assert_unsupported(runs[0], expected, after, unsupported_action)
        details.update(
            {"before": expected, "after": after, "unsupported_action": unsupported_action}
        )

    receipt = _receipt(
        case,
        mode=args.mode,
        engine=engine_name,
        deployment=deployment,
        descriptor=descriptor,
        manifest=manifest,
        runs=runs,
        details=details,
        status="passed",
        actual_exit_code=0,
        platform=args.platform,
        aws_log_prefix=getattr(args, "aws_log_prefix", None),
        deployment_receipt=deployment_receipt,
    )
    print(
        "ICEBERG_REST_MAINTENANCE="
        + json.dumps(
            {
                "mode": args.mode,
                "receipt": str(receipt),
                "target": list(identifier),
                "runs": len(runs),
            },
            sort_keys=True,
        )
    )
    return 0


def validate(args: argparse.Namespace) -> int:
    engine_name = "spark" if args.mode.startswith("spark") else "polars"
    descriptor = _resolve(args.maintenance_deployment)
    if not descriptor.is_file():
        raise FileNotFoundError(f"deployment descriptor not found: {descriptor}")
    deployment = _load_deployment(descriptor)
    deployment_receipt = _validate_deployment_receipt(descriptor, deployment)
    manifest = (
        _resolve(args.maintenance_guard_manifest)
        if args.maintenance_guard_manifest
        else None
    )
    if engine_name == "spark" and (manifest is None or not manifest.is_file()):
        raise FileNotFoundError(f"native guard manifest not found: {manifest}")
    catalog_uri = _catalog_uri(args.catalog_uri, engine=engine_name)
    endpoint = _s3_endpoint(args.s3_endpoint)
    if args.platform == "aws":
        if args.aws_bucket != AWS_BUCKET:
            raise ValueError(f"AWS qualification requires bucket {AWS_BUCKET!r}")
        if endpoint not in AWS_ENDPOINTS:
            raise ValueError(
                "AWS qualification accepts only the checked-in MinIO endpoints"
            )
    if args.platform == "aws" and not args.observe_only:
        raise ValueError("AWS qualification must observe the normal maintenance runner")
    if getattr(args, "capture_only", False):
        return _capture_observed_state(
            args,
            deployment=deployment,
            catalog_uri=catalog_uri,
            endpoint=endpoint,
        )
    if args.observe_only:
        return _validate_observed(
            args,
            engine_name=engine_name,
            deployment=deployment,
            descriptor=descriptor,
            manifest=manifest,
            catalog_uri=catalog_uri,
            endpoint=endpoint,
        )
    identifier = _target_identifier(deployment)
    namespace = identifier[:-1]
    case = resolve_case_root("maintenance")
    _materialize_fixture(case, namespace[0] if len(namespace) == 1 else ".".join(namespace))
    runs: list[DriverEvidence] = []
    details: dict[str, Any] = {"mode": args.mode, "target": list(identifier)}
    try:
        before = _table_state(catalog_uri, endpoint, identifier)
        _assert_sentinel(catalog_uri, endpoint, deployment, {"sentinel_uuid": deployment.sentinel_uuid})
        if args.mode.endswith("expiration"):
            options = {"expire_snapshots": True, "remove_orphan_files": False}
            first = _run_driver(
                case,
                engine_name=engine_name,
                deployment=deployment,
                manifest=manifest,
                catalog_uri=catalog_uri,
                endpoint=endpoint,
                stage="iceberg_rest_maintenance",
                do_compact=False,
                do_cleanup=True,
                cleanup_options=options,
                retention_hours=args.retention_hours,
            )
            runs.append(first)
            after = _table_state(catalog_uri, endpoint, identifier)
            _assert_sentinel(catalog_uri, endpoint, deployment, {"sentinel_uuid": deployment.sentinel_uuid})
            _assert_expiration(first, before, after, require_effect=True, engine_name=engine_name)

            # A second selected expiration proves the no-work contract after
            # the eligible snapshot is gone; it must succeed with a reason and
            # known zero effects rather than invoke an empty server commit.
            noop_before = after
            noop = _run_driver(
                case,
                engine_name=engine_name,
                deployment=deployment,
                manifest=manifest,
                catalog_uri=catalog_uri,
                endpoint=endpoint,
                stage="iceberg_rest_maintenance_noop",
                do_compact=False,
                do_cleanup=True,
                cleanup_options=options,
                retention_hours=args.retention_hours,
            )
            runs.append(noop)
            noop_after = _table_state(catalog_uri, endpoint, identifier)
            _assert_sentinel(catalog_uri, endpoint, deployment, {"sentinel_uuid": deployment.sentinel_uuid})
            _assert_expiration(noop, noop_before, noop_after, require_effect=False, engine_name=engine_name)
            details.update({"before": before, "after": after, "noop_after": noop_after})
        else:
            if args.mode.endswith("compact"):
                compact_options = {
                    "rewrite_data_files": True,
                    "rewrite_position_delete_files": True,
                    "rewrite_manifests": True,
                }
                cleanup_options = None
                do_compact, do_cleanup = True, False
                unsupported_action = "rewrite_data_files"
            else:
                compact_options = None
                cleanup_options = {
                    "expire_snapshots": False,
                    "remove_orphan_files": True,
                }
                do_compact, do_cleanup = False, True
                unsupported_action = "remove_orphan_files"
            evidence = _run_driver(
                case,
                engine_name=engine_name,
                deployment=deployment,
                manifest=manifest,
                catalog_uri=catalog_uri,
                endpoint=endpoint,
                stage="iceberg_rest_maintenance_unsupported",
                do_compact=do_compact,
                do_cleanup=do_cleanup,
                compact_options=compact_options,
                cleanup_options=cleanup_options,
                retention_hours=args.retention_hours,
            )
            runs.append(evidence)
            after = _table_state(catalog_uri, endpoint, identifier)
            _assert_sentinel(catalog_uri, endpoint, deployment, {"sentinel_uuid": deployment.sentinel_uuid})
            _assert_unsupported(evidence, before, after, unsupported_action)
            details.update({"before": before, "after": after, "unsupported_action": unsupported_action})

        receipt = _receipt(
            case,
            mode=args.mode,
            engine=engine_name,
            deployment=deployment,
            descriptor=descriptor,
            manifest=manifest,
            runs=runs,
            details=details,
            status="passed",
            actual_exit_code=0,
            platform=args.platform,
            aws_log_prefix=getattr(args, "aws_log_prefix", None),
            deployment_receipt=deployment_receipt,
        )
        print("ICEBERG_REST_MAINTENANCE=" + json.dumps({
            "mode": args.mode,
            "receipt": str(receipt),
            "target": list(identifier),
            "runs": len(runs),
        }, sort_keys=True))
        return 0
    except Exception as exc:
        try:
            receipt = _receipt(
                case,
                mode=args.mode,
                engine=engine_name,
                deployment=deployment,
                descriptor=descriptor,
                manifest=manifest,
                runs=runs,
                details={**details, "error": str(exc)},
                status="blocked",
                actual_exit_code=1,
                platform=args.platform,
                aws_log_prefix=getattr(args, "aws_log_prefix", None),
                deployment_receipt=deployment_receipt,
                gaps=(str(exc),),
            )
            print(f"Blocked receipt: {receipt}", file=sys.stderr)
        finally:
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--platform", choices=("local", "aws"), default="local")
    parser.add_argument("--maintenance-deployment", required=True)
    parser.add_argument("--maintenance-guard-manifest")
    parser.add_argument("--catalog-uri")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--retention-hours", type=int, default=0)
    parser.add_argument(
        "--observe-only",
        action="store_true",
        help="Observe a normal maintenance.py child run instead of launching a Driver",
    )
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="Capture current REST target state without inspecting framework logs",
    )
    parser.add_argument("--expected-state")
    parser.add_argument("--job-id")
    parser.add_argument(
        "--capture-state",
        help="Case-owned .runtime/data JSON path for a read-only state capture",
    )
    parser.add_argument("--effect-state")
    parser.add_argument("--no-work-state")
    parser.add_argument("--aws-bucket", default="datacoolie-test")
    parser.add_argument(
        "--aws-log-prefix",
        help="Case-owned log prefix below the AWS bucket, without s3://",
    )
    # The scenario runner may append this for ordinary validators.  This
    # standalone validator creates and checks its own current sessions.
    parser.add_argument("--log-session-id")
    args = parser.parse_args()
    return validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
