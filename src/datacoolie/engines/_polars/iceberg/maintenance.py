"""Bounded REST preparation over the caller's operative PyIceberg catalog."""
from __future__ import annotations

from importlib.metadata import version
from datetime import datetime, timedelta, timezone
from uuid import UUID

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.contracts.maintenance import PreparedMaintenanceTarget, native_identifier


def validate_deployment(catalog, deployment) -> None:
    from pyiceberg.catalog.rest import RestCatalog
    if type(catalog) is not RestCatalog or version("pyiceberg") != "0.12.0":
        raise ConfigurationError("REST maintenance requires qualified PyIceberg 0.12.0")
    if deployment is None:
        raise ConfigurationError("REST maintenance requires a verified deployment binding")
    deployment.validate_properties(catalog.properties)
    if catalog.uri not in deployment.routes or catalog.uri != catalog.properties.get("uri"):
        raise ConfigurationError("REST initialized operative route differs from verified properties")
    # A missing sentinel/auth/transport error propagates, never target-missing.
    sentinel = catalog.load_table(deployment.sentinel_identifier)
    if str(sentinel.metadata.table_uuid) != deployment.sentinel_uuid:
        raise ConfigurationError("REST deployment sentinel UUID changed")
    deployment.validate_location(sentinel.location())


def prepare_target(catalog, deployment, table_name: str, *, deployment_validated: bool = False,
                   table_address=None):
    from pyiceberg.exceptions import NoSuchTableError
    if deployment is None:
        raise ConfigurationError("REST maintenance requires a verified deployment binding")
    identifier = native_identifier(table_name, catalog_label=deployment.catalog_label,
                                   table_address=table_address)
    if not deployment_validated:
        validate_deployment(catalog, deployment)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        return PreparedMaintenanceTarget("iceberg", table_name, None, None, missing=True)
    native_uuid = table.metadata.table_uuid
    try:
        expected = str(UUID(str(native_uuid)))
    except (ValueError, TypeError) as exc:
        raise ConfigurationError("REST maintenance requires a native table UUID") from exc
    deployment.validate_location(table.location())
    return PreparedMaintenanceTarget("iceberg", table_name, None,
        f"{deployment.scope}:{expected}", expected_uuid=expected,
        capabilities=frozenset({"expire_snapshots"}), binding=table)


def validate_target(prepared):
    table = prepared.binding
    if prepared.missing or table is None:
        raise ConfigurationError("Missing prepared REST target cannot execute")
    # Native refresh checks UUID before assigning new metadata.
    table.refresh()
    if str(table.metadata.table_uuid) != prepared.expected_uuid:
        raise ConfigurationError("Prepared REST target UUID changed")
    return table


def execute_action(prepared, *, action: str, retention_hours: int):
    if action not in prepared.capabilities:
        raise ConfigurationError(f"Polars Iceberg maintenance does not support {action}")
    if type(retention_hours) is not int or retention_hours < 0:
        raise ConfigurationError("Maintenance retention must be nonnegative integer hours")
    table = validate_target(prepared)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    before = {snapshot.snapshot_id for snapshot in table.snapshots()}
    protected = {ref.snapshot_id for ref in table.refs().values()}
    if table.metadata.current_snapshot_id is not None:
        protected.add(table.metadata.current_snapshot_id)
    eligible = sorted(snapshot.snapshot_id for snapshot in table.snapshots()
                      if snapshot.timestamp_ms < cutoff_ms and snapshot.snapshot_id not in protected)
    if eligible:
        # Native transaction asserts UUID; do not catch native commit failures.
        table.maintenance.expire_snapshots().by_ids(eligible).commit()
        # PyIceberg assigns UUID-checked response metadata on acknowledged
        # commit. A later observation belongs to history diagnostics and must
        # not turn the acknowledged expiration into a retryable failure.
    after = {snapshot.snapshot_id for snapshot in table.snapshots()}
    expired = sorted(before - after)
    return {"action": action, "status": "succeeded",
            "reason": "Expired eligible snapshots" if expired else "No eligible unprotected snapshots",
            "metrics": {"snapshots_expired": len(expired)},
            "expired_snapshot_ids": expired, "cutoff_utc": cutoff.isoformat(),
            "file_reclamation": False}


def history(prepared, *, limit=5, start_time=None, end_time=None):
    table = validate_target(prepared)
    result = []
    for snapshot in sorted(table.snapshots(), key=lambda item: item.timestamp_ms, reverse=True):
        timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, timezone.utc)
        if (start_time is not None and timestamp < start_time) or (end_time is not None and timestamp > end_time):
            continue
        result.append({"snapshot_id": snapshot.snapshot_id, "timestamp": timestamp,
                       "operation": str(snapshot.summary.operation) if snapshot.summary else None})
    return result[:limit]
