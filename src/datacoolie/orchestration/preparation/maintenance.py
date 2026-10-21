"""Operation-scoped maintenance selection, hydration and binding ownership."""
from __future__ import annotations

from dataclasses import dataclass

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress, PreparedMaintenanceTarget
from datacoolie.orchestration.execution.activation import inactive_reason


@dataclass(frozen=True, slots=True)
class MaintenanceItem:
    metadata: object
    execution: object = None
    target: PreparedMaintenanceTarget | None = None


def prepare_selection(dataflows, *, engine, prepare_dataflow, acquired):
    """Hydrate before addressing; dedupe native UUIDs before shard selection.

    acquired belongs to the caller's finally boundary, including failures.
    Bindings never enter DataFlow or generic deep-copied attempt kwargs.
    """
    candidates = {}
    winners = {}
    blocked = []
    prepare_target = engine.maintenance_preparation()
    for metadata in sorted(dataflows, key=lambda item: item.dataflow_id or ""):
        if inactive_reason(metadata) is not None:
            blocked.append(MaintenanceItem(metadata))
            continue
        execution = prepare_dataflow(metadata, operation_type="maintenance")
        address = resolve_destination_target(execution.execution.destination, engine=engine)
        destination = execution.execution.destination
        table_address = None
        if address.addressing == "table":
            table_address = MaintenanceTableAddress(destination.connection.catalog,
                tuple(part for part in (destination.connection.database, destination.schema_name) if part),
                destination.table)
        candidate = (address.format, address.table_name, address.path, table_address)
        target = candidates.get(candidate)
        if target is None:
            target = prepare_target(fmt=address.format,
                table_name=address.table_name, path=address.path, table_address=table_address)
            if not isinstance(target, PreparedMaintenanceTarget):
                raise ConfigurationError("Engine returned an invalid prepared maintenance target")
            candidates[candidate] = target
            acquired.append(target)
        # Missing has no physical identity and cannot hide existing tables.
        key = target.identity if not target.missing else ("missing", candidate)
        if not target.missing and not target.identity:
            raise ConfigurationError("Engine returned an unqualified maintenance identity")
        winners.setdefault(key, MaintenanceItem(metadata, execution, target))
    return blocked + list(winners.values())


def release_selection(engine, acquired):
    """Attempt every release while preserving an active failure/cancellation."""
    import sys
    active_error = sys.exc_info()[0] is not None
    first_error = None
    for target in reversed(acquired):
        try:
            engine.release_maintenance_target(target)
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None and not active_error:
        raise first_error
