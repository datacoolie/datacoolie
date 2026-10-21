"""Shared helpers for orchestration-layer concerns.

Lives next to :mod:`driver`, :mod:`job_distributor`, :mod:`parallel_executor`,
and :mod:`datacoolie.utils.retry`. Hosts small, reusable utilities that coordinate
work across dataflows without pulling logic into the driver or model layer.
"""

from __future__ import annotations

from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from datacoolie.engines.base import BaseEngine

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.destinations.resolution.target import resolve_destination_target
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.orchestration.execution.activation import inactive_reason

logger = get_logger(__name__)


def dedupe_by_destination(dataflows: List[DataFlow], *, engine: BaseEngine | None = None) -> List[DataFlow]:
    """Return blocked dataflows and one runnable flow per physical destination.

    Multiple dataflows may target the same table or storage path
    (fan-in topology).  When the unit of work is the *destination*
    itself — e.g. running ``OPTIMIZE`` / ``VACUUM`` — executing the
    same work more than once would race or waste compute.  Input is
    sorted by ``dataflow_id`` first so "first-meet wins" is
    deterministic across runs regardless of metadata ordering.

    Engine-aware maintenance requires qualified physical equality and fails
    before dispatch for invalid or unknown targets. Engine-free calls retain
    the legacy inspection behavior, including warn-and-skip errors.
    """
    blocked: List[DataFlow] = []
    seen: Dict[str, DataFlow] = {}
    for df in sorted(dataflows, key=lambda d: d.dataflow_id or ""):
        try:
            if inactive_reason(df) is not None:
                blocked.append(df)
                continue
            key = resolve_destination_target(
                df.destination, engine=engine, require_identity=engine is not None
            ).identity
        except ConfigurationError as exc:
            if engine is not None:
                raise
            logger.warning(
                "Skipping dataflow %s: %s",
                df.dataflow_id,
                exc,
            )
            continue
        seen.setdefault(key, df)
    unique = blocked + list(seen.values())
    if len(unique) < len(dataflows):
        logger.debug(
            "Deduped %d dataflows into %d unique destinations",
            len(dataflows),
            len(unique),
        )
    return unique

