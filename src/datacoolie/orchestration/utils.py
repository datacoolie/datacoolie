"""Shared helpers for orchestration-layer concerns.

Lives next to :mod:`driver`, :mod:`job_distributor`, :mod:`parallel_executor`,
and :mod:`retry_handler`.  Hosts small, reusable utilities that coordinate
work across dataflows without pulling logic into the driver or model layer.
"""

from __future__ import annotations

from typing import Dict, List

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import DataFlow
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


def dedupe_by_destination(dataflows: List[DataFlow]) -> List[DataFlow]:
    """Return one dataflow per physical destination.

    Multiple dataflows may target the same table or storage path
    (fan-in topology).  When the unit of work is the *destination*
    itself — e.g. running ``OPTIMIZE`` / ``VACUUM`` — executing the
    same work more than once would race or waste compute.  Input is
    sorted by ``dataflow_id`` first so "first-meet wins" is
    deterministic across runs regardless of metadata ordering.

    Dataflows whose :attr:`~datacoolie.core.models.Destination.destination_key`
    cannot be computed are skipped with a warning rather than aborting
    the whole run.
    """
    seen: Dict[str, DataFlow] = {}
    for df in sorted(dataflows, key=lambda d: d.dataflow_id or ""):
        try:
            key = df.destination.destination_key
        except ConfigurationError as exc:
            logger.warning(
                "Skipping dataflow %s: %s",
                df.dataflow_id,
                exc,
            )
            continue
        seen.setdefault(key, df)
    unique = list(seen.values())
    if len(unique) < len(dataflows):
        logger.info(
            "Deduped %d dataflows into %d unique destinations",
            len(dataflows),
            len(unique),
        )
    return unique
