"""Runtime information produced by destination writers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from datacoolie.core.models.runtime import RuntimeInfo


@dataclass
class DestinationRuntimeInfo(RuntimeInfo):
    """Runtime metrics for one destination write or maintenance phase.

    Metrics remain nullable because a successful destination operation may not
    expose a measurement.  Consumers can distinguish that case from an
    explicitly measured zero.  ``bytes_saved`` follows the same rule and is
    therefore unavailable until both byte measurements are known.
    """

    operation_type: Optional[str] = None
    rows_written: Optional[int] = None
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    files_added: Optional[int] = None
    files_removed: Optional[int] = None
    bytes_added: Optional[int] = None
    bytes_removed: Optional[int] = None
    operation_details: list[dict[str, Any]] = field(default_factory=list)

    @property
    def bytes_saved(self) -> Optional[int]:
        """Return non-negative byte savings when both measurements exist."""

        if self.bytes_added is None or self.bytes_removed is None:
            return None
        return max(0, self.bytes_removed - self.bytes_added)


__all__ = ["DestinationRuntimeInfo"]
