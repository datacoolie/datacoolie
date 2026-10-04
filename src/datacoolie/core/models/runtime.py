"""Shared timing and status foundation; component models live with their owners."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.utils.time import utc_now


@dataclass
class RuntimeInfo:
    start_time: datetime = field(default_factory=utc_now)
    end_time: Optional[datetime] = None
    status: str = DataFlowStatus.PENDING.value
    message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
