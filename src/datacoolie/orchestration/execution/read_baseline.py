"""Run-scoped read intent shared by whole-pipeline retries."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from datacoolie.utils.time import utc_now


@dataclass
class ReadBaseline:
    reference_time: datetime = field(default_factory=utc_now)
    loaded: bool = False
    watermark: dict[str, Any] | None = None

    def resolve(self, load: Callable[[], dict[str, Any] | None]) -> dict[str, Any] | None:
        """Cache only successful loads, including a successfully loaded None."""
        if not self.loaded:
            value = load()
            self.watermark = copy.deepcopy(value)
            self.loaded = True
        return copy.deepcopy(self.watermark)
