"""Runtime projection for one source read."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from datacoolie.core.models.runtime import RuntimeInfo

if TYPE_CHECKING:
    from datacoolie.sources.contracts import SourceReadSelection


@dataclass
class SourceRuntimeInfo(RuntimeInfo):
    """Detached source-phase observation."""

    rows_read: int | None = None
    source_action: dict[str, Any] = field(default_factory=dict)
    selection: SourceReadSelection | None = None
