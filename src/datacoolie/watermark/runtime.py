"""Per-attempt watermark runtime evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from datacoolie.watermark.contracts import (
    WatermarkCandidate,
    WatermarkLoadStatus,
    WatermarkSaveStatus,
)


@dataclass
class WatermarkRuntimeInfo:
    """Provider and source watermark evidence kept separate by phase."""

    load_status: WatermarkLoadStatus = WatermarkLoadStatus.NOT_REQUESTED
    loaded_watermark: dict[str, Any] | None = None
    merge_base: dict[str, Any] | None = None
    candidate: WatermarkCandidate | None = None
    watermark_to_save: dict[str, Any] | None = None
    save_status: WatermarkSaveStatus = WatermarkSaveStatus.NOT_REQUESTED
    saved_watermark: dict[str, Any] | None = None
