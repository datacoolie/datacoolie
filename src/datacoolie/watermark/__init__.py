"""DataCoolie watermark — incremental load state management."""

from datacoolie.watermark.base import (
    BaseWatermarkManager,
    WatermarkSerializer,
    deserialize_watermark,
    is_watermark_empty,
    serialize_watermark,
)
from datacoolie.watermark.watermark_manager import WatermarkManager

__all__ = [
    "BaseWatermarkManager",
    "WatermarkManager",
    "WatermarkSerializer",
    "deserialize_watermark",
    "is_watermark_empty",
    "serialize_watermark",
]
