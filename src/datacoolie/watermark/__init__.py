"""DataCoolie watermark — incremental load state management."""

from typing import TYPE_CHECKING

from datacoolie.watermark.base import (
    BaseWatermarkManager,
    WatermarkSerializer,
    deserialize_watermark,
    is_watermark_empty,
    serialize_watermark,
)

if TYPE_CHECKING:
    from datacoolie.watermark.watermark_manager import WatermarkManager


def __getattr__(name: str):
    """Load the storage implementation only when its public export is requested."""
    if name == "WatermarkManager":
        from datacoolie.watermark.watermark_manager import WatermarkManager

        return WatermarkManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BaseWatermarkManager",
    "WatermarkManager",
    "WatermarkSerializer",
    "deserialize_watermark",
    "is_watermark_empty",
    "serialize_watermark",
]
