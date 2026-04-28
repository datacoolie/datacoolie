"""Concrete watermark manager that delegates storage to a metadata provider.

``WatermarkManager`` is the primary implementation used by the ETL Driver.
It serializes / deserializes watermarks and delegates persistence to the
injected :class:`~datacoolie.metadata.base.BaseMetadataProvider`.

For file-based standalone operation, ``FileProvider`` implements both
metadata and watermark storage, so ``WatermarkManager`` simply delegates
``save``/``get`` through the same provider.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.core.exceptions import WatermarkError
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.watermark.base import BaseWatermarkManager


class WatermarkManager(BaseWatermarkManager):
    """Watermark manager backed by a metadata provider.

    Args:
        metadata_provider: Provider responsible for watermark persistence.
    """

    def __init__(self, metadata_provider: BaseMetadataProvider) -> None:
        super().__init__()
        self._provider = metadata_provider

    def get_watermark(self, dataflow_id: str) -> Optional[Dict[str, Any]]:
        """Return the current watermark dict, or ``None``."""
        try:
            raw = self._provider.get_watermark(dataflow_id)
        except Exception as exc:
            raise WatermarkError(
                f"Failed to load watermark for dataflow {dataflow_id}"
            ) from exc

        if raw is None:
            return None

        result = self.deserialize(raw)
        return result if result else None

    def save_watermark(
        self,
        dataflow_id: str,
        watermark: Dict[str, Any],
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Serialize the watermark dict and persist via the provider."""
        try:
            serialised = self.serialize(watermark)
            self._provider.update_watermark(
                dataflow_id,
                serialised,
                job_id=job_id,
                dataflow_run_id=dataflow_run_id,
            )
        except WatermarkError:
            raise
        except Exception as exc:
            raise WatermarkError(
                f"Failed to save watermark for dataflow {dataflow_id}"
            ) from exc
