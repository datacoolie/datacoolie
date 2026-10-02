"""Small source plugin used by the replay range extension example.

The reader keeps ordinary incremental reads and bounded replay reads on the
same public source contract.  A bounded reader opts in explicitly and applies
the exact ``SourceReadRange`` to the returned frame before it observes a
watermark candidate.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.source import Source
from datacoolie.engines.base import DF
from datacoolie.sources.base import BaseSourceReader


class RangeExampleReader(BaseSourceReader[DF]):
    """Read configured records with ordinary and exact bounded filtering."""

    def _supports_read_range(self) -> bool:
        """Opt in to the source-owned ``SourceReadRange`` contract."""

        return True

    def _watermark_ordering_kinds(self, candidate: Dict[str, Any]) -> Dict[str, str]:
        """Authorize typed row maxima for the candidate merge boundary."""

        return self._typed_row_watermark_ordering_kinds(candidate)

    def _read_internal(
        self,
        source: Source,
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        frame = self._read_data(source)

        # ``BaseSourceReader.read`` stores SourceReadRange on the reader before
        # calling this hook.  Apply it to the actual frame; a capability flag
        # alone does not prove exact range support.
        read_range = self._get_read_range()
        if read_range is not None:
            frame = self._apply_read_range_filter(frame)
        elif watermark_start or watermark_end:
            frame = self._apply_watermark_filter(
                frame,
                source.watermark_columns,
                watermark_start or {},
                watermark_end,
            )

        frame = self._apply_filter_expression(frame, source)
        return self._finalize_read(
            frame,
            source.watermark_columns,
            type(self).__name__,
            source.table or "range example source",
        )

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        del configure
        records = source.configure.get("records", [])
        if not isinstance(records, list) or not all(
            isinstance(record, dict) for record in records
        ):
            raise ConfigurationError(
                "RangeExampleReader source.configure.records must be a list of objects"
            )
        return self._engine.create_dataframe(records)


__all__ = ["RangeExampleReader"]
