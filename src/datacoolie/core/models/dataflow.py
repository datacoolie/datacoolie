"""Complete dataflow model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import (
    LoadType,
    ProcessingMode,
)
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.identity import is_usable_identifier, name_to_uuid

from datacoolie.core.models.base import CompatModel, _parse_json_object
from datacoolie.core.models.destination import Destination, PartitionColumn
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform


@dataclass(init=False)
class DataFlow(CompatModel):
    """Complete ETL pipeline configuration.

    Composes :class:`Source`, :class:`Destination`, and :class:`Transform`.
    """

    source: Source
    destination: Destination
    dataflow_id: Optional[str] = None
    workspace_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    stage: Optional[str] = None
    group_number: Optional[int] = None
    execution_order: Optional[int] = None
    processing_mode: str = ProcessingMode.BATCH.value
    is_active: bool = True
    transform: Transform = field(default_factory=Transform)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _derive_dataflow_id_from_name(cls, values: Any) -> Any:
        if isinstance(values, dict) and not is_usable_identifier(
            values.get("dataflow_id")
        ):
            name = values.get("name")
            if is_usable_identifier(name):
                values["dataflow_id"] = name_to_uuid(str(name))
        return values

    @classmethod
    def _normalise_mode(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        values = self._derive_dataflow_id_from_name(
            {"dataflow_id": self.dataflow_id, "name": self.name}
        )
        self.dataflow_id = values.get("dataflow_id")
        self.processing_mode = self._normalise_mode(self.processing_mode)
        self.configure = self._parse_configure(self.configure)
        self.validate()

    # -- validation ---------------------------------------------------------

    def validate(self) -> None:
        """Validate metadata configuration; raise on invalid combinations.

        Called automatically in ``__post_init__`` to fail fast at
        metadata load time.
        """
        if self.destination.replace_by_watermark:
            if not self.source.date_backward:
                raise ConfigurationError(
                    "replace_by_watermark requires date_backward on the source "
                    "to ensure the read window covers the delete scope",
                    details={"dataflow": self.name or self.dataflow_id},
                )
            if self.load_type != LoadType.MERGE_OVERWRITE.value:
                raise ConfigurationError(
                    f"replace_by_watermark is only supported with merge_overwrite load_type, "
                    f"got {self.load_type!r}",
                    details={"dataflow": self.name or self.dataflow_id},
                )

    # -- convenience proxies ------------------------------------------------

    @property
    def load_type(self) -> str:
        return self.destination.load_type

    @property
    def merge_keys(self) -> List[str]:
        return self.destination.merge_keys

    @property
    def partition_columns(self) -> List[PartitionColumn]:
        return self.destination.partition_columns

    @property
    def partition_column_names(self) -> List[str]:
        return self.destination.partition_column_names

    @property
    def deduplicate_columns(self) -> List[str]:
        return self.transform.deduplicate_column_names(self.merge_keys)

    @property
    def order_columns(self) -> List[str]:
        """Columns used to order rows during deduplication.

        Returns ``transform.latest_data_columns`` when set, otherwise
        falls back to ``source.watermark_columns``.
        """
        return self.transform.latest_data_columns or self.source.watermark_columns
