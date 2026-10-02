"""Destination model and partition definition."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import (
    LoadType,
)
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.collections import ensure_list
from datacoolie.utils.path_utils import build_path

from datacoolie.core.models.base import CompatModel, _parse_json_object
from datacoolie.core.models.connection import Connection
from datacoolie.core.qualified_names import build_qualified_name


_MERGE_OPTION_KEYS = frozenset({"source_alias", "target_alias", "predicate"})


@dataclass(init=False)
class PartitionColumn(CompatModel):
    """Partition column definition.

    ``expression`` is an optional SQL expression used to derive the partition
    value (e.g. ``"year(event_date)"``).
    """

    column: str
    expression: Optional[str] = None

    @classmethod
    def _must_be_non_empty(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("column must be a non-empty string")
        return v

    def __post_init__(self) -> None:
        self.column = self._must_be_non_empty(self.column)


@dataclass(init=False)
class Destination(CompatModel):
    """Write-side pipeline configuration."""

    connection: Connection
    table: str
    schema_name: Optional[str] = None
    load_type: str = LoadType.APPEND.value
    merge_keys: List[str] = field(default_factory=list)
    partition_columns: List[PartitionColumn] = field(default_factory=list)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _normalise_schema_name(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str):
            stripped = v.strip()
            # Temporarily disabled: return stripped.lower() if stripped else None
            return stripped if stripped else None
        return v

    @classmethod
    def _normalise_table(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError("Destination.table must be a non-empty string")
        # Temporarily disabled: return v.strip().lower()
        return v.strip()

    @classmethod
    def _normalise_load_type(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @classmethod
    def _coerce_merge_keys(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_partition_columns(cls, v: Any) -> List[PartitionColumn]:
        if not v:
            return []
        result: list[PartitionColumn] = []
        items = v if isinstance(v, list) else [v]
        for item in items:
            if isinstance(item, dict):
                result.append(PartitionColumn(**item))
            elif isinstance(item, PartitionColumn):
                result.append(item)
            elif isinstance(item, str):
                result.append(PartitionColumn(column=item))
            else:
                result.append(item)
        return result

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return copy.deepcopy(_parse_json_object(v))

    @classmethod
    def _lift_partition_columns_from_configure(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        cfg = values.get("configure")
        if isinstance(cfg, dict) and not values.get("partition_columns"):
            pc = cfg.pop("partition_columns", None)
            if pc:
                values["partition_columns"] = pc
        return values

    def __post_init__(self) -> None:
        self.configure = self._parse_configure(self.configure)
        if (
            "partition_columns" not in self.model_fields_set
            and not self.partition_columns
        ):
            lifted = self._lift_partition_columns_from_configure(
                {
                    "configure": self.configure,
                    "partition_columns": self.partition_columns,
                }
            )
            if isinstance(lifted, dict):
                self.configure = lifted.get("configure", self.configure)
                self.partition_columns = lifted.get(
                    "partition_columns", self.partition_columns
                )
        self.schema_name = self._normalise_schema_name(self.schema_name)
        self.table = self._normalise_table(self.table)
        self.load_type = self._normalise_load_type(self.load_type)
        self.merge_keys = self._coerce_merge_keys(self.merge_keys)
        self.partition_columns = self._coerce_partition_columns(self.partition_columns)

    # -- computed properties ------------------------------------------------

    @property
    def full_table_name(self) -> str:
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            self.table,
        )

    @property
    def namespace(self) -> Optional[str]:
        """Namespace without the table: ``catalog.database.schema``."""
        return build_qualified_name(
            self.connection.catalog,
            self.connection.database,
            self.schema_name,
            None,
        )

    @property
    def path(self) -> Optional[str]:
        bp = self.connection.base_path
        if not bp or not self.table:
            return None
        return build_path(bp, self.schema_name, self.table)

    @property
    def write_options(self) -> Dict[str, Any]:
        """Merged write options: connection defaults + destination overrides."""
        opts = dict(self.connection.write_options)
        opts.update(self.configure.get("write_options", {}))
        return {
            key: value for key, value in opts.items() if key not in _MERGE_OPTION_KEYS
        }

    @property
    def merge_options(self) -> Dict[str, Any]:
        """Merged merge options with a compatibility bridge for old aliases."""
        write_opts = dict(self.connection.write_options)
        write_opts.update(self.configure.get("write_options", {}))
        opts = dict(self.connection.merge_options)
        opts.update(self.configure.get("merge_options", {}))
        for key in _MERGE_OPTION_KEYS:
            if key not in opts and key in write_opts:
                opts[key] = write_opts[key]
        return opts

    @property
    def partition_column_names(self) -> List[str]:
        return [pc.column for pc in self.partition_columns if pc.column]

    @property
    def merge_keys_extended(self) -> List[str]:
        """Return merge keys extended with partition columns."""
        keys = list(self.merge_keys)
        for col in self.partition_column_names:
            if col not in keys:
                keys.append(col)
        return keys

    @property
    def scd2_effective_column(self) -> Optional[str]:
        """SQL expression used as ``__valid_from`` for SCD2 loads.

        Read from ``destination.configure["scd2_effective_column"]``.
        Returns ``None`` when not set (non-SCD2 destinations).
        """
        return self.configure.get("scd2_effective_column") or None

    @property
    def replace_by_watermark(self) -> bool:
        """Whether merge_overwrite should use range-based window replace.

        When ``True``, the strategy deletes all target rows within the
        watermark window (watermark_effective → new_watermark) instead of
        doing key-based delete.  This handles source-side deletions.

        Requires ``date_backward`` on the source to ensure the read window
        covers the delete scope.
        """
        return bool(self.configure.get("replace_by_watermark", False))
