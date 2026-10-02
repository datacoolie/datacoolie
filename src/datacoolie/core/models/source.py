"""Source model."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datacoolie.utils.collections import ensure_list
from datacoolie.utils.path_utils import build_path

from datacoolie.core.models.base import CompatModel, _parse_json_object
from datacoolie.core.models.connection import Connection, parse_backward_config
from datacoolie.core.qualified_names import build_qualified_name


@dataclass(init=False)
class Source(CompatModel):
    """Read-side pipeline configuration."""

    connection: Connection
    schema_name: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    python_function: Optional[str] = None
    watermark_columns: List[str] = field(default_factory=list)
    filter_expression: Optional[str] = None
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _coerce_list(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return copy.deepcopy(_parse_json_object(v))

    def __post_init__(self) -> None:
        self.watermark_columns = self._coerce_list(self.watermark_columns)
        self.configure = self._parse_configure(self.configure)

    # -- computed properties ------------------------------------------------

    @property
    def full_table_name(self) -> Optional[str]:
        if not self.table:
            return None
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
    def read_options(self) -> Dict[str, Any]:
        """Merged read options: connection defaults + source overrides."""
        opts = dict(self.connection.read_options)
        opts.update(self.configure.get("read_options", {}))
        return opts

    @property
    def has_watermark_state(self) -> bool:
        """Whether ordinary execution needs persisted source state.

        A file source with date-folder discovery can emit the internal folder
        cursor even when the user did not author a row watermark column. That
        cursor must be loaded on the next run so discovery can prune folders
        older than the saved frontier.
        """

        return bool(self.watermark_columns) or bool(
            self.connection.date_folder_partitions
        )

    @property
    def date_backward(self) -> Optional[Dict[str, Any]]:
        """Backward look-back offset, source-level overrides connection-level.

        Reads from ``configure`` (same keys as
        :attr:`Connection.date_backward`).  If no source-level config
        is present, falls back to the connection's value.

        Example (YAML / source configure)::

            configure:
              backward_days: 7         # overrides connection setting
              # or
              backward: {months: 1}
              # or closing-day strategy
              backward: {closing_day: 10}
        """
        return parse_backward_config(self.configure) or self.connection.date_backward
