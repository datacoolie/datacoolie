"""Shared test infrastructure for watermark module."""

from __future__ import annotations

from typing import Any, Dict, Optional

from datacoolie.metadata.base import BaseMetadataProvider


# ============================================================================
# Stub Metadata Providers
# ============================================================================


class StubMetadataProvider(BaseMetadataProvider):
    """Minimal provider that stores watermarks in a dict."""

    def __init__(self) -> None:
        super().__init__(enable_cache=False)
        self.watermarks: Dict[str, Any] = {}

    def _fetch_connections(self, *, active_only=True):
        return []

    def _fetch_connection_by_id(self, connection_id):
        return None

    def _fetch_connection_by_name(self, name):
        return None

    def _fetch_dataflows(self, *, stages=None, active_only=True):
        return []

    def _fetch_dataflow_by_id(self, dataflow_id):
        return None

    def _fetch_schema_hints(self, connection_id, table_name, schema_name=None):
        return []

    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        return self.watermarks.get(dataflow_id)

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        self.watermarks[dataflow_id] = watermark_value


class ThreadSafeMetadataProvider(BaseMetadataProvider):
    """Thread-safe metadata provider for concurrent watermark testing."""

    def __init__(self) -> None:
        super().__init__(enable_cache=False)
        self.watermarks: Dict[str, str] = {}
        self._update_count = 0

    def _fetch_connections(self, *, active_only=True):
        return []

    def _fetch_connection_by_id(self, connection_id):
        return None

    def _fetch_connection_by_name(self, name):
        return None

    def _fetch_dataflows(self, *, stages=None, active_only=True):
        return []

    def _fetch_dataflow_by_id(self, dataflow_id):
        return None

    def _fetch_schema_hints(self, connection_id, table_name, schema_name=None):
        return []

    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        return self.watermarks.get(dataflow_id)

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        self.watermarks[dataflow_id] = watermark_value
        self._update_count += 1
