"""Tests for destination-owned runtime models."""

from __future__ import annotations

from datacoolie.core.models.runtime import RuntimeInfo
from datacoolie.destinations.runtime import DestinationRuntimeInfo


def test_destination_runtime_is_a_shared_runtime_info() -> None:
    assert issubclass(DestinationRuntimeInfo, RuntimeInfo)


def test_destination_metrics_default_to_unknown() -> None:
    info = DestinationRuntimeInfo()

    assert info.rows_written is None
    assert info.rows_inserted is None
    assert info.rows_updated is None
    assert info.rows_deleted is None
    assert info.files_added is None
    assert info.files_removed is None
    assert info.bytes_added is None
    assert info.bytes_removed is None
    assert info.bytes_saved is None


def test_bytes_saved_is_nullable_and_clamped() -> None:
    assert DestinationRuntimeInfo(bytes_added=100, bytes_removed=500).bytes_saved == 400
    assert DestinationRuntimeInfo(bytes_added=500, bytes_removed=100).bytes_saved == 0
    assert DestinationRuntimeInfo(bytes_added=100).bytes_saved is None
