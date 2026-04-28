"""Tests for WatermarkManager — delegates to BaseMetadataProvider."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import DATETIME_PATTERN
from datacoolie.core.exceptions import WatermarkError
from datacoolie.watermark.watermark_manager import WatermarkManager

from tests.unit.watermark.support import StubMetadataProvider


# ===========================================================================
# WatermarkManager.get_watermark
# ===========================================================================


class TestWatermarkManagerGet:
    """get_watermark retrieves and deserializes from the provider."""

    def test_returns_none_when_no_watermark(self) -> None:
        provider = StubMetadataProvider()
        mgr = WatermarkManager(provider)
        assert mgr.get_watermark("df-1") is None

    def test_returns_dict_from_provider_string(self) -> None:
        provider = StubMetadataProvider()
        provider.watermarks["df-1"] = json.dumps({"offset": 42})
        mgr = WatermarkManager(provider)
        result = mgr.get_watermark("df-1")
        assert result == {"offset": 42}

    def test_returns_none_for_empty_json_object(self) -> None:
        provider = StubMetadataProvider()
        provider.watermarks["df-1"] = "{}"
        mgr = WatermarkManager(provider)
        assert mgr.get_watermark("df-1") is None

    def test_datetime_roundtrip_via_string(self) -> None:
        dt = datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        serialised = json.dumps({"ts": {DATETIME_PATTERN: dt.isoformat()}})
        provider = StubMetadataProvider()
        provider.watermarks["df-1"] = serialised
        mgr = WatermarkManager(provider)
        result = mgr.get_watermark("df-1")
        assert isinstance(result["ts"], datetime)
        assert result["ts"] == dt

    def test_provider_exception_wrapped_as_watermark_error(self) -> None:
        provider = StubMetadataProvider()
        provider.get_watermark = MagicMock(side_effect=RuntimeError("boom"))
        mgr = WatermarkManager(provider)
        with pytest.raises(WatermarkError, match="Failed to load watermark"):
            mgr.get_watermark("df-1")


# ===========================================================================
# WatermarkManager.save_watermark
# ===========================================================================


class TestWatermarkManagerSave:
    """save_watermark serializes and delegates to the provider."""

    def test_save_basic(self) -> None:
        provider = StubMetadataProvider()
        mgr = WatermarkManager(provider)
        mgr.save_watermark("df-1", {"offset": 42})
        stored = provider.watermarks["df-1"]
        assert isinstance(stored, str)
        assert json.loads(stored) == {"offset": 42}

    def test_save_with_datetime(self) -> None:
        dt = datetime(2025, 6, 15, tzinfo=timezone.utc)
        provider = StubMetadataProvider()
        mgr = WatermarkManager(provider)
        mgr.save_watermark("df-1", {"ts": dt})
        parsed = json.loads(provider.watermarks["df-1"])
        assert DATETIME_PATTERN in parsed["ts"]

    def test_save_with_job_id(self) -> None:
        provider = StubMetadataProvider()
        provider.update_watermark = MagicMock()
        mgr = WatermarkManager(provider)
        mgr.save_watermark("df-1", {"x": 1}, job_id="j-1", dataflow_run_id="r-1")
        provider.update_watermark.assert_called_once()
        call_kw = provider.update_watermark.call_args
        assert call_kw.kwargs["job_id"] == "j-1"
        assert call_kw.kwargs["dataflow_run_id"] == "r-1"

    def test_provider_exception_wrapped_as_watermark_error(self) -> None:
        provider = StubMetadataProvider()
        provider.update_watermark = MagicMock(side_effect=RuntimeError("disk full"))
        mgr = WatermarkManager(provider)
        with pytest.raises(WatermarkError, match="Failed to save watermark"):
            mgr.save_watermark("df-1", {"x": 1})

    def test_watermark_error_re_raised(self) -> None:
        """WatermarkError from provider is not double-wrapped."""
        provider = StubMetadataProvider()
        provider.update_watermark = MagicMock(side_effect=WatermarkError("orig"))
        mgr = WatermarkManager(provider)
        with pytest.raises(WatermarkError, match="orig"):
            mgr.save_watermark("df-1", {"x": 1})


# ===========================================================================
# Full end-to-end roundtrip
# ===========================================================================


class TestWatermarkManagerRoundtrip:
    """Save then get back a watermark with datetimes."""

    def test_full_roundtrip(self) -> None:
        dt = datetime(2025, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
        provider = StubMetadataProvider()
        mgr = WatermarkManager(provider)
        mgr.save_watermark("df-1", {"modified_at": dt, "page": 5})
        # Provider stores the serialised string; get_watermark deserialises
        # Because StubMetadataProvider stores the raw string, we simulate
        # what would happen with a real provider that returns the string
        result = mgr.get_watermark("df-1")
        assert result is not None
        # The stored value is a JSON string, so WatermarkManager will deserialize it
        assert result["modified_at"] == dt
        assert result["page"] == 5
