"""Tests for WatermarkSerializer and module-level convenience functions."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, DATETIME_PATTERN
from datacoolie.core.exceptions import WatermarkError
from datacoolie.watermark.base import (
    BaseWatermarkManager,
    WatermarkSerializer,
    deserialize_watermark,
    is_watermark_empty,
    serialize_watermark,
)


# ===========================================================================
# WatermarkSerializer.serialize
# ===========================================================================


class TestWatermarkSerializerSerialize:
    """Serialize watermark dicts to JSON strings."""

    def test_simple_values(self) -> None:
        wm = {"offset": 42, "key": "value"}
        result = WatermarkSerializer.serialize(wm)
        parsed = json.loads(result)
        assert parsed == {"key": "value", "offset": 42}

    def test_datetime_encoding(self) -> None:
        dt = datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        wm = {"modified_at": dt}
        result = WatermarkSerializer.serialize(wm)
        parsed = json.loads(result)
        assert DATETIME_PATTERN in parsed["modified_at"]
        assert "2025-06-15" in parsed["modified_at"][DATETIME_PATTERN]

    def test_date_encoding(self) -> None:
        wm = {"day": date(2025, 6, 15)}
        result = WatermarkSerializer.serialize(wm)
        parsed = json.loads(result)
        assert parsed["day"] == {"__date__": "2025-06-15"}

    def test_time_encoding(self) -> None:
        wm = {"clock": time(10, 30, 0)}
        result = WatermarkSerializer.serialize(wm)
        parsed = json.loads(result)
        assert parsed["clock"] == {"__time__": "10:30:00"}

    def test_mixed_types(self) -> None:
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        wm = {"ts": dt, "offset": 100, "label": "test"}
        result = WatermarkSerializer.serialize(wm)
        parsed = json.loads(result)
        assert isinstance(parsed["ts"], dict)
        assert parsed["offset"] == 100
        assert parsed["label"] == "test"

    def test_empty_dict(self) -> None:
        result = WatermarkSerializer.serialize({})
        assert json.loads(result) == {}

    def test_non_serializable_raises(self) -> None:
        with pytest.raises(TypeError):
            WatermarkSerializer.serialize({"bad": object()})

    def test_decimal_and_binary_roundtrip(self) -> None:
        original = {
            "amount": Decimal("001.2300"),
            "version": b"\x00\xffv1",
        }
        restored = WatermarkSerializer.deserialize(WatermarkSerializer.serialize(original))
        assert restored == original
        assert isinstance(restored["amount"], Decimal)
        assert isinstance(restored["version"], bytes)

    @pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
    def test_nonfinite_decimal_is_rejected(self, value: Decimal) -> None:
        with pytest.raises(TypeError, match="finite"):
            WatermarkSerializer.serialize({"amount": value})

    def test_nonfinite_float_is_rejected(self) -> None:
        with pytest.raises(TypeError, match="finite"):
            WatermarkSerializer.serialize({"offset": float("nan")})

    @pytest.mark.parametrize("value", [(1, 2), {1, 2}, frozenset({1, 2})])
    def test_lossy_containers_are_rejected(self, value) -> None:
        with pytest.raises(TypeError, match="Unsupported watermark container"):
            WatermarkSerializer.serialize({"value": value})

    def test_reserved_tag_container_is_rejected_before_encoding(self) -> None:
        with pytest.raises(TypeError, match="Reserved watermark tag"):
            WatermarkSerializer.serialize({"value": {"__date__": "2025-01-01"}})

    def test_internal_date_folder_key_roundtrips_as_metadata(self) -> None:
        value = "2026-01-02T00:00:00+00:00"
        serialized = WatermarkSerializer.serialize(
            {DATE_FOLDER_PARTITION_KEY: value}
        )

        assert WatermarkSerializer.deserialize(serialized) == {
            DATE_FOLDER_PARTITION_KEY: value
        }


# ===========================================================================
# WatermarkSerializer.deserialize
# ===========================================================================


class TestWatermarkSerializerDeserialize:
    """Deserialize JSON strings back to watermark dicts."""

    def test_roundtrip(self) -> None:
        dt = datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        original = {"modified_at": dt, "offset": 42}
        json_str = WatermarkSerializer.serialize(original)
        restored = WatermarkSerializer.deserialize(json_str)
        assert restored["modified_at"] == dt
        assert restored["offset"] == 42

    @pytest.mark.parametrize(
        "raw",
        [
            None,
            "",
            "null",
            "None",
            "{}",
        ],
    )
    def test_empty_like_inputs_return_empty(self, raw) -> None:
        assert WatermarkSerializer.deserialize(raw) == {}

    def test_invalid_json_is_corruption(self) -> None:
        with pytest.raises(WatermarkError, match="Invalid watermark JSON"):
            WatermarkSerializer.deserialize("not json!!!")

    def test_non_string_json_is_corruption(self) -> None:
        with pytest.raises(WatermarkError, match="Invalid watermark JSON"):
            WatermarkSerializer.deserialize(123)  # type: ignore[arg-type]

    @pytest.mark.parametrize("raw", ['{"offset": NaN}', '{"offset": Infinity}'])
    def test_nonfinite_json_constants_are_corruption(self, raw: str) -> None:
        with pytest.raises(WatermarkError, match="Invalid watermark JSON"):
            WatermarkSerializer.deserialize(raw)

    def test_non_dict_json_is_corruption(self) -> None:
        with pytest.raises(WatermarkError, match="must contain an object"):
            WatermarkSerializer.deserialize("[1, 2, 3]")

    def test_nested_datetime_restoration(self) -> None:
        dt_iso = "2025-06-15T10:30:00+00:00"
        raw = json.dumps({"ts": {DATETIME_PATTERN: dt_iso}})
        result = WatermarkSerializer.deserialize(raw)
        assert isinstance(result["ts"], datetime)
        assert result["ts"].year == 2025

    def test_date_restoration(self) -> None:
        raw = json.dumps({"day": {"__date__": "2025-06-15"}})
        result = WatermarkSerializer.deserialize(raw)
        assert result["day"] == date(2025, 6, 15)

    def test_time_restoration(self) -> None:
        raw = json.dumps({"clock": {"__time__": "10:30:00"}})
        result = WatermarkSerializer.deserialize(raw)
        assert result["clock"] == time(10, 30, 0)

    def test_nested_dict_without_datetime(self) -> None:
        raw = json.dumps({"meta": {"key": "value"}})
        result = WatermarkSerializer.deserialize(raw)
        assert result["meta"] == {"key": "value"}

    def test_invalid_datetime_is_corruption(self) -> None:
        raw = json.dumps({"ts": {DATETIME_PATTERN: "not-a-date"}})
        with pytest.raises(WatermarkError, match="Invalid datetime"):
            WatermarkSerializer.deserialize(raw)

    def test_invalid_date_is_corruption(self) -> None:
        raw = json.dumps({"day": {"__date__": "bad-date"}})
        with pytest.raises(WatermarkError, match="Invalid date"):
            WatermarkSerializer.deserialize(raw)

    def test_invalid_time_is_corruption(self) -> None:
        raw = json.dumps({"clock": {"__time__": "bad-time"}})
        with pytest.raises(WatermarkError, match="Invalid time"):
            WatermarkSerializer.deserialize(raw)

    def test_ambiguous_tag_is_corruption(self) -> None:
        raw = json.dumps({"value": {"__date__": "2025-06-15", "extra": 1}})
        with pytest.raises(WatermarkError, match="Invalid watermark tag"):
            WatermarkSerializer.deserialize(raw)

    def test_unknown_tag_is_corruption(self) -> None:
        raw = json.dumps({"value": {"__future_type__": "value"}})
        with pytest.raises(WatermarkError, match="Invalid watermark tag"):
            WatermarkSerializer.deserialize(raw)

    def test_invalid_binary_is_corruption(self) -> None:
        raw = json.dumps({"version": {"__binary__": "not-base64!"}})
        with pytest.raises(WatermarkError, match="Invalid binary"):
            WatermarkSerializer.deserialize(raw)


# ===========================================================================
# Convenience functions
# ===========================================================================


class TestConvenienceFunctions:
    """Module-level serialize_watermark, deserialize_watermark, is_watermark_empty."""

    def test_serialize_watermark(self) -> None:
        result = serialize_watermark({"x": 1})
        assert json.loads(result) == {"x": 1}

    def test_deserialize_watermark(self) -> None:
        result = deserialize_watermark('{"x": 1}')
        assert result == {"x": 1}

    @pytest.mark.parametrize(
        ("watermark", "expected"),
        [
            (None, True),
            ({}, True),
            ({"a": None, "b": None}, True),
            ({"a": 1}, False),
            ({"a": None, "b": 1}, False),
        ],
    )
    def test_is_watermark_empty_cases(self, watermark, expected) -> None:
        assert is_watermark_empty(watermark) is expected


# ===========================================================================
# BaseWatermarkManager - ABC contract
# ===========================================================================


class TestBaseWatermarkManagerABC:
    """Cannot instantiate the ABC; concrete subclass works."""

    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError):
            BaseWatermarkManager()  # type: ignore[abstract]

    def test_concrete_subclass_serialize_deserialize(self) -> None:
        class Stub(BaseWatermarkManager):
            def get_watermark(self, dataflow_id):
                return None

            def save_watermark(self, dataflow_id, watermark, *, job_id=None, dataflow_run_id=None):
                pass

        stub = Stub()
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        s = stub.serialize({"ts": dt})
        d = stub.deserialize(s)
        assert isinstance(d["ts"], datetime)
