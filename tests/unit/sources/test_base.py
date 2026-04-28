"""Tests for BaseSourceReader and watermark logic."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import (
    DATE_FOLDER_PARTITION_KEY,
    DataFlowStatus,
)
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.base import BaseSourceReader

from tests.unit.sources.support import (
    ConcreteSourceReader,
    FailingSourceReader,
    MockEngine,
    delta_source,
    engine,
)


# ============================================================================
# BaseSourceReader tests
# ============================================================================


class TestBaseSourceReader:
    def test_cannot_instantiate_abc(self) -> None:
        with pytest.raises(TypeError):
            BaseSourceReader(MockEngine())  # type: ignore[abstract]

    def test_read_succeeds(self, engine: MockEngine, delta_source: Source) -> None:
        reader = ConcreteSourceReader(engine)
        result = reader.read(delta_source)
        assert result is not None
        info = reader.get_runtime_info()
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.start_time is not None
        assert info.end_time is not None

    def test_read_returns_none(self, engine: MockEngine, delta_source: Source) -> None:
        reader = ConcreteSourceReader(engine, return_none=True)
        result = reader.read(delta_source)
        assert result is None
        info = reader.get_runtime_info()
        assert info.status == DataFlowStatus.SUCCEEDED.value

    def test_read_failure_wraps_exception(self, engine: MockEngine, delta_source: Source) -> None:
        reader = FailingSourceReader(engine)
        with pytest.raises(SourceError, match="Failed to read source"):
            reader.read(delta_source)
        info = reader.get_runtime_info()
        assert info.status == DataFlowStatus.FAILED.value
        assert "Boom!" in (info.error_message or "")

    def test_read_records_watermark_before(self, engine: MockEngine, delta_source: Source) -> None:
        reader = ConcreteSourceReader(engine)
        wm = {"modified_at": "2024-01-01"}
        reader.read(delta_source, watermark=wm)
        info = reader.get_runtime_info()
        assert info.watermark_before == wm

    def test_get_new_watermark_initially_empty(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        assert reader.get_new_watermark() == {}


# ============================================================================
# Watermark Filter tests
# ============================================================================


class TestWatermarkFilter:
    def test_apply_watermark_filter_string(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        df = {"col": [1]}
        result = reader._apply_watermark_filter(df, ["col"], {"col": "2024-01-01"})
        assert engine._filtered is True

    def test_apply_watermark_filter_numeric(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        df = {"col": [1]}
        reader._apply_watermark_filter(df, ["col"], {"col": 100})
        assert engine._filtered is True

    def test_apply_watermark_filter_empty_columns(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        engine._filtered = False
        df = {"col": [1]}
        result = reader._apply_watermark_filter(df, [], {"col": "v"})
        assert result == df
        assert engine._filtered is False

    def test_apply_watermark_filter_empty_watermark(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        engine._filtered = False
        df = {"col": [1]}
        result = reader._apply_watermark_filter(df, ["col"], {})
        assert result == df
        assert engine._filtered is False

    def test_apply_watermark_filter_missing_key(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        engine._filtered = False
        df = {"col": [1]}
        result = reader._apply_watermark_filter(df, ["col"], {"other": "v"})
        assert result == df
        assert engine._filtered is False


# ============================================================================
# Count and Watermark tests
# ============================================================================


class TestCountAndWatermark:
    def test_no_watermark_columns(self, engine: MockEngine) -> None:
        reader = ConcreteSourceReader(engine)
        count, wm = reader._calculate_count_and_new_watermark({"x": 1}, [])
        assert count == 3
        assert wm == {}

    def test_with_watermark_columns(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = ConcreteSourceReader(engine)
        count, wm = reader._calculate_count_and_new_watermark({"x": 1}, ["modified_at"])
        assert count == 3
        assert wm == {"modified_at": "2024-06-01"}


# ============================================================================
# Backward offset tests (fixed offset)
# ============================================================================


class TestApplyBackward:
    """Tests for BaseSourceReader._apply_backward (fixed offset)."""

    def test_days(self) -> None:
        dt = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"days": 7})
        assert result == datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    def test_months(self) -> None:
        dt = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"months": 1})
        assert result == datetime(2026, 2, 8, 12, 0, tzinfo=timezone.utc)

    def test_months_clamps_day(self) -> None:
        # March 31 minus 1 month → Feb 28 (no Feb 31)
        dt = datetime(2026, 3, 31, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"months": 1})
        assert result == datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc)

    def test_hours(self) -> None:
        dt = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"hours": 6})
        assert result == datetime(2026, 3, 8, 6, 0, tzinfo=timezone.utc)

    def test_combined(self) -> None:
        dt = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"days": 3, "hours": 6})
        assert result == datetime(2026, 3, 5, 6, 0, tzinfo=timezone.utc)

    def test_months_january_wraps_year(self) -> None:
        dt = datetime(2026, 1, 15, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"months": 2})
        assert result == datetime(2025, 11, 15, 0, 0, tzinfo=timezone.utc)

    def test_years(self) -> None:
        dt = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"years": 1})
        assert result == datetime(2025, 3, 8, 12, 0, tzinfo=timezone.utc)

    def test_years_january_wraps(self) -> None:
        # 2025-01-01 minus 2 years → 2023-01-01
        dt = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"years": 2})
        assert result == datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)

    def test_years_clamps_leap_day(self) -> None:
        # 2024-02-29 (leap) minus 1 year → 2023-02-28 (clamped)
        dt = datetime(2024, 2, 29, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"years": 1})
        assert result == datetime(2023, 2, 28, 0, 0, tzinfo=timezone.utc)

    def test_combined_months_and_years(self) -> None:
        # 2026-05-15 minus 1 year 3 months → 2025-02-15
        dt = datetime(2026, 5, 15, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_backward(dt, {"years": 1, "months": 3})
        assert result == datetime(2025, 2, 15, 0, 0, tzinfo=timezone.utc)


# ============================================================================
# Closing day backward tests
# ============================================================================


class TestApplyClosingDayBackward:
    """Tests for BaseSourceReader._apply_closing_day_backward."""

    def test_before_closing_day(self) -> None:
        # March 8, closing_day=10 → Feb 1
        now = datetime(2026, 3, 8, 14, 30, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10)
        assert result == datetime(2026, 2, 1, tzinfo=timezone.utc)

    def test_on_closing_day(self) -> None:
        # March 10, closing_day=10 → Feb 1 (on = still in previous period)
        now = datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10)
        assert result == datetime(2026, 2, 1, tzinfo=timezone.utc)

    def test_after_closing_day(self) -> None:
        # March 12, closing_day=10 → Mar 1
        now = datetime(2026, 3, 12, 9, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10)
        assert result == datetime(2026, 3, 1, tzinfo=timezone.utc)

    def test_january_wraps_to_december(self) -> None:
        # Jan 5, closing_day=10 → Dec 1 of previous year
        now = datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10)
        assert result == datetime(2025, 12, 1, tzinfo=timezone.utc)

    def test_closing_day_1_always_current_month(self) -> None:
        # closing_day=1, any day > 1 → current month
        now = datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 1)
        assert result == datetime(2026, 3, 1, tzinfo=timezone.utc)

    def test_closing_day_1_on_first(self) -> None:
        # closing_day=1, day=1 → previous month
        now = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 1)
        assert result == datetime(2026, 2, 1, tzinfo=timezone.utc)

    def test_closing_day_end_of_month(self) -> None:
        # closing_day=28, Jan 15 → Dec 1 previous year
        now = datetime(2026, 1, 15, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 28)
        assert result == datetime(2025, 12, 1, tzinfo=timezone.utc)

    def test_preserves_timezone(self) -> None:
        now = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10)
        assert result.tzinfo == timezone.utc

    def test_with_extra_months(self) -> None:
        # March 8, closing_day=10 → Feb 1 (base) → Dec 1 prev year (−2 months)
        now = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10, months=2)
        assert result == datetime(2025, 12, 1, tzinfo=timezone.utc)

    def test_with_extra_years(self) -> None:
        # March 8, closing_day=10 → Feb 1 (base) → Feb 1 prev year (−1 year)
        now = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10, years=1)
        assert result == datetime(2025, 2, 1, tzinfo=timezone.utc)

    def test_with_extra_months_and_years(self) -> None:
        # March 8, closing_day=10 → Feb 1 (base) → Nov 1, 2024 (−3m −1y)
        now = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10, months=3, years=1)
        assert result == datetime(2024, 11, 1, tzinfo=timezone.utc)

    def test_after_closing_with_extra_months(self) -> None:
        # March 12, closing_day=10 → Mar 1 (base) → Jan 1 (−2 months)
        now = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10, months=2)
        assert result == datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_no_extra_offset_same_as_default(self) -> None:
        now = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        result = BaseSourceReader._apply_closing_day_backward(now, 10, months=0, years=0)
        assert result == datetime(2026, 2, 1, tzinfo=timezone.utc)


# ============================================================================
# Build effective watermark tests
# ============================================================================


class TestBuildEffectiveWatermark:
    """Tests for _build_effective_watermark with both strategies."""

    def test_none_watermark_passthrough(self) -> None:
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": "7"})
        result = BaseSourceReader._build_effective_watermark(src, None)
        assert result is None

    def test_no_backward_passthrough(self) -> None:
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn)
        wm = {"col": datetime(2026, 3, 8, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is wm

    def test_fixed_offset_strategy(self) -> None:
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": "7"})
        wm = {"modified_at": datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_closing_day_strategy(self, mock_now) -> None:
        mock_now.return_value = datetime(2026, 3, 8, 14, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"closing_day": 10}})
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 2, 1, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_closing_day_with_extra_months(self, mock_now) -> None:
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"closing_day": 10, "months": 2}})
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2025, 12, 1, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_closing_day_with_extra_years(self, mock_now) -> None:
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"closing_day": 10, "years": 1}})
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2025, 2, 1, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_closing_day_after_closing(self, mock_now) -> None:
        mock_now.return_value = datetime(2026, 3, 12, 14, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"closing_day": 10}})
        wm = {"modified_at": datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 3, 1, tzinfo=timezone.utc)

    def test_non_datetime_values_unchanged(self) -> None:
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": "7"})
        wm = {"modified_at": datetime(2026, 3, 8, tzinfo=timezone.utc), "id": 42}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["id"] == 42

    @patch("datacoolie.sources.base.utc_now")
    def test_closing_day_from_connection_fallback(self, mock_now) -> None:
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(
            name="c", connection_type="lakehouse", format="delta",
            configure={"backward": {"closing_day": 10}},
        )
        src = Source(connection=conn)
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 2, 1, tzinfo=timezone.utc)

    def test_file_partition_watermark_datetime_adjusted_only_partition_key(self) -> None:
        conn = Connection(name="c", connection_type="file", format="parquet", configure={})
        src = Source(connection=conn, configure={"backward_days": 1})
        wm = {
            DATE_FOLDER_PARTITION_KEY: datetime(2026, 3, 8, tzinfo=timezone.utc),
            "modified_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
        }
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        assert result[DATE_FOLDER_PARTITION_KEY] == datetime(2026, 3, 7, tzinfo=timezone.utc)
        assert result["modified_at"] == datetime(2026, 3, 8, tzinfo=timezone.utc)

    def test_file_partition_watermark_invalid_string_returns_original(self) -> None:
        conn = Connection(name="c", connection_type="file", format="parquet", configure={})
        src = Source(connection=conn, configure={"backward_days": 1})
        wm = {DATE_FOLDER_PARTITION_KEY: "not-a-date"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result == wm

    def test_file_partition_watermark_non_date_type_returns_original(self) -> None:
        conn = Connection(name="c", connection_type="file", format="parquet", configure={})
        src = Source(connection=conn, configure={"backward_days": 1})
        wm = {DATE_FOLDER_PARTITION_KEY: 12345}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result == wm

    # ---- shorthand top-level keys ----------------------------------------

    def test_backward_days_int(self) -> None:
        """backward_days as an integer (not a string) must still work."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 7})
        wm = {"modified_at": datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    def test_backward_months_shorthand(self) -> None:
        """backward_months: 2 shorthand subtracts 2 months from the watermark."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_months": 2})
        wm = {"modified_at": datetime(2026, 5, 15, 0, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)

    def test_backward_years_shorthand(self) -> None:
        """backward_years: 1 shorthand subtracts 1 year from the watermark."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_years": 1})
        wm = {"modified_at": datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2025, 3, 8, 12, 0, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_backward_closing_day_shorthand(self, mock_now) -> None:
        """backward_closing_day: 10 shorthand triggers the closing-day strategy."""
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_closing_day": 10})
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 2, 1, tzinfo=timezone.utc)

    # ---- nested backward dict (months / years without closing_day) ---------

    def test_backward_nested_months(self) -> None:
        """backward: {months: 1} (no closing_day) uses the fixed-offset path."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"months": 1}})
        wm = {"modified_at": datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        # April 30 − 1 month → March 30
        assert result["modified_at"] == datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    def test_backward_nested_years(self) -> None:
        """backward: {years: 1} (no closing_day) uses the fixed-offset path."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"years": 1}})
        wm = {"modified_at": datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2025, 3, 8, 12, 0, tzinfo=timezone.utc)

    def test_backward_nested_days_months_combined(self) -> None:
        """backward: {days: 3, months: 1} applies both offsets."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"days": 3, "months": 1}})
        wm = {"modified_at": datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        # April 15 − 3 days = April 12 − 1 month = March 12
        assert result["modified_at"] == datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)

    # ---- source-level backward overrides connection-level ------------------

    def test_source_overrides_connection_backward(self) -> None:
        """Source-level backward config takes priority over connection-level."""
        conn = Connection(
            name="c", connection_type="lakehouse", format="delta",
            configure={"backward_days": 30},
        )
        # Source says 7 days — should win
        src = Source(connection=conn, configure={"backward_days": 7})
        wm = {"modified_at": datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    @patch("datacoolie.sources.base.utc_now")
    def test_source_closing_day_overrides_connection_fixed(self, mock_now) -> None:
        """Source-level closing_day overrides a connection-level days offset."""
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(
            name="c", connection_type="lakehouse", format="delta",
            configure={"backward_days": 30},
        )
        src = Source(connection=conn, configure={"backward": {"closing_day": 10}})
        wm = {"modified_at": datetime(2026, 3, 5, tzinfo=timezone.utc)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        # closing_day strategy must win, not the 30-day offset
        assert result["modified_at"] == datetime(2026, 2, 1, tzinfo=timezone.utc)

    # ---- string-typed watermark values (real-world API reader path) ---------

    def test_string_watermark_days(self) -> None:
        """A string ISO datetime watermark is parsed, offset, and returned as string."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 7})
        wm = {"modified_at": "2026-03-08T12:00:00+00:00"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        expected = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        assert datetime.fromisoformat(result["modified_at"]) == expected

    def test_string_watermark_months(self) -> None:
        """backward_months applied to a string datetime watermark."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_months": 2})
        wm = {"modified_at": "2026-05-15T00:00:00+00:00"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        assert datetime.fromisoformat(result["modified_at"]) == datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)

    def test_string_watermark_years(self) -> None:
        """backward_years applied to a string datetime watermark (the reported bug)."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_years": 1})
        wm = {"modified_at": "2026-04-08T05:59:47.206048+00:00"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        result_dt = datetime.fromisoformat(result["modified_at"])
        assert result_dt.year == 2025
        assert result_dt.month == 4
        assert result_dt.day == 8

    def test_string_watermark_result_is_string(self) -> None:
        """Result type must remain str when input was str (not a datetime object)."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 1})
        wm = {"modified_at": "2026-03-08T12:00:00+00:00"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        assert isinstance(result["modified_at"], str)

    def test_string_watermark_invalid_string_unchanged(self) -> None:
        """Non-datetime strings are passed through unchanged."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 7})
        wm = {"modified_at": "not-a-date"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result["modified_at"] == "not-a-date"

    @patch("datacoolie.sources.base.utc_now")
    def test_string_watermark_closing_day(self, mock_now) -> None:
        """closing_day strategy works with string datetime watermarks."""
        mock_now.return_value = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc)
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward": {"closing_day": 10}})
        wm = {"modified_at": "2026-03-05T00:00:00+00:00"}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        assert datetime.fromisoformat(result["modified_at"]) == datetime(2026, 2, 1, tzinfo=timezone.utc)

    # ---- date-typed watermark values (polars Date column path) -------------

    def test_date_type_backward_days(self) -> None:
        """date watermark offset by days returns a date object."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 7})
        wm = {"event_date": date(2026, 3, 8)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        assert result["event_date"] == date(2026, 3, 1)

    def test_date_type_result_stays_date(self) -> None:
        """Result type must be date (not datetime) when input is a date object."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_days": 1})
        wm = {"event_date": date(2026, 3, 8)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        val = result["event_date"]
        assert type(val) is date  # must not be datetime (which IS-A date)

    def test_date_type_backward_months_clamped(self) -> None:
        """Month subtraction from a date is clamped to last valid day."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn, configure={"backward_months": 1})
        wm = {"event_date": date(2026, 3, 31)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is not None
        # March 31 − 1 month → Feb only has 28 days in 2026, so clamp to Feb 28
        assert result["event_date"] == date(2026, 2, 28)

    def test_date_type_passthrough_no_backward(self) -> None:
        """date watermark is returned unchanged when no backward config is set."""
        conn = Connection(name="c", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn)
        wm = {"event_date": date(2026, 3, 8)}
        result = BaseSourceReader._build_effective_watermark(src, wm)
        assert result is wm
