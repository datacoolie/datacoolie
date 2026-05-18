"""Tests for ReplayConfig and generate_chunk_boundaries."""

from __future__ import annotations

from datetime import date, datetime, timezone


import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import ReplayConfig
from datacoolie.utils.datetime_utils import generate_chunk_boundaries


# ============================================================================
# ReplayConfig validation
# ============================================================================


class TestReplayConfigValidation:
    """ReplayConfig __post_init__ validation."""

    def test_valid_config(self):
        cfg = ReplayConfig(
            start="2025-01-01",
            end="2025-06-01",
            chunk_interval={"months": 1},
        )
        assert cfg.save_watermark is False
        assert cfg.chunk_column is None

    def test_valid_config_no_chunking(self):
        cfg = ReplayConfig(
            start=0,
            end=100000,
        )
        assert cfg.chunk_interval is None

    def test_valid_config_with_explicit_column(self):
        cfg = ReplayConfig(
            start="2025-01-01",
            end="2025-06-01",
            chunk_column="order_date",
        )
        assert cfg.chunk_column == "order_date"

    def test_none_from_value_raises(self):
        with pytest.raises(ConfigurationError, match="start must not be None"):
            ReplayConfig(start=None, end="2025-06-01")

    def test_none_to_value_raises(self):
        with pytest.raises(ConfigurationError, match="end must not be None"):
            ReplayConfig(start="2025-01-01", end=None)


# ============================================================================
# generate_chunk_boundaries — time-based (datetime strings)
# ============================================================================


class TestChunkBoundariesTimeString:
    """Chunk generation with ISO-8601 string watermarks."""

    def test_monthly_chunks(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-04-01",
            interval={"months": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 2, 1))
        assert chunks[1] == (date(2025, 2, 1), date(2025, 3, 1))
        assert chunks[2] == (date(2025, 3, 1), date(2025, 4, 1))

    def test_daily_chunks(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-01-04",
            interval={"days": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 2))
        assert chunks[2] == (date(2025, 1, 3), date(2025, 1, 4))

    def test_partial_last_chunk(self):
        """End doesn't align to interval boundary — last chunk is smaller."""
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-03-15",
            interval={"months": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 2, 1))
        assert chunks[1] == (date(2025, 2, 1), date(2025, 3, 1))
        # Last chunk is partial
        assert chunks[2] == (date(2025, 3, 1), date(2025, 3, 15))

    def test_weekly_chunks(self):
        # Jan 1 2025 is Wednesday; first Monday boundary is Jan 6.
        # Calendar-aligned: [Jan 1, Jan 6), [Jan 6, Jan 13), [Jan 13, Jan 20), [Jan 20, Jan 22)
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-01-22",
            interval={"weeks": 1},
        )
        assert len(chunks) == 4
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 6))
        assert chunks[1] == (date(2025, 1, 6), date(2025, 1, 13))
        assert chunks[2] == (date(2025, 1, 13), date(2025, 1, 20))
        assert chunks[3] == (date(2025, 1, 20), date(2025, 1, 22))

    def test_hourly_chunks(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T03:00:00",
            interval={"hours": 1},
        )
        assert len(chunks) == 3

    def test_start_equals_end_raises(self):
        with pytest.raises(ConfigurationError, match="must be earlier than"):
            generate_chunk_boundaries(
                start="2025-01-01",
                end="2025-01-01",
                interval={"days": 1},
            )

    def test_start_after_end_raises(self):
        with pytest.raises(ConfigurationError, match="must be earlier than"):
            generate_chunk_boundaries(
                start="2025-06-01",
                end="2025-01-01",
                interval={"days": 1},
            )


# ============================================================================
# generate_chunk_boundaries — calendar-aligned chunks (all units, n=1 and n>1)
# ============================================================================


class TestChunkBoundariesCalendarAlignment:
    """Calendar alignment for every supported time unit and stride value.

    Each subsection tests two sub-cases:
    * **Aligned start** — start is already at a boundary, no partial first chunk.
    * **Non-aligned start** — start is mid-period, partial first chunk produced.

    End is always non-aligned (mid-period) to also exercise partial last chunks.
    """

    # ------------------------------------------------------------------ years

    def test_yearly_n1_aligned_start(self):
        # Jan 1 → Jan 1: two complete annual chunks
        chunks = generate_chunk_boundaries(
            start="2023-01-01",
            end="2025-01-01",
            interval={"years": 1},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2023, 1, 1), date(2024, 1, 1))
        assert chunks[1] == (date(2024, 1, 1), date(2025, 1, 1))

    def test_yearly_n1_partial_first_and_last(self):
        # Mar 15 2023 → Jun 1 2025: [Mar 15 2023, Jan 1 2024), [Jan 1 2024, Jan 1 2025), [Jan 1 2025, Jun 1 2025)
        chunks = generate_chunk_boundaries(
            start="2023-03-15",
            end="2025-06-01",
            interval={"years": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2023, 3, 15), date(2024, 1, 1))
        assert chunks[1] == (date(2024, 1, 1), date(2025, 1, 1))
        assert chunks[2] == (date(2025, 1, 1), date(2025, 6, 1))

    def test_yearly_n2_aligned_start(self):
        # 2023-01-01 → 2027-01-01 with 2-year stride (2023,2025,2027 are boundaries)
        chunks = generate_chunk_boundaries(
            start="2023-01-01",
            end="2027-01-01",
            interval={"years": 2},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2023, 1, 1), date(2025, 1, 1))
        assert chunks[1] == (date(2025, 1, 1), date(2027, 1, 1))

    def test_yearly_n2_partial_first_and_last(self):
        # Mar 1 2024 — floor to 2023, ceil = 2025; end = Jun 1 2027 (partial last)
        # chunks: [Mar 1 2024, Jan 1 2025), [Jan 1 2025, Jan 1 2027), [Jan 1 2027, Jun 1 2027)
        chunks = generate_chunk_boundaries(
            start="2024-03-01",
            end="2027-06-01",
            interval={"years": 2},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2024, 3, 1), date(2025, 1, 1))
        assert chunks[1] == (date(2025, 1, 1), date(2027, 1, 1))
        assert chunks[2] == (date(2027, 1, 1), date(2027, 6, 1))

    # ------------------------------------------------------------------ quarters (months=3)

    def test_quarterly_n3_aligned_start(self):
        # Jan 1 and Apr 1 are Q1/Q2 starts — two complete quarters
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-07-01",
            interval={"months": 3},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2025, 1, 1), date(2025, 4, 1))
        assert chunks[1] == (date(2025, 4, 1), date(2025, 7, 1))

    def test_quarterly_n3_partial_first_and_last(self):
        # Feb 15 → floor Jan 1, ceil Apr 1; end = May 20 (partial last)
        # chunks: [Feb 15, Apr 1), [Apr 1, May 20)
        chunks = generate_chunk_boundaries(
            start="2025-02-15",
            end="2025-05-20",
            interval={"months": 3},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2025, 2, 15), date(2025, 4, 1))
        assert chunks[1] == (date(2025, 4, 1), date(2025, 5, 20))

    def test_quarterly_n3_mid_quarter_start_aligned_end(self):
        # Start mid-Q2, end at Q3 boundary
        # May 20 → floor Apr 1, ceil Jul 1; end = Oct 1 (Q4 start)
        # chunks: [May 20, Jul 1), [Jul 1, Oct 1)
        chunks = generate_chunk_boundaries(
            start="2025-05-20",
            end="2025-10-01",
            interval={"months": 3},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2025, 5, 20), date(2025, 7, 1))
        assert chunks[1] == (date(2025, 7, 1), date(2025, 10, 1))

    # ------------------------------------------------------------------ months

    def test_monthly_n2_aligned_start(self):
        # Jan(0), Mar(2), May(4), Jul(6) are 2-month boundaries
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-07-01",
            interval={"months": 2},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 3, 1))
        assert chunks[1] == (date(2025, 3, 1), date(2025, 5, 1))
        assert chunks[2] == (date(2025, 5, 1), date(2025, 7, 1))

    def test_monthly_n2_partial_first(self):
        # Feb 1 (0-based idx=1) → floor Jan 1, ceil Mar 1; end = Jul 1
        # chunks: [Feb 1, Mar 1), [Mar 1, May 1), [May 1, Jul 1)
        chunks = generate_chunk_boundaries(
            start="2025-02-01",
            end="2025-07-01",
            interval={"months": 2},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 2, 1), date(2025, 3, 1))
        assert chunks[1] == (date(2025, 3, 1), date(2025, 5, 1))
        assert chunks[2] == (date(2025, 5, 1), date(2025, 7, 1))

    def test_monthly_n6_aligned_start(self):
        # Jan 1 and Jul 1 are 6-month boundaries
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2026-01-01",
            interval={"months": 6},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2025, 1, 1), date(2025, 7, 1))
        assert chunks[1] == (date(2025, 7, 1), date(2026, 1, 1))

    # ------------------------------------------------------------------ weeks

    def test_weekly_n1_aligned_start(self):
        # Jan 6 2025 is Monday — already at boundary
        chunks = generate_chunk_boundaries(
            start="2025-01-06",
            end="2025-01-27",
            interval={"weeks": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 6), date(2025, 1, 13))
        assert chunks[1] == (date(2025, 1, 13), date(2025, 1, 20))
        assert chunks[2] == (date(2025, 1, 20), date(2025, 1, 27))

    def test_weekly_n2_aligned_start(self):
        # Jan 6 is Monday; 2-week stride → [Jan 6, Jan 20), [Jan 20, Feb 3)
        chunks = generate_chunk_boundaries(
            start="2025-01-06",
            end="2025-02-03",
            interval={"weeks": 2},
        )
        assert len(chunks) == 2
        assert chunks[0] == (date(2025, 1, 6), date(2025, 1, 20))
        assert chunks[1] == (date(2025, 1, 20), date(2025, 2, 3))

    def test_weekly_n2_partial_first(self):
        # Jan 1 (Wed) — floor = Dec 30 2024 (Mon), ceil = Jan 13 2025 (Mon+2w)
        # [Jan 1, Jan 13), [Jan 13, Jan 27), [Jan 27, Feb 3)
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-02-03",
            interval={"weeks": 2},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 13))
        assert chunks[1] == (date(2025, 1, 13), date(2025, 1, 27))
        assert chunks[2] == (date(2025, 1, 27), date(2025, 2, 3))

    # ------------------------------------------------------------------ days

    def test_daily_n1_full_week(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-01-08",
            interval={"days": 1},
        )
        assert len(chunks) == 7
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 2))
        assert chunks[6] == (date(2025, 1, 7), date(2025, 1, 8))

    def test_daily_n2_aligned_start(self):
        # 2-day stride from Jan 1 — all boundaries at odd day-of-month
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-01-07",
            interval={"days": 2},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 3))
        assert chunks[1] == (date(2025, 1, 3), date(2025, 1, 5))
        assert chunks[2] == (date(2025, 1, 5), date(2025, 1, 7))

    def test_daily_n7_aligned_start(self):
        # 7-day stride — equivalent to 1-week without Monday snapping
        chunks = generate_chunk_boundaries(
            start="2025-01-01",
            end="2025-01-22",
            interval={"days": 7},
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2025, 1, 1), date(2025, 1, 8))
        assert chunks[1] == (date(2025, 1, 8), date(2025, 1, 15))
        assert chunks[2] == (date(2025, 1, 15), date(2025, 1, 22))

    # ------------------------------------------------------------------ hours

    def test_hourly_n1_aligned_start(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T04:00:00",
            interval={"hours": 1},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 1))
        assert chunks[3] == (datetime(2025, 1, 1, 3), datetime(2025, 1, 1, 4))

    def test_hourly_n2_aligned_start(self):
        # 2-hour chunks from midnight
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T08:00:00",
            interval={"hours": 2},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 2))
        assert chunks[3] == (datetime(2025, 1, 1, 6), datetime(2025, 1, 1, 8))

    def test_hourly_n2_partial_first(self):
        # Start at 01:00 — floor=00:00, ceil=02:00; partial first [01:00, 02:00)
        chunks = generate_chunk_boundaries(
            start="2025-01-01T01:00:00",
            end="2025-01-01T08:00:00",
            interval={"hours": 2},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2))
        assert chunks[1] == (datetime(2025, 1, 1, 2), datetime(2025, 1, 1, 4))
        assert chunks[3] == (datetime(2025, 1, 1, 6), datetime(2025, 1, 1, 8))

    def test_hourly_n6_aligned_start(self):
        # 6-hour chunks: 00, 06, 12, 18
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-02T00:00:00",
            interval={"hours": 6},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 6))
        assert chunks[1] == (datetime(2025, 1, 1, 6), datetime(2025, 1, 1, 12))
        assert chunks[2] == (datetime(2025, 1, 1, 12), datetime(2025, 1, 1, 18))
        assert chunks[3] == (datetime(2025, 1, 1, 18), datetime(2025, 1, 2, 0))

    def test_hourly_n6_partial_first(self):
        # Start at 03:00 — floor=00:00, ceil=06:00; partial [03:00, 06:00)
        chunks = generate_chunk_boundaries(
            start="2025-01-01T03:00:00",
            end="2025-01-02T00:00:00",
            interval={"hours": 6},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 3), datetime(2025, 1, 1, 6))
        assert chunks[1] == (datetime(2025, 1, 1, 6), datetime(2025, 1, 1, 12))
        assert chunks[2] == (datetime(2025, 1, 1, 12), datetime(2025, 1, 1, 18))
        assert chunks[3] == (datetime(2025, 1, 1, 18), datetime(2025, 1, 2, 0))

    def test_hourly_n12_aligned_start(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-02T00:00:00",
            interval={"hours": 12},
        )
        assert len(chunks) == 2
        assert chunks[0] == (datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 12))
        assert chunks[1] == (datetime(2025, 1, 1, 12), datetime(2025, 1, 2, 0))

    def test_hourly_n12_partial_first(self):
        # Start at 05:00 — floor=00:00, ceil=12:00; partial [05:00, 12:00)
        chunks = generate_chunk_boundaries(
            start="2025-01-01T05:00:00",
            end="2025-01-02T00:00:00",
            interval={"hours": 12},
        )
        assert len(chunks) == 2
        assert chunks[0] == (datetime(2025, 1, 1, 5), datetime(2025, 1, 1, 12))
        assert chunks[1] == (datetime(2025, 1, 1, 12), datetime(2025, 1, 2, 0))

    # ------------------------------------------------------------------ minutes

    def test_minutely_n1_aligned_start(self):
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:03:00",
            interval={"minutes": 1},
        )
        assert len(chunks) == 3
        assert chunks[0] == (datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 0, 1))
        assert chunks[2] == (datetime(2025, 1, 1, 0, 2), datetime(2025, 1, 1, 0, 3))

    def test_minutely_n15_aligned_start(self):
        # 00:00 → 01:00 with 15-min stride: 4 chunks
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T01:00:00",
            interval={"minutes": 15},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 0, 15))
        assert chunks[1] == (datetime(2025, 1, 1, 0, 15), datetime(2025, 1, 1, 0, 30))
        assert chunks[2] == (datetime(2025, 1, 1, 0, 30), datetime(2025, 1, 1, 0, 45))
        assert chunks[3] == (datetime(2025, 1, 1, 0, 45), datetime(2025, 1, 1, 1, 0))

    def test_minutely_n15_partial_first(self):
        # Start at 00:07 — floor=00:00, ceil=00:15; partial [00:07, 00:15)
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:07:00",
            end="2025-01-01T01:00:00",
            interval={"minutes": 15},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0, 7), datetime(2025, 1, 1, 0, 15))
        assert chunks[1] == (datetime(2025, 1, 1, 0, 15), datetime(2025, 1, 1, 0, 30))
        assert chunks[3] == (datetime(2025, 1, 1, 0, 45), datetime(2025, 1, 1, 1, 0))

    def test_minutely_n30_aligned_start(self):
        # 00:00 → 02:00 with 30-min stride: 4 chunks
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:00:00",
            end="2025-01-01T02:00:00",
            interval={"minutes": 30},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 0, 30))
        assert chunks[1] == (datetime(2025, 1, 1, 0, 30), datetime(2025, 1, 1, 1, 0))
        assert chunks[2] == (datetime(2025, 1, 1, 1, 0), datetime(2025, 1, 1, 1, 30))
        assert chunks[3] == (datetime(2025, 1, 1, 1, 30), datetime(2025, 1, 1, 2, 0))

    def test_minutely_n30_partial_first_and_last(self):
        # Start 00:20 — floor=00:00, ceil=00:30; partial first [00:20, 00:30)
        # End 01:45 — partial last chunk [01:30, 01:45)
        chunks = generate_chunk_boundaries(
            start="2025-01-01T00:20:00",
            end="2025-01-01T01:45:00",
            interval={"minutes": 30},
        )
        assert len(chunks) == 4
        assert chunks[0] == (datetime(2025, 1, 1, 0, 20), datetime(2025, 1, 1, 0, 30))
        assert chunks[1] == (datetime(2025, 1, 1, 0, 30), datetime(2025, 1, 1, 1, 0))
        assert chunks[2] == (datetime(2025, 1, 1, 1, 0), datetime(2025, 1, 1, 1, 30))
        assert chunks[3] == (datetime(2025, 1, 1, 1, 30), datetime(2025, 1, 1, 1, 45))

    # ------------------------------------------------------------------ edge cases

    def test_single_chunk_when_entire_range_within_one_period(self):
        # start and end both within the same month — one chunk
        chunks = generate_chunk_boundaries(
            start="2025-03-10",
            end="2025-03-25",
            interval={"months": 1},
        )
        assert len(chunks) == 1
        assert chunks[0] == (date(2025, 3, 10), date(2025, 3, 25))

    def test_single_chunk_when_entire_range_within_one_week(self):
        # Both within same week (Jan 6–12 is Mon–Sun)
        chunks = generate_chunk_boundaries(
            start="2025-01-07",
            end="2025-01-10",
            interval={"weeks": 1},
        )
        assert len(chunks) == 1
        assert chunks[0] == (date(2025, 1, 7), date(2025, 1, 10))

    def test_aligned_start_produces_no_partial_first_chunk(self):
        # start is exactly on a quarterly boundary — no leading partial chunk
        chunks = generate_chunk_boundaries(
            start="2025-04-01",
            end="2025-07-01",
            interval={"months": 3},
        )
        assert len(chunks) == 1
        assert chunks[0] == (date(2025, 4, 1), date(2025, 7, 1))


# ============================================================================
# generate_chunk_boundaries — time-based (datetime objects)
# ============================================================================


class TestChunkBoundariesDatetime:
    """Chunk generation with datetime objects."""

    def test_datetime_monthly(self):
        start = datetime(2025, 1, 1)
        end = datetime(2025, 3, 1)
        chunks = generate_chunk_boundaries(start, end, {"months": 1})
        assert len(chunks) == 2
        assert chunks[0] == (datetime(2025, 1, 1), datetime(2025, 2, 1))
        assert chunks[1] == (datetime(2025, 2, 1), datetime(2025, 3, 1))

    def test_date_objects(self):
        start = date(2025, 1, 1)
        end = date(2025, 4, 1)
        chunks = generate_chunk_boundaries(start, end, {"months": 1})
        assert len(chunks) == 3
        # Should return date objects
        assert chunks[0] == (date(2025, 1, 1), date(2025, 2, 1))
        assert chunks[2] == (date(2025, 3, 1), date(2025, 4, 1))

    def test_datetime_with_timezone(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 2, 1, tzinfo=timezone.utc)
        chunks = generate_chunk_boundaries(start, end, {"days": 7})
        assert len(chunks) == 5  # 31 days / 7 = 4 full + 1 partial


# ============================================================================
# generate_chunk_boundaries — integer step
# ============================================================================


class TestChunkBoundariesInteger:
    """Chunk generation with integer watermark columns."""

    def test_even_split(self):
        chunks = generate_chunk_boundaries(start=0, end=300, interval={"step": 100})
        assert len(chunks) == 3
        assert chunks[0] == (0, 100)
        assert chunks[1] == (100, 200)
        assert chunks[2] == (200, 300)

    def test_partial_last_chunk(self):
        chunks = generate_chunk_boundaries(start=0, end=250, interval={"step": 100})
        assert len(chunks) == 3
        assert chunks[0] == (0, 100)
        assert chunks[1] == (100, 200)
        assert chunks[2] == (200, 250)

    def test_partial_first_chunk(self):
        # start=150 is not aligned to step=100; first boundary = 200
        chunks = generate_chunk_boundaries(start=150, end=400, interval={"step": 100})
        assert len(chunks) == 3
        assert chunks[0] == (150, 200)
        assert chunks[1] == (200, 300)
        assert chunks[2] == (300, 400)

    def test_step_2_aligned_start(self):
        chunks = generate_chunk_boundaries(start=0, end=10, interval={"step": 2})
        assert len(chunks) == 5
        assert chunks[0] == (0, 2)
        assert chunks[4] == (8, 10)

    def test_step_2_partial_first(self):
        # start=1 → first boundary = 2
        chunks = generate_chunk_boundaries(start=1, end=11, interval={"step": 2})
        assert len(chunks) == 6
        assert chunks[0] == (1, 2)
        assert chunks[1] == (2, 4)
        assert chunks[5] == (10, 11)

    def test_single_chunk_when_range_less_than_step(self):
        chunks = generate_chunk_boundaries(start=0, end=50, interval={"step": 100})
        assert len(chunks) == 1
        assert chunks[0] == (0, 50)

    def test_large_integer_range(self):
        chunks = generate_chunk_boundaries(
            start=1000000, end=2000000, interval={"step": 100000}
        )
        assert len(chunks) == 10
        assert chunks[0] == (1000000, 1100000)
        assert chunks[-1] == (1900000, 2000000)

    def test_zero_step_raises(self):
        with pytest.raises(ConfigurationError, match="must be a positive integer"):
            generate_chunk_boundaries(start=0, end=100, interval={"step": 0})

    def test_negative_step_raises(self):
        with pytest.raises(ConfigurationError, match="must be a positive integer"):
            generate_chunk_boundaries(start=0, end=100, interval={"step": -10})

    def test_start_gte_end_raises(self):
        with pytest.raises(ConfigurationError, match="must be less than"):
            generate_chunk_boundaries(start=100, end=100, interval={"step": 10})


# ============================================================================
# generate_chunk_boundaries — error cases
# ============================================================================


class TestChunkBoundariesErrors:
    """Edge cases and error handling."""

    def test_empty_interval_raises(self):
        with pytest.raises(ConfigurationError, match="must be a non-empty dict"):
            generate_chunk_boundaries(start="2025-01-01", end="2025-02-01", interval={})

    def test_invalid_time_keys_raises(self):
        with pytest.raises(ConfigurationError, match="must contain at least one time key"):
            generate_chunk_boundaries(
                start="2025-01-01", end="2025-02-01", interval={"invalid": 1}
            )

    def test_negative_time_value_raises(self):
        with pytest.raises(ConfigurationError, match="must be positive"):
            generate_chunk_boundaries(
                start="2025-01-01", end="2025-02-01", interval={"months": -1}
            )

    def test_unparseable_string_raises(self):
        with pytest.raises(ConfigurationError, match="Cannot parse"):
            generate_chunk_boundaries(
                start="not-a-date", end="2025-02-01", interval={"days": 1}
            )

    def test_unsupported_type_raises(self):
        with pytest.raises(ConfigurationError, match="Unsupported watermark type"):
            generate_chunk_boundaries(
                start=[1, 2, 3], end="2025-02-01", interval={"days": 1}
            )

        assert chunks[0] == (1000000, 1100000)
        assert chunks[-1] == (1900000, 2000000)

    def test_zero_step_raises(self):
        with pytest.raises(ConfigurationError, match="must be a positive integer"):
            generate_chunk_boundaries(start=0, end=100, interval={"step": 0})

    def test_negative_step_raises(self):
        with pytest.raises(ConfigurationError, match="must be a positive integer"):
            generate_chunk_boundaries(start=0, end=100, interval={"step": -10})

    def test_start_gte_end_raises(self):
        with pytest.raises(ConfigurationError, match="must be less than"):
            generate_chunk_boundaries(start=100, end=100, interval={"step": 10})


# ============================================================================
# generate_chunk_boundaries — error cases
# ============================================================================


class TestChunkBoundariesErrors:
    """Edge cases and error handling."""

    def test_empty_interval_raises(self):
        with pytest.raises(ConfigurationError, match="must be a non-empty dict"):
            generate_chunk_boundaries(start="2025-01-01", end="2025-02-01", interval={})

    def test_invalid_time_keys_raises(self):
        with pytest.raises(ConfigurationError, match="must contain at least one time key"):
            generate_chunk_boundaries(
                start="2025-01-01", end="2025-02-01", interval={"invalid": 1}
            )

    def test_negative_time_value_raises(self):
        with pytest.raises(ConfigurationError, match="must be positive"):
            generate_chunk_boundaries(
                start="2025-01-01", end="2025-02-01", interval={"months": -1}
            )

    def test_unparseable_string_raises(self):
        with pytest.raises(ConfigurationError, match="Cannot parse"):
            generate_chunk_boundaries(
                start="not-a-date", end="2025-02-01", interval={"days": 1}
            )

    def test_unsupported_type_raises(self):
        with pytest.raises(ConfigurationError, match="Unsupported watermark type"):
            generate_chunk_boundaries(
                start=[1, 2, 3], end="2025-02-01", interval={"days": 1}
            )
