"""Abstract base class for source readers.

``BaseSourceReader[DF]`` uses the **Template Method** pattern:

* Public :meth:`read` handles timing, watermark filtering,
  count/max calculation, file-info columns, and error wrapping.
* Subclasses implement :meth:`_read_internal` (and optionally
  :meth:`_read_data`) with format-specific logic.

The reader delegates all DataFrame operations to a :class:`BaseEngine`.
"""

from __future__ import annotations

import calendar
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Generic, List, Optional, Tuple

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, DataFlowStatus
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source, SourceRuntimeInfo
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.utils.helpers import utc_now

logger = get_logger(__name__)


class BaseSourceReader(ABC, Generic[DF]):
    """Abstract source reader with Template Method lifecycle.

    Subclasses must implement :meth:`_read_internal`.  Optionally they
    may also implement :meth:`_read_data` for the raw read step.

    Type parameter *DF* is the concrete DataFrame class.
    """

    def __init__(self, engine: BaseEngine[DF]) -> None:
        self._engine = engine
        self._new_watermark: Dict[str, Any] = {}
        self._runtime_info = SourceRuntimeInfo()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read data from a source (Template Method).

        1. Initialise runtime info.
        2. Delegate to :meth:`_read_internal`.
        3. Record timing and watermark data.

        Args:
            source: Pipeline source configuration.
            watermark: Previous watermark values (``None`` = first run).

        Returns:
            DataFrame with source data, or ``None`` when there is
            no data to process.

        Raises:
            SourceError: On any failure during reading.
        """
        self._runtime_info = SourceRuntimeInfo(
            start_time=utc_now(),
            status=DataFlowStatus.RUNNING.value,
            watermark_before=dict(watermark) if watermark else None,
        )

        # Apply backward offset to datetime watermark columns so late-arriving
        # data is not missed.  First-run (watermark=None) is left untouched.
        effective_watermark = self._build_effective_watermark(source, watermark)
        try:
            df = self._read_internal(source, effective_watermark)

            self._runtime_info.end_time = utc_now()

            if df is None:
                self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
                self._runtime_info.watermark_after = dict(self._new_watermark) if self._new_watermark else None
                return None

            self._runtime_info.status = DataFlowStatus.SUCCEEDED.value
            self._runtime_info.watermark_after = dict(self._new_watermark) if self._new_watermark else None
            return df

        except SourceError:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            raise
        except Exception as exc:
            self._runtime_info.end_time = utc_now()
            self._runtime_info.status = DataFlowStatus.FAILED.value
            self._runtime_info.error_message = str(exc)
            raise SourceError(
                f"Failed to read source: {exc}",
                details={"source_table": source.full_table_name, "source_path": source.path},
            ) from exc

    def get_runtime_info(self) -> SourceRuntimeInfo:
        """Return runtime information for the most recent read."""
        return self._runtime_info

    def get_new_watermark(self) -> Dict[str, Any]:
        """Return the new watermark values computed during the last read."""
        return self._new_watermark

    # ------------------------------------------------------------------
    # Abstract methods (subclass contract)
    # ------------------------------------------------------------------

    @abstractmethod
    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read data from the source (subclass implementation).

        This is the core reading logic.  The base class handles timing,
        error wrapping, and runtime-info population.

        Args:
            source: Pipeline source configuration.
            watermark: Previous watermark values.

        Returns:
            DataFrame or ``None`` if no data available.
        """

    @abstractmethod
    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Perform the raw read operation (format-specific).

        Args:
            source: Pipeline source configuration.
            configure: Additional read configuration.

        Returns:
            Raw DataFrame from the data source.
        """

    # ------------------------------------------------------------------
    # Protected helpers
    # ------------------------------------------------------------------

    def _apply_watermark_filter(
        self,
        df: DF,
        watermark_columns: List[str],
        watermark: Dict[str, Any],
    ) -> DF:
        """Apply incremental watermark filter to the DataFrame.

        Builds an OR condition across watermark columns:
        ``col1 > val1 OR col2 > val2 ...``

        Delegates the actual filtering to :meth:`BaseEngine.apply_watermark_filter`
        which uses native DataFrame API.
        ``DATE_FOLDER_PARTITION_KEY`` entries are skipped (handled by
        :class:`~datacoolie.sources.file_reader.FileReader`).
        """
        if not watermark_columns or not watermark:
            return df

        # Filter out DATE_FOLDER_PARTITION_KEY and columns with no watermark value
        filtered_columns = [
            col for col in watermark_columns
            if col != DATE_FOLDER_PARTITION_KEY and watermark.get(col) is not None
        ]

        if not filtered_columns:
            return df

        filtered_watermark = {col: watermark[col] for col in filtered_columns}

        logger.debug(
            "Applying watermark filter on columns: %s",
            filtered_columns,
        )
        return self._engine.apply_watermark_filter(df, filtered_columns, filtered_watermark)

    def _calculate_count_and_new_watermark(
        self,
        df: DF,
        watermark_columns: List[str],
    ) -> Tuple[int, Dict[str, Any]]:
        """Calculate row count and new watermark values in one pass.

        If *watermark_columns* is empty, only the count is retrieved.

        Returns:
            ``(row_count, {column: max_value})``
        """
        if not watermark_columns:
            return self._engine.count_rows(df), {}

        count, max_values = self._engine.get_count_and_max_values(df, watermark_columns)
        return count, max_values

    def _set_rows_read(self, rows_read: int) -> None:
        """Record the number of rows read."""
        self._runtime_info.rows_read = rows_read

    def _set_source_action(self, source_action: Dict[str, Any]) -> None:
        """Record the actual source action performed (path, query, function, etc.)."""
        self._runtime_info.source_action = source_action

    def _set_new_watermark(self, watermark: Optional[Dict[str, Any]]) -> None:
        """Store the computed new watermark values."""
        self._new_watermark = watermark or {}

    def _finalize_read(
        self,
        df: DF,
        watermark_columns: List[str],
        reader_name: str,
        context: str,
    ) -> Optional[DF]:
        """Record count/watermark and return *df* or ``None`` on zero rows.

        This is the common epilogue shared by simple readers (Delta,
        Iceberg, Database, PythonFunction).  Readers with extra
        post-processing (FileReader, APIReader) implement the steps
        inline.

        Args:
            df: DataFrame after reading and watermark filtering.
            watermark_columns: Columns to compute new watermark from.
            reader_name: Human label for log messages (e.g. ``"DeltaReader"``).
            context: Identifier for the data source (table name, path, etc.).

        Returns:
            *df* when rows exist, ``None`` otherwise.
        """
        count, new_wm = self._calculate_count_and_new_watermark(
            df, watermark_columns,
        )

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)

        if count == 0:
            logger.info("%s: 0 rows after filtering — skipping. %s", reader_name, context)
            return None

        logger.info("%s: read %d rows from %s", reader_name, count, context)
        return df

    # ------------------------------------------------------------------
    # Backward offset helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_effective_watermark(
        source: Source,
        watermark: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Return a watermark adjusted by the source's backward offset.

        On first run (*watermark* is ``None``) or when no backward offset is
        configured, the original value is returned unchanged.

        Two offset strategies are supported (configured via ``date_backward``):

        **Fixed offset** — ``{days: 7}`` / ``{months: 1}`` / ``{hours: 6}``.
        Subtracts the offset from the stored watermark value.

        **Closing-day** — ``{closing_day: 10}``.  Computes an absolute
        start boundary from the current date (takes priority over offset keys).

        **File-reader path** (``DATE_FOLDER_PARTITION_KEY`` present):
        The offset is applied *only* to ``DATE_FOLDER_PARTITION_KEY`` (parsed
        from its ISO-8601 string and written back as ISO-8601).  All other
        watermark columns are passed through unchanged so that regular datetime
        filters remain anchored to the original stored value.

        **All other readers**: every ``datetime`` value is adjusted by the
        backward offset; non-datetime values pass through unchanged.
        """
        backward = source.date_backward
        if not watermark or not backward:
            return watermark

        closing_day = backward.get("closing_day")

        def _offset(dt: datetime) -> datetime:
            if closing_day is not None:
                return BaseSourceReader._apply_closing_day_backward(
                    utc_now(),
                    int(closing_day),
                    months=int(backward.get("months", 0)),
                    years=int(backward.get("years", 0)),
                )
            return BaseSourceReader._apply_backward(dt, backward)

        # -- file-reader path: only adjust DATE_FOLDER_PARTITION_KEY ----------
        if DATE_FOLDER_PARTITION_KEY in watermark:
            raw = watermark[DATE_FOLDER_PARTITION_KEY]
            dt: Optional[datetime] = None
            if isinstance(raw, datetime):
                dt = raw
            elif isinstance(raw, str):
                try:
                    dt = datetime.fromisoformat(raw)
                except (ValueError, TypeError):
                    pass
            if dt is None:
                return watermark
            result = dict(watermark)
            result[DATE_FOLDER_PARTITION_KEY] = _offset(dt)
            return result

        # -- all other readers: adjust every datetime column ------------------
        adjusted: Dict[str, Any] = {}
        for col, val in watermark.items():
            if isinstance(val, datetime):
                adjusted[col] = _offset(val)
            elif type(val) is date:  # exact check — datetime IS-A date, must come after datetime
                dt_mid = datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
                adjusted[col] = _offset(dt_mid).date()
            elif isinstance(val, str):
                try:
                    dt_val = datetime.fromisoformat(val)
                    adjusted[col] = _offset(dt_val).isoformat()
                except (ValueError, TypeError):
                    adjusted[col] = val
            else:
                adjusted[col] = val
        return adjusted

    @staticmethod
    def _apply_backward(dt: datetime, backward: Dict[str, Any]) -> datetime:
        """Subtract a look-back offset from *dt*.

        Accepted keys in *backward*: ``days``, ``months``, ``years``, ``hours``.
        Month/year subtraction uses calendar-safe arithmetic (no third-party deps).

        Examples::

            _apply_backward(now, {"days": 7})             # 7 days back
            _apply_backward(now, {"months": 1})           # 1 month back
            _apply_backward(now, {"years": 1})            # 1 year back
            _apply_backward(now, {"days": 3, "hours": 6})
        """
        days = int(backward.get("days", 0))
        months = int(backward.get("months", 0))
        years = int(backward.get("years", 0))
        hours = int(backward.get("hours", 0))
        result = dt - timedelta(days=days, hours=hours)
        total_months = months + years * 12
        if total_months:
            total = result.year * 12 + result.month - 1 - total_months
            year, rem = divmod(total, 12)
            month = rem + 1
            max_day = calendar.monthrange(year, month)[1]
            result = result.replace(
                year=year, month=month, day=min(result.day, max_day)
            )
        return result

    @staticmethod
    def _apply_closing_day_backward(
        now: datetime,
        closing_day: int,
        months: int = 0,
        years: int = 0,
    ) -> datetime:
        """Compute the start boundary for a monthly closing-day rule.

        First determines the base boundary month:

        * ``today.day <= closing_day`` → 1st of **previous** month.
        * ``today.day >  closing_day`` → 1st of **current** month.

        Then subtracts additional *months* and *years* from that
        boundary, allowing longer look-back windows.

        Examples (closing_day=10)::

            March  8            → Feb 1   (8 <= 10)
            March 12            → Mar 1   (12 > 10)
            March  8, months=2  → Dec 1 prev year
            March  8, years=1   → Feb 1 prev year

        Args:
            now: The reference datetime (typically *utc_now()*).
            closing_day: Day-of-month that marks the period boundary.
            months: Additional months to subtract from the boundary.
            years: Additional years to subtract from the boundary.

        Returns:
            A :class:`~datetime.datetime` set to midnight UTC on the
            1st of the applicable month.
        """
        tz = now.tzinfo
        if now.day <= closing_day:
            # Previous month
            if now.month == 1:
                base = datetime(now.year - 1, 12, 1, tzinfo=tz)
            else:
                base = datetime(now.year, now.month - 1, 1, tzinfo=tz)
        else:
            # Current month
            base = datetime(now.year, now.month, 1, tzinfo=tz)

        # Apply additional month/year offset
        extra = months + years * 12
        if extra:
            total = base.year * 12 + base.month - 1 - extra
            year, rem = divmod(total, 12)
            month = rem + 1
            base = datetime(year, month, 1, tzinfo=tz)

        return base
