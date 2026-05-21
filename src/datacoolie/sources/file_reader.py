"""File-based source reader (Parquet, CSV, JSON, Avro, etc.).

Reads flat files from storage, supporting date-folder partitioning
patterns and optional platform-based folder discovery.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from datacoolie.core.constants import (
    DATE_FOLDER_PARTITION_KEY,
    DATE_PLACEHOLDERS,
    FileInfoColumn,
)
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import FileInfo
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)


class FileReader(BaseSourceReader[DF]):
    """Source reader for flat files (Parquet, CSV, JSON, Avro, etc.).

    Supports:
    * Single path and multi-path reads.
    * Date-folder partition patterns (``{year}/{month}/{day}``...).
    * Watermark-based incremental reads with folder pruning.
    * File-info column injection (``__file_name``, etc.).
    """

    def __init__(
        self,
        engine: BaseEngine[DF],
    ) -> None:
        super().__init__(engine)

    # ------------------------------------------------------------------
    # Core reading
    # ------------------------------------------------------------------

    def _read_internal(
        self,
        source: Source,
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_end: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read files with optional date-folder pruning and watermark filter.

        Steps:
            1. Determine date-folder pattern (if configured).
            2. Discover source paths (prune by date watermark).
            3. Optionally collect file infos for mtime-watermarked sources.
            4. Read raw data via :meth:`_read_data`.
            5. Add file-info columns.
            6. Apply watermark filter (lower + optional upper end).
            7. Calculate count and new watermark.
        """
        if not source.path:
            raise SourceError("FileReader requires source.path")

        date_pattern = source.connection.date_folder_partitions
        paths: List[str]
        
        if date_pattern and self._engine.platform:
            date_start_wm: Optional[datetime] = self._parse_date_watermark(
                (watermark_start or {}).get(DATE_FOLDER_PARTITION_KEY)
            )
            date_end_wm: Optional[datetime] = self._parse_date_watermark(
                (watermark_end or {}).get(DATE_FOLDER_PARTITION_KEY)
            )
            paths = self._get_date_folder_paths(
                source.path, date_pattern, date_start_wm, date_end_wm,
            )
        else:
            paths = [source.path]
        
        # -- File-level mtime filtering -----------------------------------------
        # Collect file infos whenever (__file_modification_time is a watermark
        # column or engine is not SparkEngine) and a platform is available.
        # Covers both the first load (mtime_wm_raw=None → all files) 
        # and incremental loads (mtime set → only newer files).
        file_infos: Optional[List[FileInfo]] = None
        folder_paths = paths  # preserve for _get_max_date_folder_dt
        _tracks_mtime = (
            self._engine.platform is not None
            and (
                    FileInfoColumn.FILE_MODIFICATION_TIME in (source.watermark_columns or [])
                    or type(self._engine).__name__ not in ("SparkEngine",)
                )
        )
        if _tracks_mtime:
            mtime_wm_start_raw = (watermark_start or {}).get(FileInfoColumn.FILE_MODIFICATION_TIME)
            mtime_wm_end_raw = (watermark_end or {}).get(FileInfoColumn.FILE_MODIFICATION_TIME)
            file_infos = self._collect_file_infos(
                paths, source.connection.format, mtime_wm_start_raw, mtime_wm_end_raw,
                start_operator=self._watermark_start_operator,
                end_operator=self._watermark_end_operator,
                recursive=source.connection.use_hive_partitioning,
            )
            paths = [fi.path for fi in file_infos]

        self._set_source_action({"reader": type(self).__name__, "format": source.connection.format, "paths": paths})
        
        if not paths:
            logger.debug("%s: no paths found — skipping. Table: %s (format: %s), Source path: %s", type(self).__name__, source.full_table_name, source.connection.format, source.path)
            self._set_rows_read(0)
            self._set_new_watermark(watermark_start or {})
            return None

        # Read and enrich
        df = self._read_data(source, {"paths": paths})
        df = self._engine.add_file_info_columns(df, file_infos=file_infos)

        # Columns handled outside the DataFrame and must be excluded from both
        # the watermark filter and the DataFrame-based max calculation:
        #   - DATE_FOLDER_PARTITION_KEY  → resolved by folder pruning above
        #   - FILE_MODIFICATION_TIME     → resolved from file listing (file_infos)
        #     or set to null (no platform); never a meaningful df column to filter/max
        _wm_skip = {DATE_FOLDER_PARTITION_KEY, FileInfoColumn.FILE_MODIFICATION_TIME}

        if watermark_start or watermark_end:
            if source.watermark_columns:
                wm_filter_cols = [c for c in source.watermark_columns if c not in _wm_skip]
                if wm_filter_cols:
                    df = self._apply_watermark_filter(df, wm_filter_cols, watermark_start or {}, watermark_end)

        df = self._apply_filter_expression(df, source)

        wm_df_cols = [c for c in (source.watermark_columns or []) if c not in _wm_skip]
        count, new_wm = self._calculate_count_and_new_watermark(df, wm_df_cols)

        if FileInfoColumn.FILE_MODIFICATION_TIME in (source.watermark_columns or []) and file_infos:
            new_wm[FileInfoColumn.FILE_MODIFICATION_TIME] = max(
                fi.modification_time for fi in file_infos
            )

        # Merge date-folder watermark into new watermark (stored as ISO-8601)
        # Use the original folder paths (before _collect_file_infos rewrote
        # them to individual file paths) so the date-pattern regex matches.
        if date_pattern and paths:
            max_date_dt = self._get_max_date_folder_dt(folder_paths, date_pattern)
            if max_date_dt is not None:
                new_wm[DATE_FOLDER_PARTITION_KEY] = max_date_dt.isoformat()

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)

        if count == 0:
            logger.debug("%s: 0 rows after filtering — skipping. Table: %s (format: %s), Source path: %s", type(self).__name__, source.full_table_name, source.connection.format, source.path)
            return None

        logger.debug("%s: read %d rows from %d path(s). Table: %s (format: %s), Source path: %s", type(self).__name__, count, len(paths), source.full_table_name, source.connection.format, source.path)
        return df

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
    ) -> DF:
        """Read files in the format specified by the source connection.

        Raises:
            SourceError: If the format is unsupported.
        """
        configure = configure or {}
        paths: List[str] = configure.get("paths", [source.path])  # type: ignore[arg-type]
        fmt = source.connection.format
        options = dict(source.read_options)

        # Pass framework-level hive-partitioning flag; each engine
        # translates it to the native option (Spark → basePath,
        # Polars → hive_partitioning, etc.).
        if source.connection.use_hive_partitioning:
            options.setdefault("use_hive_partitioning", source.path)

        read_path = paths[0] if len(paths) == 1 else paths  # type: ignore[assignment]
        return self._engine.read(fmt=fmt, path=read_path, options=options)

    # ------------------------------------------------------------------
    # Watermark & backward helpers
    # ------------------------------------------------------------------

    def _collect_file_infos(
        self,
        paths: List[str],
        fmt: str,
        mtime_wm_start: Any,
        mtime_wm_end: Any = None,
        *,
        start_operator: str = ">",
        end_operator: str = "<",
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List all files under *paths* and return those within the mtime window.

        Uses :meth:`~datacoolie.platforms.base.BasePlatform.list_files` with a
        ``".{fmt}"`` extension filter.  Errors on individual paths are logged
        as warnings and that path is skipped (same behaviour as
        :meth:`_get_date_folder_paths`).

        Args:
            paths: Leaf folder paths to scan.
            fmt: Source format string — used to build the extension filter
                (e.g. ``"parquet"`` → ``".parquet"``).
            mtime_wm_start: Lower-bound watermark (ISO-8601 string, ``datetime``, or
                ``None``); ``None`` disables lower-bound filtering.
            mtime_wm_end: Upper-bound watermark (ISO-8601 string, ``datetime``,
                or ``None``); ``None`` disables upper-bound filtering.
            start_operator: Comparison operator for the lower bound
                (``">"`` or ``">="``).  Defaults to ``">"``.
            end_operator: Comparison operator for the upper bound
                (``"<"`` or ``"<="``).  Defaults to ``"<"``.
            recursive: When ``True`` (hive-partitioned sources), descend into
                sub-directories such as ``region=ABC/``.  Defaults to
                ``False`` for flat layouts.

        Returns:
            :class:`~datacoolie.platforms.base.FileInfo` list, sorted by
            ``modification_time`` ascending so the newest high-water mark is
            always the last element.
        """
        mtime_start_dt = self._parse_date_watermark(mtime_wm_start)
        mtime_end_dt = self._parse_date_watermark(mtime_wm_end)
        _cmp = {
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
        }
        lower_cmp = _cmp.get(start_operator, lambda a, b: a > b)
        upper_cmp = _cmp.get(end_operator, lambda a, b: a < b)
        ext = self._engine.FORMAT_EXTENSIONS.get(fmt, f".{fmt}")
        result: List[FileInfo] = []

        for path in paths:
            try:
                files = self._engine.platform.list_files(path, extension=ext, recursive=recursive)  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("%s: failed to list files at %s: %s", type(self).__name__, path, exc)
                continue

            for fi in files:
                if fi.is_dir:
                    continue
                if fi.modification_time is None:
                    continue
                if mtime_start_dt is not None and not lower_cmp(fi.modification_time, mtime_start_dt):
                    continue
                if mtime_end_dt is not None and not upper_cmp(fi.modification_time, mtime_end_dt):
                    continue
                result.append(fi)

        result.sort(key=lambda fi: fi.modification_time)  # type: ignore[arg-type]
        return result

    @staticmethod
    def _parse_date_watermark(raw: Any) -> Optional[datetime]:
        """Parse a stored date-folder watermark to a UTC *datetime*.

        Accepts:

        * ``None`` — returns ``None``.
        * ``datetime`` instance — returns as-is (adds UTC if naïve).
        * ISO-8601 string — e.g. ``"2024-03-15T00:00:00+00:00"``.
        """
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, str):
            dt = datetime.fromisoformat(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return None

    # ------------------------------------------------------------------
    # Date-folder discovery
    # ------------------------------------------------------------------

    def _get_date_folder_paths(
        self,
        base_path: str,
        date_pattern: str,
        watermark_start: Optional[datetime] = None,
        watermark_end: Optional[datetime] = None,
    ) -> List[str]:
        """Discover date-partitioned sub-folders under *base_path*.

        Prunes folder levels against *watermark_start* (lower bound) and
        *watermark_end* (upper bound).  Boundary folders are always
        **included** — folder pruning is inherently conservative because a
        folder covers a date range, not a point.  The precise row-level
        operator (``>``/``>=``, ``<``/``<=``) is applied later by
        :meth:`_apply_watermark_filter`, giving an effective window of
        ``[start, end]`` at folder granularity.

        Args:
            base_path: Root data path.
            date_pattern: Date pattern string (e.g. ``{year}/{month}/{day}``).
            watermark_start: Earliest folder to include as a UTC *datetime*.
                ``None`` disables lower-bound pruning.
            watermark_end: Latest folder to include as a UTC *datetime*.
                ``None`` disables upper-bound pruning.

        Returns:
            Chronologically sorted list of leaf folder paths.
        """
        if not self._engine.platform:
            return [base_path]

        levels = self._parse_date_levels(date_pattern)
        if not levels:
            return [base_path]

        def _to_parts(dt: Optional[datetime]) -> Optional[Dict[str, int]]:
            if dt is None:
                return None
            d = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return {"year": d.year, "month": d.month, "day": d.day, "hour": d.hour}

        wm_start_parts = _to_parts(watermark_start)
        wm_end_parts = _to_parts(watermark_end)

        # Frontier: (path, on_lower_boundary, on_upper_boundary)
        # on_lower_boundary=True → all parent levels == watermark_start; must still prune.
        # on_upper_boundary=True → all parent levels == watermark_end; must still prune.
        frontier: List[Tuple[str, bool, bool]] = [(base_path, wm_start_parts is not None, wm_end_parts is not None)]

        for component, level_regex in levels:
            pat = re.compile(level_regex)
            next_frontier: List[Tuple[str, bool, bool]] = []

            for path, on_lower, on_upper in frontier:
                try:
                    children = self._engine.platform.list_folders(path, recursive=False)
                except Exception as exc:
                    logger.warning(
                        "%s: failed to list folders at %s: %s", type(self).__name__, path, exc
                    )
                    continue

                for child in children:
                    folder_name = child.rstrip("/").rsplit("/", 1)[-1]
                    m = pat.match(folder_name)
                    if not m:
                        continue

                    val = int(m.group(1))

                    # Lower-bound pruning: prune val < wm_val.
                    # Boundary folders (val == wm_val) are always kept;
                    # row-level filtering applies the precise start_operator.
                    if on_lower and wm_start_parts is not None:
                        wm_val = wm_start_parts[component]
                        if val < wm_val:
                            continue  # older than watermark_start — prune
                        child_on_lower = val == wm_val
                    else:
                        child_on_lower = False

                    # Upper-bound pruning: prune val > end_val.
                    # Boundary folders (val == end_val) are always kept;
                    # row-level filtering applies the precise end_operator.
                    if on_upper and wm_end_parts is not None:
                        end_val = wm_end_parts[component]
                        if val > end_val:
                            continue  # newer than watermark_end — prune
                        child_on_upper = val == end_val
                    else:
                        child_on_upper = False

                    next_frontier.append((child, child_on_lower, child_on_upper))

            frontier = next_frontier
            if not frontier:
                break

        return sorted(p for p, _, _ in frontier)

    @staticmethod
    def _parse_date_levels(date_pattern: str) -> List[Tuple[str, str]]:
        """Break *date_pattern* into ordered per-level ``(component, regex)`` pairs.

        Each entry corresponds to one depth level (separated by ``/``) and
        contains:

        * **component** — the date component name (``"year"``, ``"month"``,
          ``"day"``, ``"hour"``).
        * **regex** — anchored pattern that matches a single folder name at
          that level and captures the numeric value in group ``1``.

        Examples::

            "{year}/{month}/{day}"
            -> [("year", r"^(\\d{4})$"),
               ("month", r"^(\\d{2})$"),
               ("day",   r"^(\\d{2})$")]

            "year={year}/month={month}"
            -> [("year",  r"^year=(\\d{4})$"),
               ("month", r"^month=(\\d{2})$")]
        """
        result: List[Tuple[str, str]] = []
        for segment in date_pattern.split("/"):
            for placeholder, (re_part, component) in DATE_PLACEHOLDERS.items():
                if placeholder in segment:
                    level_regex = "^" + segment.replace(placeholder, re_part) + "$"
                    result.append((component, level_regex))
                    break
        return result

    @staticmethod
    def _build_date_regex(pattern: str) -> Tuple[str, List[str]]:
        """Convert a date-folder pattern to a regex and group names.

        Args:
            pattern: Pattern like ``{year}/{month}/{day}``.

        Returns:
            ``(regex_string, [group_names])``
        """
        regex = pattern
        groups: List[str] = []
        for placeholder, (re_part, group_name) in DATE_PLACEHOLDERS.items():
            if placeholder in regex:
                regex = regex.replace(placeholder, re_part)
                groups.append(group_name)
        # Anchor the regex
        regex = f"^{regex}$"
        return regex, groups

    @staticmethod
    def _date_groups_to_datetime(match: re.Match, groups: List[str]) -> datetime:  # type: ignore[type-arg]
        """Convert captured date regex groups to a UTC *datetime*.

        Example: year=2024, month=03, day=15 → datetime(2024, 3, 15, tzinfo=UTC)
        """
        parts: Dict[str, str] = {}
        for i, group_name in enumerate(groups):
            parts[group_name] = match.group(i + 1)

        return datetime(
            year=int(parts.get("year", "1970")),
            month=int(parts.get("month", "1")),
            day=int(parts.get("day", "1")),
            hour=int(parts.get("hour", "0")),
            tzinfo=timezone.utc,
        )

    @staticmethod
    def _get_max_date_folder_dt(
        paths: List[str],
        date_pattern: str,
    ) -> Optional[datetime]:
        """Extract the maximum date-folder *datetime* from a list of paths.

        Returns ``None`` if no paths match the pattern.
        """
        regex, groups = FileReader._build_date_regex(date_pattern)
        max_dt: Optional[datetime] = None

        for path in paths:
            # Try matching on the trailing part of the path
            segments = path.rstrip("/").split("/")
            depth = date_pattern.count("/") + 1
            if len(segments) >= depth:
                trailing = "/".join(segments[-depth:])
                m = re.match(regex, trailing)
                if m:
                    val = FileReader._date_groups_to_datetime(m, groups)
                    if max_dt is None or val > max_dt:
                        max_dt = val

        return max_dt
