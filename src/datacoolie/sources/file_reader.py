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
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        """Read files with optional date-folder pruning and watermark filter.

        Steps:
            1. Determine date-folder pattern (if configured).
            2. Discover source paths (prune by date watermark).
            3. Optionally collect file infos for mtime-watermarked sources.
            4. Read raw data via :meth:`_read_data`.
            5. Add file-info columns.
            6. Apply watermark filter (non-mtime columns only).
            7. Calculate count and new watermark.
        """
        if not source.path:
            raise SourceError("FileReader requires source.path")

        date_pattern = source.connection.date_folder_partitions
        paths: List[str]
        
        if date_pattern and self._engine.platform:
            date_wm: Optional[datetime] = self._parse_date_watermark(
                (watermark or {}).get(DATE_FOLDER_PARTITION_KEY)
            )
            paths = self._get_date_folder_paths(source.path, date_pattern, date_wm)
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
            mtime_wm_raw = (watermark or {}).get(FileInfoColumn.FILE_MODIFICATION_TIME)
            file_infos = self._collect_file_infos(
                paths, source.connection.format, mtime_wm_raw,
                recursive=source.connection.use_hive_partitioning,
            )
            paths = [fi.path for fi in file_infos]

        self._set_source_action({"reader": type(self).__name__, "format": source.connection.format, "paths": paths})
        
        if not paths:
            logger.info("FileReader: no paths found — skipping. Table: %s (format: %s), Source path: %s", source.full_table_name, source.connection.format, source.path)
            self._set_rows_read(0)
            self._set_new_watermark(watermark or {})
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

        if watermark and source.watermark_columns:
            wm_filter_cols = [c for c in source.watermark_columns if c not in _wm_skip]
            if wm_filter_cols:
                df = self._apply_watermark_filter(df, wm_filter_cols, watermark)

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
            logger.info("FileReader: 0 rows after filtering — skipping. Table: %s (format: %s), Source path: %s", source.full_table_name, source.connection.format, source.path)
            return None

        logger.info("FileReader: read %d rows from %d path(s). Table: %s (format: %s), Source path: %s", count, len(paths), source.full_table_name, source.connection.format, source.path)
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
        mtime_wm: Any,
        *,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List all files under *paths* and return those newer than *mtime_wm*.

        Uses :meth:`~datacoolie.platforms.base.BasePlatform.list_files` with a
        ``".{fmt}"`` extension filter.  Errors on individual paths are logged
        as warnings and that path is skipped (same behaviour as
        :meth:`_get_date_folder_paths`).

        Args:
            paths: Leaf folder paths to scan.
            fmt: Source format string — used to build the extension filter
                (e.g. ``"parquet"`` → ``".parquet"``).
            mtime_wm: Watermark value (ISO-8601 string, ``datetime``, or
                ``None``); only files **strictly newer** are returned.
            recursive: When ``True`` (hive-partitioned sources), descend into
                sub-directories such as ``region=ABC/``.  Defaults to
                ``False`` for flat layouts.

        Returns:
            :class:`~datacoolie.platforms.base.FileInfo` list, sorted by
            ``modification_time`` ascending so the newest high-water mark is
            always the last element.
        """
        mtime_dt = self._parse_date_watermark(mtime_wm)
        ext = self._engine.FORMAT_EXTENSIONS.get(fmt, f".{fmt}")
        result: List[FileInfo] = []

        for path in paths:
            try:
                files = self._engine.platform.list_files(path, extension=ext, recursive=recursive)  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("FileReader: failed to list files at %s: %s", path, exc)
                continue

            for fi in files:
                if fi.is_dir:
                    continue
                if fi.modification_time is None:
                    continue
                if mtime_dt is None or fi.modification_time > mtime_dt:
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
        watermark_value: Optional[datetime] = None,
    ) -> List[str]:
        """Discover date-partitioned sub-folders under *base_path*.

        Walks folder levels **one depth at a time** (non-recursive) using the
        structure of *date_pattern*, pruning each level against *watermark_value*
        so only the minimum required folders are ever listed.

        Pruning logic per level:

        * **On boundary** (all parent levels exactly equal the watermark):
          skip folders whose component value < watermark's component.
          If equal, stay on boundary; if greater, leave boundary (no more
          pruning needed at deeper levels).
        * **Off boundary** (a parent level already exceeded the watermark):
          accept every folder — no further pruning needed.

        Args:
            base_path: Root data path.
            date_pattern: Date pattern string (e.g. ``{year}/{month}/{day}``).
            watermark_value: Earliest folder to include as a UTC *datetime*.
                ``None`` returns all matching folders.

        Returns:
            Chronologically sorted list of leaf folder paths.
        """
        if not self._engine.platform:
            return [base_path]

        levels = self._parse_date_levels(date_pattern)
        if not levels:
            return [base_path]

        # Decompose watermark into per-component integer thresholds once.
        wm_parts: Optional[Dict[str, int]] = None
        if watermark_value is not None:
            wm = (
                watermark_value
                if watermark_value.tzinfo
                else watermark_value.replace(tzinfo=timezone.utc)
            )
            wm_parts = {
                "year": wm.year,
                "month": wm.month,
                "day": wm.day,
                "hour": wm.hour,
            }

        # Frontier: (path, on_boundary)
        # on_boundary=True  → all parent levels == watermark; must still prune here.
        # on_boundary=False → a parent level already exceeded watermark; accept all.
        frontier: List[Tuple[str, bool]] = [(base_path, wm_parts is not None)]
        
        for component, level_regex in levels:
            pat = re.compile(level_regex)
            next_frontier: List[Tuple[str, bool]] = []
            
            for path, on_boundary in frontier:
                try:
                    children = self._engine.platform.list_folders(path, recursive=False)
                except Exception as exc:
                    logger.warning(
                        "FileReader: failed to list folders at %s: %s", path, exc
                    )
                    continue

                for child in children:
                    folder_name = child.rstrip("/").rsplit("/", 1)[-1]
                    m = pat.match(folder_name)
                    if not m:
                        continue

                    if on_boundary and wm_parts is not None:
                        val = int(m.group(1))
                        wm_val = wm_parts[component]
                        if val < wm_val:
                            continue  # older than watermark — prune
                        child_on_boundary = val == wm_val
                    else:
                        child_on_boundary = False

                    next_frontier.append((child, child_on_boundary))

            frontier = next_frontier
            if not frontier:
                break
        
        return sorted(p for p, _ in frontier)

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
