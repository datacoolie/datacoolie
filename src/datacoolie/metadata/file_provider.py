"""File-based metadata provider — reads connections, dataflows, and schema hints
from YAML, JSON, or Excel ``.xlsx`` configuration files.

``FileProvider`` is the primary standalone / development metadata backend.
Watermark state is stored as JSON files on the platform's file system.

Supported formats
-----------------

**YAML / JSON** — hierarchical, nested structure:

.. code-block:: yaml

    connections:
      - name: bronze_adls
        connection_type: lakehouse
        format: delta
        configure:
          base_path: abfss://bronze@storage/
          use_schema_hint: true

      - name: source_erp
        connection_type: database
        format: sql
        database: ERP
        configure:
          host: erp-server.example.com
          port: 1433
          username: etl_reader

    dataflows:
      - name: orders_bronze_to_silver
        stage: bronze2silver
        source:
          connection_name: bronze_adls
          table: orders
          watermark_columns: [modified_at]
        destination:
          connection_name: silver_lakehouse
          table: dim_orders
          load_type: merge_upsert
          merge_keys: [order_id]
        transform:
          schema_hints:
            - column_name: amount
              data_type: DECIMAL
              precision: 18
              scale: 2

    schema_hints:
      - connection_name: bronze_adls
        table_name: orders
        hints:
          - column_name: order_date
            data_type: DATE
            format: yyyy-MM-dd

**Excel (.xlsx)** — flat workbook with one or more of three supported sheets
(legacy ``.xls`` is not supported):

*connections* sheet — one row per connection:

  Required: ``name``, ``connection_type``
  Optional: ``connection_id``, ``format``, ``catalog``, ``database``, ``is_active``
  Nested ``configure`` via ``configure_*`` columns (e.g. ``configure_base_path``, ``configure_host``,
    ``configure_use_schema_hint``)
  ``catalog`` and ``database`` are top-level columns (not ``configure_catalog`` or ``configure_database``)
  ``secrets_ref``: JSON object mapping source identifiers to lists of
    ``configure`` field names (e.g. ``{"env:": ["password", "api_key"]}``)

*dataflows* sheet — one row per dataflow:

  Required: ``source_connection_name``, ``source_table``,
    ``destination_connection_name``, ``destination_table``
  Optional: ``dataflow_id``, ``name``, ``stage``, ``description``, ``group_number``,
    ``execution_order``, ``processing_mode``, ``is_active``
  ``configure``: JSON column for the dataflow's own configure dict.
  ``transform``: JSON column for the full transform dict (individual ``transform_*``
    columns take precedence when both are present).
  Source fields prefixed ``source_``; destination fields prefixed ``destination_``;
  List columns (e.g. ``source_watermark_columns``, ``destination_merge_keys``,
  ``destination_partition_columns``) accept comma-separated values.
  ``transform_select_columns`` and ``transform_drop_columns`` accept
  comma-separated names. ``transform_schema_hints``,
  ``transform_deduplicate_columns``, ``transform_latest_data_columns``,
  ``transform_additional_columns``, ``transform_rename_columns``,
  ``transform_value_rules``, ``transform_hash_columns``, and
  ``transform_masking_rules`` accept JSON strings.

*schema_hints* sheet — one row per hint (grouped internally by
  ``connection_name`` or ``connection_id`` + ``table_name`` + optional
  ``schema_name``):

  Columns: ``connection_name`` or ``connection_id``, ``table_name``,
    ``schema_name`` (optional), ``column_name``, ``data_type``, ``precision``,
    ``scale``, ``format``

**Separate files** — each section may live in its own file instead of (or
in addition to) the primary ``config_path``.  Pass any combination of:

* ``connections_path`` — file containing a ``{"connections": [...]}`` wrapper.
* ``schema_hints_path`` — file containing a ``{"schema_hints": [...]}`` wrapper.

Each override file can be YAML, JSON, or Excel in the same format as the
corresponding section in the primary file.  When a separate path is
specified, its section **replaces** the same section from the primary file.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Dict, List, Optional, Tuple

from datacoolie.core.constants import WATERMARK_FILE_NAME
from datacoolie.core.exceptions import ConfigurationError, MetadataError, PlatformError, WatermarkError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.transform import SchemaHint
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
from datacoolie.metadata.documents.parsers import (
    METADATA_SECTION_KEYS,
    SUPPORTED_METADATA_SUFFIXES,
    parse_json_document,
    parse_yaml_document,
    validate_document,
)
from datacoolie.metadata.documents.excel import parse_excel
from datacoolie.metadata.documents.mapping import (
    build_connections,
    build_dataflows,
    build_grouped_schema_hints,
)
from datacoolie.metadata.resolution.schema_hints import (
    select_schema_hints,
)
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.path_utils import (
    join_path,
    normalize_path,
    parent_path,
    normalize_optional_base_path,
)
from datacoolie.logging.runtime.manager import get_logger


logger = get_logger(__name__)


class FileProvider(BaseMetadataProvider):
    """Metadata provider backed by YAML, JSON, or Excel configuration files.

    Args:
        config_path: Exact YAML, JSON, or Excel metadata file (or an ordered
            sequence of files).  May contain ``connections``, ``dataflows``,
            and/or ``schema_hints`` sections.  Optional when
            ``metadata_base_path`` is supplied.
        platform: Optional platform used for metadata and watermark file I/O.
            It may be supplied later with :meth:`bind_platform` when the
            provider is assembled by a Driver or another caller.
        metadata_base_path: Directory containing metadata documents.  All
            supported files below the directory are discovered recursively in
            deterministic path order; each file must use section wrappers.
        connections_path: Optional separate file that provides the
            ``connections`` list.  Overrides any ``connections`` section
            in *config_path* when supplied.
        schema_hints_path: Optional separate file that provides the
            ``schema_hints`` list.  Overrides any ``schema_hints`` section
            in *config_path* when supplied.
        watermark_base_path: Optional root directory for watermark files.
            When omitted, a Driver may bind a runtime state/log-derived path;
            standalone metadata access remains valid until a watermark
            operation is requested.
        sql_base_path: One SQL root or a sequence of roots associated with
            metadata query references. Driver preparation reads the files.
        enable_cache: Enable the in-memory cache.
    """

    def __init__(
        self,
        config_path: str | Sequence[str] | None = None,
        platform: Optional[BasePlatform] = None,
        *,
        metadata_base_path: Optional[str] = None,
        connections_path: Optional[str] = None,
        schema_hints_path: Optional[str] = None,
        watermark_base_path: Optional[str] = None,
        sql_base_path: str | Sequence[str] | None = None,
        enable_cache: bool = True,
    ) -> None:
        super().__init__(enable_cache=enable_cache, sql_base_path=sql_base_path)
        if config_path is not None and metadata_base_path is not None:
            raise ConfigurationError(
                "config_path and metadata_base_path are mutually exclusive"
            )
        self._config_paths = self._normalise_config_paths(config_path)
        if metadata_base_path is not None and not str(metadata_base_path).strip():
            raise ConfigurationError("metadata_base_path must be a non-empty path when supplied")
        self._metadata_base_path = (
            normalize_path(str(metadata_base_path).strip())
            if metadata_base_path is not None
            else None
        )
        self._metadata_base_path_explicit = metadata_base_path is not None
        self._artifact_base_path: Optional[str] = None
        self._metadata_path_inferred = False
        for option_name, option_value in (
            ("connections_path", connections_path),
            ("schema_hints_path", schema_hints_path),
        ):
            if option_value is not None and not str(option_value).strip():
                raise ConfigurationError(
                    f"{option_name} must be a non-empty path when supplied"
                )
        self._connections_path = (
            normalize_path(str(connections_path).strip())
            if connections_path is not None
            else None
        )
        self._schema_hints_path = (
            normalize_path(str(schema_hints_path).strip())
            if schema_hints_path is not None
            else None
        )
        self._platform = platform
        if watermark_base_path is not None and not str(watermark_base_path).strip():
            raise ConfigurationError("watermark_base_path must be a non-empty path when supplied")
        self._watermark_base_path = (
            normalize_path(str(watermark_base_path).strip())
            if watermark_base_path is not None
            else None
        )
        self._watermark_base_path_explicit = watermark_base_path is not None
        self._data: Dict[str, Any] = {}
        self._config_loaded = False
        self._metadata_origins: Dict[int, str] = {}
        # Memoised outputs of the ``_build_*`` helpers — the underlying
        # ``self._data`` dict is immutable during the provider's
        # lifetime, so the parsed models can be cached.
        self._all_connections_cache: Optional[List[Connection]] = None
        # Full (active + inactive) dataflow list; filtered variants are
        # derived on demand from this list.
        self._all_dataflows_cache: Optional[List[DataFlow]] = None

    @staticmethod
    def _normalise_config_paths(
        config_path: str | Sequence[str] | None,
    ) -> List[str]:
        """Normalise an exact metadata file selection."""
        if config_path is None:
            return []
        values = [config_path] if isinstance(config_path, str) else list(config_path)
        if not values:
            raise ConfigurationError("config_path must contain at least one file")
        result: List[str] = []
        for value in values:
            if not isinstance(value, str) or not value.strip():
                raise ConfigurationError("config_path entries must be non-empty paths")
            result.append(normalize_path(value.strip()))
        return result

    @property
    def metadata_base_path(self) -> Optional[str]:
        """Return the configured metadata directory, when artifact mode is used."""
        return self._metadata_base_path

    @property
    def artifact_base_path(self) -> Optional[str]:
        """Return the artifact root offered by the runtime, if any."""
        return self._artifact_base_path

    @property
    def platform(self) -> Optional[BasePlatform]:
        """Return the platform bound for file reads and watermark I/O."""
        return self._platform

    def bind_platform(self, platform: BasePlatform) -> BasePlatform:
        """Bind a platform dependency without performing I/O.

        A provider owns no platform lifecycle.  Binding the exact same
        instance is idempotent; replacing an already-bound instance is
        rejected because platform instances can carry different storage
        roots or credentials even when their types match.
        """
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError(f"{type(self).__name__} is closed")
            if self._in_initialization:
                raise RuntimeError("Cannot bind platform during metadata initialization")
            if platform is None:
                raise ConfigurationError("FileProvider platform must be a non-null instance")
            if self._platform is None:
                self._platform = platform
            elif self._platform is not platform:
                raise ConfigurationError(
                    "FileProvider platform is already bound to a different instance"
                )
            return self._platform

    @staticmethod
    def _merge_bound_path(
        *,
        existing: Optional[str],
        existing_explicit: bool,
        candidate: Optional[str],
        candidate_explicit: bool,
        conflict_message: str,
        details: Dict[str, str],
    ) -> tuple[Optional[str], bool]:
        """Resolve one candidate root without publishing partial state.

        Explicit configuration outranks an implicit default.  Equal values
        are idempotent; an already-bound implicit value cannot silently move
        to a different candidate.
        """
        if candidate is None:
            return existing, existing_explicit
        if existing is None:
            return candidate, candidate_explicit
        if existing_explicit and not candidate_explicit:
            return existing, existing_explicit
        if normalize_path(existing) != normalize_path(candidate):
            raise ConfigurationError(conflict_message, details=details)
        return existing, existing_explicit or candidate_explicit

    def _configure_context(self, context: MetadataProviderStartupContext) -> None:
        """Apply runtime defaults atomically before metadata initialization."""
        platform = context.platform
        # A provider-level platform is an explicit dependency and remains
        # authoritative even when Driver carries a different execution
        # platform.  Context only fills a missing provider platform.
        resolved_platform = self._platform if self._platform is not None else platform

        try:
            metadata_context = normalize_optional_base_path(
                context.metadata_base_path,
                name="metadata_base_path",
            )
            artifact = normalize_optional_base_path(
                context.artifact_base_path,
                name="artifact_base_path",
            )
            state = normalize_optional_base_path(
                context.state_base_path,
                name="state_base_path",
            )
            log = normalize_optional_base_path(
                context.log_base_path,
                name="log_base_path",
            )
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc

        if metadata_context is not None and self._config_paths:
            raise ConfigurationError(
                "FileProvider config_path and metadata_base_path cannot be used "
                "together through startup context",
                details={
                    "config_path": ",".join(self._config_paths),
                    "metadata_base_path": metadata_context,
                },
            )

        metadata_candidate = None
        metadata_candidate_explicit = metadata_context is not None
        if metadata_context is not None:
            metadata_candidate = metadata_context
        metadata, metadata_explicit = self._merge_bound_path(
            existing=self._metadata_base_path,
            existing_explicit=self._metadata_base_path_explicit,
            candidate=metadata_candidate,
            candidate_explicit=metadata_candidate_explicit,
            conflict_message=(
                "FileProvider metadata base is already bound to a different metadata path"
            ),
            details={
                "existing": self._metadata_base_path or "",
                "requested": metadata_candidate or "",
            },
        )

        watermark_candidate = None
        if not self._watermark_base_path_explicit:
            if state:
                watermark_candidate = join_path(state, "watermarks")
            elif log:
                watermark_candidate = join_path(parent_path(log), "watermarks")
        watermark, watermark_explicit = self._merge_bound_path(
            existing=self._watermark_base_path,
            existing_explicit=self._watermark_base_path_explicit,
            candidate=watermark_candidate,
            candidate_explicit=False,
            conflict_message=(
                "FileProvider watermark base is already bound to a different runtime path"
            ),
            details={
                "existing": self._watermark_base_path or "",
                "requested": watermark_candidate or "",
            },
        )

        changed = (
            resolved_platform is not self._platform
            or artifact != self._artifact_base_path
            or metadata != self._metadata_base_path
            or watermark != self._watermark_base_path
        )
        if changed and self._initialized:
            raise ConfigurationError(
                "Cannot change FileProvider context after metadata initialization"
            )

        self._platform = resolved_platform
        self._artifact_base_path = artifact
        self._metadata_base_path = metadata
        self._metadata_base_path_explicit = metadata_explicit
        self._watermark_base_path = watermark
        self._watermark_base_path_explicit = watermark_explicit

    def _require_platform(self) -> BasePlatform:
        """Return the bound platform or fail before attempting file I/O."""
        self._ensure_open()
        platform = self._platform
        if platform is None:
            raise ConfigurationError(
                "FileProvider requires a platform before metadata or watermark I/O; "
                "bind_platform(...) or provide platform=..."
            )
        return platform

    @property
    def watermark_base_path(self) -> Optional[str]:
        """Return the effective file watermark base, if one is bound."""
        return self._watermark_base_path

    def validate_watermark_storage(self) -> None:
        """Validate that runtime watermark operations have a resolved root.

        This is intentionally a side-effect-free check used by the Driver's
        preparation phase.  The lower-level watermark methods retain their
        ``WatermarkError`` contract for standalone callers.
        """
        with self._lifecycle_lock:
            self._require_platform()
            if not self._watermark_base_path:
                raise ConfigurationError(
                    "FileProvider watermark_base_path is not configured; bind a runtime state/log path "
                    "or supply watermark_base_path explicitly"
                )

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _initialize_metadata(self) -> None:
        """Resolve deferred artifact metadata and load it once."""
        self._require_platform()
        if not self._config_loaded:
            if not self._config_paths and self._metadata_base_path is None:
                self._resolve_artifact_metadata_path()
            self._load_config()

    def _cleanup_failed_initialization(self) -> None:
        """Discard a partial parse so a later startup can retry from source."""
        self._config_loaded = False
        self._data.clear()
        self._metadata_origins.clear()
        self._all_connections_cache = None
        self._all_dataflows_cache = None
        if self._metadata_path_inferred and not self._metadata_base_path_explicit:
            self._metadata_base_path = None
            self._metadata_path_inferred = False

    def _resolve_artifact_metadata_path(self) -> None:
        """Bind the conventional metadata root below the artifact root."""
        artifact = self._artifact_base_path
        if not artifact:
            return
        # Runtime artifact mode deliberately ignores project/build manifests.
        # A custom metadata layout must be supplied explicitly through
        # ``metadata_base_path``; artifact-only startup has one stable
        # convention so it remains portable across providers and platforms.
        if self._metadata_base_path is not None:
            return
        candidate = join_path(artifact, "metadata")
        self._metadata_base_path = candidate
        self._metadata_path_inferred = True

    def _discover_metadata_files(self) -> List[str]:
        """Return deterministic metadata files below ``metadata_base_path``."""
        base = self._metadata_base_path
        if not base:
            raise ConfigurationError(
                "FileProvider requires config_path or metadata_base_path before initialization"
            )
        platform = self._require_platform()
        try:
            entries = platform.list_files(base, recursive=True)
        except Exception as exc:
            raise MetadataError(f"Cannot list metadata directory: {base}") from exc

        supported = SUPPORTED_METADATA_SUFFIXES
        overlay_paths = {
            normalize_path(path)
            for path in (self._connections_path, self._schema_hints_path)
            if path
        }
        overlay_relative_paths = {
            relative
            for path in overlay_paths
            if (relative := self._metadata_relative_path(path)) is not None
        }
        paths: List[str] = []
        for entry in entries:
            path = normalize_path(entry.path)
            if not path.lower().endswith(supported):
                continue
            relative = self._metadata_relative_path(path)
            if path in overlay_paths or (
                relative is not None and relative in overlay_relative_paths
            ):
                # Explicit section files are applied once below the primary
                # merge, even when they live inside the discovered folder.
                continue
            if relative is None:
                raise MetadataError(
                    f"Metadata file escapes metadata_base_path: {path}"
                )
            paths.append(path)
        return sorted(set(paths), key=lambda value: (value.casefold(), value))

    def _metadata_relative_path(self, path: str) -> Optional[str]:
        """Return a discovered file's relative path for scoped platform reads."""
        base = self._metadata_base_path
        if not base:
            return None
        platform = self._require_platform()
        try:
            return platform.relative_path_under_base(base, path)
        except PlatformError:
            # Explicit section overrides may live outside the metadata root.
            # Discovery rejects an out-of-root result before loading a shard.
            return None

    def _read_metadata_text(self, path: str) -> str:
        """Read a metadata text file without escaping a selected folder root."""
        platform = self._require_platform()
        relative = self._metadata_relative_path(path)
        if relative and self._metadata_base_path:
            return platform.read_file_under_base(self._metadata_base_path, relative)
        return platform.read_file(path)

    def _read_metadata_bytes(self, path: str) -> bytes:
        """Read metadata bytes through an optional canonical root guard."""
        platform = self._require_platform()
        relative = self._metadata_relative_path(path)
        if relative and self._metadata_base_path:
            return platform.read_bytes_under_base(self._metadata_base_path, relative)
        return platform.read_bytes(path)

    def _load_file(self, path: str) -> Dict[str, Any]:
        """Load and parse a single YAML, JSON, or Excel file.

        All reads go through the platform.  Excel is parsed from bytes so
        cloud-backed providers do not need an OS-visible local path.
        """
        self._require_platform()
        path_lower = path.lower()
        if path_lower.endswith(".xls"):
            raise MetadataError(
                f"Unsupported metadata config format: {path}; .xls is not supported, "
                "use .xlsx"
            )
        if not path_lower.endswith(SUPPORTED_METADATA_SUFFIXES):
            raise MetadataError(
                f"Unsupported metadata config format: {path}; "
                "expected .json, .yaml, .yml, or .xlsx"
            )
        if path_lower.endswith(".xlsx"):
            try:
                raw_bytes = self._read_metadata_bytes(path)
            except MetadataError:
                raise
            except Exception as exc:
                raise MetadataError(f"Cannot read metadata config: {path}") from exc
            try:
                return parse_excel(raw_bytes, source_path=path)
            except MetadataError:
                raise
            except Exception as exc:
                raise MetadataError(f"Cannot parse metadata config: {path}") from exc

        try:
            raw = self._read_metadata_text(path)
        except Exception as exc:
            raise MetadataError(f"Cannot read metadata config: {path}") from exc

        try:
            if path_lower.endswith((".yaml", ".yml")):
                document = parse_yaml_document(raw, path)
            else:
                document = parse_json_document(raw, path)
            if not isinstance(document, dict):
                raise MetadataError(
                    f"Metadata document must contain a mapping at root level: {path}"
                )
            return document
        except MetadataError:
            raise
        except Exception as exc:
            raise MetadataError(f"Cannot parse metadata config: {path}") from exc

    def _load_config(self) -> None:
        """Load exact files or deterministic metadata-folder shards.

        Every discovered document must expose section wrappers such as
        ``{"dataflows": [...]}``; the filename never determines a stage or
        section.  Explicit section paths replace the corresponding discovered
        section after the folder merge.
        """
        if self._config_paths:
            paths = list(self._config_paths)
        else:
            paths = self._discover_metadata_files()
        if not paths:
            raise MetadataError("No supported metadata files found")

        merged: Dict[str, Any] = {}
        self._metadata_origins = {}
        section_keys = set(METADATA_SECTION_KEYS)
        for path in paths:
            document = validate_document(
                self._load_file(path),
                path,
                section_keys=section_keys,
            )
            for key, value in document.items():
                if key in section_keys:
                    existing = merged.get(key)
                    if isinstance(existing, list):
                        existing.extend(value)
                    else:
                        merged[key] = list(value)
                    for item in value:
                        if isinstance(item, dict):
                            self._metadata_origins[id(item)] = path

        # Explicit section paths replace, rather than append to, the inferred
        # section.  The wrapper is required to make intent unambiguous.
        for path, section in (
            (self._connections_path, "connections"),
            (self._schema_hints_path, "schema_hints"),
        ):
            if path:
                overlay = validate_document(
                    self._load_file(path),
                    path,
                    section_keys=section_keys,
                    required_section=section,
                )
                if not isinstance(overlay.get(section), list):
                    raise MetadataError(
                        f"Explicit {section}_path must contain a '{section}' list: {path}"
                    )
                merged[section] = overlay[section]
                for item in overlay[section]:
                    if isinstance(item, dict):
                        self._metadata_origins[id(item)] = path

        merged.setdefault("connections", [])
        merged.setdefault("dataflows", [])
        merged.setdefault("schema_hints", [])
        self._data = merged
        self._config_loaded = True
        self._all_connections_cache = None
        self._all_dataflows_cache = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _ensure_config_loaded(self) -> None:
        """Load deferred artifact metadata for standalone getter callers."""
        if not self._config_loaded:
            self._load_config()

    def clear_cache(self) -> None:
        """Clear the shared cache and rebuild parsed models from raw metadata.

        File-backed providers retain the normalized document snapshot so a
        cache clear does not reread the platform.  Parsed model memoization is
        discarded here as well; callers cannot accidentally persist mutations
        made to a previously returned ``DataFlow`` or ``Connection`` object.
        """
        with self._lifecycle_lock:
            super().clear_cache()
            if self._cache is not None:
                self._all_connections_cache = None
                self._all_dataflows_cache = None

    def _build_connections(self, *, active_only: bool = True) -> List[Connection]:
        """Build connections from the immutable document snapshot.

        Model construction lives in metadata.mapping; this method owns
        memoization and the active-only projection.
        """
        self._ensure_config_loaded()
        if self._all_connections_cache is None:
            raw_list = self._data.get("connections", [])
            self._all_connections_cache = build_connections(
                raw_list,
                origins=self._metadata_origins,
            )
        if active_only:
            return [
                connection
                for connection in self._all_connections_cache
                if connection.is_active
            ]
        return self._all_connections_cache

    # ------------------------------------------------------------------
    # Dataflow helpers
    # ------------------------------------------------------------------

    def _build_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        """Build dataflows from the immutable document snapshot.

        Mapping/validation lives in metadata.mapping; this method owns
        memoization and cheap in-memory filtering.
        """
        self._ensure_config_loaded()
        if self._all_dataflows_cache is None:
            raw_list = self._data.get("dataflows", [])
            self._all_dataflows_cache = build_dataflows(
                raw_list,
                self._build_connections(active_only=False),
                origins=self._metadata_origins,
            )
        dataflows = self._all_dataflows_cache
        if active_only:
            dataflows = [
                dataflow
                for dataflow in dataflows
                if dataflow.is_active
            ]
        if stages is not None:
            stage_set = set(stages)
            dataflows = [
                dataflow
                for dataflow in dataflows
                if dataflow.stage in stage_set
            ]
        return dataflows

    # ------------------------------------------------------------------
    # Schema hint helpers
    # ------------------------------------------------------------------

    def _build_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        """Build ``SchemaHint`` models matching *connection_id* and *table_name*."""
        self._ensure_config_loaded()
        grouped = self._build_grouped_schema_hints(
            self._build_connections(active_only=False)
        )
        selected = select_schema_hints(
            grouped,
            connection_id,
            schema_name,
            table_name,
        )
        return selected or []

    # ------------------------------------------------------------------
    # Watermark file I/O
    # ------------------------------------------------------------------

    def _watermark_path(self, dataflow_id: str) -> str:
        """Build the watermark file path for a dataflow.

        The folder segment is ``{stage}_{name}_{dataflow_id}`` when both
        *stage* and *name* are set on the dataflow, otherwise plain
        *dataflow_id* is used (backward-compatible fallback).

        Uses :meth:`get_dataflow_by_id` so the base-class cache is
        consulted first — avoids rebuilding the full dataflow list on
        every watermark read/write after a bulk-load.
        """
        self._require_platform()
        if not self._watermark_base_path:
            raise WatermarkError(
                "FileProvider watermark_base_path is not configured; bind a runtime state/log path "
                "or supply watermark_base_path explicitly"
            )
        df = self.get_dataflow_by_id(dataflow_id, attach_schema_hints=False)
        if df is not None:
            parts = []
            if df.stage:
                parts.append(df.stage)
            if df.name:
                parts.append(df.name)
            parts.append(dataflow_id)
            folder = "_".join(parts)
        else:
            folder = dataflow_id
        # Keep URI/drive roots intact while validating the generated
        # dataflow-relative path.  String concatenation would turn a root
        # such as ``file:///`` into ``file:/`` and could let an unsafe ID
        # escape the provider's watermark base.
        try:
            return join_path(
                self._watermark_base_path,
                f"{folder}/{WATERMARK_FILE_NAME}",
            )
        except ValueError as exc:
            raise WatermarkError(
                f"Invalid watermark path for dataflow {dataflow_id!r}"
            ) from exc

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def _bulk_load(
        self,
    ) -> Tuple[
        List[Connection],
        List[DataFlow],
        Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ]:
        """Bulk-load all metadata from the in-memory ``self._data``.

        Walks ``self._data['schema_hints']`` once and groups by
        ``(connection_id, schema_name, table_name)`` so that the
        per-dataflow attach loop becomes a pure cache hit.
        """
        self._ensure_config_loaded()
        connections = self._build_connections(active_only=False)
        dataflows = self._build_dataflows(active_only=False)

        # Parse schema hints through the shared selector/validator.  This
        # keeps bulk cache population identical to direct cache-disabled reads.
        return connections, dataflows, self._build_grouped_schema_hints(connections)

    def _build_grouped_schema_hints(
        self,
        connections: List[Connection],
    ) -> Dict[Tuple[str, Optional[str], str], List[SchemaHint]]:
        """Build and normalize all schema-hint groups from the snapshot."""
        return build_grouped_schema_hints(
            self._data.get("schema_hints", []),
            connections,
            origins=self._metadata_origins,
        )

    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]:
        return self._build_connections(active_only=active_only)

    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        return next(
            (c for c in self._build_connections(active_only=False) if c.connection_id == connection_id),
            None,
        )

    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]:
        return next(
            (c for c in self._build_connections(active_only=False) if c.name == name),
            None,
        )

    def _fetch_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        return self._build_dataflows(stages=stages, active_only=active_only)

    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]:
        return next(
            (df for df in self._build_dataflows(active_only=False) if df.dataflow_id == dataflow_id),
            None,
        )

    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        """Return the raw watermark JSON string for *dataflow_id*, or ``None``."""
        with self._runtime_operation():
            platform = self._require_platform()
            path = self._watermark_path(dataflow_id)
            try:
                if not platform.file_exists(path):
                    return None
                raw = platform.read_file(path)
                return raw if raw and raw.strip() else None
            except Exception as exc:
                raise WatermarkError(f"Cannot read watermark at {path}") from exc

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Write the serialised watermark JSON to a file."""
        with self._runtime_operation():
            platform = self._require_platform()
            path = self._watermark_path(dataflow_id)
            try:
                platform.write_file(path, watermark_value, overwrite=True)
            except Exception as exc:
                raise WatermarkError(f"Cannot write watermark at {path}") from exc

    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        return self._build_schema_hints(
            connection_id=connection_id,
            table_name=table_name,
            schema_name=schema_name,
        )

    def _close_resources(self) -> None:
        """Clear parsed metadata and provider-local memoized models."""
        self._data.clear()
        self._config_loaded = False
        self._metadata_origins.clear()
        self._all_connections_cache = None
        self._all_dataflows_cache = None
