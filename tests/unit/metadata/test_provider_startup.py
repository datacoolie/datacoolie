"""Provider startup and artifact-folder contract tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import ConfigurationError, MetadataError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.metadata.api_provider import APIProvider
from datacoolie.metadata.database_provider import DatabaseProvider
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
from datacoolie.orchestration import DataCoolieDriver, create_driver
from datacoolie.platforms.local_platform import LocalPlatform


def _engine() -> MagicMock:
    engine = MagicMock()
    engine.platform = None
    engine.set_platform.side_effect = lambda platform: setattr(engine, "platform", platform)
    return engine


def _connection(name: str, connection_id: str) -> dict[str, str]:
    return {
        "connection_id": connection_id,
        "name": name,
        "connection_type": "file",
        "format": "parquet",
    }


def test_folder_mode_discovers_sorted_section_shards(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    nested = metadata / "nested"
    nested.mkdir(parents=True)
    (metadata / "z-dataflows.json").write_text(
        json.dumps({"dataflows": []}), encoding="utf-8"
    )
    (nested / "connections.json").write_text(
        json.dumps({"connections": [_connection("source", "c-1")]}), encoding="utf-8"
    )

    provider = FileProvider(platform=LocalPlatform(), metadata_base_path=str(metadata))
    provider.initialize()

    assert provider.is_initialized
    assert [item.name for item in provider.get_connections(active_only=False)] == ["source"]


def test_folder_mode_respects_local_platform_sandbox(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "connections.json").write_text(
        json.dumps({"connections": [_connection("source", "c-1")]}),
        encoding="utf-8",
    )
    (metadata / "dataflows.json").write_text(
        json.dumps({"dataflows": []}),
        encoding="utf-8",
    )

    provider = FileProvider(
        platform=LocalPlatform(base_path=str(tmp_path)),
        metadata_base_path="metadata",
    )
    provider.initialize()

    assert [item.name for item in provider.get_connections(active_only=False)] == ["source"]


def test_relative_overlay_inside_discovered_folder_is_applied_once(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "connections.json").write_text(
        json.dumps({"connections": [_connection("source", "c-1")]}),
        encoding="utf-8",
    )
    (metadata / "dataflows.json").write_text(
        json.dumps({"dataflows": []}),
        encoding="utf-8",
    )

    provider = FileProvider(
        platform=LocalPlatform(base_path=str(tmp_path)),
        metadata_base_path="metadata",
        connections_path="metadata/connections.json",
    )
    provider.initialize()

    assert [item.connection_id for item in provider.get_connections(active_only=False)] == ["c-1"]


def test_file_provider_constructor_does_not_read_metadata(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({"connections": []}), encoding="utf-8")
    platform = MagicMock(spec=LocalPlatform)
    platform.read_file.return_value = path.read_text(encoding="utf-8")
    provider = FileProvider(config_path=str(path), platform=platform)
    platform.read_file.assert_not_called()
    provider.initialize()
    platform.read_file.assert_called_once()


def test_file_provider_can_bind_platform_after_construction(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({"connections": [], "dataflows": []}), encoding="utf-8")
    provider = FileProvider(config_path=str(path))

    with pytest.raises(ConfigurationError, match="requires a platform"):
        provider.initialize()

    platform = LocalPlatform()
    assert provider.bind_platform(platform) is platform
    assert provider.bind_platform(platform) is platform
    provider.initialize()
    assert provider.is_initialized


def test_driver_binds_platform_to_unbound_file_provider(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({"connections": [], "dataflows": []}), encoding="utf-8")
    provider = FileProvider(config_path=str(path))
    engine = _engine()
    platform = LocalPlatform()
    driver = create_driver(
        engine=engine,
        platform=platform,
        metadata_provider=provider,
    )
    try:
        assert provider.platform is platform
        assert provider.is_initialized
    finally:
        driver.close()


def test_driver_keeps_explicit_provider_platform_independent_from_engine(tmp_path: Path) -> None:
    provider_root = tmp_path / "provider"
    driver_root = tmp_path / "driver"
    provider_root.mkdir()
    driver_root.mkdir()
    metadata = provider_root / "metadata.json"
    metadata.write_text(
        json.dumps({"connections": [], "dataflows": []}),
        encoding="utf-8",
    )

    provider_platform = LocalPlatform(base_path=str(provider_root))
    driver_platform = LocalPlatform(base_path=str(driver_root))
    provider = FileProvider(config_path="metadata.json", platform=provider_platform)
    engine = _engine()
    driver = create_driver(
        engine=engine,
        platform=driver_platform,
        metadata_provider=provider,
    )
    try:
        assert provider.platform is provider_platform
        assert driver._engine.platform is driver_platform
        assert provider.is_initialized
    finally:
        driver.close()


def test_binding_after_close_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({"connections": [], "dataflows": []}), encoding="utf-8")
    provider = FileProvider(config_path=str(path), platform=LocalPlatform())
    provider.close()
    with pytest.raises(RuntimeError, match="closed"):
        provider.configure_context(
            MetadataProviderStartupContext(state_base_path="runtime")
        )


def test_file_provider_metadata_conflict_does_not_bind_partial_context(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "metadata.json"
    config_path.write_text(
        json.dumps({"connections": [], "dataflows": []}), encoding="utf-8"
    )
    provider = FileProvider(config_path=str(config_path))
    platform = LocalPlatform()

    with pytest.raises(ConfigurationError, match="config_path and metadata_base_path"):
        provider.configure_context(
            MetadataProviderStartupContext(
                platform=platform,
                metadata_base_path=str(tmp_path / "metadata"),
            )
        )

    assert provider.platform is None
    assert provider.metadata_base_path is None


def test_artifact_factory_uses_metadata_default(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "metadata.json").write_text(
        json.dumps({"connections": [], "dataflows": [], "schema_hints": []}),
        encoding="utf-8",
    )

    engine = _engine()
    driver = create_driver(
        engine=engine,
        platform=LocalPlatform(),
        artifact_base_path=str(tmp_path),
    )
    try:
        assert driver._metadata_provider.metadata_base_path == str(metadata).replace("\\", "/")
        assert driver._metadata_provider.is_initialized
    finally:
        driver.close()


@pytest.mark.parametrize(
    "manifest_payload",
    [
        "{not json",
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "datacoolie_build",
                "build_id": "build-1",
                "components": {"metadata": {"path": "redirected"}},
            }
        ),
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "unrelated_descriptor",
                "build_id": "build-1",
                "components": {"metadata": {"path": "redirected"}},
            }
        ),
    ],
    ids=["malformed", "valid_redirect", "wrong_kind"],
)
def test_artifact_factory_ignores_manifest_metadata_redirect(
    tmp_path: Path,
    manifest_payload: str,
) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "metadata.json").write_text(
        json.dumps({"connections": [], "dataflows": [], "schema_hints": []}),
        encoding="utf-8",
    )
    # A project/build descriptor is not a runtime configuration source.  An
    # invalid descriptor must not prevent artifact-only startup.
    (tmp_path / "manifest.json").write_text(manifest_payload, encoding="utf-8")

    driver = create_driver(
        engine=_engine(),
        platform=LocalPlatform(),
        artifact_base_path=str(tmp_path),
    )
    try:
        assert driver._metadata_provider.metadata_base_path == str(metadata).replace("\\", "/")
        assert driver._metadata_provider.is_initialized
    finally:
        driver.close()


def test_explicit_metadata_path_overrides_artifact_default(tmp_path: Path) -> None:
    explicit = tmp_path / "custom-metadata"
    explicit.mkdir()
    (explicit / "dataflows.json").write_text(
        json.dumps({"dataflows": []}), encoding="utf-8"
    )

    engine = _engine()
    driver = create_driver(
        engine=engine,
        platform=LocalPlatform(),
        artifact_base_path=str(tmp_path / "artifact"),
        metadata_base_path=str(explicit),
    )
    try:
        assert driver._metadata_provider.metadata_base_path == str(explicit).replace("\\", "/")
    finally:
        driver.close()


def test_driver_creates_file_provider_from_metadata_path(tmp_path: Path) -> None:
    metadata = tmp_path / "custom-metadata"
    metadata.mkdir()
    (metadata / "dataflows.json").write_text(
        json.dumps({"dataflows": []}), encoding="utf-8"
    )

    driver = DataCoolieDriver(
        engine=_engine(),
        platform=LocalPlatform(),
        metadata_base_path=str(metadata),
    )
    try:
        assert isinstance(driver._metadata_provider, FileProvider)
        assert driver._metadata_provider.metadata_base_path == str(metadata).replace("\\", "/")
        assert driver._metadata_provider.is_initialized
        assert driver._watermark_manager._provider is driver._metadata_provider
    finally:
        provider = driver._metadata_provider
        driver.close()
        assert provider.is_closed


def test_driver_artifact_root_infers_file_provider(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "dataflows.json").write_text(
        json.dumps({"dataflows": []}), encoding="utf-8"
    )

    driver = DataCoolieDriver(
        engine=_engine(),
        platform=LocalPlatform(),
        artifact_base_path=str(tmp_path),
    )
    try:
        assert isinstance(driver._metadata_provider, FileProvider)
        assert driver._metadata_provider.metadata_base_path == str(metadata).replace("\\", "/")
        assert driver._metadata_provider.is_initialized
    finally:
        driver.close()


def test_factory_accepts_same_metadata_path_with_injected_file_provider(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "dataflows.json").write_text(
        json.dumps({"dataflows": []}), encoding="utf-8"
    )
    provider = FileProvider(
        platform=LocalPlatform(),
        metadata_base_path=str(metadata),
    )
    driver = create_driver(
        engine=_engine(),
        platform=LocalPlatform(),
        metadata_provider=provider,
        metadata_base_path=str(metadata),
    )
    try:
        assert driver._metadata_provider is provider
        assert provider.is_initialized
    finally:
        driver.close()
        assert not provider.is_closed


def test_driver_rejects_conflicting_metadata_path_with_injected_file_provider(
    tmp_path: Path,
) -> None:
    provider = FileProvider(
        platform=LocalPlatform(),
        metadata_base_path=str(tmp_path / "provider-metadata"),
    )
    with pytest.raises(ConfigurationError, match="different metadata path"):
        create_driver(
            engine=_engine(),
            platform=LocalPlatform(),
            metadata_provider=provider,
            metadata_base_path=str(tmp_path / "driver-metadata"),
        )
    assert not provider.is_closed


def test_driver_rejects_metadata_path_with_config_file_provider(tmp_path: Path) -> None:
    config_path = tmp_path / "metadata.json"
    config_path.write_text(
        json.dumps({"connections": [], "dataflows": []}), encoding="utf-8"
    )
    provider = FileProvider(config_path=str(config_path), platform=LocalPlatform())
    with pytest.raises(ConfigurationError, match="config_path and metadata_base_path"):
        create_driver(
            engine=_engine(),
            platform=LocalPlatform(),
            metadata_provider=provider,
            metadata_base_path=str(tmp_path / "metadata"),
        )
    assert not provider.is_closed


@pytest.mark.parametrize(
    "provider",
    [
        APIProvider(
            base_url="https://metadata.example.test",
            api_key="test-key",
            workspace_id="workspace-1",
        ),
        DatabaseProvider(
            connection_string="sqlite:///:memory:",
            workspace_id="workspace-1",
        ),
    ],
    ids=["api", "database"],
)
def test_non_file_providers_reject_explicit_metadata_path(provider) -> None:
    try:
        with pytest.raises(ConfigurationError, match="does not support metadata_base_path"):
            provider.configure_context(
                MetadataProviderStartupContext(metadata_base_path="metadata")
            )
        assert not provider.is_initialized
    finally:
        provider.close()


@pytest.mark.parametrize(
    "provider",
    [
        FileProvider(config_path="metadata.json", sql_base_path="project/sql"),
        APIProvider(
            base_url="https://metadata.example.test",
            api_key="test-key",
            workspace_id="workspace-1",
            sql_base_path=["shared/sql1", "project/sql2"],
        ),
        DatabaseProvider(
            connection_string="sqlite:///:memory:",
            workspace_id="workspace-1",
            sql_base_path="project/sql",
        ),
    ],
    ids=["file", "api", "database"],
)
def test_all_metadata_providers_expose_sql_root_configuration(provider) -> None:
    try:
        assert provider.sql_base_path in {
            "project/sql",
            ("shared/sql1", "project/sql2"),
        }
    finally:
        provider.close()


def _write_empty_metadata(path: Path) -> None:
    path.write_text(
        json.dumps({"connections": [], "dataflows": [], "schema_hints": []}),
        encoding="utf-8",
    )


def test_driver_uses_provider_sql_root_without_mutating_provider(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata.json"
    sql_root = tmp_path / "provider-sql"
    _write_empty_metadata(metadata)
    provider = FileProvider(
        config_path=str(metadata),
        platform=LocalPlatform(),
        sql_base_path=str(sql_root),
    )
    engine = _engine()
    driver = create_driver(
        engine=engine,
        platform=LocalPlatform(),
        metadata_provider=provider,
    )
    try:
        normalized_root = str(sql_root).replace("\\", "/")
        assert driver._sql_base_path == normalized_root
        assert provider.sql_base_path == normalized_root
    finally:
        driver.close()


def test_driver_preparation_reads_provider_sql_root_with_driver_platform(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata.json"
    sql_root = tmp_path / "provider-sql"
    sql_root.mkdir()
    sql_file = sql_root / "orders.sql"
    sql_file.write_text("SELECT 42;", encoding="utf-8")
    _write_empty_metadata(metadata)
    provider = FileProvider(
        config_path=str(metadata),
        platform=LocalPlatform(),
        sql_base_path=str(sql_root),
    )
    dataflow = DataFlow(
        dataflow_id="df-provider-sql",
        source=Source(
            connection=Connection(
                connection_id="source",
                name="source",
                format="sql",
                configure={},
            ),
            query="orders.sql",
        ),
        destination=Destination(
            connection=Connection(
                connection_id="destination",
                name="destination",
                format="parquet",
                configure={},
            ),
            table="orders",
        ),
    )
    driver = create_driver(
        engine=_engine(),
        platform=LocalPlatform(),
        metadata_provider=provider,
    )
    try:
        prepared = driver._prepare_execution_dataflow(dataflow)
        assert prepared.execution.source.query == "SELECT 42;"
        assert prepared.metadata.source.query == "orders.sql"
    finally:
        driver.close()


def test_driver_sql_root_is_provider_fallback_for_file_provider(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata.json"
    sql_root = tmp_path / "driver-sql"
    _write_empty_metadata(metadata)
    provider = FileProvider(
        config_path=str(metadata),
        platform=LocalPlatform(),
    )
    driver = create_driver(
        engine=_engine(),
        platform=LocalPlatform(),
        metadata_provider=provider,
        sql_base_path=str(sql_root),
    )
    try:
        normalized_root = str(sql_root).replace("\\", "/")
        assert driver._sql_base_path == normalized_root
        assert provider.sql_base_path is None
    finally:
        driver.close()


def test_provider_and_driver_sql_root_conflict_fails_before_provider_startup(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata.json"
    _write_empty_metadata(metadata)
    provider = FileProvider(
        config_path=str(metadata),
        platform=LocalPlatform(),
        sql_base_path=str(tmp_path / "provider-sql"),
    )
    engine = _engine()

    with pytest.raises(ConfigurationError, match="sql_base_path conflicts"):
        create_driver(
            engine=engine,
            platform=LocalPlatform(),
            metadata_provider=provider,
            sql_base_path=str(tmp_path / "driver-sql"),
        )

    assert not provider.is_initialized
    assert engine.platform is None


def test_startup_rejects_unknown_schema_hint_connection(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "connections": [_connection("source", "c-1")],
                "dataflows": [],
                "schema_hints": [
                    {
                        "connection_name": "missing",
                        "table_name": "orders",
                        "hints": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    provider = FileProvider(config_path=str(path), platform=LocalPlatform())
    with pytest.raises(MetadataError, match="unknown connection"):
        provider.initialize()
    assert not provider.is_initialized


def test_file_provider_failed_startup_can_reload_source(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    invalid = {
        "connections": [_connection("source", "c-1")],
        "dataflows": [],
        "schema_hints": [
            {"connection_name": "missing", "table_name": "orders", "hints": []}
        ],
    }
    path.write_text(json.dumps(invalid), encoding="utf-8")
    provider = FileProvider(config_path=str(path), platform=LocalPlatform())

    with pytest.raises(MetadataError, match="unknown connection"):
        provider.initialize()

    path.write_text(
        json.dumps({"connections": [_connection("source", "c-1")], "dataflows": []}),
        encoding="utf-8",
    )
    provider.initialize()
    assert provider.is_initialized
