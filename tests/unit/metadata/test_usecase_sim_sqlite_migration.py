"""Focused recovery checks for the usecase-sim SQLite path transition."""

from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest


DATABASE_HELPERS = Path(__file__).resolve().parents[3] / "usecase-sim" / "metadata" / "database"
sys.path.insert(0, str(DATABASE_HELPERS))
migrate_sqlite = importlib.import_module("migrate_sqlite")


def _create_wal_source(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0].lower() == "wal"
    connection.executescript(
        """
        CREATE TABLE dc_framework_connections (
            connection_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            name TEXT NOT NULL,
            configure TEXT,
            enabled INTEGER NOT NULL
        );
        CREATE TABLE dc_framework_dataflows (
            dataflow_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            name TEXT NOT NULL,
            source_connection_id TEXT NOT NULL,
            destination_connection_id TEXT NOT NULL
        );
        CREATE TABLE dc_framework_watermarks (
            watermark_id TEXT PRIMARY KEY,
            dataflow_id TEXT NOT NULL UNIQUE,
            current_value TEXT,
            previous_value TEXT,
            updated_at TEXT
        );
        INSERT INTO dc_framework_connections VALUES
            ('connection-1', 'local-workspace', 'orders', '{"base_path":"input"}', 1),
            ('connection-2', 'local-workspace', 'archive', NULL, 0);
        INSERT INTO dc_framework_dataflows VALUES
            ('flow-1', 'local-workspace', 'orders-flow', 'connection-1', 'connection-2');
        INSERT INTO dc_framework_watermarks VALUES
            ('watermark-1', 'flow-1', '{"seq":103}', '{"seq":102}', '2026-10-08 12:30:00');
        """
    )
    connection.commit()
    return connection


def _create_different_target(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE dc_framework_connections (
                connection_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                name TEXT NOT NULL,
                configure TEXT,
                enabled INTEGER NOT NULL
            );
            CREATE TABLE dc_framework_dataflows (
                dataflow_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                name TEXT NOT NULL,
                source_connection_id TEXT NOT NULL,
                destination_connection_id TEXT NOT NULL
            );
            CREATE TABLE dc_framework_watermarks (
                watermark_id TEXT PRIMARY KEY,
                dataflow_id TEXT NOT NULL UNIQUE,
                current_value TEXT,
                previous_value TEXT,
                updated_at TEXT
            );
            INSERT INTO dc_framework_connections VALUES
                ('connection-1', 'local-workspace', 'changed', '{"base_path":"other"}', 1);
            """
        )


@pytest.fixture
def wal_source(tmp_path: Path):
    path = tmp_path / "legacy.db"
    connection = _create_wal_source(path)
    try:
        yield path, connection
    finally:
        connection.close()


def _temporary_files(tmp_path: Path, target: Path) -> list[Path]:
    return sorted(tmp_path.glob(f".{target.name}.migration-*.tmp*"))


def test_wal_backup_preserves_metadata_and_watermarks(wal_source, tmp_path: Path):
    source, source_connection = wal_source
    target = tmp_path / "metadata" / "datacoolie_metadata.db"
    target.parent.mkdir()
    source_before = migrate_sqlite._read_snapshot(source)

    result = migrate_sqlite.migrate(source, target)

    assert result == migrate_sqlite.MigrationResult(source.resolve(), target.resolve(), published=True)
    assert target.is_file()
    assert migrate_sqlite._read_snapshot(target) == source_before
    assert migrate_sqlite._read_snapshot(source) == source_before
    assert source.is_file()
    assert source_connection.execute("SELECT current_value FROM dc_framework_watermarks").fetchone() == (
        '{"seq":103}',
    )
    assert not _temporary_files(tmp_path / "metadata", target)


def test_identical_target_is_idempotent(wal_source, tmp_path: Path):
    source, _source_connection = wal_source
    target = tmp_path / "metadata.db"

    first = migrate_sqlite.migrate(source, target)
    target_snapshot = migrate_sqlite._read_snapshot(target)
    second = migrate_sqlite.migrate(source, target)

    assert first.published
    assert second.already_present
    assert not second.published
    assert migrate_sqlite._read_snapshot(target) == target_snapshot
    assert source.is_file()


def test_differing_target_is_rejected_without_overwrite(wal_source, tmp_path: Path):
    source, _source_connection = wal_source
    target = tmp_path / "metadata.db"
    _create_different_target(target)
    before = target.read_bytes()
    target_snapshot = migrate_sqlite._read_snapshot(target)

    with pytest.raises(migrate_sqlite.TargetConflictError, match="different"):
        migrate_sqlite.migrate(source, target)

    assert target.read_bytes() == before
    assert migrate_sqlite._read_snapshot(target) == target_snapshot
    assert source.is_file()


def test_missing_source_fails_without_creating_target(tmp_path: Path):
    source = tmp_path / "missing.db"
    target = tmp_path / "metadata.db"

    with pytest.raises(FileNotFoundError, match="source does not exist"):
        migrate_sqlite.migrate(source, target)

    assert not target.exists()
    assert not _temporary_files(tmp_path, target)


def test_injected_backup_interruption_cleans_owned_temporary_file(wal_source, tmp_path: Path, monkeypatch):
    source, _source_connection = wal_source
    target = tmp_path / "metadata.db"
    source_before = migrate_sqlite._read_snapshot(source)

    def interrupt(_source, _destination):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(migrate_sqlite, "_backup_database", interrupt)
    with pytest.raises(RuntimeError, match="injected interruption"):
        migrate_sqlite.migrate(source, target)

    assert source.is_file()
    assert migrate_sqlite._read_snapshot(source) == source_before
    assert not target.exists()
    assert not _temporary_files(tmp_path, target)


def test_source_change_during_parity_fails_before_publish(wal_source, tmp_path: Path):
    source, source_connection = wal_source
    target = tmp_path / "metadata.db"

    original_backup = migrate_sqlite._backup_database

    def backup_then_change(source_connection_arg, destination_connection):
        original_backup(source_connection_arg, destination_connection)
        source_connection.execute(
            "UPDATE dc_framework_watermarks SET current_value = ?",
            ('{"seq":104}',),
        )
        source_connection.commit()

    # WAL mode lets the writer commit while migration's source read snapshot
    # remains open; the source guard must reject that transition.
    migrate_sqlite._backup_database = backup_then_change
    try:
        with pytest.raises(migrate_sqlite.SourceChangedError, match="changed"):
            migrate_sqlite.migrate(source, target)
    finally:
        migrate_sqlite._backup_database = original_backup

    assert not target.exists()
    assert not _temporary_files(tmp_path, target)
    assert source_connection.execute("SELECT current_value FROM dc_framework_watermarks").fetchone() == (
        '{"seq":104}',
    )


def test_publish_race_never_overwrites_existing_target(wal_source, tmp_path: Path, monkeypatch):
    source, _source_connection = wal_source
    target = tmp_path / "metadata.db"
    original_publish = migrate_sqlite._publish_without_overwrite

    def publish_after_race(temporary, target_path):
        _create_different_target(target_path)
        original_publish(temporary, target_path)

    monkeypatch.setattr(migrate_sqlite, "_publish_without_overwrite", publish_after_race)
    with pytest.raises(migrate_sqlite.TargetConflictError):
        migrate_sqlite.migrate(source, target)

    assert target.is_file()
    assert not _temporary_files(tmp_path, target)


def test_cli_requires_explicit_source_and_target():
    with pytest.raises(SystemExit):
        migrate_sqlite.main([])


def test_cli_migrates_the_explicit_paths(wal_source, tmp_path: Path):
    source, _source_connection = wal_source
    target = tmp_path / "metadata.db"

    assert migrate_sqlite.main(["--source", str(source), "--target", str(target)]) == 0
    assert target.is_file()
