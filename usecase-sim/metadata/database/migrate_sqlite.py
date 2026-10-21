"""Safely relocate a SQLite metadata database.

This helper is deliberately a path-transition primitive.  It does not seed,
reset, or reconcile metadata.  The source is opened read-only and copied with
SQLite's backup API so a source in WAL mode is read from one consistent
snapshot.  A target is published only after integrity and exact schema/table/
row parity checks succeed.

Usage::

    python usecase-sim/metadata/database/migrate_sqlite.py \
        --source /path/to/legacy.db \
        --target /path/to/metadata/datacoolie_metadata.db
"""

from __future__ import annotations

import argparse
import collections
import errno
import math
import os
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import sys
import uuid
from typing import Any, Sequence


class MigrationError(RuntimeError):
    """Base class for a rejected or incomplete SQLite transition."""


class SourceChangedError(MigrationError):
    """The source changed while the migration was checking its parity."""


class TargetConflictError(MigrationError):
    """The target exists and does not exactly match the source."""


class IntegrityError(MigrationError):
    """A source, temporary database, or target failed integrity checks."""


@dataclass(frozen=True)
class MigrationResult:
    """Outcome of a transition."""

    source: Path
    target: Path
    published: bool

    @property
    def already_present(self) -> bool:
        """Whether the target already contained the exact source snapshot."""

        return not self.published


@dataclass(frozen=True)
class _DatabaseSnapshot:
    """Comparable, order-independent representation of a SQLite database."""

    pragmas: tuple[tuple[str, Any], ...]
    schema: tuple[tuple[str, str, str, str | None], ...]
    tables: tuple[tuple[str, tuple[tuple[Any, ...], ...]], ...]
    integrity: tuple[str, ...]


@dataclass
class _SourceGuard:
    """Watch a source path and connection for changes by another writer."""

    path: Path
    connection: sqlite3.Connection
    data_version: int
    stat_marker: tuple[int, int, int, int]

    @classmethod
    def open(cls, path: Path) -> "_SourceGuard":
        connection = _connect_read_only(path)
        return cls(
            path=path,
            connection=connection,
            data_version=_data_version(connection),
            stat_marker=_stat_marker(path),
        )

    def changed(self) -> bool:
        """Return whether the source path or another connection changed it."""

        try:
            current_stat = _stat_marker(self.path)
            current_version = _data_version(self.connection)
        except (FileNotFoundError, sqlite3.Error):
            return True
        return current_stat != self.stat_marker or current_version != self.data_version

    def close(self) -> None:
        self.connection.close()


def _resolve_path(value: str | os.PathLike[str], *, label: str) -> Path:
    """Resolve a caller-supplied path to an absolute, normalized path."""

    try:
        path = Path(value).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {label} path: {value!r}") from exc
    if not path.is_absolute():  # Defensive: Path.resolve() should guarantee this.
        raise ValueError(f"{label} path must be absolute: {path}")
    return path


def _validate_paths(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> tuple[Path, Path]:
    source_path = _resolve_path(source, label="source")
    target_path = _resolve_path(target, label="target")
    if source_path == target_path:
        raise ValueError("Source and target must be different paths")
    if not source_path.exists():
        raise FileNotFoundError(f"SQLite source does not exist: {source_path}")
    if not source_path.is_file():
        raise ValueError(f"SQLite source is not a regular file: {source_path}")
    if not target_path.parent.exists():
        raise FileNotFoundError(f"Target parent does not exist: {target_path.parent}")
    if not target_path.parent.is_dir():
        raise ValueError(f"Target parent is not a directory: {target_path.parent}")
    if target_path.exists() and not target_path.is_file():
        raise ValueError(f"SQLite target is not a regular file: {target_path}")
    return source_path, target_path


def _stat_marker(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _connect_read_only(path: Path) -> sqlite3.Connection:
    """Open *path* using SQLite's read-only URI mode."""

    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        connection.execute("PRAGMA query_only = ON")
        return connection
    except sqlite3.Error as exc:
        raise MigrationError(f"Unable to open SQLite database read-only: {path}") from exc


def _data_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA data_version").fetchone()[0])


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _value_key(value: Any) -> tuple[str, Any]:
    """Make SQLite scalar values comparable without losing type distinctions."""

    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("integer", int(value))
    if isinstance(value, int):
        return ("integer", value)
    if isinstance(value, float):
        if math.isnan(value):
            return ("real", "nan")
        if math.isinf(value):
            return ("real", "+inf" if value > 0 else "-inf")
        return ("real", value.hex())
    if isinstance(value, bytes):
        return ("blob", value)
    if isinstance(value, str):
        return ("text", value)
    # sqlite3's built-in adapters return only the scalar types above.  Keep a
    # clear failure if a caller changes the connection's type configuration.
    raise TypeError(f"Unsupported SQLite value type: {type(value).__name__}")


def _schema_rows(connection: sqlite3.Connection) -> tuple[tuple[str, str, str, str | None], ...]:
    rows = connection.execute(
        "SELECT type, name, tbl_name, sql "
        "FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_autoindex_%' "
        "ORDER BY type, name"
    ).fetchall()
    return tuple((str(kind), str(name), str(table), sql) for kind, name, table, sql in rows)


def _table_rows(connection: sqlite3.Connection, table_name: str) -> tuple[tuple[Any, ...], ...]:
    identifier = _quote_identifier(table_name)
    rows = connection.execute(f"SELECT * FROM {identifier}").fetchall()
    # Counter preserves duplicate rows while avoiding an accidental dependency
    # on SQLite's unspecified SELECT order.
    return tuple(sorted(collections.Counter(tuple(_value_key(v) for v in row) for row in rows).items()))


def _integrity(connection: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(str(row[0]) for row in connection.execute("PRAGMA integrity_check").fetchall())


def _snapshot(connection: sqlite3.Connection) -> _DatabaseSnapshot:
    integrity = _integrity(connection)
    if integrity != ("ok",):
        raise IntegrityError(f"SQLite integrity_check failed: {integrity!r}")

    schema = _schema_rows(connection)
    table_names = [name for kind, name, _table, _sql in schema if kind == "table"]
    tables = tuple((name, _table_rows(connection, name)) for name in table_names)
    pragmas = tuple(
        (name, connection.execute(f"PRAGMA {name}").fetchone()[0])
        for name in ("application_id", "auto_vacuum", "encoding", "page_size", "user_version")
    )
    return _DatabaseSnapshot(
        pragmas=pragmas,
        schema=schema,
        tables=tables,
        integrity=integrity,
    )


def _read_snapshot(path: Path) -> _DatabaseSnapshot:
    connection = _connect_read_only(path)
    try:
        connection.execute("BEGIN")
        try:
            snapshot = _snapshot(connection)
        finally:
            connection.execute("ROLLBACK")
        return snapshot
    except sqlite3.Error as exc:
        raise MigrationError(f"Unable to inspect SQLite database: {path}") from exc
    finally:
        connection.close()


def _backup_database(source: sqlite3.Connection, destination: sqlite3.Connection) -> None:
    """Copy one consistent source snapshot through SQLite's backup API."""

    source.backup(destination)


def _make_temporary_path(target: Path) -> Path:
    """Create an owned sibling file name without following or removing paths."""

    for _attempt in range(20):
        candidate = target.parent / f".{target.name}.migration-{uuid.uuid4().hex}.tmp"
        try:
            descriptor = os.open(
                candidate,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
                0o600,
            )
        except FileExistsError:
            continue
        os.close(descriptor)
        return candidate.resolve(strict=True)
    raise MigrationError(f"Unable to create an owned temporary SQLite file beside {target}")


def _temporary_path_is_owned(path: Path, target: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
        target_resolved = target.resolve(strict=False)
    except (OSError, RuntimeError):
        return False
    return (
        resolved.is_absolute()
        and resolved.parent == target_resolved.parent
        and resolved.name.startswith(f".{target_resolved.name}.migration-")
        and resolved.name.endswith(".tmp")
        and resolved != target_resolved
    )


def _cleanup_temporary(path: Path | None, target: Path) -> None:
    """Remove only this operation's checked temporary file and sidecars."""

    if path is None or not _temporary_path_is_owned(path, target):
        return
    for candidate in (path, *(path.parent / f"{path.name}{suffix}" for suffix in ("-wal", "-shm", "-journal"))):
        try:
            if candidate.is_file() and candidate.resolve(strict=False).parent == path.parent:
                candidate.unlink()
        except FileNotFoundError:
            continue


def _prepare_destination(path: Path) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(path)
        # A DELETE journal keeps all copied pages in the sibling temporary main
        # file before the no-overwrite publish.  Source WAL mode is still read
        # consistently by backup(); its sidecars are never copied by hand.
        connection.execute("PRAGMA journal_mode = DELETE")
        return connection
    except sqlite3.Error as exc:
        raise MigrationError(f"Unable to prepare temporary SQLite database: {path}") from exc


def _check_source(
    guard: _SourceGuard,
    expected: _DatabaseSnapshot,
) -> None:
    """Check both writer markers and a fresh read snapshot before publishing."""

    if guard.changed():
        raise SourceChangedError(f"SQLite source changed during migration: {guard.path}")

    try:
        current = _read_snapshot(guard.path)
    except (FileNotFoundError, MigrationError) as exc:
        raise SourceChangedError(
            f"SQLite source changed or became unreadable during migration: {guard.path}"
        ) from exc
    if current != expected:
        raise SourceChangedError(f"SQLite source changed during migration: {guard.path}")
    if guard.changed():
        raise SourceChangedError(f"SQLite source changed during migration: {guard.path}")


def _publish_without_overwrite(temporary: Path, target: Path) -> None:
    """Publish by exclusive hard-link creation, which is no-overwrite on Windows."""

    try:
        # The temporary file is a sibling of target, so a hard link is on the
        # same volume and creates target atomically without replacing a race.
        os.link(temporary, target)
    except FileExistsError as exc:
        raise TargetConflictError(f"SQLite target appeared during migration: {target}") from exc
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise TargetConflictError(f"SQLite target appeared during migration: {target}") from exc
        raise MigrationError(f"Unable to publish SQLite target without overwrite: {target}") from exc


def _target_matches_source(target: Path, source_snapshot: _DatabaseSnapshot) -> bool:
    try:
        return _read_snapshot(target) == source_snapshot
    except (FileNotFoundError, MigrationError):
        return False


def migrate(
    source: str | os.PathLike[str],
    target: str | os.PathLike[str],
) -> MigrationResult:
    """Relocate *source* to *target* while retaining the source.

    Existing targets are never overwritten.  An exact existing target is an
    idempotent success; any differing target raises :class:`TargetConflictError`.
    The source and target paths are explicit arguments so callers cannot
    accidentally operate on a default or reset database.
    """

    source_path, target_path = _validate_paths(source, target)
    guard: _SourceGuard | None = None
    source_connection: sqlite3.Connection | None = None
    temporary_path: Path | None = None
    destination_connection: sqlite3.Connection | None = None

    try:
        guard = _SourceGuard.open(source_path)
        source_connection = _connect_read_only(source_path)
        source_connection.execute("BEGIN")
        source_snapshot = _snapshot(source_connection)

        if target_path.exists():
            if _target_matches_source(target_path, source_snapshot):
                _check_source(guard, source_snapshot)
                return MigrationResult(source_path, target_path, published=False)
            raise TargetConflictError(
                f"SQLite target exists with different schema or rows: {target_path}"
            )

        # Do not silently combine a newly published main file with orphaned
        # sidecars from an earlier operation.
        for suffix in ("-wal", "-shm", "-journal"):
            if (target_path.parent / f"{target_path.name}{suffix}").exists():
                raise TargetConflictError(
                    f"SQLite target sidecar exists without target database: "
                    f"{target_path.parent / f'{target_path.name}{suffix}'}"
                )

        temporary_path = _make_temporary_path(target_path)
        destination_connection = _prepare_destination(temporary_path)
        _backup_database(source_connection, destination_connection)
        destination_connection.commit()
        destination_connection.close()
        destination_connection = None

        temporary_snapshot = _read_snapshot(temporary_path)
        if temporary_snapshot != source_snapshot:
            raise MigrationError("SQLite backup failed exact schema/table/row parity")
        _check_source(guard, source_snapshot)

        try:
            _publish_without_overwrite(temporary_path, target_path)
        except TargetConflictError:
            # A concurrent migration may have published the same source.  It
            # is safe to accept that exact result after a fresh parity check;
            # a different or incomplete file remains a conflict.
            _check_source(guard, source_snapshot)
            if _target_matches_source(target_path, source_snapshot):
                return MigrationResult(source_path, target_path, published=False)
            raise
        return MigrationResult(source_path, target_path, published=True)
    except sqlite3.Error as exc:
        raise MigrationError(f"SQLite migration failed for {source_path}") from exc
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            try:
                source_connection.rollback()
            except sqlite3.Error:
                pass
            source_connection.close()
        if guard is not None:
            guard.close()
        _cleanup_temporary(temporary_path, target_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="Existing SQLite database to retain")
    parser.add_argument("--target", required=True, type=Path, help="New SQLite database path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = migrate(args.source, args.target)
    except (FileNotFoundError, MigrationError, ValueError) as exc:
        print(f"SQLite migration failed: {exc}", file=sys.stderr)
        return 2
    state = "published" if result.published else "already present"
    print(f"SQLite migration {state}: {result.source} -> {result.target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
