"""Tests for DatabaseProvider — SQLAlchemy-backed metadata provider.

Uses SQLite in-memory for fast, isolated tests.
"""

from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import MagicMock, call, patch, PropertyMock

import pytest
from sqlalchemy import MetaData, Table, Column, String, create_engine, insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from datacoolie.core.exceptions import MetadataError, WatermarkError
from datacoolie.core.models import Connection, DataFlow, SchemaHint
from datacoolie.metadata.database_provider import (
    DatabaseProvider,
    _connections_table,
    _dataflows_table,
    _schema_hints_table,
    _watermarks_table,
)
from datacoolie.utils.helpers import generate_unique_id

# ============================================================================
# Constants
# ============================================================================

WS = "ws-test-001"
CONN_SRC_ID = "conn-src-001"
CONN_DEST_ID = "conn-dest-001"
DF_ID = "df-001"


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture()
def engine() -> Engine:
    """In-memory SQLite engine."""
    return create_engine("sqlite:///:memory:")


@pytest.fixture()
def provider(engine: Engine) -> DatabaseProvider:
    """DatabaseProvider wired to the in-memory engine."""
    p = DatabaseProvider(engine=engine, workspace_id=WS)
    p.create_tables()
    return p


def _insert_connection(
    engine: Engine,
    *,
    connection_id: str = CONN_SRC_ID,
    workspace_id: str = WS,
    name: str = "source_conn",
    connection_type: str = "file",
    fmt: str = "parquet",
    configure: str = "{}",
    is_active: bool = True,
    deleted_at: Any = None,
    **extra: Any,
) -> None:
    """Insert a connection row."""
    with engine.begin() as conn:
        conn.execute(
            _connections_table.insert().values(
                connection_id=connection_id,
                workspace_id=workspace_id,
                name=name,
                connection_type=connection_type,
                format=fmt,
                configure=configure,
                is_active=is_active,
                deleted_at=deleted_at,
                **extra,
            )
        )


def _insert_dataflow(
    engine: Engine,
    *,
    dataflow_id: str = DF_ID,
    workspace_id: str = WS,
    name: str = "my_dataflow",
    stage: str = "bronze",
    source_connection_id: str = CONN_SRC_ID,
    destination_connection_id: str = CONN_DEST_ID,
    destination_table: str = "target_table",
    destination_load_type: str = "overwrite",
    is_active: bool = True,
    deleted_at: Any = None,
    **extra: Any,
) -> None:
    """Insert a dataflow row."""
    with engine.begin() as conn:
        conn.execute(
            _dataflows_table.insert().values(
                dataflow_id=dataflow_id,
                workspace_id=workspace_id,
                name=name,
                stage=stage,
                source_connection_id=source_connection_id,
                destination_connection_id=destination_connection_id,
                destination_table=destination_table,
                destination_load_type=destination_load_type,
                is_active=is_active,
                deleted_at=deleted_at,
                **extra,
            )
        )


def _insert_schema_hint(
    engine: Engine,
    *,
    connection_id: str = CONN_DEST_ID,
    table_name: str = "target_table",
    column_name: str = "col_a",
    data_type: str = "STRING",
    ordinal_position: int = 0,
    is_active: bool = True,
    deleted_at: Any = None,
    **extra: Any,
) -> None:
    """Insert a schema hint row."""
    with engine.begin() as conn:
        conn.execute(
            _schema_hints_table.insert().values(
                schema_hint_id=generate_unique_id(),
                connection_id=connection_id,
                table_name=table_name,
                column_name=column_name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                is_active=is_active,
                deleted_at=deleted_at,
                **extra,
            )
        )


def _insert_watermark(
    engine: Engine,
    *,
    dataflow_id: str = DF_ID,
    current_value: str = '{"col": "2024-01-01"}',
    previous_value: str | None = None,
) -> None:
    """Insert a watermark row."""
    with engine.begin() as conn:
        conn.execute(
            _watermarks_table.insert().values(
                watermark_id=generate_unique_id(),
                dataflow_id=dataflow_id,
                current_value=current_value,
                previous_value=previous_value,
            )
        )


# ============================================================================
# Construction
# ============================================================================


class TestDatabaseProviderInit:
    """Constructor validation."""

    def test_requires_connection_string_or_engine(self) -> None:
        with pytest.raises(MetadataError, match="Either 'connection_string' or 'engine'"):
            DatabaseProvider(workspace_id=WS)

    def test_accepts_engine(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS)
        assert p._workspace_id == WS

    def test_accepts_connection_string(self) -> None:
        p = DatabaseProvider(connection_string="sqlite:///:memory:", workspace_id=WS)
        assert p._workspace_id == WS
        p.close()

    def test_non_sqlite_connection_passes_pool_options(self) -> None:
        with patch("datacoolie.metadata.database_provider.create_engine") as mock_ce:
            mock_ce.return_value = create_engine("sqlite:///:memory:")
            p = DatabaseProvider(
                connection_string="postgresql://user:pass@localhost/db",
                workspace_id=WS,
                pool_size=7,
                max_overflow=11,
                pool_timeout=9,
                pool_recycle=123,
            )
            kwargs = mock_ce.call_args.kwargs
            assert kwargs["pool_size"] == 7
            assert kwargs["max_overflow"] == 11
            assert kwargs["pool_timeout"] == 9
            assert kwargs["pool_recycle"] == 123
            p.close()

    def test_default_warm_up_retries(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS)
        assert p._retry_handler.retry_count == 1

    def test_custom_warm_up_retries(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS, warm_up_retries=3)
        assert p._retry_handler.retry_count == 3

    def test_zero_warm_up_retries(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS, warm_up_retries=0)
        assert p._retry_handler.retry_count == 0


# ============================================================================
# Warm-up retry (serverless cold-start)
# ============================================================================


class TestWarmUpRetry:
    """Session open retries for serverless cold-start scenarios."""

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_retries_on_connection_error(self, mock_sleep, engine: Engine) -> None:
        """First session open raises, second succeeds — one retry used."""
        p = DatabaseProvider(engine=engine, workspace_id=WS, warm_up_retries=1, warm_up_delay=0.1)
        p.create_tables()

        real_session_calls = 0

        def _flaky_session(eng):
            nonlocal real_session_calls
            real_session_calls += 1
            if real_session_calls == 1:
                raise ConnectionError("serverless warming up")
            from sqlalchemy.orm import Session as _S
            return _S(eng)

        with patch.object(p._retry_handler, "execute", wraps=p._retry_handler.execute), \
             patch("datacoolie.metadata.database_provider.Session", side_effect=_flaky_session):
            result = p.get_connections()

        assert result == []
        assert real_session_calls == 2

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_raises_after_all_warm_up_retries_exhausted(self, mock_sleep, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS, warm_up_retries=1, warm_up_delay=0.1)

        with patch("datacoolie.metadata.database_provider.Session", side_effect=ConnectionError("still cold")):
            with pytest.raises(ConnectionError, match="still cold"):
                p._session()

    def test_no_retry_overhead_on_success(self, provider: DatabaseProvider) -> None:
        """Verify that a healthy DB completes in 1 attempt (no sleep)."""
        with patch("datacoolie.orchestration.retry_handler.time.sleep") as mock_sleep:
            provider.get_connections()
        mock_sleep.assert_not_called()


# ============================================================================
# Connections
# ============================================================================


class TestConnections:
    """Connection fetch methods."""

    def test_fetch_connections_empty(self, provider: DatabaseProvider) -> None:
        assert provider.get_connections() == []

    def test_fetch_connections(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="conn_a")
        _insert_connection(engine, connection_id="conn-src-002", name="conn_b")
        conns = provider.get_connections()
        assert len(conns) == 2
        names = {c.name for c in conns}
        assert names == {"conn_a", "conn_b"}

    def test_active_only_filter(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="active_conn", is_active=True)
        _insert_connection(engine, connection_id="conn-2", name="inactive_conn", is_active=False)
        assert len(provider.get_connections(active_only=True)) == 1
        assert len(provider.get_connections(active_only=False)) == 2

    def test_soft_delete_excluded(self, provider: DatabaseProvider, engine: Engine) -> None:
        from datetime import datetime, timezone
        _insert_connection(engine, name="deleted_conn", deleted_at=datetime.now(timezone.utc))
        assert provider.get_connections() == []

    def test_workspace_scoped(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="my_conn")
        _insert_connection(engine, connection_id="other", workspace_id="other-ws", name="other_conn")
        conns = provider.get_connections()
        assert len(conns) == 1
        assert conns[0].name == "my_conn"

    def test_connection_by_id(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="conn_a")
        conn = provider.get_connection_by_id(CONN_SRC_ID)
        assert conn is not None
        assert conn.name == "conn_a"

    def test_connection_by_id_not_found(self, provider: DatabaseProvider) -> None:
        assert provider.get_connection_by_id("nonexistent") is None

    def test_connection_by_name(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="my_conn")
        conn = provider.get_connection_by_name("my_conn")
        assert conn is not None
        assert conn.connection_id == CONN_SRC_ID

    def test_connection_by_name_not_found(self, provider: DatabaseProvider) -> None:
        assert provider.get_connection_by_name("nonexistent") is None

    def test_connection_config_parsed(self, provider: DatabaseProvider, engine: Engine) -> None:
        cfg = json.dumps({"host": "db.example.com", "port": 5432})
        _insert_connection(engine, name="db_conn", configure=cfg)
        conn = provider.get_connection_by_id(CONN_SRC_ID)
        assert conn is not None
        assert conn.configure["host"] == "db.example.com"

    def test_connection_use_schema_hint_from_config(self, provider: DatabaseProvider, engine: Engine) -> None:
        cfg = json.dumps({"use_schema_hint": False})
        _insert_connection(engine, name="conn_hint", configure=cfg)
        conn = provider.get_connection_by_id(CONN_SRC_ID)
        assert conn is not None
        assert conn.use_schema_hint is False


# ============================================================================
# Dataflows
# ============================================================================


class TestDataflows:
    """Dataflow fetch methods."""

    def _seed_connections(self, engine: Engine) -> None:
        _insert_connection(engine, connection_id=CONN_SRC_ID, name="src_conn")
        _insert_connection(engine, connection_id=CONN_DEST_ID, name="dest_conn")

    def test_fetch_dataflows_empty(self, provider: DatabaseProvider) -> None:
        assert provider.get_dataflows(attach_schema_hints=False) == []

    def test_fetch_dataflows(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine)
        dfs = provider.get_dataflows(attach_schema_hints=False)
        assert len(dfs) == 1
        assert dfs[0].name == "my_dataflow"

    def test_stage_filter_single(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine, dataflow_id="df-1", stage="bronze")
        _insert_dataflow(engine, dataflow_id="df-2", name="silver_df", stage="silver")
        dfs = provider.get_dataflows(stage="bronze", attach_schema_hints=False)
        assert len(dfs) == 1
        assert dfs[0].stage == "bronze"

    def test_stage_filter_list(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine, dataflow_id="df-1", stage="bronze")
        _insert_dataflow(engine, dataflow_id="df-2", name="silver_df", stage="silver")
        _insert_dataflow(engine, dataflow_id="df-3", name="gold_df", stage="gold")
        dfs = provider.get_dataflows(stage=["bronze", "gold"], attach_schema_hints=False)
        assert len(dfs) == 2
        stages = {d.stage for d in dfs}
        assert stages == {"bronze", "gold"}

    def test_active_only_dataflows(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine, dataflow_id="df-1", is_active=True)
        _insert_dataflow(engine, dataflow_id="df-2", name="inactive", is_active=False)
        assert len(provider.get_dataflows(attach_schema_hints=False)) == 1
        assert len(provider.get_dataflows(active_only=False, attach_schema_hints=False)) == 2

    def test_soft_delete_dataflow(self, provider: DatabaseProvider, engine: Engine) -> None:
        from datetime import datetime, timezone
        self._seed_connections(engine)
        _insert_dataflow(engine, deleted_at=datetime.now(timezone.utc))
        assert provider.get_dataflows(attach_schema_hints=False) == []

    def test_orphaned_dataflow_skipped(self, provider: DatabaseProvider, engine: Engine) -> None:
        """Dataflow with missing connection is skipped."""
        _insert_connection(engine, connection_id=CONN_SRC_ID, name="src_only")
        # dest connection does not exist
        _insert_dataflow(engine)
        dfs = provider.get_dataflows(attach_schema_hints=False)
        assert dfs == []

    def test_dataflow_by_id(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine)
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.dataflow_id == DF_ID

    def test_dataflow_by_id_not_found(self, provider: DatabaseProvider) -> None:
        assert provider.get_dataflow_by_id("nonexistent", attach_schema_hints=False) is None

    def test_dataflow_source_connection_linked(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine)
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.source.connection.connection_id == CONN_SRC_ID

    def test_dataflow_destination_connection_linked(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine)
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.destination.connection.connection_id == CONN_DEST_ID

    def test_dataflow_source_config_parsed(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        src_cfg = json.dumps({"fetchsize": 5000})
        _insert_dataflow(engine, source_configure=src_cfg)
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.source.configure.get("fetchsize") == 5000

    def test_dataflow_dest_merge_keys(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(
            engine,
            destination_load_type="merge_upsert",
            destination_merge_keys="id,name",
        )
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.destination.merge_keys == ["id", "name"]

    def test_dataflow_transform_config(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        transform = json.dumps({
            "deduplicate_columns": ["id", "ts"],
            "latest_data_columns": ["ts"],
        })
        _insert_dataflow(engine, transform=transform)
        df = provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False)
        assert df is not None
        assert df.transform.deduplicate_columns == ["id", "ts"]

    def test_dataflow_workspace_scoped(self, provider: DatabaseProvider, engine: Engine) -> None:
        self._seed_connections(engine)
        _insert_dataflow(engine, dataflow_id="df-mine", workspace_id=WS)
        _insert_dataflow(engine, dataflow_id="df-other", workspace_id="other-ws", name="other")
        dfs = provider.get_dataflows(attach_schema_hints=False)
        assert len(dfs) == 1
        assert dfs[0].dataflow_id == "df-mine"

    def test_dataflow_by_id_orphan_returns_none(self, provider: DatabaseProvider, engine: Engine) -> None:
        # Only source connection exists; destination is missing.
        _insert_connection(engine, connection_id=CONN_SRC_ID, name="src_conn")
        _insert_dataflow(engine)
        assert provider.get_dataflow_by_id(DF_ID, attach_schema_hints=False) is None


# ============================================================================
# Schema Hints
# ============================================================================


class TestSchemaHints:
    """Schema hint fetch methods."""

    def test_fetch_schema_hints_empty(self, provider: DatabaseProvider) -> None:
        hints = provider.get_schema_hints(CONN_DEST_ID, "no_table")
        assert hints == []

    def test_fetch_schema_hints(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_schema_hint(engine, column_name="col_a", data_type="STRING", ordinal_position=0)
        _insert_schema_hint(engine, column_name="col_b", data_type="INTEGER", ordinal_position=1)
        hints = provider.get_schema_hints(CONN_DEST_ID, "target_table")
        assert len(hints) == 2
        assert hints[0].column_name == "col_a"
        assert hints[1].column_name == "col_b"

    def test_schema_hints_ordered(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_schema_hint(engine, column_name="z_last", ordinal_position=10)
        _insert_schema_hint(engine, column_name="a_first", ordinal_position=1)
        hints = provider.get_schema_hints(CONN_DEST_ID, "target_table")
        assert hints[0].column_name == "a_first"
        assert hints[1].column_name == "z_last"

    def test_schema_hints_soft_delete(self, provider: DatabaseProvider, engine: Engine) -> None:
        from datetime import datetime, timezone
        _insert_schema_hint(engine, column_name="deleted_col", deleted_at=datetime.now(timezone.utc))
        hints = provider.get_schema_hints(CONN_DEST_ID, "target_table")
        assert hints == []

    def test_schema_hints_case_insensitive_table(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_schema_hint(engine, table_name="MyTable", column_name="col_a")
        hints = provider.get_schema_hints(CONN_DEST_ID, "mytable")
        assert len(hints) == 1

    def test_schema_hints_with_schema_filter(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_schema_hint(engine, schema_name="dbo", column_name="col_a")
        _insert_schema_hint(engine, schema_name="sales", column_name="col_b")
        hints = provider.get_schema_hints(CONN_DEST_ID, "target_table", schema_name="dbo")
        assert len(hints) == 1
        assert hints[0].column_name == "col_a"


# ============================================================================
# Watermarks
# ============================================================================


class TestWatermarks:
    """Watermark get/update operations."""

    def test_get_watermark_none(self, provider: DatabaseProvider) -> None:
        assert provider.get_watermark("nonexistent") is None

    def test_get_watermark(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_watermark(engine, current_value='{"col": "2024-06-15"}')
        wm = provider.get_watermark(DF_ID)
        assert wm == '{"col": "2024-06-15"}'

    def test_get_watermark_empty_value(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_watermark(engine, current_value="")
        assert provider.get_watermark(DF_ID) is None

    def test_update_watermark_insert(self, provider: DatabaseProvider) -> None:
        provider.update_watermark(DF_ID, '{"col": "2024-01-01"}')
        wm = provider.get_watermark(DF_ID)
        assert wm == '{"col": "2024-01-01"}'

    def test_update_watermark_upsert_rotates(self, provider: DatabaseProvider, engine: Engine) -> None:
        """Update shifts current_value → previous_value."""
        _insert_watermark(engine, current_value='{"col": "old"}')
        provider.update_watermark(DF_ID, '{"col": "new"}')
        wm = provider.get_watermark(DF_ID)
        assert wm == '{"col": "new"}'
        # Verify previous_value via raw query
        with engine.begin() as conn:
            row = conn.execute(
                _watermarks_table.select().where(_watermarks_table.c.dataflow_id == DF_ID)
            ).first()
            assert row is not None
            assert row.previous_value == '{"col": "old"}'

    def test_update_watermark_with_job_metadata(self, provider: DatabaseProvider) -> None:
        provider.update_watermark(
            DF_ID,
            '{"col": "v1"}',
            job_id="job-123",
            dataflow_run_id="run-456",
        )
        wm = provider.get_watermark(DF_ID)
        assert wm == '{"col": "v1"}'

    def test_get_watermark_invalid_json(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_watermark(engine, current_value="not-json{")
        wm = provider.get_watermark(DF_ID)
        assert wm == "not-json{"

    def test_get_watermark_db_error_wrapped(self, provider: DatabaseProvider) -> None:
        with patch.object(provider, "_session", side_effect=RuntimeError("db down")):
            with pytest.raises(WatermarkError, match="Failed to read watermark"):
                provider.get_watermark("df-x")


# ============================================================================
# Lifecycle
# ============================================================================


class TestLifecycle:
    """Context manager and close."""

    def test_context_manager(self, engine: Engine) -> None:
        with DatabaseProvider(engine=engine, workspace_id=WS) as p:
            p.create_tables()
            assert p.get_connections() == []

    def test_close_clears_cache(self, provider: DatabaseProvider, engine: Engine) -> None:
        _insert_connection(engine, name="cached")
        provider.get_connections()
        provider.close()
        # Cache was cleared (we need fresh provider to actually verify engine disposed,
        # but we can check no exception at minimum).

    def test_close_disposes_engine(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS)
        with patch.object(engine, "dispose") as dispose:
            p.close()
            dispose.assert_called_once()


class TestDatabaseProviderHelpers:
    def test_supports_row_locks_false_for_sqlite(self, provider: DatabaseProvider) -> None:
        assert provider._supports_row_locks is False

    def test_workspace_filter_matches_workspace(self, provider: DatabaseProvider) -> None:
        clause = provider._workspace_filter(_connections_table)
        assert clause is not None

    def test_soft_delete_filter_without_deleted_at_returns_true(self) -> None:
        t = Table("no_soft_delete", MetaData(), Column("id", String))
        assert DatabaseProvider._soft_delete_filter(t) is True

    def test_close_without_engine_attribute(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS)
        delattr(p, "_engine")
        p.close()  # no-op branch


class TestWatermarkAdvancedBranches:
    def test_update_watermark_uses_row_lock_when_supported(self, provider: DatabaseProvider) -> None:
        with patch.object(DatabaseProvider, "_supports_row_locks", new_callable=PropertyMock, return_value=True):
            with patch.object(provider, "_session") as session_factory:
                session = MagicMock()
                session_factory.return_value.__enter__.return_value = session

                existing = MagicMock(current_value='{"old": 1}')
                session.execute.return_value.first.return_value = existing

                provider.update_watermark("df-lock", '{"new": 1}')

                # First execute call should be the lock query.
                first_stmt = session.execute.call_args_list[0].args[0]
                assert "FOR UPDATE" in str(first_stmt).upper()

    def test_update_watermark_integrity_error_retry_path(self, provider: DatabaseProvider) -> None:
        with patch.object(DatabaseProvider, "_supports_row_locks", new_callable=PropertyMock, return_value=True):
            with patch.object(provider, "_session") as session_factory:
                session = MagicMock()
                session_factory.return_value.__enter__.return_value = session

                existing_first = None
                existing_retry = MagicMock(current_value='{"old": 1}')

                # Sequence: select existing(None) -> insert(IntegrityError) -> retry select(row) -> update(ok)
                execute_results = [
                    MagicMock(first=MagicMock(return_value=existing_first)),
                    IntegrityError("insert", params={}, orig=Exception("dup")),
                    MagicMock(first=MagicMock(return_value=existing_retry)),
                    MagicMock(),
                ]

                def _execute(*args, **kwargs):
                    r = execute_results.pop(0)
                    if isinstance(r, Exception):
                        raise r
                    return r

                session.execute.side_effect = _execute

                provider.update_watermark("df-integrity", '{"new": 1}')
                assert session.rollback.called
                assert session.commit.called

    def test_update_watermark_re_raises_watermark_error(self, provider: DatabaseProvider) -> None:
        with patch.object(provider, "_session", side_effect=WatermarkError("explicit")):
            with pytest.raises(WatermarkError, match="explicit"):
                provider.update_watermark("df-x", '{"v": 1}')

    def test_update_watermark_wraps_generic_error(self, provider: DatabaseProvider) -> None:
        with patch.object(provider, "_session", side_effect=RuntimeError("db down")):
            with pytest.raises(WatermarkError, match="Failed to update watermark"):
                provider.update_watermark("df-x", '{"v": 1}')

    def test_update_watermark_integrity_retry_without_row_locks(self, provider: DatabaseProvider) -> None:
        # sqlite backend => _supports_row_locks is False branch in retry path
        with patch.object(provider, "_session") as session_factory:
            session = MagicMock()
            session_factory.return_value.__enter__.return_value = session

            existing_first = None
            existing_retry = MagicMock(current_value='{"old": 1}')

            execute_results = [
                MagicMock(first=MagicMock(return_value=existing_first)),
                IntegrityError("insert", params={}, orig=Exception("dup")),
                MagicMock(first=MagicMock(return_value=existing_retry)),
                MagicMock(),
            ]

            def _execute(*args, **kwargs):
                r = execute_results.pop(0)
                if isinstance(r, Exception):
                    raise r
                return r

            session.execute.side_effect = _execute

            provider.update_watermark("df-integrity-2", '{"new": 1}')
            assert session.rollback.called
            assert session.commit.called

    def test_create_and_drop_tables(self, engine: Engine) -> None:
        p = DatabaseProvider(engine=engine, workspace_id=WS)
        p.create_tables()
        p.drop_tables()
        p.create_tables()  # should work again
        assert p.get_connections() == []
        p.close()
