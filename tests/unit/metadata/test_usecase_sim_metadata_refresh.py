"""Two-workspace reconciliation and isolated exact snapshot/restore regression."""

import copy
import hashlib
import importlib.util
import json
import re
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM / "metadata/database"))
from verify_metadata import verify_hints  # noqa: E402
from refresh_metadata import (  # noqa: E402
    snapshot, restore, refresh, _encode, _decode, _canonical, TABLES,
)


def _setup():
    spec = importlib.util.spec_from_file_location(
        "remediation_metadata_seed", SIM / "scripts/bootstrap/setup_metadata.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _document():
    return {
        "connections": [
            {
                "name": "target",
                "connection_id": "target-id",
                "connection_type": "file",
                "format": "parquet",
            }
        ],
        "dataflows": [
            {
                "name": "target-flow",
                "dataflow_id": "target-flow-id",
                "stage": "test",
                "source_connection_id": "target-id",
                "destination_connection_id": "target-id",
                "source": {"connection_name": "target", "table": "source"},
                "destination": {
                    "connection_name": "target",
                    "table": "orders",
                    "load_type": "append",
                },
            }
        ],
        "schema_hints": [
            {
                "connection_name": "target",
                "connection_id": "target-id",
                "table_name": "orders",
                "hints": [{"column_name": "id", "data_type": "BIGINT"}],
            }
        ],
    }


@pytest.fixture
def database(tmp_path):
    url = "sqlite:///" + (tmp_path / "isolated.db").as_posix()
    engine = create_engine(url)
    initial = _document()
    retired = copy.deepcopy(initial["dataflows"][0])
    retired.update(name="api_wm__date_fmt", dataflow_id="retired-flow-id")
    initial["dataflows"].append(retired)
    _setup().seed_db(url, initial, "local-workspace", truncate=False)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dc_framework_connections(connection_id,workspace_id,name,connection_type,format) VALUES ('other-id','unrelated','target','file','parquet')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dc_framework_schema_hints(schema_hint_id,connection_id,table_name,column_name,data_type) VALUES ('other-hint','other-id','orders','id','BIGINT'),('other-extra','other-id','orders','other','STRING')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dc_framework_watermarks(watermark_id,dataflow_id,current_value,previous_value,job_id,updated_at) VALUES ('state-id','target-flow-id',:current,:previous,'old-job','2026-01-01 12:00:00')"
            ),
            {"current": '{"seq":103}', "previous": '{"seq":102}'},
        )
        conn.execute(text(
            "INSERT INTO dc_framework_dataflows(dataflow_id,workspace_id,name,source_connection_id,destination_connection_id,destination_table,destination_load_type) VALUES ('other-flow-id','unrelated','other-flow','other-id','other-id','orders','append')"
        ))
        for key, flow in [
            ("retired-state", "retired-flow-id"),
            ("orphan-state", "preexisting-orphan-id"),
            ("other-state", "other-flow-id"),
        ]:
            conn.execute(
                text(
                    "INSERT INTO dc_framework_watermarks(watermark_id,dataflow_id,current_value,previous_value,job_id,updated_at) VALUES (:key,:flow,:current,:previous,'preserved-job','2026-01-02 12:00:00.123456')"
                ),
                {"key": key, "flow": flow, "current": ' {"date": "2026-01-02"} ',
                 "previous": '{"date":"2026-01-01"}'},
            )
        conn.execute(text("CREATE TABLE source_sentinel (id INTEGER, value TEXT)"))
        conn.execute(text("INSERT INTO source_sentinel VALUES (1,'preserve')"))
    def forbid_watermark_writes(conn, cursor, statement, parameters, context, many):
        if (
            re.search(r"\b(INSERT|UPDATE|DELETE)\b", statement, re.I)
            and "dc_framework_watermarks" in statement.lower()
            and not context.execution_options.get("test_checkpoint_update")
        ):
            raise AssertionError("Refresh/restore must never write watermarks")

    event.listen(engine, "before_cursor_execute", forbid_watermark_writes)
    yield engine, url
    engine.dispose()


def _unrelated(engine):
    with engine.connect() as c:
        return (
            [
                tuple(r)
                for r in c.execute(
                    text(
                        "SELECT * FROM dc_framework_connections WHERE workspace_id='unrelated'"
                    )
                )
            ],
            [
                tuple(r)
                for r in c.execute(
                    text(
                        "SELECT * FROM dc_framework_schema_hints WHERE connection_id='other-id'"
                    )
                )
            ],
            list(c.execute(text("SELECT * FROM dc_framework_dataflows WHERE workspace_id='unrelated'"))),
            _watermarks(engine),
            list(c.execute(text("SELECT * FROM source_sentinel"))),
        )


def _watermarks(engine):
    # Raw DB values expose accidental timestamp/JSON reserialization too.
    with engine.connect() as conn:
        return list(conn.execute(text(
            "SELECT * FROM dc_framework_watermarks ORDER BY watermark_id"
        )))


def _global_rows(engine):
    with engine.connect() as conn:
        return {
            name: sorted(tuple(r) for r in conn.execute(text(f"SELECT * FROM {name}")))
            for name in (*TABLES, "source_sentinel")
        }


@contextmanager
def _no_metadata_mutation(engine):
    def reject(conn, cursor, statement, parameters, context, many):
        if re.search(r"\b(INSERT|UPDATE|DELETE)\b", statement, re.I):
            raise AssertionError("Stale-state rejection must precede mutation")

    event.listen(engine, "before_cursor_execute", reject)
    try:
        yield
    finally:
        event.remove(engine, "before_cursor_execute", reject)


def test_workspace_hints_neither_extra_nor_mask_wrong_missing_target(database):
    engine, _ = database
    before = _unrelated(engine)
    assert verify_hints(_document(), engine, "local-workspace").ok()
    with engine.begin() as c:
        c.execute(
            text(
                "UPDATE dc_framework_schema_hints SET data_type='STRING' WHERE connection_id='target-id'"
            )
        )
    report = verify_hints(_document(), engine, "local-workspace")
    assert len(report.mismatches) == 1 and not report.extra_in_db
    with engine.begin() as c:
        c.execute(
            text(
                "DELETE FROM dc_framework_schema_hints WHERE connection_id='target-id'"
            )
        )
    report = verify_hints(_document(), engine, "local-workspace")
    assert len(report.missing_in_db) == 1 and not report.extra_in_db
    assert _unrelated(engine) == before


def test_exact_snapshot_refresh_restore_preserves_other_workspace_and_source(database):
    engine, url = database
    # Seeder skips duplicate deterministic IDs; it does not change old values.
    before = snapshot(engine)
    original = _unrelated(engine)
    changed = _document()
    changed["connections"][0]["configure"] = {"base_path": "changed"}
    _setup().seed_db(url, changed, "local-workspace", truncate=False)
    assert snapshot(engine) == before
    wire = json.dumps(before, default=_encode)
    decoded = json.loads(wire, object_hook=_decode)
    assert decoded == before
    refresh(engine, url, changed, decoded)
    assert snapshot(engine) != before
    assert (
        snapshot(engine)["rows"]["dc_framework_watermarks"]
        == before["rows"]["dc_framework_watermarks"]
    )
    assert _unrelated(engine) == original
    restore(engine, decoded)
    assert snapshot(engine) == before
    assert _unrelated(engine) == original


def test_failed_refresh_recovers_exact_rows(database, monkeypatch):
    engine, url = database
    before = snapshot(engine)
    global_before = _global_rows(engine)
    import verify_metadata

    monkeypatch.setattr(
        verify_metadata,
        "verify_connections",
        lambda *a: (_ for _ in ()).throw(RuntimeError("test failure")),
    )
    with pytest.raises(RuntimeError, match="test failure"):
        refresh(engine, url, _document(), before)
    assert snapshot(engine) == before
    assert _global_rows(engine) == global_before


def test_snapshot_tamper_rejects_before_any_change(database):
    engine, _ = database
    before = snapshot(engine)
    bad = copy.deepcopy(before)
    bad["rows"]["dc_framework_connections"][0]["name"] = "tampered"
    with pytest.raises(ValueError, match="checksum"):
        restore(engine, bad)
    assert snapshot(engine) == before


def test_restore_rejects_different_database_endpoint(database, tmp_path):
    engine, _ = database
    before = snapshot(engine)
    other = create_engine("sqlite:///" + (tmp_path / "other.db").as_posix())
    _setup().apply_schema(other)
    try:
        with pytest.raises(ValueError, match="endpoint"):
            restore(other, before)
        with other.connect() as c:
            assert (
                c.execute(
                    text("SELECT count(*) FROM dc_framework_connections")
                ).scalar()
                == 0
            )
    finally:
        other.dispose()


def test_refresh_rejects_different_seed_url(database, tmp_path):
    engine, _ = database
    before = snapshot(engine)
    with pytest.raises(ValueError, match="Seeder database endpoint"):
        refresh(
            engine,
            "sqlite:///" + (tmp_path / "wrong.db").as_posix(),
            _document(),
            before,
        )
    assert snapshot(engine) == before
    assert not (tmp_path / "wrong.db").exists()


def test_refresh_retires_removed_flow_and_restores_exact_state(database):
    engine, url = database
    before, unrelated = snapshot(engine), _unrelated(engine)
    watermarks = _watermarks(engine)
    changed = _document()
    changed["dataflows"] = []
    refresh(engine, url, changed, before)
    assert snapshot(engine)["rows"]["dc_framework_dataflows"] == []
    assert _watermarks(engine) == watermarks
    assert _unrelated(engine) == unrelated
    # Fresh backups accept checkpoints whose flow was already retired.
    retired_snapshot = snapshot(engine)
    refresh(engine, url, changed, retired_snapshot)
    restore(engine, retired_snapshot)
    assert snapshot(engine) == retired_snapshot
    restore(engine, before)
    assert snapshot(engine) == before
    assert _unrelated(engine) == unrelated


def test_refresh_rejects_cross_workspace_flow_before_deletion(database):
    engine, url = database
    with engine.begin() as c:
        c.execute(
            text(
                "UPDATE dc_framework_dataflows SET workspace_id='unrelated' WHERE dataflow_id='target-flow-id'"
            )
        )
    before = snapshot(engine)
    with pytest.raises(ValueError, match="collide|Cross-workspace"):
        refresh(engine, url, _document(), before)
    assert snapshot(engine) == before


def test_refresh_rejects_stale_snapshot(database):
    engine, url = database
    before = snapshot(engine)
    with engine.begin() as c:
        c.execute(
            text(
                "UPDATE dc_framework_connections SET description='edited' WHERE connection_id='target-id'"
            )
        )
    edited = snapshot(engine)
    with _no_metadata_mutation(engine):
        with pytest.raises(ValueError, match="changed since"):
            refresh(engine, url, _document(), before)
    assert snapshot(engine) == edited


def test_refresh_rechecks_state_after_preflight_before_clearing(database, monkeypatch):
    engine, url = database
    before = snapshot(engine)
    with engine.begin() as conn:
        conn.execution_options(test_checkpoint_update=True).execute(text(
            "UPDATE dc_framework_watermarks SET current_value='later-state' WHERE watermark_id='orphan-state'"
        ))
    edited = _global_rows(engine)
    import refresh_metadata

    # Represent preflight having observed the old snapshot just before an update.
    monkeypatch.setattr(refresh_metadata, "snapshot", lambda *args: before)
    with _no_metadata_mutation(engine):
        with pytest.raises(ValueError, match="changed since snapshot"):
            refresh(engine, url, _document(), before)
    assert _global_rows(engine) == edited


@pytest.mark.parametrize("action", ["refresh", "restore"])
@pytest.mark.parametrize("key", ["state-id", "retired-state", "orphan-state", "other-state"])
@pytest.mark.parametrize("change", ["update", "delete", "insert"])
def test_stale_global_watermark_state_rejected_before_metadata_mutation(
    database, action, key, change
):
    engine, url = database
    before = snapshot(engine)
    # Retire a flow first: restoring its declaration must never roll back state.
    if action == "restore":
        refresh(engine, url, _document(), before)
    with engine.begin() as conn:
        conn = conn.execution_options(test_checkpoint_update=True)
        if change == "update":
            conn.execute(text(
                "UPDATE dc_framework_watermarks SET current_value=:value,job_id='later-job',updated_at='2026-10-06 16:00:00.654321' WHERE watermark_id=:key"
            ), {"value": '{"seq":104}', "key": key})
        elif change == "delete":
            conn.execute(text(
                "DELETE FROM dc_framework_watermarks WHERE watermark_id=:key"
            ), {"key": key})
        else:
            conn.execute(text(
                "INSERT INTO dc_framework_watermarks(watermark_id,dataflow_id,current_value) VALUES (:new_key,:new_flow,:value)"
            ), {"new_key": key + "-later", "new_flow": key + "-later-flow",
                "value": '{"seq":104}'})
    edited = _global_rows(engine)
    with _no_metadata_mutation(engine):
        with pytest.raises(ValueError, match="changed since snapshot"):
            if action == "restore":
                restore(engine, before)
            else:
                refresh(engine, url, _document(), before)
    assert _global_rows(engine) == edited


def test_refresh_seed_failure_recovers_all_global_rows(database, monkeypatch):
    engine, url = database
    before, global_before = snapshot(engine), _global_rows(engine)
    import refresh_metadata

    actual_load = refresh_metadata.importlib.util.module_from_spec

    def load(spec):
        module = actual_load(spec)
        if spec.name == "bounded_metadata_seed":
            actual_exec = spec.loader.exec_module

            def exec_module(target):
                actual_exec(target)

                def fail_after_partial_seed(url, document, workspace, truncate):
                    assert not truncate
                    with engine.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO dc_framework_connections(connection_id,workspace_id,name,connection_type,format) VALUES ('partial','local-workspace','partial','file','parquet')"
                        ))
                    raise RuntimeError("partial seed failed")

                target.seed_db = fail_after_partial_seed

            spec.loader.exec_module = exec_module
        return module

    monkeypatch.setattr(refresh_metadata.importlib.util, "module_from_spec", load)
    with pytest.raises(RuntimeError, match="partial seed failed"):
        refresh(engine, url, _document(), before)
    assert snapshot(engine) == before
    assert _global_rows(engine) == global_before


def test_recovery_refuses_later_watermark_update_after_failed_refresh(database, monkeypatch):
    engine, url = database
    before = snapshot(engine)
    import verify_metadata

    def fail_after_checkpoint_update(*args):
        with engine.begin() as conn:
            conn.execution_options(test_checkpoint_update=True).execute(text(
                "UPDATE dc_framework_watermarks SET current_value='later-checkpoint' WHERE watermark_id='retired-state'"
            ))
        raise RuntimeError("reconciliation failed")

    monkeypatch.setattr(verify_metadata, "verify_connections", fail_after_checkpoint_update)
    with pytest.raises(ValueError, match="Watermark state changed") as failure:
        refresh(engine, url, _document(), before)
    assert isinstance(failure.value.__context__, RuntimeError)
    after = _global_rows(engine)
    assert snapshot(engine)["rows"]["dc_framework_dataflows"][0]["name"] == "target-flow"
    with _no_metadata_mutation(engine):
        with pytest.raises(ValueError, match="Watermark state changed"):
            restore(engine, before)
    assert _global_rows(engine) == after
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT current_value FROM dc_framework_watermarks WHERE watermark_id='retired-state'"
        )).scalar_one() == "later-checkpoint"


def test_obsolete_snapshot_requires_new_backup_without_mutation(database):
    engine, url = database
    before, global_before = snapshot(engine), _global_rows(engine)
    obsolete = copy.deepcopy(before)
    obsolete["version"] = 1
    obsolete["sha256"] = hashlib.sha256(_canonical(
        {k: v for k, v in obsolete.items() if k != "sha256"}
    ).encode()).hexdigest()
    original_wire = json.dumps(obsolete, default=_encode)
    with _no_metadata_mutation(engine):
        for action in (lambda: restore(engine, obsolete),
                       lambda: refresh(engine, url, _document(), obsolete)):
            with pytest.raises(ValueError, match="Obsolete.*fresh snapshot"):
                action()
    assert json.dumps(obsolete, default=_encode) == original_wire
    assert _global_rows(engine) == global_before


def test_revised_broad_metadata_reconciles_isolated_sqlite_and_exports(tmp_path):
    from runner.metadata_documents import load_metadata
    from verify_metadata import verify_connections, verify_dataflows
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.platforms.local_platform import LocalPlatform

    document = load_metadata("scenarios/workflows/local_use_cases.json", root=SIM)
    names = [f["name"] for f in document["dataflows"]]
    assert "api_request__max_pages" not in names and "api_wm__date_fmt" not in names
    setup = _setup()
    for suffix in ("json", "yaml", "xlsx"):
        path = tmp_path / f"revised.{suffix}"
        if suffix == "json":
            path.write_text(json.dumps(document))
        elif suffix == "yaml":
            setup.emit_yaml(document, path)
        else:
            setup.emit_xlsx(document, path)
        provider = FileProvider(
            config_path=str(path),
            platform=LocalPlatform(),
            watermark_base_path=str(tmp_path / suffix),
        )
        try:
            provider.initialize()
            exported = provider.get_dataflows(active_only=False)
            assert {f.name for f in exported} == set(names)
            assert len(exported) == len(names)
        finally:
            provider.close()
    url = "sqlite:///" + (tmp_path / "revised.db").as_posix()
    setup.seed_db(url, document, "local-workspace", truncate=False)
    engine = create_engine(url)
    try:
        assert verify_connections(document, engine, "local-workspace").ok()
        assert verify_dataflows(document, engine, "local-workspace").ok()
        assert verify_hints(document, engine, "local-workspace").ok()
    finally:
        engine.dispose()
