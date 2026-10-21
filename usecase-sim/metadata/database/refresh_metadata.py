"""Recoverable local-workspace framework refresh; never resets source data.

URL comes from DATACOOLIE_METADATA_REFRESH_URL (never printed). Snapshot files
contain exact framework configuration, so keep them private under .runtime.
Mutations require --apply and an already-written snapshot. No live refresh is
authorized merely by installing this helper. Stop concurrent metadata writers
before using it. Other workspaces and tables are outside its mutation scope.
All watermark rows are read-only guards, including retired flows and orphans.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import MetaData, Table, create_engine, delete, or_, select

SIM = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SIM))
from runner.metadata_documents import load_metadata  # noqa: E402

TABLES = (
    "dc_framework_connections",
    "dc_framework_dataflows",
    "dc_framework_schema_hints",
    "dc_framework_watermarks",
)
WORKSPACE = "local-workspace"
DECLARATIVE_TABLES = TABLES[:3]
SNAPSHOT_VERSION = 2


def _tables(engine):
    if engine.dialect.name not in {"sqlite", "postgresql", "mysql", "mssql", "oracle"}:
        raise ValueError("Unsupported metadata dialect")
    metadata = MetaData()
    return {name: Table(name, metadata, autoload_with=engine) for name in TABLES}


def _scope(tables):
    c, f, h = (tables[name] for name in DECLARATIVE_TABLES)
    connections = select(c.c.connection_id).where(c.c.workspace_id == WORKSPACE)
    flows = select(f.c.dataflow_id).where(f.c.workspace_id == WORKSPACE)
    return {
        c.name: c.c.workspace_id == WORKSPACE,
        f.name: f.c.workspace_id == WORKSPACE,
        h.name: or_(h.c.connection_id.in_(connections), h.c.dataflow_id.in_(flows)),
    }


def _encode(value):
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode()}
    raise TypeError(f"Unsupported snapshot type: {type(value).__name__}")


def _decode(value):
    if len(value) == 1:
        if "$datetime" in value:
            return datetime.fromisoformat(value["$datetime"])
        if "$date" in value:
            return date.fromisoformat(value["$date"])
        if "$decimal" in value:
            return Decimal(value["$decimal"])
        if "$bytes" in value:
            return base64.b64decode(value["$bytes"], validate=True)
    return value


def _canonical(value):
    return json.dumps(value, default=_encode, sort_keys=True, separators=(",", ":"))


def _target_id(engine):
    # Bind recovery to the exact DB endpoint without persisting credentials.
    url = engine.url
    return hashlib.sha256(
        _canonical(
            {
                "driver": url.drivername,
                "host": url.host,
                "port": url.port,
                "database": url.database,
                "query": dict(url.query),
            }
        ).encode()
    ).hexdigest()


def _snapshot(conn, engine, tables):
    scope = _scope(tables)
    rows = {}
    for name, table in tables.items():
        query = select(table)
        if name in scope:
            query = query.where(scope[name])
        rows[name] = [dict(r) for r in conn.execute(query).mappings()]
    for values in rows.values():
        values.sort(key=_canonical)
    payload = {
        "version": SNAPSHOT_VERSION,
        "workspace": WORKSPACE,
        "dialect": engine.dialect.name,
        "target_id": _target_id(engine),
        "columns": {name: list(table.c.keys()) for name, table in tables.items()},
        "rows": rows,
    }
    payload["sha256"] = hashlib.sha256(_canonical(payload).encode()).hexdigest()
    return payload


def snapshot(engine):
    tables = _tables(engine)
    with engine.connect() as conn:
        return _snapshot(conn, engine, tables)


def _validate(payload, engine, tables):
    body = {k: v for k, v in payload.items() if k != "sha256"}
    if hashlib.sha256(_canonical(body).encode()).hexdigest() != payload.get("sha256"):
        raise ValueError("Snapshot checksum mismatch")
    if payload.get("version") != SNAPSHOT_VERSION:
        raise ValueError("Obsolete snapshot version; take a fresh snapshot")
    if (
        payload.get("workspace") != WORKSPACE
        or payload.get("dialect") != engine.dialect.name
    ):
        raise ValueError("Snapshot version/workspace/dialect mismatch")
    if payload.get("target_id") != _target_id(engine):
        raise ValueError("Snapshot database endpoint mismatch")
    if payload.get("columns") != {name: list(t.c.keys()) for name, t in tables.items()}:
        raise ValueError("Snapshot schema mismatch")
    rows = payload["rows"]
    if set(rows) != set(TABLES):
        raise ValueError("Snapshot table scope mismatch")
    cids = {r["connection_id"] for r in rows[TABLES[0]]}
    fids = {r["dataflow_id"] for r in rows[TABLES[1]]}
    for name, values in rows.items():
        if any(set(r) != set(tables[name].c.keys()) for r in values):
            raise ValueError("Snapshot row shape mismatch")
    if any(r["workspace_id"] != WORKSPACE for name in TABLES[:2] for r in rows[name]):
        raise ValueError("Snapshot includes another workspace")
    if any(
        r["source_connection_id"] not in cids
        or r["destination_connection_id"] not in cids
        for r in rows[TABLES[1]]
    ):
        raise ValueError("Cross-workspace connection reference requires manual review")
    if any(
        r["connection_id"] not in cids
        or (r["dataflow_id"] and r["dataflow_id"] not in fids)
        for r in rows[TABLES[2]]
    ):
        raise ValueError("Cross-workspace hint reference requires manual review")
    # Global watermark rows may intentionally outlive their declarative flow.


def _guard_watermarks(conn, tables, payload):
    rows = [dict(r) for r in conn.execute(select(tables[TABLES[3]])).mappings()]
    rows.sort(key=_canonical)
    if rows != payload["rows"][TABLES[3]]:
        raise ValueError("Watermark state changed since snapshot; recovery refused")


def _clear(conn, tables):
    scope = _scope(tables)
    # Check outside references before deleting anything.
    c, f, h, _ = (tables[name] for name in TABLES)
    cids = select(c.c.connection_id).where(scope[c.name])
    fids = select(f.c.dataflow_id).where(scope[f.name])
    other_fids = select(f.c.dataflow_id).where(f.c.workspace_id != WORKSPACE)
    outside_flows = (
        select(f)
        .where(f.c.workspace_id != WORKSPACE)
        .where(
            or_(
                f.c.source_connection_id.in_(cids),
                f.c.destination_connection_id.in_(cids),
            )
        )
    )
    outside_hints = select(h).where(
        or_(
            h.c.connection_id.not_in(cids) & h.c.dataflow_id.in_(fids),
            h.c.connection_id.in_(cids) & h.c.dataflow_id.in_(other_fids),
        )
    )
    if conn.execute(outside_flows).first() or conn.execute(outside_hints).first():
        raise ValueError("Cross-workspace references prevent bounded refresh")
    for name in reversed(DECLARATIVE_TABLES):
        conn.execute(delete(tables[name]).where(scope[name]))


def restore(engine, payload):
    tables = _tables(engine)
    _validate(payload, engine, tables)
    with engine.begin() as conn:
        _guard_watermarks(conn, tables, payload)
        _clear(conn, tables)
        for name in DECLARATIVE_TABLES:
            if payload["rows"][name]:
                conn.execute(tables[name].insert(), payload["rows"][name])
    if snapshot(engine) != payload:
        raise RuntimeError("Restore did not reconcile exact framework rows")


def refresh(engine, url, document, payload):
    seed_engine = create_engine(url)
    try:
        if _target_id(seed_engine) != _target_id(engine):
            raise ValueError("Seeder database endpoint differs from snapshot engine")
    finally:
        seed_engine.dispose()
    tables = _tables(engine)
    _validate(payload, engine, tables)
    if snapshot(engine) != payload:
        raise ValueError("Metadata changed since snapshot; take a fresh snapshot")
    # Seeder INSERT-or-skip does not update changed IDs or retire removed flows.
    # Preflight global deterministic IDs against every unrelated workspace.
    from datacoolie.utils.identity import name_to_uuid

    with engine.connect() as conn:
        for name, section, key in [
            (TABLES[0], "connections", "connection_id"),
            (TABLES[1], "dataflows", "dataflow_id"),
        ]:
            ids = [
                r.get(key) or name_to_uuid(r["name"]) for r in document.get(section, [])
            ]
            table = tables[name]
            if (
                ids
                and conn.execute(
                    select(table).where(
                        table.c.workspace_id != WORKSPACE, table.c[key].in_(ids)
                    )
                ).first()
            ):
                raise ValueError("Authored IDs collide with another workspace")
    spec = importlib.util.spec_from_file_location(
        "bounded_metadata_seed", SIM / "scripts/bootstrap/setup_metadata.py"
    )
    setup = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(setup)
    # Failures here roll back their transaction; do not restore over newer rows.
    with engine.begin() as conn:
        if _snapshot(conn, engine, tables) != payload:
            raise ValueError("Metadata changed since snapshot; take a fresh snapshot")
        _clear(conn, tables)
    try:
        setup.seed_db(url, document, WORKSPACE, truncate=False)
        from verify_metadata import verify_connections, verify_dataflows, verify_hints

        if not all(
            r.ok()
            for r in [
                verify_connections(document, engine, WORKSPACE),
                verify_dataflows(document, engine, WORKSPACE),
                verify_hints(document, engine, WORKSPACE),
            ]
        ):
            raise RuntimeError("Refreshed metadata failed exact reconciliation")
        with engine.connect() as conn:
            _guard_watermarks(conn, tables, payload)
    except Exception:
        restore(engine, payload)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["snapshot", "restore", "refresh"])
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument(
        "--json-path", default="scenarios/workflows/local_use_cases.json"
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    path = args.snapshot.resolve()
    runtime = (SIM / ".runtime").resolve()
    if not path.is_relative_to(runtime) or path == runtime:
        raise ValueError("Snapshot must be under simulator .runtime")
    if args.action != "snapshot" and not args.apply:
        raise ValueError(
            "Mutation requires explicit --apply after recovery/reset review"
        )
    engine = create_engine(os.environ["DATACOOLIE_METADATA_REFRESH_URL"])
    try:
        if args.action == "snapshot":
            payload = snapshot(engine)
            _validate(payload, engine, _tables(engine))
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, default=_encode, indent=2)
        else:
            payload = json.loads(path.read_text(), object_hook=_decode)
            if args.action == "restore":
                restore(engine, payload)
            else:
                refresh(
                    engine,
                    os.environ["DATACOOLIE_METADATA_REFRESH_URL"],
                    load_metadata(args.json_path, root=SIM),
                    payload,
                )
        print(f"{args.action} complete: local-workspace framework rows only")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
