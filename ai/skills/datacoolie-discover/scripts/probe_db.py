"""Run one bounded, read-only SQL probe for a discovery evidence gap."""
from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from _artifact_io import atomic_write_json
from _probe_status import utc_now
from introspect_db import _build_odbc_url, _read_env_value

_MUTATING_TOKEN = re.compile(
    r"\b(ALTER|CALL|COPY|CREATE|DELETE|DROP|EXEC|EXECUTE|GRANT|INSERT|INTO|MERGE|REINDEX|REVOKE|TRUNCATE|UPDATE|VACUUM)\b",
    re.IGNORECASE,
)


def _without_comments_and_literals(sql: str) -> str:
    """Replace comments and quoted values before keyword validation."""
    result: list[str] = []
    index = 0
    state = "plain"
    while index < len(sql):
        char = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""
        if state == "plain" and char == "-" and nxt == "-":
            state, index = "line-comment", index + 2
            result.append("  ")
            continue
        if state == "plain" and char == "/" and nxt == "*":
            state, index = "block-comment", index + 2
            result.append("  ")
            continue
        if state == "plain" and char in {"'", '"'}:
            state = "single-quote" if char == "'" else "double-quote"
            result.append(" ")
            index += 1
            continue
        if state == "line-comment":
            if char in "\r\n":
                state = "plain"
                result.append(char)
            else:
                result.append(" ")
            index += 1
            continue
        if state == "block-comment":
            if char == "*" and nxt == "/":
                state, index = "plain", index + 2
                result.append("  ")
            else:
                result.append(" ")
                index += 1
            continue
        if state in {"single-quote", "double-quote"}:
            quote = "'" if state == "single-quote" else '"'
            if char == quote and nxt == quote:
                result.append("  ")
                index += 2
            elif char == quote:
                state = "plain"
                result.append(" ")
                index += 1
            else:
                result.append(" ")
                index += 1
            continue
        result.append(char)
        index += 1
    if state in {"single-quote", "double-quote", "block-comment"}:
        raise ValueError("SQL contains an unterminated quote or comment")
    return "".join(result)


def validate_read_only_sql(sql: str) -> str:
    checked = _without_comments_and_literals(sql).strip()
    if not checked:
        raise ValueError("SQL file is empty")
    statements = [part.strip() for part in checked.split(";") if part.strip()]
    if len(statements) != 1:
        raise ValueError("SQL probe must contain exactly one statement")
    statement = statements[0]
    if not re.match(r"^(SELECT|WITH)\b", statement, re.IGNORECASE):
        raise ValueError("SQL probe must start with SELECT or WITH")
    match = _MUTATING_TOKEN.search(statement)
    if match:
        raise ValueError(f"SQL probe contains prohibited token: {match.group(1).upper()}")
    if re.search(r"\bFOR\s+UPDATE\b", statement, re.IGNORECASE):
        raise ValueError("SQL probe cannot acquire update locks")
    return sql.strip().rstrip(";").strip()


def _enforce_read_only(connection: Any, dialect: str) -> bool:
    if dialect == "postgresql":
        connection.execute(text("SET TRANSACTION READ ONLY"))
        return True
    if dialect == "sqlite":
        connection.execute(text("PRAGMA query_only = ON"))
        return True
    return False


def _apply_timeout(connection: Any, dialect: str, seconds: int) -> bool:
    milliseconds = seconds * 1000
    if dialect == "postgresql":
        connection.execute(text(f"SET LOCAL statement_timeout = {milliseconds}"))
        return True
    if dialect == "sqlite":
        deadline = time.monotonic() + seconds
        raw = connection.connection.driver_connection
        raw.set_progress_handler(lambda: 1 if time.monotonic() >= deadline else 0, 1000)
        return True
    if dialect == "mssql":
        raw = connection.connection.driver_connection
        if hasattr(raw, "timeout"):
            raw.timeout = seconds
            return True
    return False


def run_probe(
    url: str,
    sql: str,
    output: Path,
    max_rows: int,
    timeout_seconds: int,
    allow_unenforced_timeout: bool = False,
) -> None:
    validated_sql = validate_read_only_sql(sql)
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                read_only_enforced = _enforce_read_only(connection, engine.dialect.name)
                timeout_enforced = _apply_timeout(connection, engine.dialect.name, timeout_seconds)
                if not timeout_enforced and not allow_unenforced_timeout:
                    raise ValueError(
                        "The selected driver cannot enforce the requested timeout; "
                        "use --allow-unenforced-timeout only after assessing query cost"
                    )
                result = connection.execute(text(validated_sql))
                rows = result.fetchmany(max_rows + 1)
                payload = {
                    "generated_at": utc_now(),
                    "method": "targeted-read-only-sql",
                    "dialect": engine.dialect.name,
                    "timeout_seconds": timeout_seconds,
                    "timeout_enforced": timeout_enforced,
                    "read_only_enforced": read_only_enforced,
                    "max_rows": max_rows,
                    "truncated": len(rows) > max_rows,
                    "columns": list(result.keys()),
                    "rows": [list(row) for row in rows[:max_rows]],
                }
            finally:
                transaction.rollback()
    finally:
        engine.dispose()
    atomic_write_json(output, payload)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a targeted read-only discovery SQL probe.", allow_abbrev=False,
    )
    connection = parser.add_mutually_exclusive_group(required=True)
    connection.add_argument("--url-env")
    connection.add_argument("--odbc-connstr-env")
    parser.add_argument("--dialect", default="mssql+pyodbc")
    parser.add_argument("--sql-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-rows", type=int, default=100)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--allow-unenforced-timeout", action="store_true")
    args = parser.parse_args(argv)
    if args.max_rows < 1 or args.max_rows > 1000:
        parser.error("--max-rows must be between 1 and 1000")
    if args.timeout_seconds < 1 or args.timeout_seconds > 300:
        parser.error("--timeout-seconds must be between 1 and 300")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.url_env:
        url = _read_env_value(args.url_env, "--url-env")
    else:
        url = _build_odbc_url(
            _read_env_value(args.odbc_connstr_env, "--odbc-connstr-env"), args.dialect,
        )
    try:
        sql = args.sql_file.read_text(encoding="utf-8")
        run_probe(
            url, sql, args.output, args.max_rows, args.timeout_seconds,
            args.allow_unenforced_timeout,
        )
    except (OSError, ValueError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    except SQLAlchemyError as exc:
        raise SystemExit("ERROR: Database probe failed; no query text or credentials were logged") from exc
    print(f"Wrote bounded probe evidence to {args.output}")


if __name__ == "__main__":
    main()
