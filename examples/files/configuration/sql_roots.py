"""Resolve shorthand SQL references against multiple explicit roots."""

from __future__ import annotations

import tempfile
from pathlib import Path

from datacoolie.orchestration.preparation.query import resolve_query
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    # A root's folder name becomes its required prefix when more than one
    # SQL root is configured: ``sql1/orders.sql`` and ``sql2/orders.sql``.
    with tempfile.TemporaryDirectory(prefix="datacoolie-sql-roots-") as raw_root:
        root = Path(raw_root)
        first = root / "sql1"
        second = root / "sql2"
        first.mkdir()
        second.mkdir()
        (first / "orders.sql").write_text(
            "SELECT 1 AS order_id", encoding="utf-8"
        )
        (second / "orders.sql").write_text(
            "SELECT 2 AS order_id", encoding="utf-8"
        )

        platform = LocalPlatform()
        roots = [str(first), str(second)]
        first_query = resolve_query("sql1/orders.sql", platform, sql_base_path=roots)
        second_query = resolve_query("sql2/orders.sql", platform, sql_base_path=roots)
        print(f"sql1={first_query}; sql2={second_query}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
