"""Create deterministic Delta and Iceberg fixtures for qualified-SQL scenarios."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Any, Iterable

import polars as pl


SCRIPT_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = SCRIPT_DIR.parent
FIXTURE_ROOT = USECASE_SIM_DIR / "data" / "output" / "qualified_sql" / "fixtures"

sys.path.insert(0, str(SCRIPT_DIR))

from _common import iceberg_catalog, setup_logging  # noqa: E402


logger = setup_logging("prepare_polars_qualified_sql")

SAMPLE_ROWS = [
    {"order_id": 101, "amount": 10.0, "category": "alpha"},
    {"order_id": 102, "amount": 20.0, "category": "beta"},
    {"order_id": 103, "amount": 30.0, "category": "gamma"},
]

DELTA_TABLES: dict[str, tuple[str, ...]] = {
    "delta-positive": (
        "name_4/database_B/schema_C/orders_l4",
        "name_3/database_B/schema_C/orders_l3",
        "name_2/database_B/schema_C/orders_l2",
        "name_1/database_B/schema_C/orders_l1",
        "include/database_B/schema_C/d_daily",
        "include/database_B/schema_C/x_monthly",
        "exclude/database_B/schema_C/orders_keep",
        "exclude/database_B/schema_C/orders_tmp",
        "reuse/database_B/schema_C/orders_reuse",
    ),
    "delta-ambiguity": (
        "database_A/shared/orders_ambiguous",
        "database_B/shared/orders_ambiguous",
    ),
}

ICEBERG_TABLES: dict[str, tuple[tuple[str, ...], ...]] = {
    "iceberg-positive": (
        ("qsql_positive", "default_root", "orders_default"),
        ("qsql_positive", "logical_prefix", "orders_prefixed"),
        ("qsql_positive", "short_name", "orders_short"),
        ("qsql_positive", "include", "d_daily"),
        ("qsql_positive", "include", "x_monthly"),
        ("qsql_positive", "exclude", "orders_keep"),
        ("qsql_positive", "exclude", "orders_tmp"),
        ("qsql_positive", "reuse", "orders_reuse"),
    ),
    "iceberg-ambiguity": (
        ("qsql_amb_a", "shared", "orders_ambiguous"),
        ("qsql_amb_b", "shared", "orders_ambiguous"),
    ),
}

SUITES = tuple(DELTA_TABLES) + tuple(ICEBERG_TABLES)


def _assert_scoped_delta_root(path: Path, fixture_root: Path) -> Path:
    root = fixture_root.resolve()
    resolved = path.resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError(f"Delta fixture target must be a child of {root}: {resolved}")
    return resolved


def _assert_scoped_iceberg_identifier(identifier: tuple[str, ...]) -> None:
    if len(identifier) < 2 or not identifier[0].startswith("qsql_"):
        raise ValueError(
            "Iceberg fixture identifiers must belong to a qsql_* namespace: "
            + ".".join(identifier)
        )


def prepare_delta_suite(suite: str, *, fixture_root: Path = FIXTURE_ROOT) -> None:
    """Recreate one local Delta fixture suite below the guarded test root."""

    relative_tables = DELTA_TABLES.get(suite)
    if relative_tables is None:
        raise ValueError(f"Unknown Delta fixture suite: {suite}")

    suite_root = _assert_scoped_delta_root(fixture_root / suite, fixture_root)
    if suite_root.exists():
        shutil.rmtree(suite_root)

    frame = pl.DataFrame(SAMPLE_ROWS)
    for relative_table in relative_tables:
        table_path = _assert_scoped_delta_root(
            suite_root / Path(relative_table), fixture_root
        )
        frame.write_delta(str(table_path))
        logger.info("Created Delta fixture: %s", table_path)


def _purge_tables(catalog: Any, identifiers: Iterable[tuple[str, ...]]) -> None:
    """Purge tables only from the fixed qsql_* fixture namespaces."""

    from pyiceberg.exceptions import NoSuchNamespaceError  # noqa: PLC0415

    namespaces = sorted({identifier[:-1] for identifier in identifiers})
    for namespace in namespaces:
        _assert_scoped_iceberg_identifier(namespace)
        try:
            existing = list(catalog.list_tables(namespace))
        except NoSuchNamespaceError:
            existing = []
        for table_id in existing:
            normalized = (
                tuple(table_id.split("."))
                if isinstance(table_id, str)
                else tuple(str(part) for part in table_id)
            )
            _assert_scoped_iceberg_identifier(normalized)
            catalog.purge_table(table_id)
            logger.info("Purged Iceberg fixture: %s", ".".join(normalized))


def prepare_iceberg_suite(suite: str, *, catalog: Any | None = None) -> None:
    """Recreate one Iceberg fixture suite in guarded local test namespaces."""

    identifiers = ICEBERG_TABLES.get(suite)
    if identifiers is None:
        raise ValueError(f"Unknown Iceberg fixture suite: {suite}")
    for identifier in identifiers:
        _assert_scoped_iceberg_identifier(identifier)

    active_catalog = catalog if catalog is not None else iceberg_catalog()
    if active_catalog is None:
        raise RuntimeError("Iceberg catalog is unavailable")

    _purge_tables(active_catalog, identifiers)

    from datacoolie.engines.polars_engine import PolarsEngine  # noqa: PLC0415

    engine = PolarsEngine(iceberg_catalog=active_catalog)
    frame = pl.DataFrame(SAMPLE_ROWS).lazy()
    for identifier in identifiers:
        # PolarsEngine accepts catalog.namespace.table and removes the catalog
        # component before calling pyiceberg. Prefixing the local catalog name
        # preserves the complete multi-level namespace in ``identifier``.
        table_name = ".".join(("datacoolie", *identifier))
        engine.write_to_table(frame, table_name, "overwrite", "iceberg")
        logger.info("Created Iceberg fixture: %s", ".".join(identifier))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=SUITES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.suite in DELTA_TABLES:
        prepare_delta_suite(args.suite)
    else:
        prepare_iceberg_suite(args.suite)
    logger.info("Qualified-SQL fixture ready: %s", args.suite)


if __name__ == "__main__":
    main()
