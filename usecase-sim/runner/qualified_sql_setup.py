"""Same-process table registration profiles for qualified-SQL scenarios."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable


DATACOOLIE_ROOT = Path(__file__).resolve().parents[2]
DELTA_FIXTURE_ROOT = (
    DATACOOLIE_ROOT / "usecase-sim" / "data" / "output" / "qualified_sql" / "fixtures"
)


def _assert_registered(
    suite: str,
    actual: list[str],
    expected: list[str],
) -> None:
    if sorted(actual) != sorted(expected):
        raise AssertionError(
            f"{suite}: registered tables differ; "
            f"expected={sorted(expected)!r}, actual={sorted(actual)!r}"
        )


def _method(engine: Any, name: str) -> Callable[..., list[str]]:
    method = getattr(engine, name, None)
    if not callable(method):
        raise TypeError(f"{name} requires PolarsEngine")
    return method


def _register_delta_positive(engine: Any) -> None:
    register = _method(engine, "register_delta_tables")
    root = DELTA_FIXTURE_ROOT / "delta-positive"
    registrations = (
        ("name_4", {"logical_prefix": ("catalog_A",), "recursive": True}),
        ("name_3", {"logical_prefix": ("catalog_A",), "recursive": True}),
        ("name_2", {"logical_prefix": ("catalog_A",), "recursive": True}),
        ("name_1", {"logical_prefix": ("catalog_A",), "recursive": True}),
        (
            "include",
            {
                "logical_prefix": ("catalog_A",),
                "recursive": True,
                "include": "database_B.**.d_*",
            },
        ),
        (
            "exclude",
            {
                "logical_prefix": ("catalog_A",),
                "recursive": True,
                "exclude": "**.*_tmp",
            },
        ),
        ("reuse", {"logical_prefix": ("catalog_A",), "recursive": True}),
    )
    actual: list[str] = []
    for relative_root, options in registrations:
        actual.extend(register(str(root / relative_root), **options))
    _assert_registered(
        "delta-positive",
        actual,
        [
            "catalog_A.database_B.schema_C.orders_l4",
            "catalog_A.database_B.schema_C.orders_l3",
            "catalog_A.database_B.schema_C.orders_l2",
            "catalog_A.database_B.schema_C.orders_l1",
            "catalog_A.database_B.schema_C.d_daily",
            "catalog_A.database_B.schema_C.orders_keep",
            "catalog_A.database_B.schema_C.orders_reuse",
        ],
    )


def _register_delta_ambiguity(engine: Any) -> None:
    register = _method(engine, "register_delta_tables")
    actual = register(
        str(DELTA_FIXTURE_ROOT / "delta-ambiguity"),
        logical_prefix=("catalog_A",),
        recursive=True,
    )
    _assert_registered(
        "delta-ambiguity",
        actual,
        [
            "catalog_A.database_A.shared.orders_ambiguous",
            "catalog_A.database_B.shared.orders_ambiguous",
        ],
    )


def _register_iceberg_positive(engine: Any) -> None:
    register = _method(engine, "register_iceberg_tables")
    registrations = (
        (("qsql_positive", "default_root"), {}),
        (
            ("qsql_positive", "logical_prefix"),
            {"logical_prefix": ("warehouse", "analytics", "curated")},
        ),
        (
            ("qsql_positive", "short_name"),
            {"logical_prefix": ("catalog_A", "database_B", "schema_C")},
        ),
        (
            ("qsql_positive", "include"),
            {"include": "qsql_positive.**.d_*"},
        ),
        (
            ("qsql_positive", "exclude"),
            {"exclude": "**.*_tmp"},
        ),
        (
            ("qsql_positive", "reuse"),
            {"logical_prefix": ("catalog_A", "database_B", "schema_C")},
        ),
    )
    actual: list[str] = []
    for namespace, options in registrations:
        actual.extend(register(namespace=namespace, **options))
    _assert_registered(
        "iceberg-positive",
        actual,
        [
            "datacoolie.qsql_positive.default_root.orders_default",
            "warehouse.analytics.curated.orders_prefixed",
            "catalog_A.database_B.schema_C.orders_short",
            "datacoolie.qsql_positive.include.d_daily",
            "datacoolie.qsql_positive.exclude.orders_keep",
            "catalog_A.database_B.schema_C.orders_reuse",
        ],
    )


def _register_iceberg_ambiguity(engine: Any) -> None:
    register = _method(engine, "register_iceberg_tables")
    actual = register(
        namespace=("qsql_amb_a", "shared"),
        logical_prefix=("catalog_A", "database_A", "shared"),
    )
    actual.extend(
        register(
            namespace=("qsql_amb_b", "shared"),
            logical_prefix=("catalog_A", "database_B", "shared"),
        )
    )
    _assert_registered(
        "iceberg-ambiguity",
        actual,
        [
            "catalog_A.database_A.shared.orders_ambiguous",
            "catalog_A.database_B.shared.orders_ambiguous",
        ],
    )


REGISTRATION_PROFILES: dict[str, Callable[[Any], None]] = {
    "delta-positive": _register_delta_positive,
    "delta-ambiguity": _register_delta_ambiguity,
    "iceberg-positive": _register_iceberg_positive,
    "iceberg-ambiguity": _register_iceberg_ambiguity,
}


def register_tables(*, engine: Any, args: list[str]) -> None:
    """Register one fixed fixture suite into the active Polars engine."""

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--suite", required=True, choices=REGISTRATION_PROFILES)
    parsed = parser.parse_args(args)
    REGISTRATION_PROFILES[parsed.suite](engine)
