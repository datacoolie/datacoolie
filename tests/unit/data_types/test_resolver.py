"""Pure tests for the source-aware datatype contract."""

from __future__ import annotations

from dataclasses import fields
import json
from pathlib import Path

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.data_types import (
    LogicalKind,
    TimestampKind,
    TypeSystem,
    infer_type_system,
    normalize_type_system,
    resolve_schema_hint,
)


def test_default_and_alias_type_systems() -> None:
    assert normalize_type_system(None) == "spark_sql"
    assert normalize_type_system("SQL Server") == "mssql"
    assert normalize_type_system(TypeSystem.POSTGRESQL) == "postgresql"
    assert infer_type_system("oracle", None) == "oracle"
    assert infer_type_system(None, None) == "spark_sql"
    assert infer_type_system("oracle", "postgresql") == "postgresql"
    with pytest.raises(ConfigurationError, match="schema_hint_type_system must not be blank"):
        infer_type_system("oracle", " ")


def test_postgresql_int8_is_signed_bigint() -> None:
    resolved = resolve_schema_hint("int8", type_system="postgresql")
    assert resolved.kind is LogicalKind.SIGNED_INTEGER
    assert resolved.bit_width == 64


def test_float_types_resolve_to_shared_logical_widths() -> None:
    assert resolve_schema_hint("real", type_system="postgresql").bit_width == 32
    assert resolve_schema_hint("double", type_system="spark_sql").bit_width == 64
    assert resolve_schema_hint("binary_double", type_system="oracle").bit_width == 64
    assert resolve_schema_hint("float(24)", type_system="mysql").bit_width == 32
    assert resolve_schema_hint("float(25)", type_system="mysql").bit_width == 64
    assert resolve_schema_hint("float(24)", type_system="mssql").bit_width == 32
    assert resolve_schema_hint("float(25)", type_system="mssql").bit_width == 64
    assert resolve_schema_hint("float", type_system="oracle").bit_width == 64
    assert resolve_schema_hint("float(24)", type_system="oracle").bit_width == 32
    assert resolve_schema_hint("float(25)", type_system="oracle").bit_width == 64


def test_mysql_unsigned_and_mssql_tinyint_preserve_range() -> None:
    resolved = resolve_schema_hint("tinyint unsigned", type_system="mysql")
    assert resolved.kind is LogicalKind.UNSIGNED_INTEGER
    assert resolved.unsigned is True
    assert "unsigned" not in {field.name for field in fields(resolved)}
    assert "length" not in {field.name for field in fields(resolved)}
    assert (
        resolve_schema_hint("bigint unsigned", type_system="mysql").bit_width == 64
    )
    assert (
        resolve_schema_hint("tinyint", type_system="mssql").kind
        is LogicalKind.UNSIGNED_INTEGER
    )


def test_vendor_temporal_and_binary_semantics() -> None:
    assert (
        resolve_schema_hint("DATE", type_system="oracle").kind
        is LogicalKind.TIMESTAMP
    )
    assert (
        resolve_schema_hint(
            "timestamp with time zone", type_system="postgresql"
        ).timestamp_kind is TimestampKind.INSTANT
    )
    assert (
        resolve_schema_hint("rowversion", type_system="mssql").kind
        is LogicalKind.BINARY
    )


def test_decimal_requires_complete_parameters_and_detects_conflict() -> None:
    assert (
        resolve_schema_hint("NUMBER(18,2)", type_system="oracle").precision == 18
    )
    assert (
        resolve_schema_hint(
            "DECIMAL", type_system="spark_sql", precision=18, scale=2
        ).precision == 18
    )
    with pytest.raises(ConfigurationError, match="explicit precision and scale"):
        resolve_schema_hint("DECIMAL", type_system="spark_sql")
    with pytest.raises(ConfigurationError, match="conflicts"):
        resolve_schema_hint(
            "DECIMAL(18,2)", type_system="spark_sql", precision=19, scale=2
        )


def test_vendor_decimal_defaults_and_negative_scales_are_normalized() -> None:
    assert resolve_schema_hint("NUMBER(18)", type_system="oracle").scale == 0
    assert resolve_schema_hint("numeric(18)", type_system="postgresql").scale == 0
    assert resolve_schema_hint("NUMBER(18,-2)", type_system="oracle").precision == 20
    assert resolve_schema_hint("numeric(10,-2)", type_system="postgresql").precision == 12
    with pytest.raises(ConfigurationError, match="Negative decimal scale"):
        resolve_schema_hint("decimal(10,-2)", type_system="mysql")


def test_unknown_system_and_type_fail_explicitly() -> None:
    with pytest.raises(ConfigurationError, match="Unsupported schema hint type system"):
        normalize_type_system("duckdb")
    with pytest.raises(ConfigurationError, match="Unsupported Spark SQL"):
        resolve_schema_hint("not_a_real_type", type_system="spark_sql")


def test_independent_reference_cases() -> None:
    path = (
        Path(__file__).parents[2] / "fixtures" / "data_types" / "reference_cases.json"
    )
    cases = json.loads(path.read_text(encoding="utf-8"))
    assert cases
    for case in cases:
        resolved = resolve_schema_hint(
            case["data_type"], type_system=case["type_system"]
        )
        descriptor = case["expected_descriptor"]
        assert resolved.kind.value == descriptor["kind"]
        for field in (
            "bit_width",
            "unsigned",
            "precision",
            "scale",
        ):
            if field in descriptor:
                assert getattr(resolved, field) == descriptor[field]
        if "timestamp_kind" in descriptor:
            assert resolved.timestamp_kind.value == descriptor["timestamp_kind"]
        expected = case["expected"]
        if expected.startswith("decimal"):
            assert resolved.kind in {
                LogicalKind.DECIMAL,
                LogicalKind.UNSIGNED_INTEGER,
            }
        elif expected in {"tinyint", "smallint", "int", "bigint"}:
            assert resolved.kind in {
                LogicalKind.SIGNED_INTEGER,
                LogicalKind.UNSIGNED_INTEGER,
            }
        elif expected == "double":
            assert resolved.kind is LogicalKind.FLOAT
            assert resolved.bit_width == 64
        elif expected == "binary":
            assert resolved.kind is LogicalKind.BINARY
        elif expected == "timestamp_ntz":
            assert resolved.kind is LogicalKind.TIMESTAMP
            assert resolved.timestamp_kind is TimestampKind.NAIVE
        else:
            raise AssertionError(f"Unhandled expected logical type: {expected}")
