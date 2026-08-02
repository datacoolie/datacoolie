"""Native Polars transformer contracts with no Delta Lake dependency."""

from __future__ import annotations

import importlib

import pytest

pl = pytest.importorskip("polars", reason="polars not installed")

from datacoolie.core.exceptions import EngineError, TransformError  # noqa: E402
from datacoolie.core.models import HashColumn, MaskingRule, ValueRule  # noqa: E402
from datacoolie.engines.polars_engine import PolarsEngine  # noqa: E402


@pytest.fixture()
def engine() -> PolarsEngine:
    return PolarsEngine()


def test_native_value_rule_contract(engine: PolarsEngine) -> None:
    frame = pl.DataFrame(
        {
            "left": [" \tA\u00a0 ", None],
            "right": [" B ", " C "],
        }
    ).lazy()

    result = engine.apply_value_rule(
        frame,
        ValueRule(operation="trim", columns=["left", "right"]),
    ).collect()

    assert result.to_dict(as_series=False) == {
        "left": ["\tA\u00a0", None],
        "right": ["B", "C"],
    }


def test_native_regex_replacement_is_literal(engine: PolarsEngine) -> None:
    frame = pl.DataFrame({"text": ["a", None]}).lazy()

    result = engine.apply_value_rule(
        frame,
        ValueRule(
            operation="regex_replace",
            columns=["text"],
            pattern="(a)",
            replacement=r"$1\tail",
        ),
    ).collect()

    assert result["text"].to_list() == [r"$1\tail", None]


def test_native_partial_mask_never_exposes_short_non_empty_value(
    engine: PolarsEngine,
) -> None:
    frame = pl.DataFrame({"phone": ["1234567", "12", "", None]}).lazy()

    result = engine.apply_masking_rule(
        frame,
        MaskingRule(method="partial", columns=["phone"], keep_end=2),
    ).collect()

    assert result["phone"].to_list() == ["*67", "*", "", None]


@pytest.mark.parametrize(
    "rule",
    [
        ValueRule(operation="fill_null", columns=["amount"], value="invalid"),
        MaskingRule(method="redact", columns=["amount"], value="invalid"),
    ],
)
def test_invalid_typed_literal_fails_before_collection(
    engine: PolarsEngine,
    rule: ValueRule | MaskingRule,
) -> None:
    frame = pl.DataFrame({"amount": [1, None]}).lazy()

    with pytest.raises(TransformError, match="literal"):
        if isinstance(rule, ValueRule):
            engine.apply_value_rule(frame, rule)
        else:
            engine.apply_masking_rule(frame, rule)


def test_all_missing_ignored_rule_returns_same_lazy_frame(engine: PolarsEngine) -> None:
    frame = pl.DataFrame({"present": ["value"]}).lazy()

    result = engine.apply_value_rule(
        frame,
        ValueRule(operation="trim", columns=["missing"]),
        missing_column_policy="ignore",
    )

    assert result is frame


def test_polars_hash_dependency_is_lazy_and_explicit(
    engine: PolarsEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pl.DataFrame({"id": [1]}).lazy()
    real_import = importlib.import_module

    def reject_polars_hash(name: str, package: str | None = None):
        if name == "polars_hash":
            raise ImportError("not installed")
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", reject_polars_hash)

    with pytest.raises(EngineError, match="optional polars-hash"):
        engine.add_hash_column(
            frame,
            HashColumn(target_column="id_hash", columns=["id"]),
        )
