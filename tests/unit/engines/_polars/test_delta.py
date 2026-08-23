"""Focused tests for Polars Delta helpers."""

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.delta import (
    build_merge_options,
    raise_if_target_missing,
)


def test_build_merge_options_preserves_aliases_and_builds_predicate() -> None:
    options, source, target = build_merge_options(
        ["ID", "date"], {"source_alias": "s", "target_alias": "t"}
    )
    assert (source, target) == ("s", "t")
    assert options["predicate"] == "t.`ID` = s.`ID` AND t.`date` = s.`date`"


def test_missing_target_is_translated() -> None:
    with pytest.raises(EngineError, match="target path does not exist"):
        raise_if_target_missing(RuntimeError("not found"), "/missing", "Merge")
