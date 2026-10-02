"""Focused tests for Polars Delta helpers."""

import pytest
import polars as pl
from unittest.mock import MagicMock, patch

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.delta import (
    build_merge_options,
    merge_overwrite_path,
    raise_if_target_missing,
    table_exists,
    validate_write_options,
)


def test_build_merge_options_preserves_aliases_and_builds_predicate() -> None:
    options, source, target = build_merge_options(
        ["ID", "date"], {"source_alias": "s", "target_alias": "t"}
    )
    assert (source, target) == ("s", "t")
    assert options["predicate"] == "t.`ID` = s.`ID` AND t.`date` = s.`date`"


def test_build_merge_options_preserves_explicit_predicate() -> None:
    options, _, _ = build_merge_options(
        ["id"],
        {"source_alias": "s", "target_alias": "t", "predicate": "custom"},
    )
    assert options["predicate"] == "custom"


@pytest.mark.parametrize(
    ("option", "value", "expected"),
    [
        ("mergeSchema", True, "merge"),
        ("mergeSchema", False, None),
        ("overwriteSchema", True, "overwrite"),
        ("overwriteSchema", False, None),
    ],
)
def test_validate_write_options_maps_canonical_schema_controls(
    option: str, value: bool, expected: str | None
) -> None:
    mode = "overwrite" if option == "overwriteSchema" else "append"
    result = validate_write_options({option: value}, mode=mode)
    assert result == {"schema_mode": expected}


def test_validate_write_options_rejects_conflicting_schema_controls() -> None:
    with pytest.raises(EngineError, match="mergeSchema.*overwriteSchema"):
        validate_write_options(
            {"mergeSchema": "true", "overwriteSchema": "false"}, mode="append"
        )


def test_validate_write_options_rejects_overwrite_schema_for_append() -> None:
    with pytest.raises(EngineError, match="only valid"):
        validate_write_options({"overwriteSchema": True}, mode="append")


def test_missing_target_is_translated() -> None:
    with pytest.raises(EngineError, match="target path does not exist"):
        raise_if_target_missing(RuntimeError("not found"), "/missing", "Merge")


def test_table_exists_propagates_platform_probe_errors() -> None:
    platform = MagicMock()
    platform.folder_exists.side_effect = PermissionError("denied")

    with pytest.raises(PermissionError, match="denied"):
        table_exists(
            "/table",
            platform=platform,
            storage_options={},
            delta_table_cls=MagicMock(),
        )


def test_merge_overwrite_keeps_merge_and_append_options_separate() -> None:
    frame = pl.DataFrame({"id": [1], "value": ["new"]}).lazy()
    merge_builder = MagicMock()
    merge_builder.when_matched_delete.return_value = merge_builder

    with (
        patch(
            "datacoolie.engines._polars.delta.sink_or_write_delta",
            return_value=merge_builder,
        ) as sink,
        patch("datacoolie.engines._polars.delta.write_path") as append,
    ):
        merge_overwrite_path(
            frame,
            "/tmp/events",
            ["id"],
            None,
            {"source_alias": "merge_src", "target_alias": "merge_tgt"},
            storage_options={},
            delta_table_cls=MagicMock(),
            write_options={"schema_mode": "merge"},
        )

    merge_kwargs = sink.call_args.kwargs
    assert merge_kwargs["delta_merge_options"]["source_alias"] == "merge_src"
    assert merge_kwargs["delta_merge_options"]["target_alias"] == "merge_tgt"
    assert "schema_mode" not in merge_kwargs["delta_merge_options"]
    append.assert_called_once()
    assert append.call_args.args[4] == {"schema_mode": "merge"}


def test_merge_overwrite_rejects_invalid_write_options_before_delete() -> None:
    frame = pl.DataFrame({"id": [1], "value": ["new"]}).lazy()
    with (
        patch("datacoolie.engines._polars.delta.sink_or_write_delta") as sink,
        patch("datacoolie.engines._polars.delta.write_path") as append,
        pytest.raises(EngineError, match="schema_mode"),
    ):
        merge_overwrite_path(
            frame,
            "/tmp/events",
            ["id"],
            None,
            None,
            storage_options={},
            delta_table_cls=MagicMock(),
            write_options={"schema_mode": "invalid"},
        )
    sink.assert_not_called()
    append.assert_not_called()


def test_scd2_wraps_custom_predicate_before_current_guard() -> None:
    frame = pl.DataFrame({"id": [1], "value": ["new"]}).lazy()
    merge_builder = MagicMock()
    merge_builder.when_matched_update.return_value = merge_builder
    with patch(
        "datacoolie.engines._polars.delta.sink_or_write_delta",
        return_value=merge_builder,
    ) as sink, patch("datacoolie.engines._polars.delta.write_path"):
        from datacoolie.engines._polars.delta import scd2_path

        scd2_path(
            frame,
            "/tmp/events",
            ["id"],
            None,
            {"predicate": "a OR b"},
            storage_options={},
            delta_table_cls=MagicMock(),
        )
    assert sink.call_args.kwargs["delta_merge_options"]["predicate"] == (
        "(a OR b) AND target.`__is_current` = true"
    )
