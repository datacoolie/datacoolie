import pytest

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY
from datacoolie.core.exceptions import TransformError
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.orchestration.execution.window import build_watermark_window
from datacoolie.orchestration.execution.window import map_watermark_window
from datacoolie.transformers.base import ColumnMapping


def test_rolling_window_uses_observed_upper_and_or_across_columns() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective={"updated_at": "2026-01-01", "ingested_at": "2026-01-02"},
        watermark_after={"updated_at": "2026-01-10", "ingested_at": "2026-01-11"},
        explicit_start=None,
        explicit_end=None,
        start_operator=">",
        end_operator="<",
    )

    assert window == WindowSpec(
        bounds={
            "updated_at": ("2026-01-01", "2026-01-10"),
            "ingested_at": ("2026-01-02", "2026-01-11"),
        },
        lower_operator=">",
        upper_operator="<=",
        combine_operator="OR",
    )


def test_observed_window_preserves_attempt_local_bounds() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective={"modified_at": "2026-09-01T00:00:00+00:00"},
        watermark_after={
            "modified_at": "2026-09-02T00:00:00+00:00",
            "sequence_id": 20,
        },
        explicit_start=None,
        explicit_end=None,
        start_operator=">",
        end_operator="<",
    )

    assert window == WindowSpec(
        bounds={
            "modified_at": (
                "2026-09-01T00:00:00+00:00",
                "2026-09-02T00:00:00+00:00",
            ),
        },
        lower_operator=">",
        upper_operator="<=",
        combine_operator="OR",
    )


def test_replay_window_keeps_explicit_empty_column_out_of_scope() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective=None,
        watermark_after=None,
        explicit_start={"modified_at": "2026-09-01"},
        explicit_end={"modified_at": "2026-09-02"},
        start_operator=">=",
        end_operator="<",
    )

    assert window == WindowSpec(
        bounds={"modified_at": ("2026-09-01", "2026-09-02")},
        lower_operator=">=",
        upper_operator="<",
        combine_operator="OR",
    )


def test_request_end_observation_keeps_exclusive_upper_scope() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective={"updated_at": "2026-01-01"},
        watermark_after={"updated_at": "2026-01-10"},
        explicit_start=None,
        explicit_end=None,
        start_operator=">=",
        end_operator="<=",
        watermark_kind="request_end",
    )

    assert window == WindowSpec(
        bounds={"updated_at": ("2026-01-01", "2026-01-10")},
        lower_operator=">=",
        upper_operator="<",
        combine_operator="OR",
    )


@pytest.mark.parametrize("include_row", [False, True], ids=["folder-only", "mixed-folder-row"])
def test_folder_watermark_cannot_create_replacement_scope(include_row) -> None:
    # Discovery-only and mixed row/discovery keys are independent regressions,
    # so a failure in the first scenario cannot hide the mixed-key assertion.
    lower = {DATE_FOLDER_PARTITION_KEY: "2026-01-01"}
    upper = {DATE_FOLDER_PARTITION_KEY: "2026-01-10"}
    if include_row:
        lower["updated_at"] = "2026-01-01"
        upper["updated_at"] = "2026-01-10"
    window = build_watermark_window(
        enabled=True, watermark_effective=lower, watermark_after=upper,
        explicit_start=None, explicit_end=None, start_operator=">", end_operator="<",
    )
    if include_row:
        assert window is not None
        assert window.bounds == {"updated_at": ("2026-01-01", "2026-01-10")}
    else:
        assert window is None


def test_replay_window_preserves_explicit_half_open_bounds() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective={"updated_at": "ignored"},
        watermark_after={"updated_at": "ignored"},
        explicit_start={"updated_at": "2026-02-01"},
        explicit_end={"updated_at": "2026-03-01"},
        start_operator=">=",
        end_operator="<",
    )

    assert window == WindowSpec(
        bounds={"updated_at": ("2026-02-01", "2026-03-01")},
        lower_operator=">=",
        upper_operator="<",
        combine_operator="OR",
    )


def test_missing_bound_is_not_widened_to_a_delete_scope() -> None:
    window = build_watermark_window(
        enabled=True,
        watermark_effective={"updated_at": "2026-01-01", "created_at": None},
        watermark_after={"updated_at": "2026-01-10"},
        explicit_start=None,
        explicit_end=None,
        start_operator=">",
        end_operator="<=",
    )

    assert window == WindowSpec(bounds={"updated_at": ("2026-01-01", "2026-01-10")})


def test_disabled_replacement_has_no_window() -> None:
    assert build_watermark_window(
        enabled=False,
        watermark_effective={"updated_at": "2026-01-01"},
        watermark_after={"updated_at": "2026-01-10"},
        explicit_start=None,
        explicit_end=None,
        start_operator=">",
        end_operator="<=",
    ) is None


def test_window_bounds_are_immutable() -> None:
    window = WindowSpec(bounds={"updated_at": (1, 2)})
    with pytest.raises(TypeError):
        window.bounds["updated_at"] = (2, 3)  # type: ignore[index]


def test_window_mapping_follows_rename_and_sanitization() -> None:
    mapped = map_watermark_window(
        WindowSpec(bounds={"Updated At": (1, 2)}),
        column_mapping=ColumnMapping({"Updated At": "updated_at"}),
        output_columns=["updated_at", "value"],
    )

    assert mapped == WindowSpec(bounds={"updated_at": (1, 2)})


def test_window_mapping_rejects_removed_or_unknown_column_before_writer() -> None:
    with pytest.raises(TransformError, match="removed or is unmapped"):
        map_watermark_window(
            WindowSpec(bounds={"updated_at": (1, 2)}),
            column_mapping=ColumnMapping({"updated_at": None}),
            output_columns=["value"],
        )

    with pytest.raises(TransformError, match="Cannot resolve"):
        map_watermark_window(
            WindowSpec(bounds={"updated_at": (1, 2)}),
            column_mapping=ColumnMapping({}, known=False),
            output_columns=["updated_at"],
        )
