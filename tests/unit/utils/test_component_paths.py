from __future__ import annotations

from pathlib import Path

import pytest

from datacoolie.utils.component_paths import (
    ComponentPath,
    ComponentPathError,
    normalize_component_paths,
    select_prefixed_root,
)


def test_normalize_component_paths_detaches_and_rejects_duplicate_prefixes() -> None:
    values = ["release/sql1", "release/sql2"]
    roots = normalize_component_paths(values, name="sql_base_path")

    assert roots == (
        ComponentPath("release/sql1", "sql1"),
        ComponentPath("release/sql2", "sql2"),
    )
    values[0] = "release/changed"
    assert roots[0].base_path == "release/sql1"

    with pytest.raises(ComponentPathError, match="duplicate folder prefix"):
        normalize_component_paths(
            ["team-a/sql", "team-b/SQL"],
            name="sql_base_path",
        )


def test_select_prefixed_root_requires_prefix_for_multiple_roots() -> None:
    roots = (
        ComponentPath("/release/sql1", "sql1"),
        ComponentPath("/release/sql2", "sql2"),
    )

    selected, relative = select_prefixed_root(
        roots,
        "sql2/orders.sql",
        name="SQL file",
    )
    assert selected == roots[1]
    assert relative == "orders.sql"

    with pytest.raises(ComponentPathError, match="must start with one of"):
        select_prefixed_root(roots, "orders.sql", name="SQL file")


def test_single_root_accepts_root_relative_form_and_artifact_roots(tmp_path: Path) -> None:
    roots = normalize_component_paths(
        "artifact:/queries",
        name="sql_base_path",
        artifact_base_path=str(tmp_path),
    )
    assert roots == (ComponentPath(f"{tmp_path.as_posix()}/queries", "queries"),)

    selected, relative = select_prefixed_root(
        roots,
        "orders/incremental.sql",
        name="SQL file",
        allow_unprefixed_single=True,
    )
    assert selected == roots[0]
    assert relative == "orders/incremental.sql"


def test_deferred_artifact_root_is_validated_without_expanding() -> None:
    roots = normalize_component_paths(
        ["artifact:/shared/sql", "project/query"],
        name="sql_base_path",
        allow_deferred_artifact=True,
    )

    assert roots == (
        ComponentPath("artifact:/shared/sql", "sql"),
        ComponentPath("project/query", "query"),
    )

    with pytest.raises(ComponentPathError, match="requires artifact_base_path"):
        normalize_component_paths(
            "artifact:/shared/sql",
            name="sql_base_path",
        )


def test_empty_runtime_roots_are_rejected() -> None:
    with pytest.raises(ComponentPathError, match="at least one path"):
        normalize_component_paths([], name="sql_base_path")
