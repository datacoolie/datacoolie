"""Execute the dataflow recipes published with the public examples."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.docs._public_recipe_support import (
    DATAFLOWS_PAGE,
    extract_project,
    recipe_commands,
    run_documented_checks,
    run_recipe_command,
    section,
)
from tests.integration.docs.test_public_examples import (
    PROJECT,
    _python_environment,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("anchor", "section_anchors", "source", "expected"),
    [
        (
            "artifact-project-fixture",
            ("artifact-project-fixture", "artifact-project-recipe"),
            PROJECT,
            [(1, "physical"), (2, "digital"), (3, "physical")],
        ),
        (
            "function-project-recipe",
            ("function-project", "function-project-recipe"),
            PROJECT.parent / "function",
            [(1, "hardware"), (2, "software"), (3, "hardware")],
        ),
        (
            "transform-project",
            ("transform-project", "transform-project-recipe"),
            PROJECT.parent / "transform",
            [("1", "hardware"), ("2", "software"), ("3", "hardware")],
        ),
    ],
    ids=["artifact", "function", "transform"],
)
def test_dataflow_project_recipe_executes_authored_command(
    tmp_path: Path,
    anchor: str,
    section_anchors: tuple[str, ...],
    source: Path,
    expected: list[tuple[object, str]],
) -> None:
    """Each dataflow page recipe runs its downloaded project and checks rows."""
    pytest.importorskip("polars")
    markdown = section(DATAFLOWS_PAGE, *section_anchors)
    commands = recipe_commands(markdown)
    command = next(
        (
            candidate
            for candidate in commands
            if any(
                candidate[index].replace("\\", "/").endswith("runners/dev/run.py")
                for index in range(1, len(candidate))
            )
        ),
        None,
    )
    assert command is not None, f"No project runner command in #{anchor}"
    project = extract_project(source, tmp_path)
    completed = run_recipe_command(
        command, project=project, environment=_python_environment()
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

    import polars as pl

    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files
    output = pl.read_parquet(output_files).sort("order_id")
    if anchor == "artifact-project-fixture":
        actual = output.select("order_id", "category_group").rows()
    else:
        actual = output.select("order_id", "category").rows()
    assert actual == expected
    run_documented_checks(markdown, project=project, environment=_python_environment())


@pytest.mark.integration
def test_recipe_resolution_rejects_old_checkout_arguments(tmp_path: Path) -> None:
    """A stale checkout command remains stale instead of being silently repaired."""
    from tests.integration.docs._public_recipe_support import (
        resolve_recipe_command,
        run,
    )

    project = extract_project(PROJECT, tmp_path)
    command = [
        "python",
        "runners/dev/run.py",
        "--metadata-path",
        "docs/examples/files/projects/artifact/metadata",
        "--working-directory",
        "D:/checkout/datacoolie/docs/examples/files/projects/artifact",
        "--state-base-path",
        ".runtime",
    ]
    resolved = resolve_recipe_command(command, project)
    assert resolved[1:] == command[1:]
    completed = run(resolved, cwd=project, environment=_python_environment())
    assert completed.returncode != 0
