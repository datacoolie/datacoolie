"""Execute the dataflow recipes published with the public examples."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.docs._public_recipe_support import (
    DATAFLOWS_PAGE,
    copy_selected_environment,
    extract_project,
    recipe_commands,
    read_incremental_output,
    run_documented_checks,
    run_cli_json,
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
@pytest.mark.parametrize(
    ("anchor", "section_anchors", "source", "expected", "column"),
    [
        (
            "artifact-project-fixture",
            ("artifact-project-fixture", "artifact-project-recipe"),
            PROJECT,
            [(1, "physical"), (2, "digital"), (3, "physical")],
            "category_group",
        ),
        (
            "function-project-recipe",
            ("function-project", "function-project-recipe"),
            PROJECT.parent / "function",
            [(1, "hardware"), (2, "software"), (3, "hardware")],
            "category",
        ),
    ],
    ids=["artifact-built-copy", "function-built-copy"],
)
def test_built_recipe_runs_from_fresh_copy_and_preserves_originals(
    tmp_path: Path,
    anchor: str,
    section_anchors: tuple[str, ...],
    source: Path,
    expected: list[tuple[object, str]],
    column: str,
) -> None:
    """The requested artifact proof runs the CLI-selected bytes in isolation."""
    pytest.importorskip("polars")
    markdown = section(DATAFLOWS_PAGE, *section_anchors)
    command = next(
        (
            candidate
            for candidate in recipe_commands(markdown)
            if any(
                candidate[index].replace("\\", "/").endswith("runners/dev/run.py")
                for index in range(1, len(candidate))
            )
        ),
        None,
    )
    assert command is not None, f"No project runner command in #{anchor}"

    project = extract_project(source, tmp_path / "source")
    environment = _python_environment()
    validated, validation = run_cli_json(
        project,
        "validate",
        "--project-dir",
        str(project),
        "--format",
        "json",
        environment=environment,
    )
    assert validated.returncode == 0, validated.stdout + validated.stderr
    assert validation["ok"] is True

    built, build_payload = run_cli_json(
        project,
        "build",
        "--project-dir",
        str(project),
        "--format",
        "json",
        environment=environment,
    )
    assert built.returncode == 0, built.stdout + built.stderr
    assert build_payload["ok"] is True
    for artifact_path in (
        Path(build_payload["data"]["build_path"]),
        Path(build_payload["data"]["current_path"]),
    ):
        retained, retained_payload = run_cli_json(
            project,
            "validate",
            "--artifact-path",
            str(artifact_path),
            "--format",
            "json",
            environment=environment,
        )
        assert retained.returncode == 0, retained.stdout + retained.stderr
        assert retained_payload["ok"] is True
    execution_parent = tmp_path / "execution copy with spaces"
    execution_parent.mkdir()
    execution, built_command, selected = copy_selected_environment(
        project,
        build_payload,
        execution_parent / anchor,
        command,
    )
    assert not (execution / "data" / "output").exists()
    assert not (execution / ".runtime").exists()
    if anchor == "function-project-recipe":
        packages = selected["functions_artifact"]
        assert packages and packages[0]["packaging"] == "zip"

    first = run_recipe_command(
        built_command, project=execution, environment=environment
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert "completed=1 failed=0 total=1" in first.stdout + first.stderr
    output_files, output = read_incremental_output(execution)
    actual = output.select("order_id", column).rows()
    assert actual == expected
    state_files = [
        path
        for path in (execution / ".runtime").rglob("*")
        if path.is_file() and path.read_bytes()
    ]
    assert state_files
    run_documented_checks(markdown, project=execution, environment=environment)

    second = run_recipe_command(
        built_command, project=execution, environment=environment
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert "completed=1 failed=0 total=1" in second.stdout + second.stderr
    rerun_files, rerun_output = read_incremental_output(execution)
    assert rerun_files == output_files
    assert rerun_output.select("order_id", column).rows() == expected

    for artifact_path in (
        Path(build_payload["data"]["build_path"]),
        Path(build_payload["data"]["current_path"]),
    ):
        retained, retained_payload = run_cli_json(
            project,
            "validate",
            "--artifact-path",
            str(artifact_path),
            "--format",
            "json",
            environment=environment,
        )
        assert retained.returncode == 0, retained.stdout + retained.stderr
        assert retained_payload["ok"] is True


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
