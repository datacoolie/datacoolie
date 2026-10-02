"""Execute the incremental and replay recipes published with the examples."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from tests.integration.docs._public_recipe_support import (
    OPERATIONS_PAGE,
    append_snippet,
    extract_project,
    read_incremental_output,
    read_watermark,
    recipe_commands,
    run,
    run_documented_checks,
    run_recipe_command,
    section,
)
from tests.integration.docs.test_public_examples import (
    INCREMENTAL_PROJECT,
    _python_environment,
)


@pytest.mark.integration
def test_replay_and_recovery_recipes_use_relative_metadata_from_project_cwd(
    tmp_path: Path,
) -> None:
    """Authored replay commands work from fresh extracted projects and can repeat."""
    pytest.importorskip("polars")
    markdown = section(OPERATIONS_PAGE, "replay")
    commands = recipe_commands(markdown)
    replay_command = next(
        (candidate for candidate in commands if Path(candidate[1]).name == "replay.py"),
        None,
    )
    recovery_command = next(
        (
            candidate
            for candidate in commands
            if Path(candidate[1]).name == "replay_recovery.py"
        ),
        None,
    )
    assert replay_command is not None, "No replay command in #replay"
    assert recovery_command is not None, "No recovery command in #replay"
    assert "--working-directory" in replay_command
    assert "--working-directory" in recovery_command
    environment = _python_environment()

    replay_project = extract_project(INCREMENTAL_PROJECT, tmp_path / "replay")
    replay = run_recipe_command(
        replay_command, project=replay_project, environment=environment
    )
    assert replay.returncode == 0, replay.stdout + replay.stderr
    _, first_rows = read_incremental_output(replay_project)
    assert first_rows.select("order_id").to_series().to_list() == [1, 2]
    assert not list(
        (replay_project / ".runtime" / "watermarks").rglob("watermark_value.json")
    )

    recovery_project = extract_project(INCREMENTAL_PROJECT, tmp_path / "recovery")
    recovery = run_recipe_command(
        recovery_command, project=recovery_project, environment=environment
    )
    assert recovery.returncode == 0, recovery.stdout + recovery.stderr
    summary = json.loads(recovery.stdout)
    assert summary["job_id"] == "replay-attempt-2"
    assert summary["total"] == summary["succeeded"] == 1
    assert summary["failed"] == 0
    assert summary["save_watermark"] is True

    recovery_files, output = read_incremental_output(recovery_project)
    assert output.select("order_id").to_series().to_list() == [1, 2]
    assert len(recovery_files) == 2
    assert read_watermark(recovery_project / ".runtime") == {"updated_sequence": 2}

    repeated_recovery = list(recovery_command)
    job_id_index = repeated_recovery.index("--job-id") + 1
    repeated_recovery[job_id_index] = "replay-attempt-3"
    repeated = run_recipe_command(
        repeated_recovery, project=recovery_project, environment=environment
    )
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    repeated_summary = json.loads(repeated.stdout)
    assert repeated_summary["job_id"] == "replay-attempt-3"
    assert repeated_summary["total"] == repeated_summary["succeeded"] == 1
    output_files, output = read_incremental_output(recovery_project)
    assert len(output_files) == 4
    assert output.select("order_id").to_series().to_list() == [1, 1, 2, 2]
    assert read_watermark(recovery_project / ".runtime") == {"updated_sequence": 2}


@pytest.mark.integration
def test_incremental_project_recipe_checks_first_nochange_and_append_runs(
    tmp_path: Path,
) -> None:
    """The authored incremental recipe preserves no-change state and appends rows."""
    pytest.importorskip("polars")
    markdown = section(
        OPERATIONS_PAGE, "incremental-project", "incremental-project-recipe"
    )
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
    assert command is not None, (
        "No incremental project runner command in operations recipe"
    )
    source_snippet = append_snippet(markdown)
    assert source_snippet is not None, (
        "No incremental append snippet in operations recipe"
    )
    project = extract_project(INCREMENTAL_PROJECT, tmp_path)
    environment = _python_environment()
    runtime = project / ".runtime"

    first = run_recipe_command(command, project=project, environment=environment)
    assert first.returncode == 0, first.stdout + first.stderr
    first_files, first_rows = read_incremental_output(project)
    assert first_rows.select("order_id").to_series().to_list() == [1, 2]
    assert read_watermark(runtime) == {"updated_sequence": 2}
    first_bytes = {path.name: path.read_bytes() for path in first_files}

    nochange = run_recipe_command(command, project=project, environment=environment)
    assert nochange.returncode == 0, nochange.stdout + nochange.stderr
    assert "completed=0 failed=0 total=1" in nochange.stdout
    nochange_files, nochange_rows = read_incremental_output(project)
    assert [path.name for path in nochange_files] == [path.name for path in first_files]
    assert nochange_rows.rows() == first_rows.rows()
    assert {path.name: path.read_bytes() for path in nochange_files} == first_bytes
    assert read_watermark(runtime) == {"updated_sequence": 2}

    append_source = run(
        [sys.executable, "-c", source_snippet],
        cwd=project,
        environment=environment,
    )
    assert append_source.returncode == 0, append_source.stdout + append_source.stderr
    append = run_recipe_command(command, project=project, environment=environment)
    assert append.returncode == 0, append.stdout + append.stderr
    output_files, output = read_incremental_output(project)
    assert len(output_files) == len(first_files) + 1
    assert output.select("order_id").to_series().to_list() == [1, 2, 3]
    assert read_watermark(runtime) == {"updated_sequence": 3}
    run_documented_checks(markdown, project=project, environment=environment)
