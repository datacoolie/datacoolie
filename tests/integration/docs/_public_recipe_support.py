"""Helpers for executing the maintained public example recipes."""

from __future__ import annotations

import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import zipfile

from tests.integration.docs.test_public_examples import (
    ROOT,
    REPLAY_RECOVERY,
    REPLAY_RUNNER,
    _zip_project,
)


DATAFLOWS_PAGE = ROOT / "docs" / "examples" / "dataflows.md"
OPERATIONS_PAGE = ROOT / "docs" / "examples" / "operations.md"
_HEADING = re.compile(r"^(?P<marks>#{1,6})\s+.*\{#(?P<anchor>[\w-]+)\}\s*$")
_FENCE = re.compile(r"^\s*(?P<marks>`{3,}|~{3,})(?P<language>[^\s]*)\s*$")


def section(path: Path, *anchors: str) -> str:
    """Return one authored Markdown section, including nested recipe headings."""
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        heading = _HEADING.match(line)
        if heading is None or heading.group("anchor") not in anchors:
            continue
        level = len(heading.group("marks"))
        end = len(lines)
        for candidate, candidate_line in enumerate(lines[index + 1 :], index + 1):
            next_heading = _HEADING.match(candidate_line)
            if next_heading is not None and len(next_heading.group("marks")) <= level:
                end = candidate
                break
        return "\n".join(lines[index:end])
    names = ", ".join(anchors)
    raise AssertionError(
        f"No authored examples section with anchor {names!r} in {path}"
    )


def fenced_blocks(markdown: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    active: tuple[str, str, list[str]] | None = None
    for line in markdown.splitlines():
        fence = _FENCE.match(line)
        if active is None:
            if fence is not None:
                active = (fence.group("marks"), fence.group("language").lower(), [])
            continue
        marks, language, body = active
        if line.strip().startswith(marks[0] * len(marks)):
            blocks.append((language, "\n".join(body)))
            active = None
        else:
            body.append(line)
    assert active is None, "Unclosed recipe code fence"
    return blocks


def recipe_commands(markdown: str) -> list[list[str]]:
    """Parse executable Python commands from authored shell recipe blocks."""
    commands: list[list[str]] = []
    for language, body in fenced_blocks(markdown):
        if language not in {"bash", "sh", "shell"}:
            continue
        pending = ""
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            pending = f"{pending} {line}".strip()
            if pending.endswith("\\"):
                pending = pending[:-1].rstrip()
                continue
            tokens = shlex.split(pending)
            pending = ""
            if not tokens or Path(tokens[0]).name not in {"python", "python3"}:
                continue
            if len(tokens) >= 3 and tokens[1:3] == ["-m", "pip"]:
                continue
            commands.append(tokens)
    return commands


def python_checks(markdown: str) -> list[str]:
    """Return executable assertion snippets authored beside a recipe."""
    return [
        textwrap.dedent(body)
        for language, body in fenced_blocks(markdown)
        if language in {"python", "py"} and "assert " in body
    ]


def append_snippet(markdown: str) -> str | None:
    for language, body in fenced_blocks(markdown):
        if (
            language in {"python", "py"}
            and "source.open" in body
            and "handle.write" in body
        ):
            return textwrap.dedent(body)
    return None


def extract_project(source: Path, tmp_path: Path) -> Path:
    """Extract a generator-produced project archive outside the checkout."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    archive_path = tmp_path / f"{source.name}.zip"
    _zip_project(source, archive_path)
    extract_root = tmp_path / "extracted"
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)
    return extract_root / source.name


def run(
    command: list[str], *, cwd: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _materialize_downloaded_runner(project: Path, script: str) -> str:
    """Copy only the two literal separately downloaded operation runners."""
    source = {
        "replay.py": REPLAY_RUNNER,
        "replay_recovery.py": REPLAY_RECOVERY,
    }.get(script)
    if source is None:
        return script
    target = project / script
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return script


def resolve_recipe_command(command: list[str], project: Path) -> list[str]:
    """Replace only the documented Python executable and preserve every argument."""
    assert command and Path(command[0]).name in {"python", "python3"}
    resolved = [sys.executable, *command[1:]]
    resolved[1] = _materialize_downloaded_runner(project, resolved[1])
    script = Path(resolved[1])
    assert not script.is_absolute(), f"Recipe script must be project-relative: {script}"
    project_root = project.resolve()
    script_path = (project / script).resolve()
    assert script_path.is_relative_to(project_root), script_path
    assert script_path.is_file(), (
        f"Recipe script is not in the extracted project: {script}"
    )
    return resolved


def run_recipe_command(
    command: list[str], *, project: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    resolved = resolve_recipe_command(command, project)
    assert project.is_dir()
    return run(resolved, cwd=project, environment=environment)


def run_documented_checks(
    markdown: str, *, project: Path, environment: dict[str, str]
) -> None:
    for check in python_checks(markdown):
        completed = run(
            [sys.executable, "-c", check], cwd=project, environment=environment
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr


def read_incremental_output(project: Path):
    import polars as pl

    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files
    return output_files, pl.concat(
        [pl.read_parquet(path) for path in output_files]
    ).sort("order_id")


def read_watermark(runtime: Path) -> dict[str, object]:
    candidates = sorted((runtime / "watermarks").rglob("watermark_value.json"))
    assert len(candidates) == 1
    payload = json.loads(candidates[0].read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
