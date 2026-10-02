"""Focused contract checks for the public CLI walkthrough and recipes."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
CLI_GUIDE = DOCS / "guide" / "cli"


def _read(name: str) -> str:
    return (CLI_GUIDE / name).read_text(encoding="utf-8")


def test_walkthrough_uses_the_canonical_download_and_stops_at_verified_artifact() -> None:
    content = _read("quickstart.md")

    for fragment in (
        "../../examples/downloads/artifact.zip",
        "docs/examples/files/projects/artifact",
        "validate --project-dir .",
        "inspect config --project-dir .",
        "inspect metadata --project-dir .",
        "build --project-dir . --dry-run",
        "build --project-dir .",
        "inspect artifact --project-dir .",
        "validate --artifact-path .builds/current",
        "../operations/run-stage.md",
    ):
        assert fragment in content, fragment

    assert "does not execute a dataflow" in content
    assert "not create .builds" in content
    assert "data.details.current_comparison.performed: true" in content
    assert "data.details.limited_scope: true" in content


def test_walkthrough_teaches_machine_readable_success_and_dynamic_build_paths() -> None:
    content = _read("quickstart.md")

    assert content.count("--format json") >= 7
    assert "process exit code" in content
    assert "top-level ok" in content
    assert "hard-coded build ID" in content
    assert "data.not_performed" in content
    assert "data.artifact_type: \"datacoolie_build\"" in content

    llms = (DOCS / "llms.txt").read_text(encoding="utf-8")
    assert "guide/cli/quickstart/" in llms


def test_cli_reference_distinguishes_validate_and_inspect_scope_fields() -> None:
    content = _read("commands.md")

    assert "data.details.limited_scope" in content
    assert "data.limited_scope" in content
    assert "data.details.current_comparison" in content
    assert "retained build root" in content
    assert "environment-only path" in content
    assert "without requiring a current comparison" in content


def test_overlay_example_declares_a_complete_dataflow_and_valid_common_fixture() -> None:
    content = _read("project.md")

    assert '"query": "SELECT 1"' in content
    assert '"table": "new_report"' in content
    assert "connection_type: database" in content
    assert "format: sql" in content
    assert "database_type" in content
    assert "common snapshot already contains" in content
    assert "complete dataflow" in content
