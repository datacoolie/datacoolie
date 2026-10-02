from __future__ import annotations

import json
from pathlib import Path

import yaml


AI_DIR = Path(__file__).resolve().parents[3]
REPO_ROOT = AI_DIR.parent
SKILLS_DIR = AI_DIR / "skills"
TARGET_SKILLS = {
    "datacoolie-discover",
    "datacoolie-design",
    "datacoolie-build",
    "datacoolie-provision",
    "datacoolie-release",
}


def _read(relative: str) -> str:
    return (AI_DIR / relative).read_text(encoding="utf-8")


def test_exactly_five_lifecycle_skills_remain() -> None:
    actual = {path.parent.name for path in SKILLS_DIR.glob("datacoolie-*/SKILL.md")}
    assert actual == TARGET_SKILLS
    for name in TARGET_SKILLS:
        content = _read(f"skills/{name}/SKILL.md")
        frontmatter = yaml.safe_load(content.split("---", 2)[1])
        assert frontmatter["name"] == name
        assert len(content.splitlines()) <= 210


def test_agents_and_build_skill_use_one_cli_project_contract() -> None:
    agents = _read("AGENTS.md")
    build = _read("skills/datacoolie-build/SKILL.md")
    combined = f"{agents}\n{build}"
    for token in ("datacoolie.yml", "dc validate", "dc build", "dc inspect", "run_attributes", "log_base_path"):
        assert token in combined
    for stale_positive in (
        "config.yaml environment-to-platform mapping",
        "current/build.json identifies",
        "SHA256SUMS sidecar",
        "scripts/materialize.py",
    ):
        assert stale_positive not in combined


def test_build_consumes_hash_bound_design_approval_without_copying_validator() -> None:
    build = _read("skills/datacoolie-build/SKILL.md")
    design = _read("skills/datacoolie-design/SKILL.md")
    for token in (
        "design_approval.py verify",
        "--workspace <project>",
        "--architecture <project>/architecture/current.md",
        "missing, stale or unavailable",
        "do not copy its hash/receipt",
        "validator into Build",
    ):
        assert token in build
    assert "Any byte change invalidates its receipt" in design


def test_design_and_provision_use_configured_component_roots() -> None:
    design = _read("skills/datacoolie-design/SKILL.md")
    template = _read("skills/datacoolie-design/templates/architecture.tpl.md")
    provision = _read("skills/datacoolie-provision/SKILL.md")
    assert "each configured functions root" in design
    assert "different\npackaging results" in template
    assert "configured metadata component" in provision
    assert "fixed target components named `metadata`" not in provision


def test_runner_and_project_docs_match_current_layout() -> None:
    agents = (REPO_ROOT / "ai" / "AGENTS.md").read_text(encoding="utf-8")
    assert "https://datacoolie.github.io/datacoolie/examples/" in agents
    assert "runtime runner" in agents

    project_docs = (REPO_ROOT / "docs" / "guide" / "cli" / "project.md").read_text(
        encoding="utf-8"
    )
    for token in (
        "datacoolie.yml",
        "dataflows/",
        "runners/<env>",
        "artifacts/<build_id>/",
        "current/",
        "manifest.json",
        "deployment_path",
        "artifact:/",
        "sql/orders/incremental.sql",
    ):
        assert token in project_docs
    assert "config.yaml" not in project_docs
    assert "current/build.json" not in project_docs

    runner = _read("skills/datacoolie-build/references/runner-contract.md")
    for token in ("artifact_base_path", "metadata_base_path", "sql_base_path", "state_base_path", "log_base_path", "run_attributes"):
        assert token in runner
    assert "base_log_path` is absent" in runner


def test_build_and_release_have_no_duplicate_implementation_files() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    for relative in (
        "scripts/materialize.py",
        "scripts/merge.py",
        "scripts/validate.py",
        "scripts/validate_config.py",
        "scripts/validate_build.py",
        "scripts/validate_functions.py",
        "scripts/convert.py",
        "scripts/_loaders.py",
        "scripts/_schema_resolver.py",
        "scripts/lint.py",
        "scripts/inspect_capabilities.py",
    ):
        assert not (build_dir / relative).exists(), relative
    release_dir = SKILLS_DIR / "datacoolie-release"
    for relative in (
        "scripts/_artifact_validation.py",
        "scripts/validate_release.py",
        "schemas/release-receipt.schema.json",
        "templates/release-receipt.json.example",
    ):
        assert not (release_dir / relative).exists(), relative
    assert (build_dir / "scripts/render_automation.py").is_file()
    assert (release_dir / "scripts/validate_upload_record.py").is_file()
    assert not (build_dir / "references/capability-catalog.md").exists()
    assert not (build_dir / "templates/project-structure.md").exists()


def test_release_is_upload_only_and_reads_destination_from_project_contract() -> None:
    release = _read("skills/datacoolie-release/SKILL.md")
    for token in (
        "upload-only",
        "environments.<env>.deployment_path",
        "<deployment_path>/artifacts/<build_id>/",
        "<deployment_path>/current/",
        "current_comparison.ok",
        "partial_failure",
    ):
        assert token in release
    for forbidden in ("activation", "package installation", "compare remote hashes", "build.json"):
        assert forbidden in release  # each is explicitly marked out of scope
    assert "## Routing" in release


def test_functions_contract_documents_per_root_auto_rule() -> None:
    content = _read("skills/datacoolie-build/references/python-functions-contract.md")
    for token in ("one or more functions roots", "root-level `__init__.py`", "nested package", "wheel", "copy"):
        assert token in content
    assert "`functions_artifact` is a list" in content


def test_cli_aliases_are_declared() -> None:
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'dc = "datacoolie.cli.main:main"' in pyproject
    assert 'datacoolie = "datacoolie.cli.main:main"' in pyproject


def test_all_lifecycle_skills_have_machine_readable_evals() -> None:
    for name in TARGET_SKILLS:
        path = SKILLS_DIR / name / "evals" / "evals.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["skill_name"] == name
        assert document["eval_schema_version"] == 2
        assert set(document["case_kinds"]) == {"decision", "execution"}
        assert document["capability_families"]
        assert len(document["evals"]) >= 3
        for case in document["evals"]:
            assert case.get("prompt")
            assert case.get("expected_output")
            assert "files" in case
            assert len(case.get("expectations", [])) >= 2


def test_ai_sources_do_not_reintroduce_removed_project_workflows() -> None:
    for path in SKILLS_DIR.glob("datacoolie-*/**/*"):
        if not path.is_file() or path.suffix.lower() not in {".md", ".py", ".json", ".yaml", ".yml", ".example"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        assert "project_management/phases" not in text, path
        assert "gate-reviews" not in text, path
