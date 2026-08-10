"""Contract tests for the outcome-based DataCoolie AI workflow."""

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
REMOVED_SKILLS = {
    "datacoolie-architect",
    "datacoolie-init",
    "datacoolie-metadata",
    "datacoolie-development",
    "datacoolie-deploy",
}


def _read(relative: str) -> str:
    return (AI_DIR / relative).read_text(encoding="utf-8")


def test_exactly_five_lifecycle_skills_remain() -> None:
    actual = {
        path.parent.name
        for path in SKILLS_DIR.glob("datacoolie-*/SKILL.md")
    }
    assert actual == TARGET_SKILLS
    for name in TARGET_SKILLS:
        content = _read(f"skills/{name}/SKILL.md")
        frontmatter = yaml.safe_load(content.split("---", 2)[1])
        assert frontmatter["name"] == name
        assert len(content.splitlines()) <= 180


def test_agents_is_compact_state_based_orchestrator() -> None:
    agents = _read("AGENTS.md")
    normalized = " ".join(agents.split())
    assert len(agents.splitlines()) <= 130
    for skill in TARGET_SKILLS:
        assert f"`{skill}`" in agents
    assert "discover?" not in agents  # routes are explicit, not a mandatory pseudo-sequence
    assert "bootstraps missing project structure" in normalized
    assert "approval never authorizes deployment" in normalized
    assert "consumes an exact verified build" in normalized
    for build_owned_detail in (
        "run_{platform}",
        "driver.run(stage=group)",
        "YYMMDD",
        "dataflows/{stage}",
    ):
        assert build_owned_detail not in agents
    assert "project_management/phases" not in agents
    assert "gate-reviews" not in agents
    assert "scope.md" not in agents


def test_package_readme_matches_current_ai_workflow() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("## AI-assisted project workflow", 1)[1].split("## Testbed", 1)[0]
    normalized = " ".join(section.split())
    for stale in (
        "stage-level architecture",
        "generated deploy artifacts",
        "project-management gate journals",
        "source2bronze",
        "bronze2silver",
        "silver2gold",
    ):
        assert stale not in normalized
    for current in ("optional discovery", "material design", "conditional provisioning"):
        assert current in normalized


def test_skill_boundaries_have_one_owner() -> None:
    discover = _read("skills/datacoolie-discover/SKILL.md")
    design = _read("skills/datacoolie-design/SKILL.md")
    build = _read("skills/datacoolie-build/SKILL.md")
    provision = _read("skills/datacoolie-provision/SKILL.md")
    release = _read("skills/datacoolie-release/SKILL.md")

    assert "never creates runtime metadata" in discover
    assert "does not inspect sources, author exact metadata or code" in " ".join(design.split())
    assert "sole implementation skill" in build.split("---", 2)[1]
    assert "Resource creation requires explicit approval" not in build
    assert "conditional dependency, not a mandatory lifecycle phase" in provision
    assert "never authors metadata" in release.split("---", 2)[1]
    assert "never rebuilds or repairs the artifact" in release
    assert "Do not edit, regenerate, or rebuild artifacts here" in release


def test_design_is_neutral_single_source_and_build_checks_approval() -> None:
    agents = _read("AGENTS.md")
    design = _read("skills/datacoolie-design/SKILL.md")
    build = _read("skills/datacoolie-build/SKILL.md")
    template = _read("skills/datacoolie-design/templates/architecture.tpl.md")

    normalized_design = " ".join(design.split())
    normalized_agents = " ".join(agents.split())
    normalized_build = " ".join(build.split())
    assert "architecture/current.md" in design
    assert "only design source of truth" in normalized_design
    assert "without binding stages to engines" in normalized_design
    assert "build requires its exact matching receipt" in normalized_agents
    assert "missing, malformed, misnamed, or stale receipt" in normalized_agents
    assert "reject a missing, malformed, or stale" in normalized_build
    assert "approval_required" not in design
    assert "approval_required" not in build
    assert "approval_required" not in template
    assert "architecture_path_and_hash" not in template
    for forbidden in (
        "approval_state", "Medallion", "source2bronze", "bronze2silver",
        "silver2gold", "Key Vault", "architecture/amendments",
    ):
        assert forbidden not in template
    design_templates = SKILLS_DIR / "datacoolie-design" / "templates"
    assert not list(design_templates.glob("layer-*.tpl.md"))


def test_build_owns_all_deterministic_workspace_tooling() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    required = [
        "scripts/validate_config.py",
        "scripts/merge.py",
        "scripts/validate.py",
        "scripts/materialize.py",
        "scripts/render_automation.py",
        "schemas/workspace-config.schema.json",
        "schemas/0.1.0/metadata.schema.json",
        "templates/project-structure.md",
        "references/capability-catalog.md",
        "references/framework-boundary.md",
        "references/runner-contract.md",
    ]
    for relative in required:
        assert (build_dir / relative).is_file(), relative

    materializer = (build_dir / "scripts/materialize.py").read_text(encoding="utf-8")
    assert "--config-validator" not in materializer
    assert "--metadata-merger" not in materializer
    assert 'workspace / ".builds"' in materializer
    assert "input_digest" in materializer
    assert "verify_build" in materializer


def test_workspace_contract_is_canonical_and_minimal() -> None:
    template = _read("skills/datacoolie-build/templates/project-structure.md")
    for layout in (
        "metadata/dataflows.json",
        "metadata/dataflows/{branch}.json",
        "metadata/dataflows/{stage}.json",
        "metadata/dataflows/{branch}/{stage}.json",
        "metadata/dataflows/{stage}/{dataflow}.json",
    ):
        assert layout in template
    assert "Paths never infer or override runtime stage" in template
    assert ".builds/" in template and "{YYMMDD}-{12-char-content-digest}" in template
    assert ".runtime/" in template
    assert "environment-to-platform mapping" in template
    assert "project_management" not in template
    assert "generated/" not in template
    assert "initialization phase" in template


def test_runner_contract_preserves_runtime_semantics() -> None:
    contract = _read("skills/datacoolie-build/references/runner-contract.md")
    for token in (
        "StageGroup = str | list[str]",
        "StagePlan  = list[StageGroup]",
        'action="append"',
        'nargs="+"',
        "driver.run(stage=stage_group)",
        "Stop after a failed group",
        "driver.run(stage=None)",
        "No runtime `--env`",
        "outside `.builds/`",
    ):
        assert token in contract


def test_build_references_have_narrow_non_overlapping_boundaries() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    references = {
        path.name: path.read_text(encoding="utf-8")
        for path in (build_dir / "references").glob("*.md")
    }
    for name in (
        "capability-catalog.md",
        "framework-boundary.md",
        "schema-quick-reference.md",
        "runner-contract.md",
        "operations-contract.md",
    ):
        assert "## Scope" in references[name], name

    assert "StageGroup =" not in references["framework-boundary.md"]
    assert "metadata/\n" not in references["framework-boundary.md"]
    assert "DataCoolieDriver" not in references["capability-catalog.md"]
    assert "references/runner-contract.md" in references["operations-contract.md"]
    assert "inherits common identity" in references["operations-contract.md"].lower()
    assert not (build_dir / "references/framework-usage.md").exists()


def test_build_verification_receipt_contract_is_named_explicitly() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    schema = build_dir / "schemas/build-verification-receipt.schema.json"
    template = build_dir / "templates/build-verification-receipt.json.example"
    assert schema.is_file()
    assert template.is_file()
    assert not (build_dir / "schemas/build-verification.schema.json").exists()
    assert not (build_dir / "templates/build-verification.json.example").exists()
    assert json.loads(schema.read_text(encoding="utf-8"))["title"].endswith("Receipt")
    assert json.loads(template.read_text(encoding="utf-8"))["artifact_type"] == "build_verification"


def test_provision_resources_have_narrow_machine_checked_boundaries() -> None:
    provision_dir = SKILLS_DIR / "datacoolie-provision"
    terraform = (provision_dir / "references/terraform-contract.md").read_text(encoding="utf-8")
    tooling = (provision_dir / "references/platform-tooling.md").read_text(encoding="utf-8")
    schema = json.loads(
        (provision_dir / "schemas/provision-receipt.schema.json").read_text(encoding="utf-8")
    )
    for content in (terraform, tooling):
        assert "## Scope" in content
    assert "does not select resources" in terraform
    assert "does not select resources" in tooling
    assert not list((provision_dir / "references").glob("*.tf.example"))
    assert schema["title"] == "DataCoolie Provision Receipt"
    assert (provision_dir / "scripts/validate_provision.py").is_file()


def test_release_resources_are_consume_only_and_machine_checked() -> None:
    release_dir = SKILLS_DIR / "datacoolie-release"
    references = {
        path.name: path.read_text(encoding="utf-8")
        for path in (release_dir / "references").glob("*.md")
    }
    for name in ("deployment-contract.md", "automation-contract.md", "platform-tooling.md"):
        assert "## Scope" in references[name]
    assert "never materializes" in references["automation-contract.md"]
    assert "installed skill directories" in " ".join(references["automation-contract.md"].split())
    assert not list((release_dir / "references").glob("*.yml.example"))
    schema = json.loads(
        (release_dir / "schemas/release-receipt.schema.json").read_text(encoding="utf-8")
    )
    assert schema["title"] == "DataCoolie Release Receipt"
    assert (release_dir / "scripts/validate_release.py").is_file()


def test_receipt_templates_are_machine_readable() -> None:
    for relative in (
        "skills/datacoolie-build/templates/build-verification-receipt.json.example",
        "skills/datacoolie-release/templates/release-receipt.json.example",
        "skills/datacoolie-provision/templates/provision-receipt.json.example",
    ):
        data = json.loads(_read(relative))
        assert data["schema_version"] == 1
        assert "status" in data
        assert "unresolved_issues" in data


def test_ai_schema_ids_use_one_canonical_namespace() -> None:
    prefix = "https://datacoolie.github.io/datacoolie/schema/"
    schemas = SKILLS_DIR.glob("datacoolie-*/schemas/**/*.json")
    identifiers = []
    for path in schemas:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "$id" in payload:
            identifiers.append((path, payload["$id"]))
    assert identifiers
    for path, identifier in identifiers:
        assert identifier.startswith(prefix), path


def test_framework_package_has_no_project_lifecycle_cli() -> None:
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[project.scripts]" not in pyproject
    assert "datacoolie project" not in pyproject


def test_maintained_ai_sources_do_not_reference_removed_workflow() -> None:
    allowed_roots = [AI_DIR / "AGENTS.md", *SKILLS_DIR.glob("datacoolie-*/**/*")]
    text_files = [
        path
        for path in allowed_roots
        if path.is_file() and path.suffix.lower() in {".md", ".py", ".json", ".yaml", ".yml", ".example"}
    ]
    for path in text_files:
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for removed in REMOVED_SKILLS:
            assert removed not in content, f"{removed} remains in {path}"
        assert "project_management/phases" not in content, path
        assert "gate-reviews" not in content, path


def test_every_lifecycle_skill_has_behavioral_evals() -> None:
    for name in TARGET_SKILLS:
        eval_path = SKILLS_DIR / name / "evals" / "evals.json"
        data = json.loads(eval_path.read_text(encoding="utf-8"))
        assert data["skill_name"] == name
        assert len(data["evals"]) >= 3
        for case in data["evals"]:
            assert case["prompt"]
            assert case["expected_output"]
            assert len(case.get("expectations", [])) >= 2
