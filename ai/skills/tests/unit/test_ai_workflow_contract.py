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
    for current in ("mandatory new-project discovery", "material design", "conditional provisioning"):
        assert current in normalized


def test_new_projects_discover_every_declared_source_before_design() -> None:
    agents = " ".join(_read("AGENTS.md").split())
    discover = " ".join(_read("skills/datacoolie-discover/SKILL.md").split())
    design = " ".join(_read("skills/datacoolie-design/SKILL.md").split())
    assert "New project | `discover -> design -> build`" in agents
    assert "probe each source even when" in discover
    for probe in ("introspect_db.py", "introspect_files.py", "introspect_api.py", "introspect_lakehouse.py"):
        assert probe in discover
    assert "new project requires discovery evidence for every declared source" in design.lower()


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
        "scripts/validate_functions.py",
        "scripts/render_automation.py",
        "schemas/workspace-config.schema.json",
        "schemas/current-build.schema.json",
        "schemas/0.1.0/metadata.schema.json",
        "templates/project-structure.md",
        "references/capability-catalog.md",
        "references/platform-contract.md",
        "references/framework-boundary.md",
        "references/python-functions-contract.md",
        "references/runner-contract.md",
        "references/polars-qualified-sql.md",
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
    assert ".builds/artifacts/" in template
    assert "{YYMMDD-HHMMSS}-{12-char-content-digest}" in template
    assert ".builds/current/build.json" in template
    assert "execute and validate `.builds/current` directly" in template
    normalized = " ".join(template.split())
    assert "validates and emits all configured environments" in normalized
    assert "not a Build parameter" in normalized
    assert ".builds/evidence/{build_id}/{env}/{receipt_id}.json" in template
    assert ".evidence/" not in template
    assert ".runtime/" in template
    assert "environment-to-platform mapping" in template
    assert "canonical < patches in array order < exact keyed overrides" in template
    assert "unchanged canonical snapshot" in template
    assert "connection/schema/table/column grain" in template
    assert "select `type: dataflows`" in normalized
    assert "project_management" not in template
    assert "generated/" not in template
    assert "initialization phase" in template


def test_runner_contract_preserves_runtime_semantics() -> None:
    contract = _read("skills/datacoolie-build/references/runner-contract.md")
    for token in (
        "one optional `--stage` string",
        "driver.run(stage=stage)",
        "Do not split comma strings",
        "one framework operation",
        "transport default (`None` or an empty scalar)",
        "No runtime `--env`",
        "framework and platform own path interpretation and validation",
    ):
        assert token in contract
    for stale in (
        "STAGE_GROUPS_JSON",
        'action="append"',
        'nargs="+"',
        "StagePlan",
        "path validation",
        "Reject log or watermark paths",
    ):
        assert stale not in contract


def test_build_owns_source_choice_and_schema_hint_authoring_boundaries() -> None:
    skill = _read("skills/datacoolie-build/SKILL.md")
    framework = _read("skills/datacoolie-build/references/framework-boundary.md")
    schema = _read("skills/datacoolie-build/references/schema-quick-reference.md")
    skill_text = " ".join(skill.split())
    schema_text = " ".join(schema.split())
    assert "## Source expression order" in framework
    direct = framework.index("Address the source object directly")
    query = framework.index("Use one bounded source query")
    function = framework.index("Use a metadata-addressed Python function")
    assert direct < query < function
    assert "Do not replace a supported direct address with an equivalent `SELECT *`" in framework
    assert "the framework appends each non-empty dataflow `schema_name` and `table`" in skill_text
    assert "`base_path/{schema_name}/{table}`" in schema
    assert "Do not embed schema or table segments in `base_path`" in schema_text
    assert "authoring source of truth for exact types observed from a source" in schema
    assert "many columns, repeated mappings, or bulk treatment across dataflows" in schema_text
    assert "Use `transform.schema_hints` only when a few columns or a few dataflows" in schema_text
    assert "the two sources are not merged at runtime" in schema_text
    assert "author the complete effective hint set for that dataflow" in schema_text
    assert "connection + schema + table + column" in schema_text
    assert "Global selectors never find local hints" in schema
    assert "dataflow-local hint patches never modify the global" in schema_text


def test_build_reuses_framework_audit_columns_and_native_file_date_routing() -> None:
    skill = _read("skills/datacoolie-build/SKILL.md")
    schema = _read("skills/datacoolie-build/references/schema-quick-reference.md")
    skill_text = " ".join(skill.split())
    schema_text = " ".join(schema.split())

    assert (
        "do not duplicate framework write-time or driver-managed dataflow run identity"
        in skill_text
    )
    assert "Driver-managed runs also receive `__dataflow_run_id` from their execution ID" in schema_text
    assert "standalone transformer usage without that ID does not add it" in schema_text
    assert "Preserve source-created, source-modified, event, transaction" in schema_text
    assert "omit it and use the framework output" in schema_text
    assert "keep it only with that justification" in schema_text
    assert "prefer `connection.configure.date_folder_partitions`" in schema_text
    assert "without adding an ingestion-date column solely for the folder path" in schema_text
    assert "`partition_columns` takes precedence over `date_folder_partitions`" in schema_text


def test_discover_and_build_define_backward_and_file_watermark_boundaries() -> None:
    discover = _read("skills/datacoolie-discover/SKILL.md")
    observations = _read(
        "skills/datacoolie-discover/references/observation-contract.md"
    )
    build = _read("skills/datacoolie-build/SKILL.md")
    schema = _read("skills/datacoolie-build/references/schema-quick-reference.md")
    discover_text = " ".join(discover.split())
    observation_text = " ".join(observations.split())
    build_text = " ".join(build.split())
    schema_text = " ".join(schema.split())

    assert "a backward fallback, not as complete change coverage" in discover_text
    assert "whether modification times are stable" in discover_text
    assert "real year/month/day/hour levels" in discover_text
    assert "Do not present that date as equivalent to a true change watermark" in observations
    assert "framework values, not source schema columns" in observation_text

    assert "Use backward lookback primarily when discovery found no reliable change signal" in build_text
    assert "prefer `__file_modification_time`" in build_text
    assert "Destination `date_folder_partitions` is a separate" in build_text
    assert "plain append can duplicate rows" in schema_text
    assert "does nothing on the first run" in schema_text
    assert "do not add that internal value to `source.watermark_columns`" in schema_text
    assert "Folder pruning runs first" in schema_text
    assert "It is independent of source pruning" in schema_text


def test_build_references_have_narrow_non_overlapping_boundaries() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    references = {
        path.name: path.read_text(encoding="utf-8")
        for path in (build_dir / "references").glob("*.md")
    }
    for name in (
        "capability-catalog.md",
        "platform-contract.md",
        "framework-boundary.md",
        "schema-quick-reference.md",
        "runner-contract.md",
        "polars-qualified-sql.md",
        "operations-contract.md",
        "python-functions-contract.md",
    ):
        assert "## Scope" in references[name], name

    assert "StageGroup =" not in references["framework-boundary.md"]
    assert "metadata/\n" not in references["framework-boundary.md"]
    assert "DataCoolieDriver" not in references["capability-catalog.md"]
    assert "references/runner-contract.md" in references["operations-contract.md"]
    assert "inherits common identity" in references["operations-contract.md"].lower()
    assert not (build_dir / "references/framework-usage.md").exists()


def test_build_platform_contract_matches_portable_runtime_boundaries() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    skill = (build_dir / "SKILL.md").read_text(encoding="utf-8")
    platform = (build_dir / "references/platform-contract.md").read_text(
        encoding="utf-8"
    )
    platform_text = " ".join(platform.split())
    runner = (build_dir / "references/runner-contract.md").read_text(encoding="utf-8")
    schema = (build_dir / "references/schema-quick-reference.md").read_text(
        encoding="utf-8"
    )
    design = (
        SKILLS_DIR / "datacoolie-design/templates/architecture.tpl.md"
    ).read_text(encoding="utf-8")
    design_skill = (SKILLS_DIR / "datacoolie-design/SKILL.md").read_text(
        encoding="utf-8"
    )
    integration_requirements = (
        SKILLS_DIR / "tests/requirements-integration.txt"
    ).read_text(encoding="utf-8")

    assert "references/platform-contract.md" in skill
    assert "Platform is not the execution host" in platform
    for token in (
        'FabricPlatform(runtime="fabric")',
        'FabricPlatform(runtime="external")',
        'DatabricksPlatform(runtime="databricks")',
        'DatabricksPlatform(runtime="external")',
        "datacoolie[fabric-external]",
        "datacoolie[databricks-external]",
        "DefaultAzureCredential",
        "Databricks unified authentication",
        "/Volumes/<catalog>/<schema>/<volume>/...",
        "DBFS root and DBFS mounts are unsupported",
        "standard boto3 credential chain",
        "applies automatically only to the S3 client",
        "With `base_path`, pass only relative paths",
        "read_file` and `read_bytes` return the complete file",
        "Never use them as a head/stat/existence probe",
    ):
        assert token in platform_text

    assert "execution host's parameter transport" in runner
    assert "DataCoolie platform adapter may differ" in runner
    assert "Execution hosts and platform runtime modes" in design
    assert "Record execution host and" in design_skill
    assert "separately from platform intent" in design_skill
    assert "/mnt/lake/bronze" not in schema
    assert "boto3>=1.43.2,<2" in integration_requirements


def test_build_teaches_polars_qualified_sql_registration_boundary() -> None:
    build_dir = SKILLS_DIR / "datacoolie-build"
    skill = (build_dir / "SKILL.md").read_text(encoding="utf-8")
    qualified = (build_dir / "references/polars-qualified-sql.md").read_text(
        encoding="utf-8"
    )
    runner = (build_dir / "references/runner-contract.md").read_text(encoding="utf-8")
    schema = (build_dir / "references/schema-quick-reference.md").read_text(
        encoding="utf-8"
    )
    generic_polars_runner = (
        build_dir / "templates/runners/run_local_polars.py.example"
    ).read_text(encoding="utf-8")
    qualified_text = " ".join(qualified.split())
    runner_text = " ".join(runner.split())

    assert "references/polars-qualified-sql.md" in skill
    assert "same active `PolarsEngine` before constructing or running the driver" in skill
    assert "## Metadata and runner ownership" in qualified
    assert "register_delta_tables" in qualified
    assert "register_iceberg_tables" in qualified
    assert "`logical_prefix=None` (the default)" in qualified_text
    assert "one to four non-empty components" in qualified_text
    assert "There is no configurable separator and no `max_sql_name_levels`" in qualified_text
    assert "The default `preload=False` only enumerates and indexes descriptors" in qualified_text
    assert "reuses those bindings for later queries on the same engine" in qualified_text
    assert "Exclude patterns win" not in qualified  # wording is "Any matching exclude wins"
    assert "Any matching exclude wins" in qualified
    assert "datacoolie[polars-sql,polars-delta]" in qualified
    assert "datacoolie[polars-sql,polars-iceberg]" in qualified

    for forbidden_metadata_setting in (
        "`logical_prefix`",
        "`recursive`",
        "`include`",
        "`exclude`",
    ):
        assert forbidden_metadata_setting in qualified
    assert "Do not put `logical_prefix`" in qualified
    assert "never `source.configure`" in schema
    assert "before driver construction" in runner_text
    assert "Omit this setup from Polars runners" in runner_text

    assert "register_delta_tables" not in generic_polars_runner
    assert "register_iceberg_tables" not in generic_polars_runner


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


def test_provision_and_release_route_fabric_tools_by_control_plane_safely() -> None:
    agents = " ".join(_read("AGENTS.md").split())
    provision = _read("skills/datacoolie-provision/references/platform-tooling.md")
    release = _read("skills/datacoolie-release/references/platform-tooling.md")
    for contract in (provision, release):
        for cli_name in ("AWS CLI", "Azure CLI", "Databricks CLI", "Microsoft Fabric CLI"):
            assert cli_name in contract
        assert "Azure/ARM" in contract
        assert "Fabric-native" in contract
        assert "`fab api`" in contract
        assert "`az rest`" in contract
        assert "skills-for-fabric" in contract
        assert "not a runtime dependency" in contract
        assert "Resolve only the executable and extension required" in contract
        assert "do not hardcode a minimum version" in contract
        assert "Do not assume `az` and `fab` share authentication state" in contract
        assert "current user" in contract
        assert "administrator" in contract
        assert "never install" in contract.lower()
    assert "existing IaC source" in provision
    assert "az fabric capacity" in provision
    assert "already-approved project deployment mechanism" in release
    assert "route any required resource mutation to Provision" in release
    assert "route Fabric by control plane" in agents
    assert "Azure/ARM uses Azure CLI" in agents
    assert "Fabric-native work uses Fabric CLI" in agents

    provision_evals = json.loads(
        _read("skills/datacoolie-provision/evals/evals.json")
    )["evals"]
    release_evals = json.loads(
        _read("skills/datacoolie-release/evals/evals.json")
    )["evals"]
    provision_behavior = " ".join(
        eval_case["prompt"]
        + " "
        + eval_case["expected_output"]
        + " "
        + " ".join(eval_case["expectations"])
        for eval_case in provision_evals
    )
    release_behavior = " ".join(
        eval_case["prompt"]
        + " "
        + eval_case["expected_output"]
        + " "
        + " ".join(eval_case["expectations"])
        for eval_case in release_evals
    )
    for behavior in (provision_behavior, release_behavior):
        assert "Fabric-native" in behavior
        assert "fab api" in behavior
        assert "az rest" in behavior
    assert "Fabric Capacity routes to Azure CLI" in provision_behavior
    assert "high-level Fabric CLI commands" in release_behavior
    assert "verified remaining gap permits az rest" in release_behavior


def test_release_resources_are_consume_only_and_machine_checked() -> None:
    release_dir = SKILLS_DIR / "datacoolie-release"
    references = {
        path.name: path.read_text(encoding="utf-8")
        for path in (release_dir / "references").glob("*.md")
    }
    for name in (
        "deployment-contract.md",
        "automation-contract.md",
        "platform-tooling.md",
        "python-functions-deployment.md",
        "runner-deployment-mapping.md",
    ):
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
    expected_versions = {
        "skills/datacoolie-build/templates/build-verification-receipt.json.example": 4,
        "skills/datacoolie-release/templates/release-receipt.json.example": 7,
        "skills/datacoolie-provision/templates/provision-receipt.json.example": 1,
    }
    for relative, version in expected_versions.items():
        data = json.loads(_read(relative))
        assert data["schema_version"] == version
        assert "status" in data
        assert "unresolved_issues" in data


def test_control_storage_and_runtime_state_have_one_owner_per_outcome() -> None:
    agents = " ".join(_read("AGENTS.md").split())
    design = " ".join(_read("skills/datacoolie-design/SKILL.md").split())
    provision = " ".join(_read("skills/datacoolie-provision/SKILL.md").split())
    build = " ".join(_read("skills/datacoolie-build/SKILL.md").split())
    operations = " ".join(
        _read("skills/datacoolie-build/references/operations-contract.md").split()
    )
    release = " ".join(_read("skills/datacoolie-release/SKILL.md").split())

    assert "environment-isolated control-storage" in agents
    assert "Define one environment-isolated DataCoolie control-storage boundary" in design
    assert "without forcing a no-op apply" in provision
    assert "approved persistent control namespace" in build
    assert "{environment, watermark_base_path, dataflow_id}" in operations
    assert "resource-readiness" in release
    assert "runtime-state-preflight" in release
    assert "Earlier receipt schemas remain audit evidence only" in release


def test_catalog_namespace_and_hybrid_runtime_contracts_are_explicit() -> None:
    schema = " ".join(
        _read("skills/datacoolie-build/references/schema-quick-reference.md").split()
    )
    platform = " ".join(
        _read("skills/datacoolie-build/references/platform-contract.md").split()
    )

    assert "<catalog>.<database-or-schema>.<table>" in schema
    assert "<workspace>.<lakehouse>.<schema>.<table>" in schema
    assert "Neither `catalog` nor `database` is automatically appended to `base_path`" in schema
    assert "A relational database table read retains" in schema
    assert "external cloud adapter runs on premises" in platform
    assert "scheduler, container, VM, or host remains the release target" in platform


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
