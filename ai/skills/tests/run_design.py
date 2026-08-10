"""Validate the neutral, outcome-based datacoolie-design skill."""
from __future__ import annotations

import json
from pathlib import Path


SKILL_DIR = Path(__file__).parent.parent / "datacoolie-design"
SKILL = SKILL_DIR / "SKILL.md"
TEMPLATE = SKILL_DIR / "templates" / "architecture.tpl.md"
REQUIRED_SKILL_TOKENS = (
    "# DataCoolie Design",
    "## Outcome And Boundary",
    "## Trigger Decision",
    "## Inputs And Resources",
    "## Decision Workflow",
    "## Output And Handoff",
    "architecture/current.md",
    ".approvals/design/{architecture_sha256}.json",
    "helper exists only to make hashing and receipt validation deterministic",
)
REQUIRED_RESOURCES = (
    "templates/architecture.tpl.md",
    "templates/design-approval.json.example",
    "schemas/design-approval.schema.json",
    "scripts/design_approval.py",
    "scripts/requirements.txt",
    "evals/evals.json",
)
REQUIRED_TEMPLATE_TOKENS = (
    "## Evidence And Assumptions",
    "## Stage Graph",
    "## Transition Contracts",
    "## Framework Capability Intent",
    "## Runtime Selection Intent",
    "## Environment And Resource Requirements",
    "## Release And Approval Policy",
    "## Build Handoff",
    "## Unresolved Questions",
)
FORBIDDEN_TEMPLATE_TOKENS = (
    "approval_state",
    "Medallion",
    "source2bronze",
    "bronze2silver",
    "silver2gold",
    "Delta",
    "Key Vault",
    "architecture/amendments",
    "stage-to-engine matrix",
    "approval_required",
    "architecture_path_and_hash",
    "updated_at:",
    "version:",
)


def main() -> int:
    content = SKILL.read_text(encoding="utf-8")
    template = TEMPLATE.read_text(encoding="utf-8")
    normalized = " ".join(content.split())
    checks = [(token, token in normalized) for token in REQUIRED_SKILL_TOKENS]
    checks.extend(
        (
            f"resource:{path}",
            (SKILL_DIR / path).is_file() and (path == "evals/evals.json" or path in content),
        )
        for path in REQUIRED_RESOURCES
    )
    checks.extend((f"template:{token}", token in template) for token in REQUIRED_TEMPLATE_TOKENS)
    checks.extend(
        (f"template-excludes:{token}", token not in template)
        for token in FORBIDDEN_TEMPLATE_TOKENS
    )
    checks.append(("single-architecture-template", not list(
        (SKILL_DIR / "templates").glob("layer-*.tpl.md")
    )))
    checks.append(("skill-line-budget", len(content.splitlines()) <= 180))
    checks.append(("template-line-budget", len(template.splitlines()) <= 180))
    try:
        json.loads((SKILL_DIR / "schemas" / "design-approval.schema.json").read_text())
        json.loads((SKILL_DIR / "templates" / "design-approval.json.example").read_text())
        json.loads((SKILL_DIR / "evals" / "evals.json").read_text())
        json_valid = True
    except json.JSONDecodeError:
        json_valid = False
    checks.append(("json-resources", json_valid))

    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} design checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
