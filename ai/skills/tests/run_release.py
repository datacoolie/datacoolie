"""Validate the consume-only datacoolie-release skill."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


SKILL_DIR = Path(__file__).parent.parent / "datacoolie-release"
TOKENS = (
    "# DataCoolie Release",
    "## Outcome And Boundary",
    "## Inputs And Authorization",
    "## Resource Routing",
    "## Preflight",
    "## Deployment Transaction",
    "## Release Automation",
    "## Evidence And Handoff",
    "Release never rebuilds or repairs the artifact",
    ".builds/artifacts/{build_id}",
    "explicitly selected successful build receipt",
    "never deploy from `current`, `latest`, or",
    "--require-success",
)


def main() -> int:
    content = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    checks = [(token, token in content) for token in TOKENS]
    checks.append(("line-budget", len(content.splitlines()) <= 180))

    resources = (
        "references/deployment-contract.md",
        "references/automation-contract.md",
        "references/platform-tooling.md",
        "schemas/release-receipt.schema.json",
        "scripts/_artifact_validation.py",
        "scripts/validate_release.py",
        "scripts/requirements.txt",
        "templates/release-receipt.json.example",
    )
    checks.extend((relative, (SKILL_DIR / relative).is_file()) for relative in resources)
    checks.append(("no-platform-workflow-examples", not list((SKILL_DIR / "references").glob("*.yml.example"))))
    for name in ("deployment-contract.md", "automation-contract.md", "platform-tooling.md"):
        reference = (SKILL_DIR / "references" / name).read_text(encoding="utf-8")
        checks.append((f"{name}-scope", "## Scope" in reference))

    evals = json.loads((SKILL_DIR / "evals/evals.json").read_text(encoding="utf-8"))
    checks.append(("behavioral-evals", len(evals.get("evals", [])) >= 8))
    schema = json.loads(
        (SKILL_DIR / "schemas/release-receipt.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    receipt = json.loads(
        (SKILL_DIR / "templates/release-receipt.json.example").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(receipt)
    checks.append(("typed-release-receipt", receipt.get("artifact_type") == "release_receipt"))

    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} release checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
