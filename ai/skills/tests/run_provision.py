"""Validate the conditional datacoolie-provision skill."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


SKILL_DIR = Path(__file__).parent.parent / "datacoolie-provision"
TOKENS = (
    "# DataCoolie Provision",
    "## Outcome And Boundary",
    "## Inputs And Approval",
    "## Resource Routing",
    "## Decision Workflow",
    "## Evidence And Handoff",
    "conditional dependency, not a mandatory lifecycle phase",
    "exact plan hash",
    "provision/evidence/{env}/receipts/{receipt_id}.json",
    "--require-apply-success",
)


def main() -> int:
    content = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    checks = [(token, token in content) for token in TOKENS]
    checks.append(("line-budget", len(content.splitlines()) <= 180))

    resources = (
        "references/terraform-contract.md",
        "references/platform-tooling.md",
        "schemas/provision-receipt.schema.json",
        "scripts/validate_provision.py",
        "scripts/requirements.txt",
        "templates/provision-receipt.json.example",
    )
    checks.extend((relative, (SKILL_DIR / relative).is_file()) for relative in resources)
    checks.append(("no-prescriptive-tf-examples", not list((SKILL_DIR / "references").glob("*.tf.example"))))
    for name in ("terraform-contract.md", "platform-tooling.md"):
        reference = (SKILL_DIR / "references" / name).read_text(encoding="utf-8")
        checks.append((f"{name}-scope", "## Scope" in reference))

    evals = json.loads((SKILL_DIR / "evals/evals.json").read_text(encoding="utf-8"))
    checks.append(("behavioral-evals", len(evals.get("evals", [])) >= 8))
    schema = json.loads(
        (SKILL_DIR / "schemas/provision-receipt.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    receipt = json.loads(
        (SKILL_DIR / "templates/provision-receipt.json.example").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(receipt)
    checks.append(("typed-provision-receipt", receipt.get("artifact_type") == "provision_receipt"))

    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} provision checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
