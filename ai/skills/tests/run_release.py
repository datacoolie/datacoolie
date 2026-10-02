"""Validate the upload-only datacoolie-release skill contract."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


SKILL_DIR = Path(__file__).parent.parent / "datacoolie-release"


def main() -> int:
    content = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    tokens = (
        "# DataCoolie release",
        "## Outcome and boundary",
        "<deployment_path>/artifacts/<build_id>/",
        "<deployment_path>/current/",
        "dc validate --project-dir",
        "current_comparison.ok",
        "`build.json` pointer or `SHA256SUMS` sidecar",
        "create a job",
        "partial_failure",
    )
    checks = [(token, token in content) for token in tokens]
    checks.append(("line-budget", len(content.splitlines()) <= 190))
    resources = (
        "references/deployment-contract.md",
        "references/automation-contract.md",
        "references/platform-tooling.md",
        "references/python-functions-deployment.md",
        "references/runner-deployment-mapping.md",
        "schemas/upload-record.schema.json",
        "scripts/validate_upload_record.py",
        "scripts/upload_local.py",
        "templates/upload-record.json.example",
    )
    checks.extend((relative, (SKILL_DIR / relative).is_file()) for relative in resources)
    schema = json.loads((SKILL_DIR / "schemas/upload-record.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    record = json.loads((SKILL_DIR / "templates/upload-record.json.example").read_text(encoding="utf-8"))
    errors = list(Draft202012Validator(schema).iter_errors(record))
    checks.append(("upload-record-schema", not errors))
    checks.append(("no-legacy-release-files", not any(
        (SKILL_DIR / relative).exists()
        for relative in (
            "schemas/release-receipt.schema.json",
            "scripts/_artifact_validation.py",
            "scripts/validate_release.py",
            "templates/release-receipt.json.example",
        )
    )))
    for name in (
        "deployment-contract.md",
        "automation-contract.md",
        "platform-tooling.md",
        "python-functions-deployment.md",
        "runner-deployment-mapping.md",
    ):
        reference = (SKILL_DIR / "references" / name).read_text(encoding="utf-8")
        checks.append((f"{name}-scope", "#" in reference and "upload" in reference.lower()))
    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} release checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
