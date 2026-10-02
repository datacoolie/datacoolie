"""Validate the CLI-first datacoolie-build skill contract."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).parent
SKILL_DIR = HERE.parent / "datacoolie-build"
SKILL = SKILL_DIR / "SKILL.md"
PRODUCT_ROOT = SKILL_DIR.parents[2]


def main() -> int:
    content = SKILL.read_text(encoding="utf-8")
    tokens = (
        "# DataCoolie Build",
        "## Outcome and boundary",
        "## Route only the needed resource",
        "dc init [PATH]",
        "dc validate --project-dir",
        "dc build --project-dir",
        "dc agents update",
        "DataCoolieRunConfig(run_attributes=",
        ".builds/current/manifest.json",
        "`build.json`, `SHA256SUMS` or deployment path",
    )
    checks = [(token, token in content) for token in tokens]
    resources = (
        "references/platform-contract.md",
        "references/framework-boundary.md",
        "references/python-functions-contract.md",
        "references/runner-contract.md",
        "references/schema-quick-reference.md",
        "references/polars-qualified-sql.md",
        "references/operations-contract.md",
        "references/public-examples.md",
        "scripts/render_automation.py",
    )
    checks.extend((relative, (SKILL_DIR / relative).is_file()) for relative in resources)
    project_schema = PRODUCT_ROOT / "src" / "datacoolie" / "project" / "schemas" / "0.2.0" / "metadata.schema.json"
    checks.append(("project-owned-metadata-schema", project_schema.is_file()))
    checks.append((
        "public-schema-index-guidance",
        "https://datacoolie.github.io/datacoolie/schema/index.json" in content,
    ))
    checks.append((
        "public-latest-schema-guidance",
        "https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json" in content,
    ))
    checks.append(("line-budget", len(content.splitlines()) <= 220))
    obsolete = (
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
    )
    checks.extend((f"removed:{path}", not (SKILL_DIR / path).exists()) for path in obsolete)
    result = subprocess.run(
        [sys.executable, "-m", "datacoolie", "--format", "json", "inspect", "capabilities"],
        cwd=PRODUCT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PRODUCT_ROOT / "src")},
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(result.stdout)
        cli_ok = result.returncode == 0 and payload.get("ok") is True
    except json.JSONDecodeError:
        cli_ok = False
    checks.append(("installed-cli-capabilities", cli_ok))
    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} build checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
