"""Validate the outcome-based datacoolie-build skill and bundled schema."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).parent
SKILL_DIR = HERE.parent / "datacoolie-build"
SKILL = SKILL_DIR / "SKILL.md"
TOKENS = (
    "# DataCoolie Build",
    "## Outcome And Boundary",
    "## Inputs And Gates",
    "## Resource Routing",
    "### 2. Prove capability fit",
    "### 5. Materialize and verify",
    "### 6. Add automation only when requested",
    "DataCoolieDriver.run(...)",
    ".builds/artifacts/{build_id}",
    ".builds/current/{env}.json",
    "scripts/render_automation.py",
)
RESOURCES = (
    "scripts/validate_config.py",
    "scripts/convert.py",
    "scripts/lint.py",
    "scripts/merge.py",
    "scripts/validate.py",
    "scripts/inspect_capabilities.py",
    "scripts/materialize.py",
    "scripts/validate_build.py",
    "scripts/render_automation.py",
    "scripts/requirements.txt",
    "scripts/requirements-excel.txt",
    "schemas/workspace-config.schema.json",
    "schemas/0.1.0/metadata.schema.json",
    "schemas/build-verification-receipt.schema.json",
    "schemas/current-build.schema.json",
    "references/capability-catalog.md",
    "references/framework-boundary.md",
    "references/runner-contract.md",
    "references/operations-contract.md",
    "templates/project-structure.md",
    "templates/runners/README.md",
    "templates/build-verification-receipt.json.example",
    "templates/runners/replay_local_polars.py.example",
    "templates/runners/maintenance_local_polars.py.example",
    "templates/runners/replay_databricks_spark.ipynb.example",
    "templates/runners/maintenance_databricks_spark.ipynb.example",
)


def main() -> int:
    content = SKILL.read_text(encoding="utf-8")
    checks = [(token, token in content) for token in TOKENS]
    checks.extend((relative, (SKILL_DIR / relative).is_file()) for relative in RESOURCES)
    checks.append(("line-budget", len(content.splitlines()) <= 180))
    for fixture in ("local_use_cases.json", "transformer_features.json"):
        path = SKILL_DIR.parents[2] / "usecase-sim/metadata/file" / fixture
        result = subprocess.run(
            [sys.executable, str(SKILL_DIR / "scripts/validate.py"), str(path), "--quiet"],
            check=False,
        )
        checks.append((f"schema:{fixture}", result.returncode == 0))
    for name, passed in checks:
        print(f"  {'✓' if passed else '✗'} {name}")
    failed = [name for name, passed in checks if not passed]
    print(f"{len(checks) - len(failed)}/{len(checks)} build checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
