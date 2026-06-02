"""
datacoolie-deploy — Knowledge-based skill validation.
Validates that SKILL.md contains all required sections for the knowledge-based skill.

Usage (from datacoolie/ai/skills/tests/):
  python run_deploy.py
"""
import sys
from pathlib import Path

HERE = Path(__file__).parent
SKILL_MD = HERE.parent / "datacoolie-deploy" / "SKILL.md"

REQUIRED_SECTIONS = [
    "# datacoolie-deploy",
    "## Supported Platforms",
    "## AI Workflow",
    "## Configuration",
    "## Full Deploy Flow",
    "## References",
    "## Script Status",
    "## Prerequisites",
    "## Input Contracts",
    "## Output Contracts",
]


def run() -> None:
    print(f"\n{'='*60}")
    print("  datacoolie-deploy — SKILL.md validation")
    print(f"{'='*60}")

    if not SKILL_MD.exists():
        print(f"  ✗ SKILL.md not found at {SKILL_MD}")
        sys.exit(1)

    content = SKILL_MD.read_text(encoding="utf-8")
    summary: list[tuple[str, str]] = []

    # Check required sections
    for section in REQUIRED_SECTIONS:
        found = section in content
        status = "✓" if found else "✗"
        print(f"  {status} section: {section}")
        summary.append((section, status))

    # Check minimum content length
    min_length = 1500
    length_ok = len(content) >= min_length
    status = "✓" if length_ok else "✗"
    print(f"  {status} content length: {len(content)} chars (min {min_length})")
    summary.append(("content-length", status))

    # Check workflow steps
    workflow_steps = [
        "### 0. Read Upstream Artifacts",
        "### 1. Preflight Checklist",
        "### 2. Generate Runner",
        "### 3. Package Functions",
        "### 4. Platform Deploy",
        "### 5. Promote",
        "### 6. Generate CI/CD",
    ]
    for step in workflow_steps:
        found = step in content
        status = "✓" if found else "✗"
        print(f"  {status} workflow: {step}")
        summary.append((step, status))

    print(f"\n{'='*60}")
    print("  DEPLOY SUMMARY")
    print(f"{'='*60}")
    failed = sum(1 for _, s in summary if s == "✗")
    passed = sum(1 for _, s in summary if s == "✓")
    for name, status in summary:
        print(f"  {status} {name}")
    print(f"\n  {passed}/{len(summary)} checks passed")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run()
