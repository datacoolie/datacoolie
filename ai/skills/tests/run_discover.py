"""
datacoolie-discover — Knowledge-based skill validation.
Validates that SKILL.md contains all required sections for the knowledge-based skill.

Usage (from datacoolie/ai/skills/tests/):
  python run_discover.py
"""
import sys
from pathlib import Path

HERE = Path(__file__).parent
SKILL_MD = HERE.parent / "datacoolie-discover" / "SKILL.md"

REQUIRED_SECTIONS = [
    "# datacoolie-discover",
    "## Scope",
    "## Modes",
    "## AI Workflow",
    "## Source Introspection Rules",
    "## Report Generation",
    "## Output Format",
    "## Security Policy",
    "## Cross-Skill Dependencies",
]


def run(filter_names: list[str] | None = None) -> None:
    print(f"\n{'='*60}")
    print("  datacoolie-discover — SKILL.md validation")
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

    # Check minimum content length (knowledge-based skills should have substantial content)
    min_length = 2000
    length_ok = len(content) >= min_length
    status = "✓" if length_ok else "✗"
    print(f"  {status} content length: {len(content)} chars (min {min_length})")
    summary.append(("content-length", status))

    # Check subsections for Source Introspection Rules
    subsections = ["### Database Introspection", "### File Introspection",
                   "### API Introspection", "### Lakehouse Catalog Introspection"]
    for sub in subsections:
        found = sub in content
        status = "✓" if found else "✗"
        print(f"  {status} subsection: {sub}")
        summary.append((sub, status))

    print(f"\n{'='*60}")
    print("  DISCOVER SUMMARY")
    print(f"{'='*60}")
    failed = sum(1 for _, s in summary if s == "✗")
    passed = sum(1 for _, s in summary if s == "✓")
    for name, status in summary:
        print(f"  {status} {name}")
    print(f"\n  {passed}/{len(summary)} checks passed")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    filter_names = sys.argv[1:] if len(sys.argv) > 1 else None
    run(filter_names)
