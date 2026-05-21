"""cicd.py — Generate CI/CD workflow files for datacoolie deployment.

Renders platform-specific GitHub Actions workflow from templates.

Usage:
    python scripts/cicd.py --platform aws --env prod
    python scripts/cicd.py --platform databricks --env staging
    python scripts/cicd.py --platform fabric --env prod
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

PLATFORMS = ("aws", "fabric", "databricks")

TEMPLATE_MAP = {
    "aws": "github-actions-aws.yml.j2",
    "databricks": "github-actions-databricks.yml.j2",
    "fabric": "github-actions-fabric.yml.j2",
}


def load_config(project_dir: Path) -> dict:
    """Load .datacoolie/config.yaml or config.json."""
    config_path = project_dir / ".datacoolie" / "config.yaml"
    if config_path.is_file():
        try:
            import yaml  # noqa: PLC0415

            with open(config_path) as f:
                return yaml.safe_load(f) or {}
        except ImportError:
            pass
    config_json = project_dir / ".datacoolie" / "config.json"
    if config_json.is_file():
        import json

        with open(config_json) as f:
            return json.load(f)
    return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CI/CD workflow for datacoolie deploy")
    parser.add_argument("--platform", required=True, choices=PLATFORMS, help="Target platform")
    parser.add_argument("--env", required=True, help="Environment name for deployment target")
    parser.add_argument("--output", type=Path, default=None, help="Output file path")
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Project root")
    args = parser.parse_args()

    skill_dir = Path(__file__).resolve().parent.parent
    template_dir = skill_dir / "templates"
    project_dir = args.project_dir.resolve()

    # Default output to .github/workflows/
    output_path = args.output or (project_dir / ".github" / "workflows" / f"deploy-{args.platform}.yml")

    # Load config for template variables
    config = load_config(project_dir)
    env_config = config.get("environments", {}).get(args.env, {})
    platform_config = env_config.get(args.platform, {})

    variables = {
        "env_name": args.env,
        "project_name": config.get("project_name", "datacoolie"),
    }

    # Platform-specific variables
    if args.platform == "aws":
        variables["bucket"] = platform_config.get("bucket", "de-dev-0001")
        variables["region"] = platform_config.get("region", "ap-southeast-1")
    elif args.platform == "databricks":
        variables["host"] = platform_config.get("host", "")
    elif args.platform == "fabric":
        variables["workspace"] = platform_config.get("workspace", "MyWorkspace")

    # Render template
    template_name = TEMPLATE_MAP[args.platform]
    if not (template_dir / template_name).is_file():
        print(f"ERROR: Template not found: {template_dir / template_name}", file=sys.stderr)
        sys.exit(1)

    jinja_env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        keep_trailing_newline=True,
    )
    content = jinja_env.get_template(template_name).render(**variables)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")

    print(f"\nDataCoolie Deploy \u2014 CI/CD Generated")
    print("-" * 50)
    print(f"  \u2713 Workflow: {output_path}")
    print(f"  Platform: {args.platform}")
    print(f"  Environment: {args.env}")
    print()

    # Print required secrets reminder
    secrets_map = {
        "aws": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
        "databricks": ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"],
        "fabric": ["FABRIC_CLIENT_ID", "FABRIC_CLIENT_SECRET", "FABRIC_TENANT_ID"],
    }
    print("  Required GitHub Secrets:")
    for secret in secrets_map[args.platform]:
        print(f"    - {secret}")
    print()


if __name__ == "__main__":
    main()
