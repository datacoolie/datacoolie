"""generate.py — Generate platform-specific runner scripts/notebooks.

Renders Jinja2 templates into deployable runners for each target platform.

Usage:
    python scripts/generate.py --platform aws --env prod
    python scripts/generate.py --platform fabric --env dev
    python scripts/generate.py --platform databricks --env prod
    python scripts/generate.py --platform local --env dev
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


PLATFORMS = ("aws", "fabric", "databricks", "local")

# Map platform → template file name
TEMPLATE_MAP = {
    "local": "run_local.py.j2",
    "aws": "run_aws_glue.py.j2",
    "fabric": "run_fabric.ipynb.j2",
    "databricks": "run_databricks.ipynb.j2",
}

# Map platform → output file name
OUTPUT_MAP = {
    "local": "run_local.py",
    "aws": "run_aws_glue.py",
    "fabric": "run_fabric.ipynb",
    "databricks": "run_databricks.ipynb",
}


def load_config(project_dir: Path) -> dict:
    """Load .datacoolie/config.yaml if it exists."""
    config_path = project_dir / ".datacoolie" / "config.yaml"
    if not config_path.is_file():
        return {}
    try:
        import yaml  # noqa: PLC0415

        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        # Fallback: try JSON config
        config_json = project_dir / ".datacoolie" / "config.json"
        if config_json.is_file():
            with open(config_json) as f:
                return json.load(f)
        return {}


def resolve_variables(platform: str, env_name: str, config: dict) -> dict:
    """Build template variables from config + CLI args."""
    env_config = config.get("environments", {}).get(env_name, {})
    platform_config = env_config.get(platform, {})

    variables = {
        "platform_name": platform,
        "env_name": env_name,
        "project_name": config.get("project_name", "datacoolie-project"),
        "engine": config.get("engine", "spark"),
        "max_workers": config.get("max_workers", 8),
    }

    # Platform-specific variables
    if platform == "aws":
        variables.update(
            {
                "region": platform_config.get("region", "ap-southeast-1"),
                "bucket": platform_config.get("bucket", "de-dev-0001"),
                "glue_version": platform_config.get("glue_version", "5.0"),
                "worker_type": platform_config.get("worker_type", "G.1X"),
                "number_of_workers": platform_config.get("number_of_workers", 4),
            }
        )
    elif platform == "fabric":
        variables.update(
            {
                "workspace": platform_config.get("workspace", "MyWorkspace"),
                "lakehouse": platform_config.get("lakehouse", "ETL_Lakehouse"),
                "root_path": platform_config.get(
                    "root_path",
                    f"abfss://{platform_config.get('workspace', 'MyWorkspace')}@onelake.dfs.fabric.microsoft.com/"
                    f"{platform_config.get('lakehouse', 'ETL_Lakehouse')}.Lakehouse",
                ),
            }
        )
    elif platform == "databricks":
        variables.update(
            {
                "host": platform_config.get("host", ""),
                "catalog": platform_config.get("catalog", "workspace"),
                "schema": platform_config.get("schema", "default"),
                "volume": platform_config.get("volume", "datacoolie"),
                "volume_root": platform_config.get(
                    "volume_root",
                    f"/Volumes/{platform_config.get('catalog', 'workspace')}/"
                    f"{platform_config.get('schema', 'default')}/"
                    f"{platform_config.get('volume', 'datacoolie')}",
                ),
            }
        )
    elif platform == "local":
        variables.update(
            {
                "metadata_path": platform_config.get("metadata_path", "metadata/use_cases.json"),
                "log_base_path": platform_config.get("log_base_path", "logs/datacoolie"),
            }
        )

    return variables


def render_template(template_dir: Path, template_name: str, variables: dict) -> str:
    """Render a Jinja2 template with given variables."""
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    return template.render(**variables)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate datacoolie runner for target platform")
    parser.add_argument("--platform", required=True, choices=PLATFORMS, help="Target platform")
    parser.add_argument("--env", required=True, help="Environment name (matches config.yaml)")
    parser.add_argument("--output", type=Path, default=None, help="Output file path")
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Project root")
    args = parser.parse_args()

    # Resolve paths
    skill_dir = Path(__file__).resolve().parent.parent
    template_dir = skill_dir / "templates"
    project_dir = args.project_dir.resolve()
    output_dir = project_dir / ".datacoolie" / "generated"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = args.output or (output_dir / OUTPUT_MAP[args.platform])

    # Load config and resolve variables
    config = load_config(project_dir)
    variables = resolve_variables(args.platform, args.env, config)

    # Render
    template_name = TEMPLATE_MAP[args.platform]
    if not (template_dir / template_name).is_file():
        print(f"ERROR: Template not found: {template_dir / template_name}", file=sys.stderr)
        sys.exit(1)

    content = render_template(template_dir, template_name, variables)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    print(f"  \u2713 Generated: {output_path}")

    # Validate syntax for Python files
    if output_path.suffix == ".py":
        try:
            compile(content, str(output_path), "exec")
            print("  \u2713 Syntax check passed")
        except SyntaxError as e:
            print(f"  \u2717 Syntax error in generated file: {e}", file=sys.stderr)
            sys.exit(1)

    # Validate JSON for notebooks
    if output_path.suffix == ".ipynb":
        try:
            json.loads(content)
            print("  \u2713 Notebook JSON valid")
        except json.JSONDecodeError as e:
            print(f"  \u2717 Invalid notebook JSON: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
