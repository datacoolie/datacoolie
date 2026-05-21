"""apply.py — Orchestrate datacoolie deployment to a target platform.

Runs preflight → package → upload artifacts → create/update job.

Usage:
    python scripts/apply.py --platform aws --env prod
    python scripts/apply.py --platform aws --env prod --skip-upload
    python scripts/apply.py --platform databricks --env dev --run
    python scripts/apply.py --platform fabric --env prod --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

PLATFORMS = ("aws", "fabric", "databricks", "local")


def _run_cmd(cmd: list[str], dry_run: bool = False, check: bool = True) -> tuple[int, str]:
    """Execute a shell command, or print it in dry-run mode."""
    cmd_str = " ".join(cmd)
    if dry_run:
        print(f"  [DRY-RUN] {cmd_str}")
        return 0, ""
    print(f"  > {cmd_str}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}", file=sys.stderr)
    return result.returncode, (result.stdout + result.stderr).strip()


def _run_script(script_name: str, args: list[str], dry_run: bool = False) -> int:
    """Run a sibling script from scripts/ directory."""
    script_path = Path(__file__).resolve().parent / script_name
    cmd = [sys.executable, str(script_path)] + args
    if dry_run:
        print(f"  [DRY-RUN] {' '.join(cmd)}")
        return 0
    result = subprocess.run(cmd)
    return result.returncode


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
        with open(config_json) as f:
            return json.load(f)
    return {}


def apply_aws(env_name: str, config: dict, dry_run: bool, skip_upload: bool, run_after: bool, project_dir: Path = None) -> int:
    """Deploy to AWS Glue."""
    if project_dir is None:
        project_dir = Path.cwd()
    env_config = config.get("environments", {}).get(env_name, {})
    aws_config = env_config.get("aws", {})
    bucket = aws_config.get("bucket", "de-dev-0001")
    region = aws_config.get("region", "ap-southeast-1")
    role_arn = aws_config.get("role_arn", "")
    glue_version = aws_config.get("glue_version", "5.0")
    worker_type = aws_config.get("worker_type", "G.1X")
    number_of_workers = aws_config.get("number_of_workers", 4)
    job_name = f"{config.get('project_name', 'datacoolie')}-{env_name}"

    # Upload artifacts to S3
    if not skip_upload:
        print("\n  Uploading artifacts to S3...")
        generated_dir = project_dir / ".datacoolie" / "generated"
        dist_dir = generated_dir / "dist"

        # Upload runner script
        runner_path = generated_dir / "run_aws_glue.py"
        if runner_path.is_file():
            _run_cmd(["aws", "s3", "cp", str(runner_path), f"s3://{bucket}/scripts/run_aws_glue.py"], dry_run)

        # Upload wheels
        for whl in dist_dir.glob("*.whl"):
            _run_cmd(["aws", "s3", "cp", str(whl), f"s3://{bucket}/libs/{whl.name}"], dry_run)

        # Sync metadata
        metadata_dir = project_dir / "metadata"
        if metadata_dir.is_dir():
            _run_cmd(["aws", "s3", "sync", str(metadata_dir), f"s3://{bucket}/metadata/"], dry_run)

    # Create or update Glue job
    print("\n  Creating/updating Glue job...")
    script_location = f"s3://{bucket}/scripts/run_aws_glue.py"

    # Check if job exists
    code, output = _run_cmd(["aws", "glue", "get-job", "--job-name", job_name, "--region", region], dry_run, check=False)

    # Find wheel files for extra-py-files
    dist_dir = project_dir / ".datacoolie" / "generated" / "dist"
    wheel_files = list(dist_dir.glob("*.whl")) if dist_dir.is_dir() else []
    extra_py_files = ",".join(f"s3://{bucket}/libs/{w.name}" for w in wheel_files)

    default_arguments = {
        "--REGION": region,
        "--BUCKET": bucket,
        "--STAGE": "",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
    }
    if extra_py_files:
        default_arguments["--extra-py-files"] = extra_py_files

    job_definition = {
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": default_arguments,
        "GlueVersion": glue_version,
        "WorkerType": worker_type,
        "NumberOfWorkers": number_of_workers,
    }

    if code == 0 and not dry_run:
        # Job exists — update
        _run_cmd(
            [
                "aws", "glue", "update-job",
                "--job-name", job_name,
                "--job-update", json.dumps(job_definition),
                "--region", region,
            ],
            dry_run,
        )
        print(f"  \u2713 Updated Glue job: {job_name}")
    else:
        # Job doesn't exist — create
        _run_cmd(
            [
                "aws", "glue", "create-job",
                "--name", job_name,
                "--region", region,
                "--command", json.dumps(job_definition["Command"]),
                "--role", role_arn,
                "--default-arguments", json.dumps(default_arguments),
                "--glue-version", glue_version,
                "--worker-type", worker_type,
                "--number-of-workers", str(number_of_workers),
            ],
            dry_run,
        )
        print(f"  \u2713 Created Glue job: {job_name}")

    # Optionally run
    if run_after:
        print("\n  Starting Glue job run...")
        _run_cmd(["aws", "glue", "start-job-run", "--job-name", job_name, "--region", region], dry_run)

    return 0


def apply_databricks(env_name: str, config: dict, dry_run: bool, run_after: bool, project_dir: Path = None) -> int:
    """Deploy to Databricks via bundle."""
    if project_dir is None:
        project_dir = Path.cwd()
    skill_dir = Path(__file__).resolve().parent.parent
    template_dir = skill_dir / "templates"

    # Render databricks.yml if template exists
    databricks_yml_template = template_dir / "databricks.yml.j2"
    if databricks_yml_template.is_file():
        from jinja2 import Environment, FileSystemLoader

        env_config = config.get("environments", {}).get(env_name, {})
        dbx_config = env_config.get("databricks", {})
        variables = {
            "project_name": config.get("project_name", "datacoolie"),
            "env_name": env_name,
            "host": dbx_config.get("host", ""),
            "volume_root": dbx_config.get(
                "volume_root",
                f"/Volumes/{dbx_config.get('catalog', 'workspace')}/"
                f"{dbx_config.get('schema', 'default')}/"
                f"{dbx_config.get('volume', 'datacoolie')}",
            ),
        }
        jinja_env = Environment(loader=FileSystemLoader(str(template_dir)), keep_trailing_newline=True)
        content = jinja_env.get_template("databricks.yml.j2").render(**variables)
        bundle_path = project_dir / "databricks.yml"
        if not dry_run:
            bundle_path.write_text(content, encoding="utf-8")
        print(f"  \u2713 Rendered databricks.yml")

    # Validate bundle
    code, _ = _run_cmd(["databricks", "bundle", "validate"], dry_run)
    if code != 0 and not dry_run:
        return 1

    # Deploy
    _run_cmd(["databricks", "bundle", "deploy", "-t", env_name, "--auto-approve"], dry_run)
    print(f"  \u2713 Deployed Databricks bundle to {env_name}")

    # Optionally run
    if run_after:
        job_key = f"{config.get('project_name', 'datacoolie')}_{env_name}"
        _run_cmd(["databricks", "bundle", "run", "-t", env_name, job_key], dry_run)

    return 0


def apply_fabric(env_name: str, config: dict, dry_run: bool, run_after: bool, project_dir: Path = None) -> int:
    """Deploy to Microsoft Fabric via fab CLI."""
    if project_dir is None:
        project_dir = Path.cwd()
    env_config = config.get("environments", {}).get(env_name, {})
    fabric_config = env_config.get("fabric", {})
    workspace = fabric_config.get("workspace", "MyWorkspace")
    lakehouse = fabric_config.get("lakehouse", "ETL_Lakehouse")

    generated_dir = project_dir / ".datacoolie" / "generated"
    dist_dir = generated_dir / "dist"

    # Upload wheel/zip packages to Lakehouse Files/libraries/
    if dist_dir.is_dir():
        for pkg in list(dist_dir.glob("*.whl")) + list(dist_dir.glob("*.zip")):
            dest = f"{workspace}.Workspace/{lakehouse}.Lakehouse/Files/libraries/{pkg.name}"
            _run_cmd(["fab", "cp", str(pkg), dest], dry_run)

    # Upload metadata
    metadata_dir = project_dir / "metadata"
    if metadata_dir.is_dir():
        for json_file in metadata_dir.glob("*.json"):
            dest = f"{workspace}.Workspace/{lakehouse}.Lakehouse/Files/metadata/{json_file.name}"
            _run_cmd(["fab", "cp", str(json_file), dest], dry_run)

    # Deploy via fab deploy (if config.yml exists)
    config_yml = generated_dir / "config.yml"
    if config_yml.is_file():
        _run_cmd(["fab", "deploy", "--config", str(config_yml), "--target_env", env_name, "--force"], dry_run)
    else:
        # Direct import of notebook
        notebook_path = generated_dir / "run_fabric.ipynb"
        if notebook_path.is_file():
            dest = f"{workspace}.Workspace"
            _run_cmd(["fab", "import", str(notebook_path), dest], dry_run)

    print(f"  \u2713 Deployed to Fabric workspace: {workspace}")

    # Optionally run
    if run_after:
        notebook_name = f"{config.get('project_name', 'datacoolie')}_{env_name}"
        _run_cmd(["fab", "start", f"{workspace}.Workspace/{notebook_name}.Notebook"], dry_run)

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy datacoolie to target platform")
    parser.add_argument("--platform", required=True, choices=PLATFORMS, help="Target platform")
    parser.add_argument("--env", required=True, help="Environment name")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--run", action="store_true", help="Start a job run after deploy")
    parser.add_argument("--skip-upload", action="store_true", help="Skip artifact upload (CI mode, already uploaded)")
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Project root")
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()
    config = load_config(project_dir)

    print(f"\nDataCoolie Deploy — Apply ({args.platform} / {args.env})")
    print("-" * 50)

    if args.dry_run:
        print("  [DRY-RUN MODE — no changes will be made]\n")

    # Step 1: Preflight
    print("Step 1: Preflight checks")
    rc = _run_script("preflight.py", ["--platform", args.platform, "--project-dir", str(project_dir)], args.dry_run)
    if rc != 0 and not args.dry_run:
        print("\nPreflight failed. Aborting.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Package (skip for local)
    if args.platform != "local":
        print("\nStep 2: Package functions")
        rc = _run_script("package.py", ["--output", str(project_dir / ".datacoolie" / "generated" / "dist")], args.dry_run)
        if rc != 0 and not args.dry_run:
            print("\nPackaging failed. Aborting.", file=sys.stderr)
            sys.exit(1)

    # Step 3: Generate runner (if not already present)
    generated_dir = project_dir / ".datacoolie" / "generated"
    from generate import OUTPUT_MAP

    expected_output = generated_dir / OUTPUT_MAP[args.platform]
    if not expected_output.is_file():
        print("\nStep 3: Generate runner")
        rc = _run_script("generate.py", ["--platform", args.platform, "--env", args.env, "--project-dir", str(project_dir)], args.dry_run)
        if rc != 0 and not args.dry_run:
            print("\nGeneration failed. Aborting.", file=sys.stderr)
            sys.exit(1)

    # Step 4: Deploy
    print(f"\nStep 4: Deploy to {args.platform}")
    if args.platform == "aws":
        rc = apply_aws(args.env, config, args.dry_run, args.skip_upload, args.run, project_dir)
    elif args.platform == "databricks":
        rc = apply_databricks(args.env, config, args.dry_run, args.run, project_dir)
    elif args.platform == "fabric":
        rc = apply_fabric(args.env, config, args.dry_run, args.run, project_dir)
    elif args.platform == "local":
        print("  Local platform — no deployment needed. Run the generated script directly.")
        rc = 0

    print("-" * 50)
    if rc == 0:
        print("Deploy complete.\n")
    else:
        print("Deploy failed.\n", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
