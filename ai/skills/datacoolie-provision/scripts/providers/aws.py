"""AWS CLI provider for datacoolie-provision.

Handles AWS resources: S3 buckets, Glue databases, IAM roles.
"""

from __future__ import annotations

import json
import subprocess


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    """Run a CLI command. Returns (exit_code, output)."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode, (result.stdout + result.stderr).strip()
    except FileNotFoundError:
        return 127, f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 1, f"Timed out: {' '.join(cmd)}"


def check_exists(resource_type: str, name: str) -> bool:
    """Check if an AWS resource exists."""
    check_map = {
        "s3": ["aws", "s3api", "head-bucket", "--bucket", name],
        "s3 bucket": ["aws", "s3api", "head-bucket", "--bucket", name],
        "glue database": ["aws", "glue", "get-database", "--name", name],
        "glue_database": ["aws", "glue", "get-database", "--name", name],
        "iam role": ["aws", "iam", "get-role", "--role-name", name],
        "iam_role": ["aws", "iam", "get-role", "--role-name", name],
    }
    cmd = check_map.get(resource_type.lower())
    if not cmd:
        return False

    code, _ = _run(cmd)
    return code == 0


def create_command(resource_type: str, name: str) -> list[str] | None:
    """Generate the CLI command to create an AWS resource."""
    rt = resource_type.lower()

    if rt in ("s3", "s3 bucket", "s3_bucket"):
        return ["aws", "s3", "mb", f"s3://{name}"]

    if rt in ("glue database", "glue_database"):
        db_input = json.dumps({"Name": name})
        return ["aws", "glue", "create-database", "--database-input", db_input]

    if rt in ("iam role", "iam_role"):
        # Placeholder — actual policy document needed
        return [
            "aws", "iam", "create-role",
            "--role-name", name,
            "--assume-role-policy-document", "file://trust-policy.json",
        ]

    return None
