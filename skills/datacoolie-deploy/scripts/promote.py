"""promote.py — Promote datacoolie project from one environment to another.

Runs the full promotion pipeline:
  1. Preflight on the target environment
  2. Metadata validate + lint on merged metadata
  3. Merge env overlay for target
  4. Deploy to target via apply.py
  5. Write promotion log

Usage:
    python scripts/promote.py --from dev --to prod
    python scripts/promote.py --from dev --to test --project-dir /path/to/project
    python scripts/promote.py --from dev --to prod --skip-auth --confirm

Exit codes:
    0 = promotion succeeded
    1 = promotion failed (preflight, validate, or deploy error)
    2 = usage error
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

ENVS = ("dev", "test", "prod")
PROD_ENVS = ("prod",)  # Environments that require --confirm


def _run_script(
    script_name: str,
    script_args: list[str],
    project_dir: Path,
    label: str,
) -> tuple[int, str]:
    """Run a sibling script from scripts/ and return (exit_code, output)."""
    script_path = Path(__file__).resolve().parent / script_name
    cmd = [sys.executable, str(script_path)] + script_args
    print(f"\n[{label}] Running: {script_name} {' '.join(script_args)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_dir))
    output = (result.stdout + result.stderr).strip()
    if output:
        for line in output.split("\n"):
            print(f"  {line}")
    return result.returncode, output


def _find_metadata_scripts() -> Path | None:
    """Locate the datacoolie-metadata scripts directory."""
    # Relative to this script: ../../datacoolie-metadata/scripts/
    here = Path(__file__).resolve().parent
    candidate = here.parent.parent / "datacoolie-metadata" / "scripts"
    return candidate if candidate.is_dir() else None


def _run_metadata_script(
    script_name: str,
    script_args: list[str],
    metadata_scripts: Path,
    project_dir: Path,
    label: str,
) -> tuple[int, str]:
    """Run a datacoolie-metadata script and return (exit_code, output)."""
    script_path = metadata_scripts / script_name
    cmd = [sys.executable, str(script_path)] + script_args
    print(f"\n[{label}] Running: {script_name} {' '.join(script_args)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_dir))
    output = (result.stdout + result.stderr).strip()
    if output:
        for line in output.split("\n"):
            print(f"  {line}")
    return result.returncode, output


def write_promotion_log(
    steps: list[dict],
    from_env: str,
    to_env: str,
    project_dir: Path,
) -> Path:
    """Write a promotion log markdown file to .datacoolie/promote/."""
    today = date.today().strftime("%y%m%d")
    log_dir = project_dir / ".datacoolie" / "promote"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{today}_promote-{from_env}-to-{to_env}.md"

    passed = sum(1 for s in steps if s["status"] == "pass")
    failed = sum(1 for s in steps if s["status"] == "fail")
    overall = "Success" if failed == 0 else "Failed"

    lines = [
        "# Promotion Log",
        "",
        f"**Date:** {date.today().isoformat()}",
        f"**From:** {from_env}",
        f"**To:** {to_env}",
        f"**Status:** {overall}",
        "",
        "---",
        "",
        "## Steps",
        "",
        "| Step | Status | Detail |",
        "|---|---|---|",
    ]
    for step in steps:
        symbol = "✓" if step["status"] == "pass" else "✗"
        detail = step.get("detail", "")[:120]
        lines.append(f"| {step['name']} | {symbol} {step['status']} | {detail} |")

    lines.extend([
        "",
        "---",
        "",
        "## Summary",
        "",
        f"- **Steps passed:** {passed}",
        f"- **Steps failed:** {failed}",
    ])

    if failed > 0:
        lines.extend(["", "## Errors", ""])
        for step in steps:
            if step["status"] == "fail":
                lines.append(f"- **{step['name']}**: {step.get('detail', '')}")

    log_path.write_text("\n".join(lines), encoding="utf-8")
    return log_path


def promote(
    from_env: str,
    to_env: str,
    platform: str,
    project_dir: Path,
    skip_auth: bool,
    confirm: bool,
) -> int:
    """Execute the full promotion pipeline. Returns exit code."""
    steps: list[dict] = []

    print(f"\n{'='*60}")
    print(f"  Promoting: {from_env} → {to_env}  (platform: {platform})")
    print(f"{'='*60}")

    if to_env in PROD_ENVS and not confirm:
        print(
            f"\nERROR: Promotion to '{to_env}' requires --confirm.\n"
            f"       Review the pipeline steps above, then re-run with --confirm.\n",
            file=sys.stderr,
        )
        return 2

    # ------------------------------------------------------------------ #
    # Step 1: Preflight on target environment
    # ------------------------------------------------------------------ #
    preflight_args = [
        "--platform", platform,
        "--project-dir", str(project_dir),
        "--env", to_env,
    ]
    if skip_auth:
        preflight_args.append("--skip-auth")

    rc, output = _run_script("preflight.py", preflight_args, project_dir, "1/4 Preflight")
    steps.append({"name": "Preflight", "status": "pass" if rc == 0 else "fail", "detail": output.split("\n")[-1] if output else ""})
    if rc != 0:
        print(f"\nPREFLIGHT FAILED — aborting promotion.", file=sys.stderr)
        _write_and_exit(steps, from_env, to_env, project_dir, 1)

    # ------------------------------------------------------------------ #
    # Step 2: Metadata validate + lint on merged view
    # ------------------------------------------------------------------ #
    metadata_scripts = _find_metadata_scripts()
    if metadata_scripts:
        metadata_dir = project_dir / "metadata"
        meta_files = list(metadata_dir.glob("*.json")) + list(metadata_dir.glob("*.yaml")) if metadata_dir.is_dir() else []
        validate_ok = True
        validate_detail = ""
        for meta_file in meta_files:
            rc_v, out_v = _run_metadata_script(
                "validate.py", [str(meta_file)], metadata_scripts, project_dir, "2/4 Validate"
            )
            if rc_v != 0:
                validate_ok = False
                validate_detail = out_v.split("\n")[0] if out_v else "validation error"
                break
        if validate_ok:
            validate_detail = f"{len(meta_files)} file(s) valid"
        steps.append({"name": "Metadata validate", "status": "pass" if validate_ok else "fail", "detail": validate_detail})
        if not validate_ok:
            print("\nMETADATA VALIDATION FAILED — aborting promotion.", file=sys.stderr)
            _write_and_exit(steps, from_env, to_env, project_dir, 1)
    else:
        steps.append({"name": "Metadata validate", "status": "pass", "detail": "metadata scripts not found (skip)"})

    # ------------------------------------------------------------------ #
    # Step 3: Merge env overlay
    # ------------------------------------------------------------------ #
    if metadata_scripts:
        rc_m, out_m = _run_metadata_script(
            "merge.py",
            ["--base", str(project_dir / "metadata"), "--env", to_env],
            metadata_scripts,
            project_dir,
            "3/4 Merge",
        )
        steps.append({"name": "Metadata merge", "status": "pass" if rc_m == 0 else "fail", "detail": out_m.split("\n")[0] if out_m else ""})
        if rc_m != 0:
            print("\nMETADATA MERGE FAILED — aborting promotion.", file=sys.stderr)
            _write_and_exit(steps, from_env, to_env, project_dir, 1)
    else:
        steps.append({"name": "Metadata merge", "status": "pass", "detail": "metadata scripts not found (skip)"})

    # ------------------------------------------------------------------ #
    # Step 4: Deploy to target
    # ------------------------------------------------------------------ #
    apply_args = [
        "--platform", platform,
        "--env", to_env,
        "--project-dir", str(project_dir),
    ]
    rc_a, out_a = _run_script("apply.py", apply_args, project_dir, "4/4 Deploy")
    steps.append({"name": "Deploy", "status": "pass" if rc_a == 0 else "fail", "detail": out_a.split("\n")[-1] if out_a else ""})

    # ------------------------------------------------------------------ #
    # Write log + print summary
    # ------------------------------------------------------------------ #
    log_path = write_promotion_log(steps, from_env, to_env, project_dir)
    print(f"\nPromotion log: {log_path}")

    failed = sum(1 for s in steps if s["status"] == "fail")
    if failed > 0:
        print(f"\n⚠ {failed} step(s) failed.", file=sys.stderr)
        return 1

    print(f"\n✓ Promotion {from_env} → {to_env} succeeded.")
    return 0


def _write_and_exit(steps: list[dict], from_env: str, to_env: str, project_dir: Path, rc: int) -> None:
    """Write partial log and exit immediately."""
    log_path = write_promotion_log(steps, from_env, to_env, project_dir)
    print(f"\nPartial promotion log: {log_path}")
    sys.exit(rc)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Promote datacoolie project from one environment to another."
    )
    parser.add_argument(
        "--from",
        dest="from_env",
        required=True,
        choices=ENVS,
        help="Source environment",
    )
    parser.add_argument(
        "--to",
        dest="to_env",
        required=True,
        choices=ENVS,
        help="Target environment",
    )
    parser.add_argument(
        "--platform",
        default="local",
        choices=("aws", "fabric", "databricks", "local"),
        help="Target platform (default: local)",
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=Path.cwd(),
        help="Project root directory (default: CWD)",
    )
    parser.add_argument(
        "--skip-auth",
        action="store_true",
        help="Skip authentication check in preflight",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required for production promotions",
    )

    args = parser.parse_args()

    if args.from_env == args.to_env:
        print(f"ERROR: --from and --to must be different environments.", file=sys.stderr)
        return 2

    return promote(
        from_env=args.from_env,
        to_env=args.to_env,
        platform=args.platform,
        project_dir=args.project_dir,
        skip_auth=args.skip_auth,
        confirm=args.confirm,
    )


if __name__ == "__main__":
    sys.exit(main())
