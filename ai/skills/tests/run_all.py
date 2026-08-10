"""Run the five outcome-based DataCoolie skill validators and unit tests.

Default execution is fast and does not start external services. Pass ``--integration`` to start the
Docker fixture stack and run discovery's Docker probes.
"""

from __future__ import annotations

import subprocess
import sys
import os
from pathlib import Path


HERE = Path(__file__).parent
SKILL_RUNNERS = {
    "discover": "run_discover.py",
    "design": "run_design.py",
    "build": "run_build.py",
    "provision": "run_provision.py",
    "release": "run_release.py",
}

SHARED_UNIT_TESTS = (
    "unit/test_ai_workflow_contract.py",
    "unit/test_test_harness.py",
)
SKILL_UNIT_TESTS = {
    "discover": (
        "unit/test_discovery_dependencies.py",
        "unit/test_discovery_evidence.py",
        "unit/test_introspect_api.py",
        "unit/test_introspect_db.py",
        "unit/test_introspect_files.py",
        "unit/test_introspect_lakehouse.py",
    ),
    "design": ("unit/test_design_approval.py",),
    "build": (
        "unit/test_build_tooling_contract.py",
        "unit/test_metadata_convert.py",
        "unit/test_metadata_lint.py",
        "unit/test_metadata_merge.py",
        "unit/test_metadata_validate.py",
        "unit/test_operational_runner_contract.py",
        "unit/test_project_automation.py",
        "unit/test_runner_operational_safety.py",
        "unit/test_runner_platform_adapters.py",
        "unit/test_workspace_config_and_runner_contract.py",
        "unit/test_workspace_materialization.py",
    ),
    "provision": ("unit/test_provision_receipt.py",),
    "release": ("unit/test_release_receipt.py",),
}

INTEGRATION_ENVIRONMENT = {
    "DATACOOLIE_TEST_POSTGRES_URL": (
        "postgresql+psycopg2://datacoolie:datacoolie@localhost:5442/pagila"
    ),
    "DATACOOLIE_TEST_MYSQL_URL": (
        "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila"
    ),
    "DATACOOLIE_TEST_MSSQL_URL": (
        "mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    ),
}


def _run(
    command: list[str], *, cwd: Path = HERE, environment: dict[str, str] | None = None
) -> int:
    print("\n> " + " ".join(command))
    return subprocess.run(command, cwd=cwd, env=environment, check=False).returncode


def _unit_targets(selected: list[str]) -> list[str]:
    if not selected:
        return ["unit"]
    targets = list(SHARED_UNIT_TESTS)
    for skill in selected:
        for target in SKILL_UNIT_TESTS[skill]:
            if target not in targets:
                targets.append(target)
    return targets


def _validate_selection(selected: list[str], *, integration: bool) -> None:
    unknown = sorted(set(selected) - set(SKILL_RUNNERS))
    if unknown:
        raise ValueError(f"Unknown skill validator(s): {', '.join(unknown)}")
    if integration and selected and "discover" not in selected:
        raise ValueError("--integration requires the discover validator")


def _integration_environment(base: dict[str, str] | None = None) -> dict[str, str]:
    environment = dict(os.environ if base is None else base)
    environment.update(INTEGRATION_ENVIRONMENT)
    return environment


def _start_integration_services() -> int:
    return _run(
        [
            "docker",
            "compose",
            "-f",
            str(HERE / "docker-compose.yml"),
            "up",
            "-d",
            "--wait",
        ]
    )


def _seed_integration_services() -> int:
    mssql = _run(
        [
            "docker", "exec", "skills-test-mssql", "bash", "-lc",
            "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa "
            "-P \"$MSSQL_SA_PASSWORD\" -No -i /opt/mssql-scripts/adventureworks-lt.sql",
        ]
    )
    if mssql != 0:
        return mssql
    return _run([sys.executable, str(HERE / "fixtures/iceberg/seed_iceberg.py")])


def _stop_integration_services() -> int:
    return _run(
        [
            "docker", "compose", "-f", str(HERE / "docker-compose.yml"),
            "down", "--volumes", "--remove-orphans",
        ]
    )


def main() -> int:
    args = sys.argv[1:]
    integration = "--integration" in args
    keep_integration = "--keep-integration" in args
    selected = [arg for arg in args if not arg.startswith("--")]
    try:
        _validate_selection(selected, integration=integration)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    skills = selected or list(SKILL_RUNNERS)

    results: dict[str, int] = {}
    results["unit"] = _run(
        [sys.executable, "-m", "pytest", "-o", "addopts=", *_unit_targets(selected), "-q"]
    )
    results["behavioral-definitions"] = _run(
        [
            sys.executable,
            str(HERE / "verify_behavioral_evidence.py"),
            "verify-definitions",
            *(str(HERE.parent / f"datacoolie-{skill}") for skill in skills),
        ]
    )
    integration_attempted = integration
    try:
        if integration:
            results["docker-start"] = _start_integration_services()
            if results["docker-start"] == 0:
                results["docker-seed"] = _seed_integration_services()
                integration = results["docker-seed"] == 0
            else:
                integration = False
        for skill in skills:
            command = [sys.executable, str(HERE / SKILL_RUNNERS[skill])]
            environment = None
            if skill == "discover" and integration:
                command.append("--docker")
                environment = _integration_environment()
            results[skill] = _run(command, environment=environment)
    finally:
        if integration_attempted and not keep_integration:
            results["docker-stop"] = _stop_integration_services()

    print("\nDataCoolie AI skill verification")
    for name, result in results.items():
        print(f"  [{'PASS' if result == 0 else 'FAIL'}] {name}")
    return 1 if any(result != 0 for result in results.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
