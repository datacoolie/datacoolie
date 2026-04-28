"""Scenario runner — dispatch named scenarios to the appropriate runner script.

Usage:
    python usecase-sim/runner/run_scenario.py --scenario local_spark_file_csv2delta
    python usecase-sim/runner/run_scenario.py --all
    python usecase-sim/runner/run_scenario.py --priority P0
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
RUNNER_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = RUNNER_DIR.parent
DATACOOLIE_ROOT = USECASE_SIM_DIR.parent

SCENARIOS_PATH = USECASE_SIM_DIR / "scenarios" / "scenarios.json"
LOG_DIR = USECASE_SIM_DIR / "logs"
SCENARIO_LOG_DIR = LOG_DIR / "scenarios"

# For AWS-platform scenarios the driver's loggers route through AWSPlatform,
# which requires an s3:// URI. Use the same MinIO bucket the scenarios
# already target for data.
AWS_LOG_PATH = "s3://datacoolie-test/logs"

RUN_SCRIPT = RUNNER_DIR / "run.py"
MAINTENANCE_SCRIPT = RUNNER_DIR / "maintenance.py"

# Stale JVM artifacts that can block the next Spark session.
SPARK_CLEANUP_DIRS = [
    DATACOOLIE_ROOT / "spark-warehouse",
    DATACOOLIE_ROOT / "metastore_db",
]
SPARK_COOLDOWN_SECS = 6

# Default per-scenario timeouts (seconds). Override via scenario["timeout_seconds"].
DEFAULT_TIMEOUTS = {
    "maintenance": 600,
    "spark": 450,
    "polars": 300,
}


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Windows default cp1252 can't encode box-drawing / non-ASCII chars.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass


_configure_logging()
logger = logging.getLogger("run_scenario")


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------
def load_scenarios(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _is_spark(scenario: dict) -> bool:
    return scenario.get("engine") == "spark"


def _resolve_timeout(scenario: dict) -> int:
    if scenario.get("timeout_seconds") is not None:
        return int(scenario["timeout_seconds"])
    if scenario.get("metadata_type") == "maintenance":
        return DEFAULT_TIMEOUTS["maintenance"]
    if _is_spark(scenario):
        return DEFAULT_TIMEOUTS["spark"]
    return DEFAULT_TIMEOUTS["polars"]


# ---------------------------------------------------------------------------
# Command building
# ---------------------------------------------------------------------------
def _add_flag(cmd: list[str], scenario: dict, key: str, flag: str) -> None:
    if scenario.get(key):
        cmd.append(flag)


def _metadata_source_args(name: str, scenario: dict) -> list[str]:
    meta_type = scenario["metadata_type"]
    if meta_type == "file":
        return ["--metadata-path", scenario["metadata_path"]]
    if meta_type == "database":
        return [
            "--metadata-db-connection-string", scenario["metadata_db_connection_string"],
            "--metadata-workspace-id", scenario["metadata_workspace_id"],
        ]
    if meta_type == "api":
        args = [
            "--metadata-api-url", scenario["metadata_api_url"],
            "--metadata-workspace-id", scenario["metadata_workspace_id"],
        ]
        if scenario.get("metadata_api_key"):
            args += ["--metadata-api-key", scenario["metadata_api_key"]]
        return args
    raise ValueError(f"Unknown metadata_type={meta_type} for scenario {name}")


def build_command(name: str, scenario: dict) -> list[str]:
    """Build a subprocess command from a scenario definition."""
    meta_type = scenario["metadata_type"]
    script = MAINTENANCE_SCRIPT if meta_type == "maintenance" else RUN_SCRIPT
    platform = scenario.get("platform", "local")
    log_path = AWS_LOG_PATH if platform == "aws" else str(LOG_DIR)

    # `-u` forces unbuffered stdout in the child so the tee streams live.
    cmd = [
        sys.executable, "-u", str(script),
        "--engine", scenario["engine"],
        "--platform", platform,
        "--log-path", log_path,
    ]

    if meta_type == "maintenance":
        if "metadata_path" in scenario:
            cmd += ["--metadata-path", scenario["metadata_path"]]
        if scenario.get("connection"):
            cmd += ["--connection", scenario["connection"]]
        _add_flag(cmd, scenario, "dry_run", "--dry-run")
        _add_flag(cmd, scenario, "skip_api_sources", "--skip-api-sources")
        return cmd

    cmd += ["--metadata-source", meta_type]
    cmd += _metadata_source_args(name, scenario)
    cmd += ["--stage", scenario.get("stage", "")]
    if scenario.get("column_name_mode"):
        cmd += ["--column-name-mode", scenario["column_name_mode"]]
    _add_flag(cmd, scenario, "dry_run", "--dry-run")
    _add_flag(cmd, scenario, "skip_api_sources", "--skip-api-sources")
    if scenario.get("max_workers") is not None:
        cmd += ["--max-workers", str(scenario["max_workers"])]
    return cmd


# ---------------------------------------------------------------------------
# Spark housekeeping
# ---------------------------------------------------------------------------
def _cleanup_spark_state(reason: str) -> None:
    """Remove stale Derby metastore + warehouse dirs left by a prior JVM."""
    for d in SPARK_CLEANUP_DIRS:
        if not d.exists():
            continue
        try:
            shutil.rmtree(d)
            logger.info("  [%s] removed stale dir %s", reason, d.name)
        except OSError as exc:
            logger.warning(
                "  [%s] could not remove %s: %s (JVM may still hold a lock)",
                reason, d, exc,
            )


def _spark_cooldown(last_spark_finish: float) -> None:
    if last_spark_finish <= 0:
        return
    remaining = SPARK_COOLDOWN_SECS - (time.monotonic() - last_spark_finish)
    if remaining > 0:
        logger.info("  [cooldown] waiting %.1f s for JVM cleanup …", remaining)
        time.sleep(remaining)
    _cleanup_spark_state("cooldown")


# ---------------------------------------------------------------------------
# Subprocess tee runner
# ---------------------------------------------------------------------------
def _run_with_tee(cmd: list[str], console_log: Path, timeout: int) -> tuple[int, str]:
    """Run `cmd`, stream stdout to terminal + `console_log`, enforce `timeout`."""
    with open(console_log, "w", encoding="utf-8") as log_fh:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        deadline = time.monotonic() + timeout
        try:
            for line in proc.stdout:  # type: ignore[union-attr]
                sys.stdout.write(line)
                sys.stdout.flush()
                log_fh.write(line)
                if time.monotonic() > deadline:
                    proc.kill()
                    proc.wait()
                    return 124, f"FAIL (timeout after {timeout}s)"
            rc = proc.wait(timeout=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return 124, f"FAIL (timeout after {timeout}s)"
    return rc, ("PASS" if rc == 0 else f"FAIL (exit {rc})")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------
def _setup_log_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SCENARIO_LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(SCENARIO_LOG_DIR / "run_scenario.log", mode="w", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logging.getLogger().addHandler(fh)
    logger.info("Framework log dir: %s", LOG_DIR)
    logger.info("Scenario log dir:  %s", SCENARIO_LOG_DIR)


def _print_summary(results: dict[str, int]) -> int:
    passed = sum(1 for rc in results.values() if rc == 0)
    failed = len(results) - passed
    logger.info("=" * 50)
    logger.info("Total: %d | PASS: %d | FAIL: %d", len(results), passed, failed)
    if failed:
        logger.info("Failed scenarios:")
        for name, rc in results.items():
            if rc != 0:
                logger.info("  - %s (exit %d)", name, rc)
    return 0 if failed == 0 else 1


def run_scenarios(names: list[str], scenarios: dict) -> int:
    _setup_log_dirs()

    results: dict[str, int] = {}
    last_spark_finish = 0.0

    if any(_is_spark(scenarios.get(n, {})) for n in names):
        _cleanup_spark_state("pre-flight")

    for name in names:
        if name not in scenarios:
            logger.error("Unknown scenario: %s", name)
            results[name] = 1
            continue

        scenario = scenarios[name]

        if _is_spark(scenario):
            _spark_cooldown(last_spark_finish)

        try:
            cmd = build_command(name, scenario)
        except ValueError as exc:
            logger.error("Scenario %s: %s", name, exc)
            results[name] = 1
            continue

        console_log = SCENARIO_LOG_DIR / f"{name}.console.log"
        timeout = _resolve_timeout(scenario)

        logger.info("▸ Running scenario: %s", name)
        logger.info("  Command: %s", " ".join(cmd))
        logger.info("  Console log: %s", console_log)

        rc, status = _run_with_tee(cmd, console_log, timeout)
        results[name] = rc
        logger.info("  Result: %s", status)

        if _is_spark(scenario):
            last_spark_finish = time.monotonic()

    return _print_summary(results)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _select_names(scenarios: dict, args: argparse.Namespace) -> list[str]:
    if args.scenario:
        return [args.scenario]
    if args.all:
        return list(scenarios.keys())
    names = [n for n, s in scenarios.items() if s.get("priority") == args.priority]
    if not names:
        logger.error("No scenarios with priority %s", args.priority)
        sys.exit(1)
    return names


def main() -> None:
    parser = argparse.ArgumentParser(description="Run named scenarios")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scenario", help="Name of a single scenario to run")
    group.add_argument("--all", action="store_true", help="Run all scenarios")
    group.add_argument("--priority", help="Run all scenarios with this priority (P0, P1, P2)")
    parser.add_argument("--scenarios-path", default=str(SCENARIOS_PATH), help="Path to scenarios.json")
    args = parser.parse_args()

    scenarios = load_scenarios(Path(args.scenarios_path))
    names = _select_names(scenarios, args)
    sys.exit(run_scenarios(names, scenarios))


if __name__ == "__main__":
    main()
