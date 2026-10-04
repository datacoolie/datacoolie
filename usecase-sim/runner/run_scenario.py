"""Scenario runner — dispatch named scenarios to the appropriate runner script.

Usage:
    python usecase-sim/runner/run_scenario.py --scenario local_spark_file_csv2delta
    python usecase-sim/runner/run_scenario.py --all
    python usecase-sim/runner/run_scenario.py --priority P0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
RUNNER_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = RUNNER_DIR.parent
DATACOOLIE_ROOT = USECASE_SIM_DIR.parent

# ``run_scenario.py`` is an executable module rather than an installed
# package.  Make the import-light path module available when this file is
# loaded directly by Python or by the unit-test loader.
if str(USECASE_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(USECASE_SIM_DIR))
from runtime_paths import (  # noqa: E402
    DATA_DIR,
    LOG_DIR,
    AWS_LOG_PATH,
    AWS_WATERMARK_PATH,
    RUNTIME_URI_PREFIXES,
    USECASE_SIM_DIR as CANONICAL_USECASE_SIM_DIR,
    WATERMARKS_DIR,
    spark_data_dir,
    spark_profile,
    runtime_child,
)
from runtime_logs import job_sessions  # noqa: E402

# Keep the path aliases derived from the canonical module.  The dispatcher
# tests monkeypatch these module attributes to isolate their receipts.
USECASE_SIM_DIR = CANONICAL_USECASE_SIM_DIR
DATACOOLIE_ROOT = USECASE_SIM_DIR.parent

SCENARIOS_PATH = USECASE_SIM_DIR / "scenarios" / "scenarios.json"
RUNTIME_DIR = USECASE_SIM_DIR / ".runtime"
SCENARIO_LOG_DIR = LOG_DIR / "scenarios"

RUN_SCRIPT = RUNNER_DIR / "run.py"
MAINTENANCE_SCRIPT = RUNNER_DIR / "maintenance.py"

# Docker container used for Spark execution on Windows.
# When the container is running, all spark-engine scenarios are dispatched
# via `docker exec` to avoid Windows JVM / PySpark issues.
DOCKER_SPARK_CONTAINER = "datacoolie-spark"
CONTAINER_ROOT = "/datacoolie"
DOCKER_COMPOSE_FILE = str(USECASE_SIM_DIR / "docker" / "docker-compose.yml")

# Docker Compose service names that must be running for certain metadata types.
# The runner will auto-start them via `docker compose up -d` when needed.
METADATA_TYPE_SERVICES: dict[str, str] = {
    "api": "metadata-api",  # metadata-api container (port 8000)
}
# Spark scenarios that don't skip API sources also need the mock-api data server.
MOCK_API_SERVICE = "mock-api"  # mock-api container (port 8082)

# Stale JVM artifacts that can block the next Spark session.
SPARK_CLEANUP_DIRS = [
    spark_data_dir("spark35") / "warehouse",
    spark_data_dir("spark35") / "metastore_db",
]
SPARK_COOLDOWN_SECS = 6

# Grace window (seconds) a cancelled child gets to flush + push logs AND tear
# down Spark/JVM before a hard kill. Spark session shutdown alone can take
# 30-60s, so this is deliberately generous; a healthy child exits as soon as
# teardown finishes and does not wait the full window.
GRACEFUL_SHUTDOWN_SECS = 120

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


def _spark_container(scenario: dict) -> str:
    """Return the Docker Spark container selected by one scenario."""
    return str(scenario.get("spark_container") or DOCKER_SPARK_CONTAINER)


def _docker_spark_running(container: str = DOCKER_SPARK_CONTAINER) -> bool:
    """Return True when the selected Docker Spark container is running."""
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}}",
                container,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() == "true"
    except Exception:
        return False


def _ensure_service_running(service: str) -> None:
    """Start a docker-compose service if its container is not already running.

    Uses the compose file next to this runner so the service joins the same
    network as all other datacoolie containers.
    """
    container = f"datacoolie-{service}"
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", container],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.stdout.strip() == "true":
            return  # already up
    except Exception:
        pass
    logger.info(
        "[docker] Service '%s' not running — starting via docker compose ...", service
    )
    subprocess.run(
        ["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d", "--wait", service],
        timeout=120,
    )


def _to_container_path(windows_path: Path) -> str:
    """Convert an absolute path inside DATACOOLIE_ROOT to the container equivalent."""
    try:
        rel = windows_path.relative_to(DATACOOLIE_ROOT)
        return CONTAINER_ROOT + "/" + rel.as_posix()
    except ValueError:
        return str(windows_path)


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


def _scenario_path_value(value: object, *, use_docker: bool) -> str:
    """Render a repository path for the host or the mounted Spark container."""
    text = str(value)
    candidate = Path(text)
    if use_docker and candidate.is_absolute():
        return _to_container_path(candidate)
    return text


_join_runtime_child = runtime_child


def _scenario_watermark_root(scenario: dict) -> str:
    """Return the explicit FileProvider watermark root for a scenario."""

    configured = scenario.get("watermark_path")
    if configured:
        return str(configured)
    state = scenario.get("state_base_path")
    if state:
        state_text = str(state)
        if state_text.startswith(RUNTIME_URI_PREFIXES):
            return _join_runtime_child(state_text, "watermarks")

        return _join_runtime_child(state_text, "watermarks")
    return AWS_WATERMARK_PATH if scenario.get("platform") == "aws" else str(WATERMARKS_DIR)


def _scenario_log_root(scenario: dict) -> Path:
    """Require local scenarios to share the canonical framework log root."""
    log_root = LOG_DIR.resolve()
    if scenario.get("derive_log_paths_from_state"):
        raise ValueError("Scenario logs must use usecase-sim/.runtime/logs, independent of state")
    if scenario.get("log_base_path"):
        root = (DATACOOLIE_ROOT / str(scenario["log_base_path"])).resolve()
        if root != log_root:
            raise ValueError(f"Scenario logs must use usecase-sim/.runtime/logs: {root}")
    return log_root


def _metadata_source_args(
    name: str, scenario: dict, *, use_docker: bool = False
) -> list[str]:
    meta_type = scenario["metadata_type"]
    if meta_type == "file":
        if scenario.get("metadata_path") and scenario.get("metadata_base_path"):
            raise ValueError(
                f"File scenario {name} cannot combine metadata_path and metadata_base_path"
            )
        args: list[str] = []
        if scenario.get("metadata_path"):
            args += [
                "--metadata-path",
                _scenario_path_value(scenario["metadata_path"], use_docker=use_docker),
            ]
        if scenario.get("metadata_base_path"):
            args += [
                "--metadata-base-path",
                _scenario_path_value(
                    scenario["metadata_base_path"], use_docker=use_docker
                ),
            ]
        if scenario.get("artifact_base_path"):
            args += [
                "--artifact-base-path",
                _scenario_path_value(
                    scenario["artifact_base_path"], use_docker=use_docker
                ),
            ]
        if not args:
            raise ValueError(
                f"File scenario {name} requires metadata_path, metadata_base_path, "
                "or artifact_base_path"
            )
        return args
    if meta_type == "database":
        return [
            "--metadata-db-connection-string",
            scenario["metadata_db_connection_string"],
            "--metadata-workspace-id",
            scenario["metadata_workspace_id"],
        ]
    if meta_type == "api":
        args = [
            "--metadata-api-url",
            scenario["metadata_api_url"],
            "--metadata-workspace-id",
            scenario["metadata_workspace_id"],
        ]
        if scenario.get("metadata_api_key"):
            args += ["--metadata-api-key", scenario["metadata_api_key"]]
        return args
    raise ValueError(f"Unknown metadata_type={meta_type} for scenario {name}")


def _append_runtime_args(
    cmd: list[str], scenario: dict, *, use_docker: bool
) -> None:
    """Append path, correlation, and logging overrides shared by runners."""

    state_base_path = scenario.get("state_base_path")
    if state_base_path:
        cmd.extend([
            "--state-base-path",
            _scenario_path_value(state_base_path, use_docker=use_docker),
        ])

    # FileProvider watermarks are independent from data and framework logs.
    # A scenario-owned state root keeps the historical inner ``watermarks``
    # directory; otherwise the runner's canonical runtime default is used.
    cmd.extend([
        "--watermark-path",
        _scenario_path_value(
            _scenario_watermark_root(scenario),
            use_docker=use_docker,
        ),
    ])

    sql_base_path = scenario.get("sql_base_path")
    if sql_base_path:
        # The framework contract accepts one root or an ordered collection of
        # roots.  Repeat the CLI flag so each root remains an independent
        # value; do not serialize a list into one shell argument.
        sql_roots = (
            sql_base_path
            if isinstance(sql_base_path, (list, tuple))
            else [sql_base_path]
        )
        for root in sql_roots:
            cmd.extend([
                "--sql-base-path",
                _scenario_path_value(root, use_docker=use_docker),
            ])

    if scenario.get("job_id"):
        cmd.extend(["--job-id", str(scenario["job_id"])])

    run_attributes = scenario.get("run_attributes")
    if run_attributes is not None:
        if not isinstance(run_attributes, dict):
            raise ValueError(f"Scenario {scenario.get('name', '<unnamed>')} run_attributes must be an object")
        encoded = json.dumps(run_attributes, separators=(",", ":"))
        cmd.extend(["--run-attributes", encoded])

    for key, flag in (
        ("log_persistence_mode", "--log-persistence-mode"),
        ("log_flush_interval_seconds", "--log-flush-interval-seconds"),
        ("log_flush_batch_bytes", "--log-flush-batch-bytes"),
        ("log_console_color", "--log-console-color"),
    ):
        if scenario.get(key) is not None:
            cmd.extend([flag, str(scenario[key])])


def build_command(
    name: str,
    scenario: dict,
    use_docker: bool = False,
    spark_container: str = DOCKER_SPARK_CONTAINER,
) -> list[str]:
    """Build a subprocess command from a scenario definition."""
    meta_type = scenario["metadata_type"]
    if meta_type == "maintenance":
        if scenario.get("metadata_path") and scenario.get("metadata_base_path"):
            raise ValueError(
                f"Maintenance scenario {name} cannot combine metadata_path and metadata_base_path"
            )
        if not any(
            (
                scenario.get("metadata_path"),
                scenario.get("metadata_base_path"),
                scenario.get("artifact_base_path"),
            )
        ):
            raise ValueError(
                f"Maintenance scenario {name} requires metadata_path, metadata_base_path, "
                "or artifact_base_path"
            )
    script = MAINTENANCE_SCRIPT if meta_type == "maintenance" else RUN_SCRIPT
    platform = scenario.get("platform", "local")
    log_path = (
        str(scenario.get("log_base_path") or AWS_LOG_PATH)
        if platform == "aws" else str(_scenario_log_root(scenario))
    )

    if use_docker:
        # Run inside the datacoolie-spark container (Linux, no Windows JVM issues).
        # The container volume-mounts DATACOOLIE_ROOT → /datacoolie, so Windows
        # absolute paths are converted to their container equivalents.
        script_path = _to_container_path(script)
        # AWS-platform loggers route through AWSPlatform, which needs an s3:// URI;
        # only local-platform loggers write to the container filesystem path.
        docker_log = log_path if platform == "aws" else _to_container_path(Path(log_path))
        # `-e` sets PYTHONUNBUFFERED so tee streams work the same as -u on the host.
        cmd = [
            "docker",
            "exec",
            "-e",
            "PYTHONUNBUFFERED=1",
            spark_container,
            "python3",
            script_path,
            "--engine",
            scenario["engine"],
            "--platform",
            platform,
        ]
        cmd.extend(["--log-path", docker_log])
    else:
        # `-u` forces unbuffered stdout in the child so the tee streams live.
        cmd = [
            sys.executable,
            "-u",
            str(script),
            "--engine",
            scenario["engine"],
            "--platform",
            platform,
        ]
        cmd.extend(["--log-path", log_path])

    _append_runtime_args(cmd, scenario, use_docker=use_docker)

    if meta_type == "maintenance":
        if scenario.get("metadata_path"):
            cmd += [
                "--metadata-path",
                _scenario_path_value(scenario["metadata_path"], use_docker=use_docker),
            ]
        if scenario.get("metadata_base_path"):
            cmd += [
                "--metadata-base-path",
                _scenario_path_value(
                    scenario["metadata_base_path"], use_docker=use_docker
                ),
            ]
        if scenario.get("artifact_base_path"):
            cmd += [
                "--artifact-base-path",
                _scenario_path_value(
                    scenario["artifact_base_path"], use_docker=use_docker
                ),
            ]
        if scenario.get("connection"):
            cmd += ["--connection", scenario["connection"]]
        _add_flag(cmd, scenario, "dry_run", "--dry-run")
        _add_flag(cmd, scenario, "skip_api_sources", "--skip-api-sources")
        return cmd

    cmd += ["--metadata-source", meta_type]
    cmd += _metadata_source_args(name, scenario, use_docker=use_docker)
    cmd += ["--stage", scenario.get("stage", "")]
    if scenario.get("column_name_mode"):
        cmd += ["--column-name-mode", scenario["column_name_mode"]]
    _add_flag(cmd, scenario, "dry_run", "--dry-run")
    _add_flag(cmd, scenario, "needs_iceberg", "--needs-iceberg")
    _add_flag(cmd, scenario, "skip_api_sources", "--skip-api-sources")
    if scenario.get("max_workers") is not None:
        cmd += ["--max-workers", str(scenario["max_workers"])]
    engine_setup = scenario.get("engine_setup")
    if engine_setup:
        function_path = engine_setup.get("python_function")
        if not function_path:
            raise ValueError(f"engine_setup.python_function is required for {name}")
        cmd += ["--engine-setup-function", str(function_path)]
        for arg in engine_setup.get("args", []):
            cmd.append(f"--engine-setup-arg={arg}")
    # Replay mode — append --replay-* args when present in the scenario.
    if scenario.get("replay_start"):
        cmd += ["--replay-start", str(scenario["replay_start"])]
        cmd += ["--replay-end", str(scenario["replay_end"])]
        for kv in scenario.get("replay_chunk_interval", []):
            cmd += ["--replay-chunk-interval", str(kv)]
        if scenario.get("replay_save_watermark"):
            cmd.append("--replay-save-watermark")
        if scenario.get("replay_chunk_column"):
            cmd += ["--replay-chunk-column", str(scenario["replay_chunk_column"])]
    return cmd


def _scenario_invocations(scenario: dict) -> list[dict]:
    """Return the ordered child runs for one scenario.

    Most registry entries still represent one process.  Surface-sync cases
    use the small declarative extension to exercise recovery across process
    boundaries while keeping setup, cleanup, and final validation scenario
    owned.  The child dictionaries override only command fields; they are not
    persisted or merged into the registry.
    """
    raw = scenario.get("invocations")
    if raw is None:
        return [{}]
    if not isinstance(raw, list) or not raw:
        raise ValueError("scenario invocations must be a non-empty list")
    if any(not isinstance(item, dict) for item in raw):
        raise ValueError("scenario invocation entries must be objects")
    return raw


def _invocation_scenario(scenario: dict, invocation: dict) -> dict:
    effective = dict(scenario)
    effective.pop("invocations", None)
    effective.update(invocation)
    return effective


def _invocation_expected_exit(scenario: dict, invocation: dict, count: int) -> int:
    if "expected_exit_code" in invocation:
        return int(invocation["expected_exit_code"])
    if count == 1:
        return int((scenario.get("validation") or {}).get("expected_exit_code", 0))
    return 0


def _state_snapshot(scenario: dict) -> dict:
    """Hash the explicit watermark root for keyed recovery assertions."""

    raw = _scenario_watermark_root(scenario)
    if str(raw).startswith(RUNTIME_URI_PREFIXES):
        return {"root": raw, "files": [], "sha256": None}
    root = (DATACOOLIE_ROOT / str(raw)).resolve()
    if not root.exists():
        return {"root": str(root), "files": [], "sha256": None}
    files: list[dict[str, object]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        item: dict[str, object] = {
            "path": str(path.relative_to(root)),
            "sha256": digest,
        }
        # Watermark payloads are small, non-secret state and are needed to
        # prove multi-invocation semantics after a later invocation advances
        # the same state root. Keep the historical value in the receipt while
        # retaining hashes for every other file.
        if path.name == "watermark_value.json":
            try:
                item["value"] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        files.append(item)
    combined = hashlib.sha256(
        json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {"root": str(root), "files": files, "sha256": combined}


# ---------------------------------------------------------------------------
# Spark housekeeping
# ---------------------------------------------------------------------------
def _cleanup_spark_state(reason: str) -> None:
    """Remove stale Derby metastore + warehouse dirs left by a prior JVM."""
    profile = spark_profile()
    cleanup_dirs = [
        spark_data_dir(profile) / "warehouse",
        spark_data_dir(profile) / "metastore_db",
    ]
    for d in cleanup_dirs:
        if not d.exists():
            continue
        try:
            shutil.rmtree(d)
            logger.info("  [%s] removed stale dir %s", reason, d.name)
        except OSError as exc:
            logger.warning(
                "  [%s] could not remove %s: %s (JVM may still hold a lock)",
                reason,
                d,
                exc,
            )


def _spark_cooldown(last_spark_finish: float) -> None:
    if last_spark_finish <= 0:
        return
    remaining = SPARK_COOLDOWN_SECS - (time.monotonic() - last_spark_finish)
    if remaining > 0:
        logger.info("  [cooldown] waiting %.1f s for JVM cleanup …", remaining)
        time.sleep(remaining)
    _cleanup_spark_state("cooldown")


def _pre_clean_paths(scenario: dict) -> None:
    """Delete stale data/watermark paths after validating the full set."""

    allowed_roots = (DATA_DIR.resolve(), WATERMARKS_DIR.resolve())
    candidates: list[Path] = []
    for rel_path in scenario.get("pre_clean_paths", []):
        path = (DATACOOLIE_ROOT / str(rel_path)).resolve()
        if any(path != root and path.is_relative_to(root) for root in allowed_roots):
            candidates.append(path)
        else:
            raise ValueError(
                "pre_clean_paths must resolve strictly below usecase-sim/.runtime/data "
                "or usecase-sim/.runtime/watermarks: "
                f"{rel_path}"
            )

    # Validate every target before deleting any one of them.  This protects a
    # run from partial cleanup when a later registry entry contains an unsafe
    # path.
    for path in candidates:
        if not any(path != root and path.is_relative_to(root) for root in allowed_roots):
            raise ValueError(f"pre-clean target escaped simulator runtime: {path}")

    for path in candidates:
        if not path.exists():
            continue
        try:
            shutil.rmtree(path)
            logger.info("  [pre-clean] removed stale output: %s", path)
        except OSError as exc:
            logger.warning("  [pre-clean] could not remove %s: %s", path, exc)


def _pre_clean_job_logs(scenario: dict) -> None:
    """Remove prior simulator log files for an explicitly stable job id.

    Snapshot logs intentionally contain one file per job.  A repeated
    scenario run must therefore clear only that job's files before starting;
    no broad runtime/logs cleanup is permitted.
    """
    job_id = scenario.get("job_id")
    if not job_id or scenario.get("platform", "local") != "local":
        return
    token = str(job_id)
    log_root = _scenario_log_root(scenario)
    if not log_root.exists():
        return
    removed = 0
    paths = (
        path for category in ("execution_logs", "system_logs")
        for path in (log_root / category).rglob("*.json")
    )
    for path in paths:
        try:
            records = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, json.JSONDecodeError):
            continue
        if not records or not all(
            isinstance(record, dict) and record.get("job_id") == token
            for record in records
        ):
            continue
        try:
            path.unlink()
            removed += 1
        except OSError as exc:
            logger.warning("  [pre-clean] could not remove log %s: %s", path, exc)
    if removed:
        logger.info("  [pre-clean] removed %d log file(s) for job_id=%s", removed, token)


def _fresh_standalone_replay_scenario(
    scenario: dict, *, use_docker: bool
) -> dict:
    """Give the native replay validator a fresh, contained fixture root.

    The registry keeps stable root prefixes for discoverability, while the
    validator owns every fixture and receipt below the selected suffix. Old
    roots are intentionally left in place so their native receipts remain
    auditable and cannot be overwritten on a rerun.
    """
    validation = scenario.get("validation") or {}
    validator = str(validation.get("script") or "").replace("\\", "/")
    if not validator.endswith("/validate_replay_window_safety.py"):
        return scenario

    args = [str(arg) for arg in validation.get("args", [])]
    try:
        root_index = args.index("--root")
        configured_root = args[root_index + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError(
            "standalone replay validation must declare --root"
        ) from exc

    requested = Path(configured_root)
    requested = (
        requested if requested.is_absolute() else DATACOOLIE_ROOT / requested
    ).resolve()
    data_base = DATA_DIR.resolve()
    if requested == data_base or not requested.is_relative_to(data_base):
        raise ValueError(
            "standalone replay validation root must stay strictly below "
            f"{data_base}: {requested}"
        )

    relative_parent = requested.relative_to(data_base).parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for sequence in range(1, 1000):
        candidate = requested.with_name(
            f"{requested.name}_{stamp}_{sequence:02d}"
        )
        relative = relative_parent / candidate.name
        mapped_log = (LOG_DIR / relative).resolve()
        mapped_watermark = (WATERMARKS_DIR / relative).resolve()
        if candidate.exists() or mapped_log.exists() or mapped_watermark.exists():
            continue

        if use_docker:
            rendered = _to_container_path(candidate)
        else:
            try:
                rendered = candidate.relative_to(DATACOOLIE_ROOT).as_posix()
            except ValueError:
                rendered = str(candidate)
        copied = dict(scenario)
        copied_validation = dict(validation)
        copied_args = list(args)
        copied_args[root_index + 1] = rendered
        copied_validation["args"] = copied_args
        copied["validation"] = copied_validation
        logger.info("  [fresh-root] standalone replay root: %s", candidate)
        return copied

    raise ValueError(
        "could not allocate a fresh standalone replay root after 999 attempts"
    )


def _run_scenario_setup(name: str, scenario: dict) -> tuple[int, str]:
    """Run an optional repository-local setup script before the ETL child."""

    setup = scenario.get("setup")
    if not setup:
        return 0, "SKIP"
    script = setup.get("script")
    if not script:
        return 1, "FAIL (setup.script is required)"

    repo_root = DATACOOLIE_ROOT.resolve()
    script_path = (repo_root / str(script)).resolve()
    if not script_path.is_relative_to(repo_root):
        return 1, "FAIL (setup script must stay inside repository root)"
    if not script_path.is_file():
        return 1, f"FAIL (setup script not found: {script})"

    setup_log = SCENARIO_LOG_DIR / f"{name}.setup.log"
    cmd = [sys.executable, str(script_path)]
    cmd.extend(str(arg) for arg in setup.get("args", []))
    logger.info("  Setup: %s", " ".join(cmd))
    logger.info("  Setup log: %s", setup_log)

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=int(setup.get("timeout_seconds", 60)),
        )
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        setup_log.write_text(output, encoding="utf-8")
        return 124, "FAIL (setup timed out)"
    except OSError as exc:
        setup_log.write_text(str(exc), encoding="utf-8")
        return 1, f"FAIL (could not run setup script: {exc})"

    output = (completed.stdout or "") + (completed.stderr or "")
    setup_log.write_text(output, encoding="utf-8")
    if output:
        sys.stdout.write(output)
    if completed.returncode != 0:
        return completed.returncode, f"FAIL (setup exit {completed.returncode})"
    return 0, "PASS"


def _validate_scenario_result(
    scenario: dict,
    actual_exit_code: int,
    console_log: Path,
    *,
    use_docker: bool = False,
    spark_container: str = DOCKER_SPARK_CONTAINER,
    log_session_id: str | None = None,
) -> tuple[int, str]:
    """Apply declarative exit, console, and output validation to one run."""
    if actual_exit_code == 124:
        return 124, "FAIL (timeout)"

    validation = scenario.get("validation") or {}
    if validation.get("require_log_session") and not log_session_id:
        return 1, "FAIL (no unique current framework log session)"
    expected_exit_code = int(validation.get("expected_exit_code", 0))
    if actual_exit_code != expected_exit_code:
        return (
            actual_exit_code or 1,
            f"FAIL (expected exit {expected_exit_code}, got {actual_exit_code})",
        )

    required_text = validation.get("required_console_text", [])
    if isinstance(required_text, str):
        required_text = [required_text]
    console_text = console_log.read_text(encoding="utf-8", errors="replace")
    missing_text = [text for text in required_text if text not in console_text]
    if missing_text:
        return 1, f"FAIL (console missing expected text: {missing_text})"

    validator = validation.get("script")
    if validator:
        validator_path = (DATACOOLIE_ROOT / str(validator)).resolve()
        if not validator_path.is_relative_to(DATACOOLIE_ROOT.resolve()):
            return 1, "FAIL (validation script must stay inside repository root)"
        if not validator_path.is_file():
            return 1, f"FAIL (validation script not found: {validator})"

        if validation.get("in_container") and use_docker:
            validator_cmd = [
                "docker",
                "exec",
                "-e",
                "PYTHONUNBUFFERED=1",
                spark_container,
                "python3",
                _to_container_path(validator_path),
            ]
        else:
            validator_cmd = [sys.executable, str(validator_path)]
        validator_cmd.extend(str(arg) for arg in validation.get("args", []))
        if validation.get("require_log_session"):
            validator_cmd.extend(["--log-session-id", log_session_id])
        try:
            completed = subprocess.run(
                validator_cmd,
                cwd=str(DATACOOLIE_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=int(validation.get("timeout_seconds", 60)),
            )
        except subprocess.TimeoutExpired:
            return 1, "FAIL (validation script timed out)"
        except OSError as exc:
            return 1, f"FAIL (could not run validation script: {exc})"
        validator_output = (completed.stdout or "") + (completed.stderr or "")
        if validator_output:
            sys.stdout.write(validator_output)
            with console_log.open("a", encoding="utf-8") as log_fh:
                log_fh.write("\n--- scenario validation ---\n")
                log_fh.write(validator_output)
        if completed.returncode != 0:
            return (
                completed.returncode,
                f"FAIL (validation exit {completed.returncode})",
            )

    if expected_exit_code:
        return 0, f"PASS (expected exit {expected_exit_code})"
    return 0, "PASS"


# ---------------------------------------------------------------------------
# Subprocess tee runner
# ---------------------------------------------------------------------------
def _send_cancel_signal(
    proc: subprocess.Popen, cmd: list[str], is_docker: bool
) -> None:
    """Send a *graceful* cancel signal so the child can flush + push its logs.

    Mirrors real-platform cancellation (SIGTERM). Never hard-kills here — the
    caller owns the grace window and the hard-kill fallback.
    """
    try:
        if is_docker:
            # Signal only the target process *inside* the shared container —
            # never ``docker stop`` the container itself (other scenarios reuse
            # it).  Best-effort; falls back to killing the local exec client.
            marker = next(
                (Path(a).name for a in cmd if a.endswith(".py")),
                "run.py",
            )
            try:
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        cmd[4] if len(cmd) > 4 else DOCKER_SPARK_CONTAINER,
                        "pkill",
                        "-TERM",
                        "-f",
                        marker,
                    ],
                    timeout=10,
                    capture_output=True,
                    text=True,
                )
            except (OSError, subprocess.SubprocessError):
                pass
        elif os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGTERM)
    except (OSError, ValueError):
        pass


def _run_with_tee(cmd: list[str], console_log: Path, timeout: int) -> tuple[int, str]:
    """Run `cmd`, stream stdout to terminal + `console_log`, enforce `timeout`.

    A watchdog thread owns cancellation timing (soft timeout → graceful signal →
    grace window → hard kill) using ``proc.wait`` only.  The main thread keeps
    draining stdout the whole time so the child never blocks writing to a full
    pipe while it flushes logs and tears down Spark/JVM on cancel.
    """
    is_docker = cmd[:2] == ["docker", "exec"]
    # Put the child in its own process group so a graceful cancel signal
    # (CTRL_BREAK on Windows / SIGTERM on POSIX) can be delivered without
    # also hitting this parent runner.
    popen_kwargs: dict = {}
    if os.name == "nt":
        if not is_docker:
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True

    timed_out = threading.Event()

    with open(console_log, "w", encoding="utf-8") as log_fh:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(DATACOOLIE_ROOT),
            **popen_kwargs,
        )

        def _watchdog() -> None:
            # Wait out the soft timeout; if the child finishes first, do nothing.
            try:
                proc.wait(timeout=timeout)
                return
            except subprocess.TimeoutExpired:
                pass
            timed_out.set()
            logger.warning(
                "  [cancel] timeout after %ss - sending graceful signal (grace=%ss)",
                timeout,
                GRACEFUL_SHUTDOWN_SECS,
            )
            _send_cancel_signal(proc, cmd, is_docker)
            # Give the child time to flush + push logs and tear down cleanly.
            try:
                proc.wait(timeout=GRACEFUL_SHUTDOWN_SECS)
                logger.info("  [cancel] child flushed and exited within grace window")
            except subprocess.TimeoutExpired:
                logger.warning(
                    "  [cancel] child did not exit within %ss - hard kill",
                    GRACEFUL_SHUTDOWN_SECS,
                )
                proc.kill()

        watchdog = threading.Thread(target=_watchdog, daemon=True)
        watchdog.start()

        # Drain stdout continuously until the child exits (pipe closes). This
        # keeps the pipe empty so a cancelled child can keep logging/tearing
        # down without blocking on a full stdout buffer.
        for line in proc.stdout:  # type: ignore[union-attr]
            sys.stdout.write(line)
            sys.stdout.flush()
            log_fh.write(line)

        rc = proc.wait()
        watchdog.join(timeout=5)

    if timed_out.is_set():
        return 124, f"FAIL (timeout after {timeout}s)"
    return rc, ("PASS" if rc == 0 else f"FAIL (exit {rc})")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------
def _setup_log_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SCENARIO_LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(
        SCENARIO_LOG_DIR / "run_scenario.log", mode="w", encoding="utf-8"
    )
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
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

    # Check each selected Spark container independently. This lets the opt-in
    # Spark 4.x gate coexist with the pinned Spark 3.5 service and fall back to
    # host PySpark only for a missing profile.
    spark_container_status: dict[str, bool] = {}
    for scenario in (scenarios.get(n, {}) for n in names):
        if not _is_spark(scenario):
            continue
        container = _spark_container(scenario)
        if container in spark_container_status:
            continue
        running = _docker_spark_running(container)
        spark_container_status[container] = running
        if running:
            logger.info(
                "[spark] Docker container '%s' is running — Spark scenarios will execute inside the container.",
                container,
            )
        else:
            logger.warning(
                "[spark] Docker container '%s' is NOT running — falling back to local PySpark.",
                container,
            )
            _cleanup_spark_state("pre-flight")

    # Explicit services apply to every engine. Preserve the implicit services
    # historically required by Docker-hosted Spark scenarios.
    needed: set[str] = set()
    for n in names:
        s = scenarios.get(n, {})
        needed.update(str(service) for service in s.get("services", []))
        if (
            _is_spark(s)
            and spark_container_status.get(_spark_container(s), False)
        ):
            # metadata-api: needed when metadata source is "api"
            svc = METADATA_TYPE_SERVICES.get(s.get("metadata_type", ""))
            if svc:
                needed.add(svc)
            # mock-api: needed when the scenario actually runs API data-source
            # dataflows (i.e. skip_api_sources is not set)
            if not s.get("skip_api_sources", False):
                needed.add(MOCK_API_SERVICE)
    for svc in sorted(needed):
        _ensure_service_running(svc)

    for name in names:
        if name not in scenarios:
            logger.error("Unknown scenario: %s", name)
            results[name] = 1
            continue

        scenario = scenarios[name]
        spark_container = _spark_container(scenario)
        use_docker = (
            _is_spark(scenario)
            and spark_container_status.get(spark_container, False)
        )

        if _is_spark(scenario) and not use_docker:
            _spark_cooldown(last_spark_finish)

        _pre_clean_paths(scenario)
        _pre_clean_job_logs(scenario)

        setup_rc, setup_status = _run_scenario_setup(name, scenario)
        if setup_rc != 0:
            results[name] = setup_rc
            logger.info("  Result: %s", setup_status)
            continue

        # A small registry-owned escape hatch is used for qualification
        # scripts that construct their own fresh fixture and execute a real
        # Driver.  It keeps those scripts out of the ordinary metadata/dataflow
        # child path while retaining scenario cleanup, console capture, and
        # validation receipts.
        if scenario.get("skip_pipeline"):
            combined_console = SCENARIO_LOG_DIR / f"{name}.console.log"
            combined_console.write_text("", encoding="utf-8")
            logger.info("▸ Running standalone validation: %s", name)
            try:
                validation_scenario = _fresh_standalone_replay_scenario(
                    scenario, use_docker=use_docker
                )
            except ValueError as exc:
                final_rc, status = 1, f"FAIL (fresh root: {exc})"
            else:
                final_rc, status = _validate_scenario_result(
                    validation_scenario,
                    0,
                    combined_console,
                    use_docker=use_docker,
                    spark_container=spark_container,
                )
            results[name] = final_rc
            logger.info("  Result: %s", status)
            continue

        try:
            invocations = _scenario_invocations(scenario)
        except ValueError as exc:
            logger.error("Scenario %s: %s", name, exc)
            results[name] = 1
            continue

        # Each invocation gets its own console receipt.  The combined file is
        # fed to the existing final validator so single-invocation scenarios
        # retain their original output contract.
        combined_console = SCENARIO_LOG_DIR / f"{name}.console.log"
        combined_console.write_text("", encoding="utf-8")
        invocation_receipts: list[dict] = []
        invocation_failed = False
        log_session_id = None
        for index, invocation in enumerate(invocations, start=1):
            effective = _invocation_scenario(scenario, invocation)
            try:
                cmd = build_command(
                    name,
                    effective,
                    use_docker=use_docker,
                    spark_container=spark_container,
                )
            except ValueError as exc:
                logger.error("Scenario %s invocation %d: %s", name, index, exc)
                results[name] = 1
                invocation_failed = True
                break

            label = str(invocation.get("label") or f"run{index}")
            if len(invocations) == 1:
                console_log = combined_console
            else:
                console_log = SCENARIO_LOG_DIR / f"{name}.{label}.console.log"
            timeout = int(invocation.get("timeout_seconds", _resolve_timeout(effective)))

            logger.info("▸ Running scenario: %s [%s]", name, label)
            logger.info("  Command: %s", " ".join(cmd))
            logger.info("  Console log: %s", console_log)

            if _is_spark(effective) and not use_docker:
                _spark_cooldown(last_spark_finish)
            state_before = _state_snapshot(effective)
            check_logs = (scenario.get("validation") or {}).get("require_log_session", False)
            log_root = _scenario_log_root(effective) if check_logs else None
            prior_sessions = job_sessions(log_root, str(effective["job_id"])) if check_logs else set()
            rc, _ = _run_with_tee(cmd, console_log, timeout)
            if check_logs:
                new_sessions = job_sessions(log_root, str(effective["job_id"])) - prior_sessions
                log_session_id = next(iter(new_sessions)) if len(new_sessions) == 1 else None
            state_after = _state_snapshot(effective)
            expected = _invocation_expected_exit(scenario, invocation, len(invocations))
            invocation_receipts.append(
                {
                    "label": label,
                    "command": cmd,
                    "expected_exit_code": expected,
                    "actual_exit_code": rc,
                    "state_before": state_before,
                    "state_after": state_after,
                    "console_log": str(console_log),
                    "log_session_id": log_session_id,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if rc != expected:
                logger.error(
                    "  Invocation %s failed: expected exit %s, got %s",
                    label,
                    expected,
                    rc,
                )
                results[name] = rc or 1
                invocation_failed = True
                break

            required_text = invocation.get("required_console_text", [])
            if isinstance(required_text, str):
                required_text = [required_text]
            invocation_text = console_log.read_text(
                encoding="utf-8", errors="replace"
            )
            missing = [text for text in required_text if text not in invocation_text]
            if missing:
                logger.error("  Invocation %s missing console text: %s", label, missing)
                results[name] = 1
                invocation_failed = True
                break

            if console_log != combined_console:
                with combined_console.open("a", encoding="utf-8") as merged:
                    merged.write(f"\n--- invocation {label} ---\n")
                    merged.write(invocation_text)
            if _is_spark(effective) and not use_docker:
                last_spark_finish = time.monotonic()

        receipt_path = SCENARIO_LOG_DIR / f"{name}.invocations.json"
        receipt_path.write_text(
            json.dumps(invocation_receipts, indent=2) + "\n",
            encoding="utf-8",
        )
        if invocation_failed:
            continue

        final_rc, status = _validate_scenario_result(
            scenario,
            0,
            combined_console,
            use_docker=False,
            spark_container=spark_container,
            log_session_id=log_session_id,
        )
        results[name] = final_rc
        logger.info("  Result: %s", status)

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
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description="Run named scenarios")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scenario", help="Name of a single scenario to run")
    group.add_argument("--all", action="store_true", help="Run all scenarios")
    group.add_argument(
        "--priority", help="Run all scenarios with this priority (P0, P1, P2)"
    )
    parser.add_argument(
        "--scenarios-path", default=str(SCENARIOS_PATH), help="Path to scenarios.json"
    )
    args = parser.parse_args()

    scenarios = load_scenarios(Path(args.scenarios_path))
    names = _select_names(scenarios, args)
    sys.exit(run_scenarios(names, scenarios))


if __name__ == "__main__":
    main()
