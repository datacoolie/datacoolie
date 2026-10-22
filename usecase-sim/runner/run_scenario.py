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
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from contextvars import ContextVar
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
from runner.runtime_paths import (  # noqa: E402
    DATA_DIR,
    LOG_DIR,
    AWS_LOG_PATH,
    AWS_WATERMARK_PATH,
    RUNTIME_URI_PREFIXES,
    USECASE_SIM_DIR as CANONICAL_USECASE_SIM_DIR,
    WATERMARKS_DIR,
    spark_state_dir,
    spark_profile,
    runtime_child,
)
from runner.owned_process import run_owned, TeardownError, CANCEL  # noqa: E402
from runner.admission import declaration, execute  # noqa: E402
from cases.selection import scenario_dependencies  # noqa: E402
from runner.runtime_profile import MODE_ENV  # noqa: E402
from runner.compose_config import compose_file_arguments, compose_startup_arguments  # noqa: E402
from runner.runtime_workspace import enabled as workspace_enabled, command_arguments, verify_export  # noqa: E402
from runner.runtime_timing import measured, scope, read_runtime_timing  # noqa: E402
from runner.runtime_logs import job_sessions  # noqa: E402
from runner.suite_archive import (  # noqa: E402
    SuiteArchiveError,
    acquire_suite_writer_lock,
    finalize_suite_archive,
)
from cases.catalog import (  # noqa: E402
    CatalogError,
    is_scenario_bundle,
    list_groups,
    runtime_identity,
    scenario_leaf_names,
    scenario_leaf_views,
    select_group_scenarios,
    worktree_fingerprint,
)

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
    spark_state_dir("spark35") / "warehouse",
    spark_state_dir("spark35") / "metastore_db",
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
_attempt = ContextVar("sim_attempt", default=None)


def _record_process(path, completed):
    payload = {"attempt_id": _attempt.get(), **completed.execution}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _process_record(path):
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("attempt_id") == _attempt.get():
            return payload
    return None


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
    compose_files = compose_file_arguments(DOCKER_COMPOSE_FILE)
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
        ["docker", "compose", *compose_files, "up", "-d", "--wait",
         *compose_startup_arguments(), service],
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
    # Watermarks have one simulator-wide default root.  A scenario that needs
    # a scoped state chain must declare ``watermark_path`` explicitly; this
    # prevents an unrelated state-base setting from silently creating a second
    # watermark namespace.
    base = AWS_WATERMARK_PATH if scenario.get("platform") == "aws" else str(WATERMARKS_DIR)
    return _join_runtime_child(base, str(scenario.get("engine") or "polars"))


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


def _metadata_path_values(name: str, value: object) -> list[str]:
    """Validate and preserve an exact metadata file selection order."""
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise ValueError(
            f"File scenario {name} metadata_path must be a string or list of strings"
        )
    if not values or any(not isinstance(path, str) or not path.strip() for path in values):
        raise ValueError(f"File scenario {name} metadata_path must contain non-empty paths")
    return values


def _append_section_path_args(
    cmd: list[str], name: str, scenario: dict, *, use_docker: bool
) -> None:
    """Transport explicitly selected shared FileProvider section files."""
    for key, flag in (
        ("connections_path", "--connections-path"),
        ("schema_hints_path", "--schema-hints-path"),
    ):
        value = scenario.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"File scenario {name} {key} must be a non-empty path")
        cmd.extend([flag, _scenario_path_value(value, use_docker=use_docker)])


def _metadata_source_args(
    name: str, scenario: dict, *, use_docker: bool = False
) -> list[str]:
    meta_type = scenario["metadata_type"]
    if meta_type == "file":
        metadata_path = scenario.get("metadata_path")
        metadata_base_path = scenario.get("metadata_base_path")
        if metadata_path is not None and metadata_base_path is not None:
            raise ValueError(
                f"File scenario {name} cannot combine metadata_path and metadata_base_path"
            )
        args: list[str] = []
        if metadata_path is not None:
            for path in _metadata_path_values(name, metadata_path):
                args += [
                    "--metadata-path",
                    _scenario_path_value(path, use_docker=use_docker),
                ]
        if metadata_base_path:
            args += [
                "--metadata-base-path",
                _scenario_path_value(
                    metadata_base_path, use_docker=use_docker
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
        _append_section_path_args(args, name, scenario, use_docker=use_docker)
        return args
    if meta_type == "database":
        connection_string = scenario["metadata_db_connection_string"]
        if connection_string.startswith("sqlite:///"):
            database = connection_string[len("sqlite:///"):]
            if database and database != ":memory:":
                database_path = Path(database)
                if not database_path.is_absolute():
                    database_path = DATACOOLIE_ROOT / database_path
                connection_string = "sqlite:///" + _scenario_path_value(database_path.resolve(), use_docker=use_docker)
        return [
            "--metadata-db-connection-string",
            connection_string,
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


def _append_file_selectors(cmd: list[str], name: str, scenario: dict) -> None:
    """Forward explicit file-provider dataflow/binding selectors.

    These selectors are intentionally rejected for database and API recipes.
    File-backed maintenance recipes use the same exact provider document and
    may select their maintenance flows; a selector against an opaque provider
    would make a scenario appear reproducible while the provider still
    controls the actual flow set.
    """

    dataflows = scenario.get("dataflows")
    bindings = scenario.get("metadata_bindings")
    if dataflows is not None or bindings is not None:
        if scenario.get("metadata_type") not in {"file", "maintenance"}:
            raise ValueError(
                f"Scenario {name} dataflow selectors and metadata_bindings require a file-backed metadata source"
            )
    if dataflows is not None:
        if (
            not isinstance(dataflows, list)
            or not dataflows
            or any(not isinstance(value, str) or not value.strip() for value in dataflows)
            or len(dataflows) != len(set(dataflows))
        ):
            raise ValueError(f"Scenario {name} dataflows must be a non-empty unique string list")
        for value in dataflows:
            cmd.extend(["--dataflow", value])
    if bindings is not None:
        if not isinstance(bindings, dict):
            raise ValueError(f"Scenario {name} metadata_bindings must be an object")
        cmd.extend(["--metadata-bindings", json.dumps(bindings, separators=(",", ":"))])


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
        metadata_path = scenario.get("metadata_path")
        metadata_base_path = scenario.get("metadata_base_path")
        if metadata_path is not None and metadata_base_path is not None:
            raise ValueError(
                f"Maintenance scenario {name} cannot combine metadata_path and metadata_base_path"
            )
        if metadata_path is not None:
            _metadata_path_values(name, metadata_path)
        if not any(
            (
                metadata_path,
                metadata_base_path,
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
            "-e",
            f"{MODE_ENV}={os.environ.get(MODE_ENV, 'sequential')}",
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
    _append_file_selectors(cmd, name, scenario)

    if meta_type == "maintenance":
        if metadata_path is not None:
            for path in _metadata_path_values(name, metadata_path):
                cmd += [
                    "--metadata-path",
                    _scenario_path_value(path, use_docker=use_docker),
                ]
        if metadata_base_path:
            cmd += [
                "--metadata-base-path",
                _scenario_path_value(
                    metadata_base_path, use_docker=use_docker
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
        _append_section_path_args(cmd, name, scenario, use_docker=use_docker)
        for key in ("maintenance_deployment", "maintenance_guard_manifest"):
            if scenario.get(key):
                cmd += ["--" + key.replace("_", "-"),
                    _scenario_path_value(scenario[key], use_docker=use_docker)]
        for key in ("compact_options", "cleanup_options"):
            if scenario.get(key) is not None:
                cmd += ["--" + key.replace("_", "-"), json.dumps(scenario[key], separators=(",", ":"))]
        for key in ("do_compact", "do_cleanup"):
            if scenario.get(key) is False:
                cmd.append("--no-" + key.removeprefix("do_"))
        if scenario.get("retention_hours") is not None:
            cmd += ["--retention-hours", str(scenario["retention_hours"])]
        if scenario.get("max_workers") is not None:
            cmd += ["--max-workers", str(scenario["max_workers"])]
        for key, value in scenario.get("spark_config", {}).items():
            cmd += ["--spark-config", f"{key}={value}"]
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
        spark_state_dir(profile) / "warehouse",
        spark_state_dir(profile) / "metastore_db",
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


def _fresh_maintenance_scenario(scenario: dict) -> dict:
    """Preserve previous REST qualification fixtures on repeated invocations."""
    setup = scenario.get("setup") or {}
    if setup.get("script") != "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py":
        return scenario
    args = list(setup.get("args", []))
    try:
        descriptor = args[args.index("--descriptor") + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError("REST qualification setup requires --descriptor") from exc
    requested = (DATACOOLIE_ROOT / str(descriptor)).resolve()
    base = (DATA_DIR / "maintenance_t5").resolve()
    if requested.parent.parent != base or requested.name != "deployment.json":
        raise ValueError("REST qualification descriptor must use a direct maintenance_t5 case root")
    token = "maintenance_t5/" + requested.parent.name
    fresh = token + "_" + uuid.uuid4().hex

    def replace(value):
        if isinstance(value, str):
            return value.replace(token, fresh)
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        return value

    return replace(scenario)


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


@measured("prerequisite_s")
def _prepare_native_guard(name: str, scenario: dict, *, use_docker: bool, container: str):
    """Build/verify the explicit dependency before REST fixture seed or Spark."""
    if not (_is_spark(scenario) and scenario.get("maintenance_guard_manifest") and
            (scenario.get("setup") or {}).get("script") ==
            "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"):
        return 0, "SKIP"
    script = USECASE_SIM_DIR / "runner/native_guard.py"
    command = (["docker", "exec", container, "python3", "/datacoolie/usecase-sim/runner/native_guard.py"]
               if use_docker else [sys.executable, str(script)])
    command += ["--manifest", str(scenario["maintenance_guard_manifest"])]
    log = SCENARIO_LOG_DIR / f"{name}.prerequisite.log"
    try:
        completed = run_owned(command, cwd=str(DATACOOLIE_ROOT), timeout=120)
    except subprocess.TimeoutExpired as exc:
        _record_process(log.with_suffix(".process.json"), exc)
        record = json.loads(log.with_suffix(".process.json").read_text(encoding="utf-8"))
        record["actual_exit_code"] = 124
        log.with_suffix(".process.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        log.write_text(str(exc.stdout or "") + str(exc.stderr or ""), encoding="utf-8")
        return 124, "FAIL (native guard prerequisite timed out)"
    except (OSError, ValueError) as exc:
        log.write_text(str(exc), encoding="utf-8")
        return 1, f"FAIL (native guard prerequisite: {exc})"
    _record_process(log.with_suffix(".process.json"), completed)
    record = json.loads(log.with_suffix(".process.json").read_text(encoding="utf-8"))
    record["actual_exit_code"] = completed.returncode
    log.with_suffix(".process.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    output = (completed.stdout or "") + (completed.stderr or "")
    log.write_text(output, encoding="utf-8")
    if output:
        sys.stdout.write(output)
    return (completed.returncode,
            "PASS" if completed.returncode == 0 else "FAIL (native guard prerequisite)")


@measured("setup_s")
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
        completed = run_owned(
            cmd,
            cwd=str(repo_root),
            timeout=int(setup.get("timeout_seconds", 60)),
        )
    except subprocess.TimeoutExpired as exc:
        _record_process(setup_log.with_suffix(".process.json"), exc)
        output = str(exc.stdout or "") + str(exc.stderr or "")
        setup_log.write_text(output, encoding="utf-8")
        return 124, "FAIL (setup timed out)"
    except OSError as exc:
        setup_log.write_text(str(exc), encoding="utf-8")
        return 1, f"FAIL (could not run setup script: {exc})"

    output = (completed.stdout or "") + (completed.stderr or "")
    _record_process(setup_log.with_suffix(".process.json"), completed)
    setup_log.write_text(output, encoding="utf-8")
    if output:
        sys.stdout.write(output)
    if completed.returncode != 0:
        return completed.returncode, f"FAIL (setup exit {completed.returncode})"
    return 0, "PASS"


@measured("validation_s")
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
        arguments = [str(arg) for arg in validation.get("args", [])]
        if validation.get("require_log_session"):
            arguments.extend(["--log-session-id", log_session_id])
        workspace = workspace_enabled(scenario, use_docker=use_docker)
        if workspace:
            wrapped = command_arguments(_attempt.get(), str(validator), arguments)
            validator_cmd[-1:] = [_to_container_path(Path(wrapped[0])), *wrapped[1:]]
            console_log.with_suffix(".validation.launch.json").write_text(json.dumps({
                "attempt_id": _attempt.get(), "command": validator_cmd,
                "status": "launch_requested", "container": spark_container,
            }, indent=2), encoding="utf-8")
        else:
            validator_cmd.extend(arguments)
        try:
            completed = run_owned(
                validator_cmd,
                cwd=str(DATACOOLIE_ROOT),
                timeout=int(validation.get("timeout_seconds", 60)),
            )
        except subprocess.TimeoutExpired as exc:
            _record_process(console_log.with_suffix(".validation.process.json"), exc)
            return 1, "FAIL (validation script timed out)"
        except OSError as exc:
            return 1, f"FAIL (could not run validation script: {exc})"
        validator_output = (completed.stdout or "") + (completed.stderr or "")
        _record_process(console_log.with_suffix(".validation.process.json"), completed)
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
        if workspace:
            export = SCENARIO_LOG_DIR / "native" / _attempt.get()
            try:
                manifest = verify_export(export, _attempt.get(), revision=worktree_fingerprint(USECASE_SIM_DIR))
            except (OSError, ValueError) as exc:
                return 1, f"FAIL (workspace export: {exc})"
            scenario["_workspace_export"] = {
                "attempt_id": _attempt.get(), "manifest_path": str(export / "manifest.json"),
                "runtime_root": manifest["runtime_root"], "archive_path": str(export / "runtime.zip"),
                "member_prefix": manifest["member_prefix"],
                "files": len(manifest["files"]), "complete": True,
            }

    if expected_exit_code:
        return 0, f"PASS (expected exit {expected_exit_code})"
    return 0, "PASS"


# ---------------------------------------------------------------------------
# Subprocess tee runner
# ---------------------------------------------------------------------------
@measured("invocation_wall_s")
def _run_with_tee(cmd: list[str], console_log: Path, timeout: int) -> tuple[int, str]:
    """Stream an owned child tree and confirm teardown before returning."""
    with console_log.open("w", encoding="utf-8") as log_fh:
        def emit(line):
            sys.stdout.write(line)
            sys.stdout.flush()
            log_fh.write(line)
        try:
            completed = run_owned(cmd, cwd=str(DATACOOLIE_ROOT), timeout=timeout,
                                  grace=GRACEFUL_SHUTDOWN_SECS, on_line=emit)
        except subprocess.TimeoutExpired as exc:
            _record_process(console_log.with_suffix(".process.json"), exc)
            return 124, f"FAIL (timeout after {timeout}s)"
        except InterruptedError:
            return 130, "FAIL (suite cancelled)"
    rc = completed.returncode
    _record_process(console_log.with_suffix(".process.json"), completed)
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


def _validation_command(scenario: dict) -> list[str]:
    """Render the declared validator command without executing it."""

    validation = scenario.get("validation") or {}
    script = validation.get("script")
    if not script:
        return []
    validator_path = (DATACOOLIE_ROOT / str(script)).resolve()
    return [
        sys.executable,
        str(validator_path),
        *(str(arg) for arg in validation.get("args", [])),
    ]


def _setup_command(scenario: dict) -> list[str]:
    """Render the declared setup command for a qualification receipt."""

    setup = scenario.get("setup") or {}
    script = setup.get("script")
    if not script:
        return []
    script_path = (DATACOOLIE_ROOT / str(script)).resolve()
    return [
        sys.executable,
        str(script_path),
        *(str(arg) for arg in setup.get("args", [])),
    ]


def _write_qualification_receipt(
    name: str,
    scenario: dict,
    *,
    invocation_receipts: list[dict] | None = None,
    final_exit_code: int,
    final_status: str,
    validation_scenario: dict | None = None,
    setup_command: list[str] | None = None,
    runtime_verified: bool = True,
    execution_mode: str = "host",
    container: str | None = None,
) -> None:
    """Persist an explicit current-worktree qualification identity.

    The older ``.invocations.json`` format remains untouched for existing
    tooling.  This final receipt is written only after validation so an exit
    code alone cannot be mistaken for qualification evidence.
    """

    invocations = invocation_receipts or []
    commands = [
        item.get("command")
        for item in invocations
        if isinstance(item, dict) and item.get("command")
    ]
    command: object = commands[0] if len(commands) == 1 else commands
    if not command:
        command = setup_command or _validation_command(validation_scenario or scenario) or ["scenario", name]
    status = (
        "passed"
        if final_status.startswith("PASS") and final_exit_code == 0
        else "blocked"
        if "blocked" in final_status.lower() or final_exit_code == 124
        else "failed"
    )
    if not runtime_verified:
        status = "blocked"
    runtime = runtime_identity()
    if (not runtime_verified and final_status.startswith("PASS") and final_exit_code == 0
            and execution_mode == "docker" and container
            and scenario.get("_executed_maintenance_runtime")
            and (scenario.get("setup") or {}).get("script") ==
            "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"):
        try:
            runtime = _maintenance_container_runtime(str(container))
            before_runtime = scenario["_executed_maintenance_runtime"]
            if {key: value for key, value in runtime.items() if key != "verified_at"} != {
                    key: value for key, value in before_runtime.items() if key != "verified_at"}:
                raise ValueError("REST maintenance container changed during execution")
            runtime["execution_verified_at"] = before_runtime["verified_at"]
            runtime_verified = True
            status = "passed"
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            logger.error("REST maintenance runtime receipt remains unverified: %s", exc)
    payload = {
        "schema_version": 1,
        "attempt_id": _attempt.get(),
        "scenario": name,
        "revision": worktree_fingerprint(USECASE_SIM_DIR),
        "profile": {
            "engine": scenario.get("engine"),
            "platform": scenario.get("platform", "local"),
            "execution_mode": execution_mode,
            "container": container,
        },
        "runtime": runtime,
        "runtime_verified": runtime_verified,
        "execution_mode": execution_mode,
        "container": container,
        "command": command,
        "commands": commands,
        "invocation_outcomes": invocations,
        "validation_command": _validation_command(validation_scenario or scenario),
        "validation_execution": _process_record((SCENARIO_LOG_DIR / f"{name}.console.log").with_suffix(".validation.process.json")),
        "setup_execution": _process_record(SCENARIO_LOG_DIR / f"{name}.setup.process.json"),
        "prerequisite_execution": _process_record(SCENARIO_LOG_DIR / f"{name}.prerequisite.process.json"),
        "actual_exit_code": final_exit_code,
        "expected_exit_code": 0,
        "status": status,
        "result": final_status,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    from runner.metadata_sources import scenario_metadata_provenance

    payload["metadata_provenance"] = scenario_metadata_provenance(scenario)
    validation_source = validation_scenario or scenario
    if workspace_enabled(validation_source, use_docker=execution_mode == "docker"):
        execution = payload["validation_execution"]
        launch = _process_record((SCENARIO_LOG_DIR / f"{name}.console.log").with_suffix(".validation.launch.json"))
        if launch:
            payload["validation_launch"] = launch
        command_evidence = execution or launch
        if command_evidence:
            payload["validation_command"] = command_evidence["command"]
            if not commands:
                payload["command"] = command_evidence["command"]
        export = validation_source.get("_workspace_export")
        if export and export.get("attempt_id") == _attempt.get():
            payload["runtime_workspace"] = export
    if scenario.get("skip_pipeline") and payload["validation_execution"]:
        # A current-attempt process sidecar guards against stale console markers
        # when setup/preflight failed before launching this standalone validator.
        console = SCENARIO_LOG_DIR / f"{name}.console.log"
        payload["validation_runtime_timing"] = read_runtime_timing(console)
        payload["validation_runtime_profile"] = read_runtime_timing(console, "SIM_RUNTIME_PROFILE=")
        metadata = read_runtime_timing(console, "SIM_METADATA_PROVENANCE=")
        if metadata:
            payload["metadata_provenance"] = metadata["invocations"]
    receipt_path = SCENARIO_LOG_DIR / f"{name}.receipt.json"
    receipt_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _maintenance_container_runtime(container: str) -> dict:
    """Read the executed container profile without creating a Spark session."""
    probe = subprocess.run([
        "docker", "exec", container, "python3", "-c",
        "import importlib.metadata,json,platform;print(json.dumps({"
        "'python':platform.python_version(),'platform':platform.system(),"
        "'packages':{p:importlib.metadata.version(p) for p in "
        "('datacoolie','pyspark','pyiceberg')}}))",
    ], capture_output=True, text=True, timeout=15, check=True)
    runtime = json.loads(probe.stdout)
    if runtime.get("packages", {}).get("pyspark") != "3.5.9":
        raise ValueError("executed REST maintenance container is not Spark 3.5.9")
    inspected = subprocess.run([
        "docker", "inspect", "--format", "{{.Id}} {{.Image}}", container,
    ], capture_output=True, text=True, timeout=15, check=True)
    runtime["container_id"], runtime["container_image"] = inspected.stdout.strip().split()
    runtime["verified_at"] = datetime.now(timezone.utc).isoformat()
    return runtime


def _write_bundle_receipt(
    bundle_name: str,
    leaf_names: list[str],
    results: dict[str, int],
    revision: str,
    *,
    expected_leaf_names: list[str] | None = None,
) -> None:
    """Write the profile-neutral aggregate receipt for a bundle dispatch.

    ``results`` contains process exit codes, but a zero exit code is not
    sufficient for qualification: a Docker run can be explicitly blocked
    because its runtime profile was not verified.  The aggregate therefore
    derives each step status from its current leaf receipt and keeps the leaf
    attempt ID as an exact provenance reference.
    """

    steps: list[dict[str, object]] = []
    for leaf in leaf_names:
        receipt = SCENARIO_LOG_DIR / f"{leaf}.receipt.json"
        try:
            receipt_ref = receipt.resolve().relative_to(USECASE_SIM_DIR.resolve()).as_posix()
        except ValueError:
            receipt_ref = str(receipt)
        leaf_payload: dict[str, object] | None = None
        try:
            candidate = json.loads(receipt.read_text(encoding="utf-8"))
            if isinstance(candidate, dict):
                leaf_payload = candidate
        except (OSError, UnicodeError, json.JSONDecodeError):
            leaf_payload = None
        result = int(results.get(leaf, 1))
        attempt_id = leaf_payload.get("attempt_id") if leaf_payload else None
        raw_status = leaf_payload.get("status") if leaf_payload else None
        runtime_verified = leaf_payload.get("runtime_verified", True) if leaf_payload else False
        current = bool(
            leaf_payload
            and leaf_payload.get("revision") == revision
            and isinstance(attempt_id, str)
            and bool(attempt_id)
            and raw_status in {"passed", "failed", "blocked"}
        )
        if not current or runtime_verified is False or raw_status == "blocked":
            step_status = "blocked"
        elif raw_status == "failed" or result != 0:
            step_status = "failed"
        elif raw_status == "passed":
            step_status = "passed"
        else:
            step_status = "blocked"
        steps.append(
            {
                "ref": leaf,
                "receipt": receipt_ref,
                "result": result,
                "attempt_id": attempt_id,
                "status": step_status,
            }
        )
    expected = set(expected_leaf_names) if expected_leaf_names is not None else set(leaf_names)
    complete = len(leaf_names) == len(expected) and set(leaf_names) == expected
    statuses = [str(item["status"]) for item in steps]
    if not complete:
        status = "blocked"
        final_status = "BLOCKED (partial bundle selection)"
    elif not statuses or "blocked" in statuses:
        status = "blocked"
        final_status = "BLOCKED (bundle step)"
    elif "failed" in statuses:
        status = "failed"
        final_status = "FAIL (bundle step)"
    else:
        status = "passed"
        final_status = "PASS"
    payload = {
        "schema_version": 2,
        "scenario": bundle_name,
        "kind": "bundle",
        "revision": revision,
        "status": status,
        "final_status": final_status,
        "complete": complete,
        "steps": steps,
        "results": {item["ref"]: item["result"] for item in steps},
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    SCENARIO_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (SCENARIO_LOG_DIR / f"{bundle_name}.receipt.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def _finalize_suite_artifacts(
    *,
    bundle_dispatch: dict[str, list[str]],
    scenarios: dict,
    names: list[str],
    requested_names: list[str],
    results: dict[str, int],
    revision: str,
    suite_id: str,
    suite_started: float,
) -> None:
    """Write rolling completion artifacts and retain their evidence closure."""

    for bundle_name, leaves in bundle_dispatch.items():
        _write_bundle_receipt(
            bundle_name,
            leaves,
            results,
            revision,
            expected_leaf_names=scenario_leaf_names(scenarios, bundle_name),
        )
    total_seconds = time.perf_counter() - suite_started
    revision_after = worktree_fingerprint(USECASE_SIM_DIR)
    (SCENARIO_LOG_DIR / "suite.timing.json").write_text(json.dumps({
        "suite_id": suite_id,
        "total_s": total_seconds,
        "requested": requested_names, "selected": names, "results": results,
        "revision_before": revision, "revision_after": revision_after,
    }, indent=2) + "\n", encoding="utf-8")
    try:
        archive = finalize_suite_archive(
            scenario_log_dir=SCENARIO_LOG_DIR,
            selected_names=names,
            bundle_names=list(bundle_dispatch),
            requested_names=requested_names,
            results=results,
            revision_before=revision,
            revision_after=revision_after,
            total_seconds=total_seconds,
            suite_id=suite_id,
        )
        logger.info("Suite evidence archive: %s", archive.archive_dir)
    except SuiteArchiveError as archive_exc:
        logger.error("Suite evidence archive rejected: %s", archive_exc)


def _run_scenario(name, scenarios, spark_container_status, last_spark_finish=0.0):
    results = {}
    if name not in scenarios:
        logger.error("Unknown scenario: %s", name)
        results[name] = 1
        return results[name], last_spark_finish

    try:
        scenario = _fresh_maintenance_scenario(scenarios[name])
        if scenario.get("maintenance_guard_manifest"):
            from runner.native_guard import resolve_scenario

            scenario = resolve_scenario(scenario)
    except ValueError as exc:
        logger.error("Scenario %s: %s", name, exc)
        results[name] = 1
        return results[name], last_spark_finish
    spark_container = _spark_container(scenario)
    use_docker = (
        _is_spark(scenario)
        and spark_container_status.get(spark_container, False)
    )
    prerequisite_rc, prerequisite_status = _prepare_native_guard(
        name, scenario, use_docker=use_docker, container=spark_container)
    if prerequisite_rc:
        prerequisite_log = SCENARIO_LOG_DIR / f"{name}.prerequisite.log"
        output = prerequisite_log.read_text(encoding="utf-8") if prerequisite_log.is_file() else ""
        (SCENARIO_LOG_DIR / f"{name}.console.log").write_text(
            f"attempt_id={_attempt.get()}\n{prerequisite_status}\n{output}", encoding="utf-8")
        _write_qualification_receipt(
            name, scenario, final_exit_code=prerequisite_rc, final_status=prerequisite_status,
            runtime_verified=False, execution_mode="docker" if use_docker else "host",
            container=spark_container if use_docker else None)
        logger.error("  Result: %s", prerequisite_status)
        return prerequisite_rc, last_spark_finish
    if (use_docker and (scenario.get("setup") or {}).get("script") ==
            "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"):
        scenario["_executed_maintenance_runtime"] = _maintenance_container_runtime(spark_container)

    if _is_spark(scenario) and not use_docker:
        _spark_cooldown(last_spark_finish)

    if not workspace_enabled(scenario, use_docker=use_docker):
        _pre_clean_paths(scenario)
        _pre_clean_job_logs(scenario)

    setup_rc, setup_status = _run_scenario_setup(name, scenario)
    if setup_rc != 0:
        results[name] = setup_rc
        _write_qualification_receipt(
            name,
            scenario,
            final_exit_code=setup_rc,
            final_status=setup_status,
            setup_command=_setup_command(scenario),
            runtime_verified=not use_docker,
            execution_mode="docker" if use_docker else "host",
            container=spark_container if use_docker else None,
        )
        logger.info("  Result: %s", setup_status)
        return results[name], last_spark_finish

    # A small registry-owned escape hatch is used for qualification
    # scripts that construct their own fresh fixture and execute a real
    # Driver.  It keeps those scripts out of the ordinary metadata/dataflow
    # child path while retaining scenario cleanup, console capture, and
    # validation receipts.
    if scenario.get("skip_pipeline"):
        combined_console = SCENARIO_LOG_DIR / f"{name}.console.log"
        combined_console.write_text("", encoding="utf-8")
        logger.info("▸ Running standalone validation: %s", name)
        validation_for_receipt = scenario
        try:
            validation_scenario = _fresh_standalone_replay_scenario(
                scenario, use_docker=use_docker
            )
        except ValueError as exc:
            final_rc, status = 1, f"FAIL (fresh root: {exc})"
        else:
            validation_for_receipt = validation_scenario
            final_rc, status = _validate_scenario_result(
                validation_scenario,
                0,
                combined_console,
                use_docker=use_docker,
                spark_container=spark_container,
            )
        results[name] = final_rc
        _write_qualification_receipt(
            name,
            scenario,
            final_exit_code=final_rc,
            final_status=status,
            validation_scenario=validation_for_receipt,
            setup_command=_setup_command(scenario),
            runtime_verified=not use_docker,
            execution_mode="docker" if use_docker else "host",
            container=spark_container if use_docker else None,
        )
        logger.info("  Result: %s", status)
        return results[name], last_spark_finish

    try:
        invocations = _scenario_invocations(scenario)
    except ValueError as exc:
        logger.error("Scenario %s: %s", name, exc)
        results[name] = 1
        _write_qualification_receipt(
            name,
            scenario,
            final_exit_code=1,
            final_status=f"FAIL (invocations: {exc})",
            setup_command=_setup_command(scenario),
            runtime_verified=not use_docker,
            execution_mode="docker" if use_docker else "host",
            container=spark_container if use_docker else None,
        )
        return results[name], last_spark_finish

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
                "runtime_timing": read_runtime_timing(console_log),
                "runtime_profile": read_runtime_timing(console_log, "SIM_RUNTIME_PROFILE="),
                "execution": _process_record(console_log.with_suffix(".process.json")),
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

        if invocation.get("validation"):
            step = {"validation": dict(invocation["validation"])}
            step["validation"].setdefault("expected_exit_code", expected)
            step_rc, step_status = _validate_scenario_result(
                step, rc, console_log, use_docker=use_docker,
                spark_container=spark_container, log_session_id=log_session_id,
            )
            invocation_receipts[-1]["validation_command"] = _validation_command(step)
            invocation_receipts[-1]["validation_status"] = step_status
            invocation_receipts[-1]["validation_execution"] = _process_record(console_log.with_suffix(".validation.process.json"))
            if step_rc:
                results[name] = step_rc
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
        _write_qualification_receipt(
            name,
            scenario,
            invocation_receipts=invocation_receipts,
            final_exit_code=results.get(name, 1),
            final_status="FAIL (invocation)",
            setup_command=_setup_command(scenario),
            runtime_verified=not use_docker,
            execution_mode="docker" if use_docker else "host",
            container=spark_container if use_docker else None,
        )
        return results[name], last_spark_finish

    final_rc, status = _validate_scenario_result(
        scenario,
        0,
        combined_console,
        use_docker=False,
        spark_container=spark_container,
        log_session_id=log_session_id,
    )
    results[name] = final_rc
    _write_qualification_receipt(
        name,
        scenario,
        invocation_receipts=invocation_receipts,
        final_exit_code=final_rc,
        final_status=status,
        setup_command=_setup_command(scenario),
        runtime_verified=not use_docker,
        execution_mode="docker" if use_docker else "host",
        container=spark_container if use_docker else None,
    )
    logger.info("  Result: %s", status)

    return results[name], last_spark_finish


def run_scenarios(names: list[str], scenarios: dict) -> int:
    CANCEL.clear()
    requested_names = list(names)
    try:
        leaf_scenarios = scenario_leaf_views(scenarios)
    except CatalogError as exc:
        logger.error("Invalid scenario registry: %s", exc)
        return 1

    # Canonical bundle names are accepted by --scenario/--all.  Catalog group
    # selection normally supplies qualified leaves directly.  Expand only
    # after validating the complete request so an unknown step cannot start a
    # service or create a partial receipt set.
    bundle_dispatch: dict[str, list[str]] = {}
    bundle_leaf_owners: dict[str, str] = {}
    for bundle_name, bundle in scenarios.items():
        if not is_scenario_bundle(bundle):
            continue
        for leaf in scenario_leaf_names(scenarios, bundle_name):
            bundle_leaf_owners[leaf] = bundle_name
    expanded: list[str] = []
    unknown: list[str] = []
    for name in requested_names:
        if name in scenarios and is_scenario_bundle(scenarios[name]):
            leaves = scenario_leaf_names(scenarios, name)
            bundle_dispatch.setdefault(name, []).extend(leaves)
            expanded.extend(leaves)
        elif name in leaf_scenarios:
            expanded.append(name)
            owner = bundle_leaf_owners.get(name)
            if owner is not None:
                # Group and priority selection return qualified leaves. Keep
                # their bundle membership so ordered execution and the
                # profile-neutral aggregate receipt apply on every dispatch
                # route, not only ``--scenario <bundle>``.
                bundle_dispatch.setdefault(owner, []).append(name)
        else:
            unknown.append(name)
    if unknown:
        for name in unknown:
            logger.error("Unknown scenario: %s", name)
        return 1
    names = list(dict.fromkeys(expanded))
    if not names:
        logger.error("No executable scenarios selected")
        return 1
    for bundle_name, leaves in list(bundle_dispatch.items()):
        selected = set(leaves)
        declared_order = scenario_leaf_names(scenarios, bundle_name)
        bundle_dispatch[bundle_name] = [
            leaf for leaf in declared_order if leaf in selected
        ]

    # ``admission.execute`` uses the pending list order for ready work.  Keep
    # selected bundle leaves in their declared order even when a direct caller
    # supplies qualified refs in a different order; ordinary scenarios keep
    # their existing slots so they can still overlap when declarations allow.
    for leaves in bundle_dispatch.values():
        order = iter(leaves)
        names = [next(order) if name in leaves else name for name in names]

    # Validate the complete dispatch set before creating log directories,
    # starting services, or cleaning any scenario state.  Group selection
    # already performs this check, but keeping the guard here protects direct
    # callers of the runner as well.
    try:
        dependencies = scenario_dependencies(USECASE_SIM_DIR, scenarios)
        # Bundle steps are deliberately independent probes.  A failed error
        # message probe must not block later probes in the same bundle; a
        # stateful failure/recovery chain remains an ordinary owning scenario.
        for leaves in bundle_dispatch.values():
            for leaf in leaves:
                dependencies[leaf] = set()
        bundle_leaves = {leaf for leaves in bundle_dispatch.values() for leaf in leaves}
        declarations = {
            # Bundle probes are intentionally independent, but an unknown
            # declaration makes each one exclusive.  This preserves their
            # relative pending-list order without turning a failed probe into
            # a prerequisite that blocks later probes.
            name: None if name in bundle_leaves else declaration(leaf_scenarios[name], DATACOOLIE_ROOT)
            for name in names
        }
    except ValueError as exc:
        logger.error("Invalid dispatch admission: %s", exc)
        return 1
    suite_started = time.perf_counter()
    _setup_log_dirs()
    writer_lock = acquire_suite_writer_lock(SCENARIO_LOG_DIR / "suites")
    try:
        attempts = {name: uuid.uuid4().hex for name in names}
        suite_id = uuid.uuid4().hex
        revision = worktree_fingerprint(USECASE_SIM_DIR)
        results: dict[str, int] = {}
        for name in names:
            # Invalidate an earlier PASS before any cleanup/setup can fail. Pending
            # cases also remain visibly blocked if the suite aborts mid-dispatch.
            (SCENARIO_LOG_DIR / f"{name}.receipt.json").write_text(json.dumps({
                "schema_version": 1, "scenario": name, "attempt_id": attempts[name],
                "revision": revision, "status": "blocked", "runtime_verified": False,
                "final_status": "BLOCKED (execution in progress)",
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }, indent=2) + "\n", encoding="utf-8")
    except BaseException:
        writer_lock.release()
        raise

    def abort(exc):
        try:
            return _abort(exc)
        finally:
            writer_lock.release()

    def _abort(exc):
        logger.error("Suite stopped: %s: %s", type(exc).__name__, exc)
        for name in names:
            path = SCENARIO_LOG_DIR / f"{name}.receipt.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("attempt_id") == attempts[name] and payload.get("final_status") == "BLOCKED (execution in progress)":
                payload["final_status"] = f"BLOCKED (suite aborted: {exc})"
                path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        (SCENARIO_LOG_DIR / "suite.timing.json").write_text(json.dumps({
            "suite_id": suite_id,
            "total_s": time.perf_counter() - suite_started, "selected": names,
            "aborted": True, "error": str(exc), "revision": revision,
        }, indent=2) + "\n", encoding="utf-8")
        for bundle_name, leaves in bundle_dispatch.items():
            _write_bundle_receipt(
                bundle_name,
                leaves,
                results,
                revision,
                expected_leaf_names=scenario_leaf_names(scenarios, bundle_name),
            )
        try:
            archive = finalize_suite_archive(
                scenario_log_dir=SCENARIO_LOG_DIR,
                selected_names=names,
                bundle_names=list(bundle_dispatch),
                requested_names=requested_names,
                results=results,
                revision_before=revision,
                revision_after=worktree_fingerprint(USECASE_SIM_DIR),
                total_seconds=time.perf_counter() - suite_started,
                aborted=True,
                error=str(exc),
                suite_id=suite_id,
            )
            logger.info("Suite evidence archive: %s", archive.archive_dir)
        except SuiteArchiveError as archive_exc:
            logger.error("Suite evidence archive rejected: %s", archive_exc)
        return 130 if isinstance(exc, KeyboardInterrupt) else 1
    last_spark_finish = 0.0

    try:
        # Check each selected Spark container independently. This lets the opt-in
        # Spark 4.x gate coexist with the pinned Spark 3.5 service and fall back to
        # host PySpark only for a missing profile.
        spark_container_status: dict[str, bool] = {}
        for scenario in (leaf_scenarios.get(n, {}) for n in names):
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
            s = leaf_scenarios.get(n, {})
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

    except (Exception, KeyboardInterrupt) as exc:
        return abort(exc)

    def run(name):
        nonlocal last_spark_finish
        token = _attempt.set(attempts[name])
        with scope() as timing:
            try:
                rc, finished = _run_scenario(name, leaf_scenarios, spark_container_status, last_spark_finish)
                if finished:
                    last_spark_finish = max(last_spark_finish, finished)
            except BaseException as exc:
                status = "BLOCKED" if isinstance(exc, (TeardownError, KeyboardInterrupt)) else "FAIL"
                container = _spark_container(leaf_scenarios[name])
                docker = _is_spark(leaf_scenarios[name]) and spark_container_status.get(container, False)
                _write_qualification_receipt(name, leaf_scenarios[name], final_exit_code=1,
                                             final_status=f"{status} ({type(exc).__name__}: {exc})",
                                             runtime_verified=not docker,
                                             execution_mode="docker" if docker else "host",
                                             container=container if docker else None)
                raise
            finally:
                receipt = SCENARIO_LOG_DIR / f"{name}.receipt.json"
                payload = json.loads(receipt.read_text(encoding="utf-8"))
                if payload.get("attempt_id") != attempts[name]:
                    _attempt.reset(token)
                    raise TeardownError(f"Receipt attempt changed for {name}")
                if payload.get("final_status") == "BLOCKED (execution in progress)":
                    _write_qualification_receipt(name, leaf_scenarios[name], final_exit_code=1,
                                                 final_status="FAIL (scenario preparation failed)")
                    payload = json.loads(receipt.read_text(encoding="utf-8"))
                payload["timing"] = timing.snapshot()
                payload.setdefault("profile", {})["optimization_mode"] = os.environ.get(MODE_ENV, "sequential")
                receipt.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
                _attempt.reset(token)
        logger.info("  Timing [%s]: %s", name, timing.snapshot())
        return rc

    def blocked(name, reason):
        token = _attempt.set(attempts[name])
        try:
            _write_qualification_receipt(name, leaf_scenarios[name], final_exit_code=1,
                                         final_status=f"BLOCKED ({reason})")
        finally:
            _attempt.reset(token)
        logger.error("Scenario %s blocked: %s", name, reason)
        return 1

    try:
        results = execute(names, declarations, dependencies, run, blocked,
                          # Error-message leaves are exclusive admission units
                          # and stay in declared order; ordinary declared
                          # scenarios retain optimized cross-scenario overlap.
                          parallel=os.environ.get(MODE_ENV) == "optimized",
                          cancel=CANCEL.set)
    except (Exception, KeyboardInterrupt) as exc:
        return abort(exc)
    try:
        _finalize_suite_artifacts(
            bundle_dispatch=bundle_dispatch,
            scenarios=scenarios,
            names=names,
            requested_names=requested_names,
            results=results,
            revision=revision,
            suite_id=suite_id,
            suite_started=suite_started,
        )
    finally:
        writer_lock.release()
    return _print_summary(results)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _select_names(scenarios: dict, args: argparse.Namespace) -> list[str]:
    if args.scenario:
        return [args.scenario]
    if args.group:
        return select_group_scenarios(USECASE_SIM_DIR, args.group, scenarios)
    if args.all:
        return list(scenarios.keys())
    names: list[str] = []
    for name, scenario in scenarios.items():
        if is_scenario_bundle(scenario):
            names.extend(
                leaf
                for leaf in scenario_leaf_names(scenarios, name)
                if scenario_leaf_views(scenarios)[leaf].get("priority") == args.priority
            )
        elif scenario.get("priority") == args.priority:
            names.append(name)
    if not names:
        logger.error("No scenarios with priority %s", args.priority)
        sys.exit(1)
    return names


def main() -> None:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description="Run named scenarios")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--scenario", help="Name of a single scenario to run")
    group.add_argument("--all", action="store_true", help="Run all scenarios")
    group.add_argument(
        "--priority", help="Run all scenarios with this priority (P0, P1, P2)"
    )
    group.add_argument("--group", help="Run the group's dedicated scenarios")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List catalog groups and cases without dispatching scenarios",
    )
    parser.add_argument(
        "--execution-mode",
        choices=("sequential", "optimized"),
        default="optimized",
        help="Execution mode (default: optimized; use sequential for serial dispatch)",
    )
    parser.add_argument(
        "--scenarios-path", default=str(SCENARIOS_PATH), help="Path to scenarios.json"
    )
    args = parser.parse_args()

    scenarios = load_scenarios(Path(args.scenarios_path))
    if args.list:
        rows = list_groups(USECASE_SIM_DIR)
        if args.group:
            rows = [row for row in rows if row["group"] == args.group]
            if not rows:
                logger.error("Unknown group: %s", args.group)
                sys.exit(1)
        for row in rows:
            marker = "runnable" if row["runnable"] else "empty"
            print(f"{row['group']} ({marker})")
            print(f"  catalog cases: {len(row['case_ids'])}")
            if not row["runnable"]:
                print("  execution scenarios: 0")
                continue
            try:
                execution_names = select_group_scenarios(
                    USECASE_SIM_DIR, row["group"], scenarios
                )
            except CatalogError as exc:
                logger.error("%s", exc)
                if args.group:
                    sys.exit(1)
                print(f"  execution selection error: {exc}")
                continue
            print(f"  execution scenarios ({len(execution_names)}):")
            for index, name in enumerate(execution_names, start=1):
                print(f"    {index}. {name}")
        return
    if not any((args.scenario, args.all, args.priority, args.group)):
        parser.error("one of --scenario, --all, --priority, or --group is required")
    try:
        names = _select_names(scenarios, args)
    except CatalogError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    os.environ[MODE_ENV] = args.execution_mode
    sys.exit(run_scenarios(names, scenarios))


if __name__ == "__main__":
    main()
