"""Start or stop the usecase-sim docker-compose stack.

Usage:
    python usecase-sim/scripts/bootstrap/setup_platform.py              # up -d, wait for health
    python usecase-sim/scripts/bootstrap/setup_platform.py --down       # compose down
    python usecase-sim/scripts/bootstrap/setup_platform.py --services postgres rustfs
"""
from __future__ import annotations

import os

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


from _common import COMPOSE_FILE, ensure_bucket, port_open, setup_logging  # noqa: E402
from runner.compose_config import compose_file_arguments, compose_startup_arguments  # noqa: E402
from runner.s3_config import get_s3_config  # noqa: E402

logger = setup_logging("setup_platform")

# service_name → (host, port) used for readiness probing
_SERVICE_PORTS: dict[str, tuple[str, int]] = {
    "postgres":     ("localhost", 5432),
    "mysql":        ("localhost", 3306),
    "mssql":        ("localhost", 1433),
    "oracle":       ("localhost", 1521),
    "rustfs":       ("localhost", 9000),
    "iceberg-rest": ("localhost", 8181),
    "trino":        ("localhost", 8080),
    "mock-api":     ("localhost", 8082),
    "metadata-api": ("localhost", 8000),
    "sqlpad":       ("localhost", 3000),
}


def _compose(*args: str) -> int:
    compose_files = compose_file_arguments(COMPOSE_FILE)
    docker = shutil.which("docker")
    if docker is None:
        logger.error("docker CLI not found on PATH")
        return 1
    if args and args[0] == "up":
        args = (args[0], *compose_startup_arguments(), *args[1:])
    cmd = [docker, "compose", *compose_files, *args]
    logger.info("$ %s", " ".join(cmd))
    return subprocess.call(cmd)


def _wait_for_ports(services: list[str], timeout: int) -> bool:
    deadline = time.monotonic() + timeout
    remaining = {s for s in services if s in _SERVICE_PORTS}
    while remaining and time.monotonic() < deadline:
        ready = {s for s in remaining if port_open(*_SERVICE_PORTS[s], timeout=1.0)}
        for s in sorted(ready):
            logger.info("%-15s ✓ ready", s)
        remaining -= ready
        if remaining:
            time.sleep(2)
    for s in sorted(remaining):
        logger.warning("%-15s ✗ not reachable within %ds", s, timeout)
    return not remaining


def _ensure_mssql_database(timeout: int = 60) -> bool:
    """Create the `datacoolie` user database on MSSQL if missing.

    The mcr.microsoft.com/mssql/server image has no env var equivalent to
    POSTGRES_DB / MYSQL_DATABASE, so the `datacoolie` DB must be created out
    of band. Without it, pymssql connections fail with error 18456 state 38
    ("Failed to open the explicitly specified database 'datacoolie'"), which
    is easy to mistake for an auth failure.
    """
    docker = shutil.which("docker")
    if docker is None:
        logger.warning("mssql: docker CLI not found; cannot verify datacoolie database")
        return False
    deadline = time.monotonic() + timeout
    cmd = [
        docker, "exec", "datacoolie-mssql",
        "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
        "-P", "Datacoolie@1", "-No", "-Q",
        "IF DB_ID('datacoolie') IS NULL CREATE DATABASE datacoolie;",
    ]
    last_err = b""
    while time.monotonic() < deadline:
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode == 0:
            logger.info("mssql           ✓ datacoolie database present")
            return True
        last_err = proc.stderr or proc.stdout
        time.sleep(2)
    logger.warning("mssql: could not ensure datacoolie database (%s)",
                   last_err.decode(errors="replace").strip()[:200])
    return False


def _ensure_rustfs_bucket() -> bool:
    """Create the simulator bucket before Iceberg or S3-backed writers run."""

    settings = get_s3_config()
    try:
        if ensure_bucket(bucket=settings.bucket):
            logger.info("rustfs          ✓ bucket %s present", settings.bucket)
            return True
        else:
            logger.warning("rustfs: bucket %s could not be verified", settings.bucket)
            return False
    except Exception as exc:  # pragma: no cover - depends on the local Docker daemon
        logger.warning("rustfs: could not ensure bucket %s (%s)", settings.bucket, exc)
        return False


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description="Start/stop usecase-sim docker stack")
    parser.add_argument("--down", action="store_true", help="Stop the stack")
    parser.add_argument("--volumes", action="store_true", help="With --down, also remove volumes")
    parser.add_argument("--services", nargs="+", default=None,
                        help="Subset of services (default: all)")
    parser.add_argument("--timeout", type=int, default=180,
                        help="Seconds to wait for service health and exposed ports")
    parser.add_argument("--no-wait", action="store_true", help="Skip readiness polling")
    args = parser.parse_args()

    if args.down:
        compose_args = ["down"]
        if args.volumes:
            compose_args.append("-v")
        if args.services:
            compose_args.extend(args.services)
        return _compose(*compose_args)

    up_args = ["up", "-d"]
    if not args.no_wait:
        # An open listener is not database readiness: Oracle can accept TCP
        # before its PDB/app user exists. Honor the Compose healthchecks first.
        up_args.extend(["--wait", "--wait-timeout", str(args.timeout)])
    if args.services:
        up_args.extend(args.services)
    rc = _compose(*up_args)
    if rc != 0:
        return rc

    if args.no_wait:
        return 0

    services = args.services or list(_SERVICE_PORTS.keys())
    ok = _wait_for_ports(services, args.timeout)
    bootstrap_ok = True
    if ok and any(service in services for service in ("rustfs", "iceberg-rest", "trino")):
        bootstrap_ok = _ensure_rustfs_bucket() and bootstrap_ok
    if "mssql" in services:
        bootstrap_ok = _ensure_mssql_database() and bootstrap_ok
    return 0 if ok and bootstrap_ok else 2


if __name__ == "__main__":
    sys.exit(main())
