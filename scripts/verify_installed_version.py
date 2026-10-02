"""Check producer version contracts inside the release gate's installed-wheel venv."""

from __future__ import annotations

from importlib.metadata import version
import json
import logging
from pathlib import Path
import subprocess
import sys
import tempfile

import datacoolie
from datacoolie.core import (
    Connection,
    DataFlow,
    Destination,
    Source,
)
from datacoolie.core.models.runtime import DataFlowRuntimeInfo
from datacoolie.logging import ExecutionLogger, LogConfig, SystemLogger
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.project.schema import resolve_metadata_schema


def verify_installed_version(expected: str) -> None:
    """Exercise installed entry points and real local log persistence."""

    assert Path(datacoolie.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), (
        "Smoke check must load the installed wheel, not an editable checkout"
    )
    assert datacoolie.__version__ == version("datacoolie") == expected
    assert resolve_metadata_schema().framework_version == expected

    executables = Path(sys.executable).parent
    suffix = ".exe" if sys.platform == "win32" else ""
    invocations = [
        [sys.executable, "-m", "datacoolie"],
        [str(executables / f"dc{suffix}")],
        [str(executables / f"datacoolie{suffix}")],
    ]
    for invocation in invocations:
        result = subprocess.run(
            [*invocation, "--version"], capture_output=True, text=True, check=True,
        )
        assert result.stdout.strip() == expected, result.stdout
        for arguments, exit_code in (
            (["inspect", "capabilities"], 0),
            (["metadata", "convert"], 2),
        ):
            result = subprocess.run(
                [*invocation, "--format", "json", *arguments],
                capture_output=True, text=True, check=False,
            )
            assert result.returncode == exit_code, result.stderr or result.stdout
            payload = json.loads(result.stdout)
            assert payload["datacoolie_version"] == expected, payload
            assert payload["schema_version"] == 1, payload
            if exit_code == 0:
                assert payload["data"]["datacoolie_version"] == expected, payload

    with tempfile.TemporaryDirectory(prefix="datacoolie-version-logs-") as directory:
        platform = LocalPlatform(base_path=directory)
        connection = Connection(name="smoke", format="csv")
        dataflow = DataFlow(
            dataflow_id="version-smoke",
            source=Source(connection=connection, table="input"),
            destination=Destination(connection=connection, table="output"),
        )
        runtime = DataFlowRuntimeInfo(dataflow_id=dataflow.dataflow_id, status="succeeded")
        for mode in ("snapshot", "batch"):
            root = Path(directory) / mode
            system = SystemLogger(
                LogConfig(output_path=f"{mode}/system", persistence_mode=mode, flush_interval_seconds=0),
                platform,
            )
            execution = ExecutionLogger(
                LogConfig(output_path=f"{mode}/execution", persistence_mode=mode, flush_interval_seconds=0),
                platform,
            )
            try:
                system.activate()
                execution.activate()
                logging.getLogger("datacoolie.version_smoke").info("version smoke")
                execution.log(dataflow, runtime)
                execution.finish_job("succeeded")
            finally:
                try:
                    execution.close()
                finally:
                    system.close()
            records = [
                json.loads(line)
                for path in root.rglob("*.json")
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            assert {row["_type"] for row in records} == {
                "system_log", "job_run_log", "dataflow_run_log",
            }
            for row in records:
                assert row["datacoolie_version"] == expected, row
                assert row["log_schema_version"] == 4, row
                assert list(row)[:2] == ["log_schema_version", "_type"], row
    print(f"Installed package, CLI and persisted log versions match {expected}.")


if __name__ == "__main__":
    verify_installed_version(sys.argv[1])
