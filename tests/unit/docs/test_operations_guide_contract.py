"""Verify operational examples and the previously missing guide boundaries."""

from __future__ import annotations

import re
import runpy
import shlex
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datacoolie.orchestration.scheduling.parallel_executor import ExecutionResult


ROOT = Path(__file__).resolve().parents[3]
GUIDE = ROOT / "docs" / "guide" / "operations"


@pytest.mark.parametrize(
    ("selected_ids", "result", "raises", "ran"),
    [
        (["orders_bronze2silver"], ExecutionResult(total=1, succeeded=1), False, True),
        ([], ExecutionResult(), True, False),
        (["unexpected"], ExecutionResult(total=1, succeeded=1), True, False),
        (["orders_bronze2silver"], ExecutionResult(total=1, failed=1), True, True),
        (["orders_bronze2silver"], ExecutionResult(total=1, pending=1), True, True),
        (["orders_bronze2silver"], ExecutionResult(total=0), True, True),
        (["orders_bronze2silver"], ExecutionResult(total=1, skipped=1), False, True),
    ],
)
def test_documented_stage_guard_rejects_missing_work_and_terminal_failure(
    selected_ids: list[str], result: ExecutionResult, raises: bool, ran: bool
) -> None:
    text = (GUIDE / "run-stage.md").read_text(encoding="utf-8")
    snippet = re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)[0]
    driver = MagicMock()
    driver.__enter__.return_value = driver
    driver.load_dataflows.return_value = [
        SimpleNamespace(dataflow_id=value) for value in selected_ids
    ]
    driver.run.return_value = result
    namespace = {
        "DataCoolieDriver": MagicMock(return_value=driver),
        "engine": object(), "platform": object(), "metadata": object(),
    }

    if raises:
        with pytest.raises(RuntimeError):
            exec(compile(snippet, "run-stage.md", "exec"), namespace)
    else:
        exec(compile(snippet, "run-stage.md", "exec"), namespace)

    assert driver.run.called is ran
    # The policy permits a no-data skip but requires prose explaining review
    # of skips/output and why nonempty selection is not mandatory per shard.
    assert "Review skipped reasons" in text
    assert "shard may legitimately return `total == 0`" in text


def test_documented_maintenance_command_matches_canonical_parser(monkeypatch) -> None:
    text = (GUIDE / "maintenance.md").read_text(encoding="utf-8")
    block = re.findall(r"```powershell\n(.*?)```", text, flags=re.DOTALL)[0]
    command = " ".join(
        line.rstrip().removesuffix("`").strip()
        for line in block.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    argv = shlex.split(command)
    assert argv[:2] == ["python", "maintenance.py"]
    runner = ROOT / "docs/examples/files/runners/local/maintenance.py"
    namespace = runpy.run_path(str(runner), run_name="guide_parser_check")
    monkeypatch.setattr("sys.argv", [str(runner), *argv[2:]])
    args = namespace["parse_args"]()
    assert args.confirm_maintenance is True
    assert args.retention_hours == 168
    assert args.metadata_path and args.watermark_base_path and args.log_base_path
    assert "usecase-sim/" not in text
    assert "within one `run_maintenance()` invocation" in text
    assert "enforce_retention_duration=False" in text
    for cell in ("Polars / Delta", "Spark / Delta", "Polars / named Iceberg", "Spark / named Iceberg"):
        assert cell in text


def test_logging_guide_covers_session_identity_and_conditional_persistence() -> None:
    guide = (GUIDE / "logging.md").read_text(encoding="utf-8")
    reference = (ROOT / "docs/reference/concepts/logging.md").read_text(encoding="utf-8")
    for text in (guide, reference):
        assert "`log_session_id`" in text
        assert "`dataflow_run_id`" in text
        assert "RunConfig" in text
        assert "no second logging-session identifier" not in text
        assert "job file is always" not in text
    assert "`LogConfig.output_path`" in guide
    assert "best effort" in guide


def test_troubleshooting_routes_runtime_failures_without_simulator_commands() -> None:
    text = (GUIDE / "troubleshooting.md").read_text(encoding="utf-8")
    assert "usecase-sim/" not in text
    assert "watermark_range_start" in text
    assert "`log_session_id`" in text
    assert "destination_operation_details" in text
    assert "format: \"jsonl\"" in text
    assert "platforms/index.md" in text
