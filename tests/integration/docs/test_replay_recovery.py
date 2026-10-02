"""Repeatability and failure-boundary checks for the public replay example.

These checks intentionally use a child process for hard-stop cases. The hooks
are test-local and terminate only that child; replay itself never treats the
saved watermark as replay progress.
"""

from __future__ import annotations

import json
from pathlib import Path
import os
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "incremental"
RECOVERY_RUNNER = ROOT / "docs" / "examples" / "files" / "operations" / "replay_recovery.py"


def _python_environment() -> dict[str, str]:
    environment = dict(os.environ)
    source_path = str(ROOT / "src")
    environment["PYTHONPATH"] = (
        source_path
        if not environment.get("PYTHONPATH")
        else source_path + os.pathsep + environment["PYTHONPATH"]
    )
    return environment


def _read_output(project: Path):
    import polars as pl

    files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert files
    return pl.concat([pl.read_parquet(path) for path in files])


def _read_watermark(runtime: Path) -> dict[str, object]:
    files = sorted((runtime / "watermarks").rglob("watermark_value.json"))
    assert len(files) == 1
    return json.loads(files[0].read_text(encoding="utf-8"))


def _enable_delta_merge_upsert(project: Path) -> None:
    """Turn the copied incremental fixture into a keyed Delta target."""
    connections_path = project / "metadata" / "connections.json"
    connections = json.loads(connections_path.read_text(encoding="utf-8"))
    output = next(item for item in connections["connections"] if item["name"] == "orders_output")
    output["connection_type"] = "lakehouse"
    output["format"] = "delta"
    connections_path.write_text(
        json.dumps(connections, indent=2) + "\n", encoding="utf-8"
    )

    dataflows_path = project / "metadata" / "dataflows" / "orders_incremental.json"
    dataflows = json.loads(dataflows_path.read_text(encoding="utf-8"))
    destination = dataflows["dataflows"][0]["destination"]
    destination["load_type"] = "merge_upsert"
    destination["merge_keys"] = ["order_id"]
    dataflows_path.write_text(
        json.dumps(dataflows, indent=2) + "\n", encoding="utf-8"
    )


def _recovery_command(
    project: Path,
    runtime: Path,
    *,
    job_id: str,
    step: int = 2,
) -> list[str]:
    return [
        sys.executable,
        str(RECOVERY_RUNNER),
        "--metadata-path",
        str(project / "metadata"),
        "--watermark-base-path",
        str(runtime / "watermarks"),
        "--log-base-path",
        str(runtime / "logs"),
        "--working-directory",
        str(project),
        "--start",
        "1",
        "--end",
        "4",
        "--chunk-interval-json",
        json.dumps({"step": step}),
        "--save-watermark",
        "--confirm-save-watermark",
        "--job-id",
        job_id,
    ]


@pytest.mark.integration
@pytest.mark.parametrize("stored", [-1, 2, 4, 9], ids=["before", "inside", "at-end", "after"])
@pytest.mark.parametrize("save", [False, True], ids=["save-off", "save-on"])
def test_recovery_runner_repeats_the_requested_range(tmp_path: Path, stored, save) -> None:
    """A second session reruns every requested chunk."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()
    first = subprocess.run(
        _recovery_command(project, runtime, job_id="recovery-first"),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    first_rows = _read_output(project)

    state_file, = (runtime / "watermarks").rglob("watermark_value.json")
    seeded = json.dumps({"updated_sequence": stored, "aux": "retained"}).encode()
    state_file.write_bytes(seeded)
    command = _recovery_command(project, runtime, job_id="recovery-second")
    if not save:
        command.remove("--save-watermark")
        command.remove("--confirm-save-watermark")

    second = subprocess.run(
        command,
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    second_rows = _read_output(project)
    assert sorted(first_rows["order_id"].to_list()) == [1, 2]
    assert second_rows.height == first_rows.height * 2
    assert sorted(second_rows["order_id"].to_list()) == [1, 1, 2, 2]
    if save:
        assert _read_watermark(runtime) == {"updated_sequence": max(stored, 2), "aux": "retained"}
    else:
        assert state_file.read_bytes() == seeded


@pytest.mark.integration
def test_ordinary_run_history_replay_then_run_keeps_production_max(tmp_path):
    """A new provider session continues ordinary ingestion after historical replay."""
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    ordinary = [sys.executable, str(ROOT / "docs/examples/files/runners/local/run.py"),
                "--metadata-base-path", str(project / "metadata"),
                "--working-directory", str(project), "--state-base-path", str(runtime)]

    def execute(command):
        result = subprocess.run(command, cwd=tmp_path, env=_python_environment(),
                                capture_output=True, text=True, check=False, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr

    execute(ordinary)
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 2]
    state_file, = (runtime / "watermarks").rglob("watermark_value.json")
    state_file.write_text(json.dumps({"updated_sequence": 2, "aux": "retained"}), encoding="utf-8")
    history = _recovery_command(project, runtime, job_id="historical")
    history[history.index("--end") + 1] = "2"
    execute(history)
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 1, 2]
    assert _read_watermark(runtime) == {"updated_sequence": 2, "aux": "retained"}
    source = project / "data/input/orders/orders.csv"
    with source.open("a", encoding="utf-8") as stream:
        stream.write("3,39.00,3\n")
    execute(ordinary)
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 1, 2, 3]
    assert _read_watermark(runtime) == {"updated_sequence": 3, "aux": "retained"}


@pytest.mark.integration
def test_failed_chunk_stops_later_chunks_and_rerun_reprocesses_all(
    tmp_path: Path,
) -> None:
    """A failed chunk stops later work; a later run starts from the range."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()

    child_code = f"""
import importlib.util
import json
from pathlib import Path
from datacoolie.orchestration.driver import DataCoolieDriver

spec = importlib.util.spec_from_file_location('replay_recovery', {str(RECOVERY_RUNNER)!r})
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

from datacoolie.sources.base import BaseSourceReader

reader_calls = 0
original_read = BaseSourceReader.read
def record_read(self, *args, **kwargs):
    global reader_calls
    reader_calls += 1
    return original_read(self, *args, **kwargs)
BaseSourceReader.read = record_read

calls = 0
original = DataCoolieDriver._run_single_pipeline
def fail_before_second_chunk(self, *args, **kwargs):
    global calls
    calls += 1
    if calls == 2:
        raise RuntimeError('synthetic failure before destination write')
    return original(self, *args, **kwargs)

DataCoolieDriver._run_single_pipeline = fail_before_second_chunk
summary = module.run_once(
    metadata_path={str(project / 'metadata')!r},
    watermark_base_path={str(runtime / 'watermarks')!r},
    log_base_path={str(runtime / 'logs')!r},
    working_directory={str(project)!r},
    start=1,
    end=4,
    chunk_interval={{'step': 1}},
    job_id='recovery-failed-chunk',
    save_watermark=True,
)
Path({str(runtime / 'failed_chunk_calls.json')!r}).write_text(
    json.dumps({{"calls": calls, "reader_calls": reader_calls}}), encoding='utf-8'
)
print(json.dumps(summary, sort_keys=True, default=str))
raise SystemExit(1 if summary['failed'] else 0)
    """
    failed = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert failed.returncode == 1, failed.stdout + failed.stderr
    call_record = json.loads((runtime / "failed_chunk_calls.json").read_text(encoding="utf-8"))
    assert call_record == {"calls": 2, "reader_calls": 1}
    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert len(output_files) == 1
    assert sorted(_read_output(project)["order_id"].to_list()) == [1]
    assert _read_watermark(runtime) == {"updated_sequence": 1}

    rerun = subprocess.run(
        _recovery_command(project, runtime, job_id="recovery-failed-rerun", step=1),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert rerun.returncode == 0, rerun.stdout + rerun.stderr
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 1, 2]
    assert _read_watermark(runtime) == {"updated_sequence": 2}


@pytest.mark.integration
def test_abrupt_exit_after_chunk_completion_does_not_skip_on_restart(
    tmp_path: Path,
) -> None:
    """A hard stop after a chunk still leaves the next run repeatable."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()

    child_code = f"""
import importlib.util
import os
from datacoolie.orchestration.driver import DataCoolieDriver

spec = importlib.util.spec_from_file_location('replay_recovery', {str(RECOVERY_RUNNER)!r})
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

def stop_after_chunk(self, result):
    os._exit(74)

DataCoolieDriver._record_replay_chunk_complete = stop_after_chunk
module.run_once(
    metadata_path={str(project / 'metadata')!r},
    watermark_base_path={str(runtime / 'watermarks')!r},
    log_base_path={str(runtime / 'logs')!r},
    working_directory={str(project)!r},
    start=1,
    end=4,
    chunk_interval={{'step': 1}},
    job_id='recovery-after-chunk',
    save_watermark=True,
)
"""
    interrupted = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert interrupted.returncode == 74, interrupted.stdout + interrupted.stderr
    assert sorted(_read_output(project)["order_id"].to_list()) == [1]
    assert _read_watermark(runtime) == {"updated_sequence": 1}

    rerun = subprocess.run(
        _recovery_command(project, runtime, job_id="recovery-after-chunk-rerun", step=1),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert rerun.returncode == 0, rerun.stdout + rerun.stderr
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 1, 2]
    assert _read_watermark(runtime) == {"updated_sequence": 2}


@pytest.mark.integration
def test_empty_tail_chunks_do_not_advance_observed_watermark(
    tmp_path: Path,
) -> None:
    """Empty chunks do not fabricate the requested range upper bound."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()

    first = subprocess.run(
        _recovery_command(project, runtime, job_id="recovery-empty-first", step=1),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 2]
    assert _read_watermark(runtime) == {"updated_sequence": 2}

    second = subprocess.run(
        _recovery_command(project, runtime, job_id="recovery-empty-second", step=1),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    assert sorted(_read_output(project)["order_id"].to_list()) == [1, 1, 2, 2]
    assert _read_watermark(runtime) == {"updated_sequence": 2}


@pytest.mark.integration
def test_interrupted_after_write_exposes_append_duplicate_risk(tmp_path: Path) -> None:
    """A hard stop after output commit can replay rows on the next run."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()

    # Use a temporary child so os._exit cannot terminate pytest.  The hook is
    # deliberately scoped to this test; it is not part of the public runner.
    child_code = f"""
import importlib.util
import os
from datacoolie.watermark.watermark_manager import WatermarkManager

spec = importlib.util.spec_from_file_location('replay_recovery', {str(RECOVERY_RUNNER)!r})
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

def stop_before_watermark_save(*args, **kwargs):
    os._exit(73)

WatermarkManager.save_watermark = stop_before_watermark_save
module.run_once(
    metadata_path={str(project / 'metadata')!r},
    watermark_base_path={str(runtime / 'watermarks')!r},
    log_base_path={str(runtime / 'logs')!r},
    working_directory={str(project)!r},
    start=1,
    end=4,
    chunk_interval={{'step': 4}},
    job_id='recovery-interrupted',
    save_watermark=True,
)
"""
    interrupted = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert interrupted.returncode == 73, interrupted.stdout + interrupted.stderr
    assert not list((runtime / "watermarks").rglob("watermark_value.json"))

    committed = _read_output(project)
    assert sorted(committed["order_id"].to_list()) == [1, 2]

    rerun = subprocess.run(
        _recovery_command(
            project,
            runtime,
            job_id="recovery-after-interrupt",
            step=4,
        ),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert rerun.returncode == 0, rerun.stdout + rerun.stderr
    replayed = _read_output(project)
    assert sorted(replayed["order_id"].to_list()) == [1, 1, 2, 2]
    assert list((runtime / "watermarks").rglob("watermark_value.json"))


@pytest.mark.integration
def test_keyed_delta_retry_reconciles_rows_after_the_same_gap(tmp_path: Path) -> None:
    """A keyed Delta target removes duplicate business rows on replay."""
    pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    project = tmp_path / PROJECT.name
    shutil.copytree(PROJECT, project)
    _enable_delta_merge_upsert(project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()

    child_code = f"""
import importlib.util
import os
from datacoolie.watermark.watermark_manager import WatermarkManager

spec = importlib.util.spec_from_file_location('replay_recovery', {str(RECOVERY_RUNNER)!r})
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

def stop_before_watermark_save(*args, **kwargs):
    os._exit(75)

WatermarkManager.save_watermark = stop_before_watermark_save
module.run_once(
    metadata_path={str(project / 'metadata')!r},
    watermark_base_path={str(runtime / 'watermarks')!r},
    log_base_path={str(runtime / 'logs')!r},
    working_directory={str(project)!r},
    start=1,
    end=4,
    chunk_interval={{'step': 4}},
    job_id='recovery-keyed-interrupted',
    save_watermark=True,
)
"""
    interrupted = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert interrupted.returncode == 75, interrupted.stdout + interrupted.stderr

    import polars as pl

    delta_path = project / "data" / "output" / "orders"
    first = pl.read_delta(delta_path)
    assert sorted(first["order_id"].to_list()) == [1, 2]
    assert not list((runtime / "watermarks").rglob("watermark_value.json"))

    rerun = subprocess.run(
        _recovery_command(
            project,
            runtime,
            job_id="recovery-keyed-rerun",
            step=4,
        ),
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert rerun.returncode == 0, rerun.stdout + rerun.stderr
    second = pl.read_delta(delta_path)
    assert sorted(second["order_id"].to_list()) == [1, 2]
    assert second.height == first.height == 2
    assert _read_watermark(runtime) == {"updated_sequence": 2}
