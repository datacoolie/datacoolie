"""Local execution checks for downloadable public examples."""

from __future__ import annotations

import ast
from io import BytesIO
import json
from pathlib import Path
import os
import re
import shutil
import subprocess
import sys
import zipfile
import importlib.util

import pytest

from docs.scripts._examples import is_excluded, iter_public_files


ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "artifact"
FUNCTION_PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "function"
INCREMENTAL_PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "incremental"
TRANSFORM_PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "transform"
PROVIDER_FIXTURE = ROOT / "docs" / "examples" / "files" / "configuration" / "provider_fixtures.py"
STANDALONE_PROVIDER = ROOT / "docs" / "examples" / "files" / "configuration" / "standalone_file_provider.py"
SQL_ROOTS_SAMPLE = ROOT / "docs" / "examples" / "files" / "configuration" / "sql_roots.py"
REPLAY_RUNNER = ROOT / "docs" / "examples" / "files" / "runners" / "local" / "replay.py"
REPLAY_RECOVERY = ROOT / "docs" / "examples" / "files" / "operations" / "replay_recovery.py"
MAINTENANCE_RUNNER = ROOT / "docs" / "examples" / "files" / "runners" / "local" / "maintenance.py"
LOCAL_RUNNER = ROOT / "docs" / "examples" / "files" / "runners" / "local" / "run.py"
PLATFORM_PROJECT = ROOT / "docs" / "examples" / "files" / "projects" / "platform-smoke"
LOCAL_SPARK_RUNNER = ROOT / "docs" / "examples" / "files" / "runners" / "local" / "run_spark.py"
PLUGIN_SOURCE = ROOT / "docs" / "examples" / "files" / "plugins" / "pii_masker.py"
PLUGIN_PROJECT = ROOT / "docs" / "examples" / "files" / "plugins"
EXAMPLES_GENERATOR = ROOT / "docs" / "scripts" / "gen_examples.py"


def _load_project_archive():
    """Load the generator's pure archive helpers without running MkDocs hooks."""
    source = EXAMPLES_GENERATOR.read_text(encoding="utf-8")
    module = ast.parse(source, filename=str(EXAMPLES_GENERATOR))
    names = {"_iter_archive_files", "_project_archive"}
    nodes = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {
        "BytesIO": BytesIO,
        "Path": Path,
        "is_excluded": is_excluded,
        "iter_public_files": iter_public_files,
        "zipfile": zipfile,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(EXAMPLES_GENERATOR), "exec"), namespace)
    return namespace["_project_archive"]


_PROJECT_ARCHIVE = _load_project_archive()


def _zip_project(source: Path, destination: Path) -> None:
    """Write the same deterministic archive payload used by the docs build."""
    destination.write_bytes(_PROJECT_ARCHIVE(source))


def _copy_public_tree(source: Path, destination: Path) -> None:
    """Copy a project using the public inventory, excluding generated output."""
    destination.mkdir(parents=True, exist_ok=True)
    for path in iter_public_files(source, project_roots=(source,)):
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)


def _python_environment() -> dict[str, str]:
    environment = dict(os.environ)
    source_path = str(ROOT / "src")
    environment["PYTHONPATH"] = (
        source_path
        if not environment.get("PYTHONPATH")
        else source_path + os.pathsep + environment["PYTHONPATH"]
    )
    return environment


@pytest.mark.integration
def test_platform_smoke_download_build_and_local_execution(tmp_path: Path) -> None:
    """Validate every overlay, resolve actual targets, and verify extracted CSV→Delta."""
    import polars as pl

    from datacoolie.destinations.resolution.target import resolve_destination_target
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.platforms.local_platform import LocalPlatform

    archive_path = tmp_path / "platform-smoke.zip"
    _zip_project(PLATFORM_PROJECT, archive_path)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(tmp_path)
    project = tmp_path / PLATFORM_PROJECT.name
    runner = tmp_path / "run_local.py"
    shutil.copyfile(LOCAL_RUNNER, runner)
    environment = _python_environment()

    def invoke(arguments):
        result = subprocess.run([sys.executable, *arguments], cwd=tmp_path,
                                env=environment, capture_output=True, text=True, check=False)
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    invoke(["-m", "datacoolie", "--project-dir", str(project), "validate", "--format", "json"])
    build = invoke(["-m", "datacoolie", "--project-dir", str(project), "build", "--format", "json"])
    current = Path(json.loads(build.stdout)["data"]["current_path"])
    expected = {
        "local": ("path", "data/output/orders_platform_smoke"),
        "fabric": ("path", "/Files/datacoolie-example/data/output/orders_platform_smoke"),
        "databricks": ("table", "main.default.orders_platform_smoke"),
        "aws": ("path", "s3://your-bucket/datacoolie-example/data/output/orders_platform_smoke"),
        "aws-iceberg": ("table", "glue_catalog.datacoolie_example.orders_platform_smoke"),
    }
    for name, (addressing, target_suffix) in expected.items():
        invoke(["-m", "datacoolie", "--project-dir", str(project), "inspect", "metadata",
                "--env", name, "--section", "connections", "--full", "--format", "json"])
        provider = FileProvider(config_path=str(current / name / "metadata/metadata.json"),
                                platform=LocalPlatform(), watermark_base_path=str(tmp_path / "state"))
        provider.initialize()
        flows = provider.get_dataflows(stage="platform_smoke", active_only=True)
        assert len(flows) == 1 and flows[0].name == "orders_platform_smoke"
        assert flows[0].destination.load_type == "overwrite"
        target = resolve_destination_target(flows[0].destination)
        assert target.addressing == addressing
        handle = target.table_name if addressing == "table" else target.path
        assert handle.replace("`", "").endswith(target_suffix)
        assert target.format == ("iceberg" if name == "aws-iceberg" else "delta")
        provider.close()

    for _ in range(2):  # Rerun must overwrite rather than duplicate the three rows.
        invoke([str(runner), "--working-directory", str(project),
                "--artifact-base-path", ".builds/current/local",
                "--state-base-path", ".runtime", "--stage", "platform_smoke"])
        output = pl.read_delta(str(project / "data/output/orders_platform_smoke"))
        business = output.select("order_id", "customer_id", "amount").sort("order_id")
        assert business.rows() == [(1, 100, 20), (2, 100, 43), (3, 101, 7)]
        assert business.dtypes == [pl.Int64] * 3
        assert business["amount"].sum() == 70
    assert list((project / ".runtime/logs/execution_logs").rglob("*.json"))


@pytest.mark.integration
def test_downloaded_artifact_project_runs_outside_checkout(tmp_path: Path) -> None:
    """The complete project source can be extracted and run independently."""
    pytest.importorskip("polars")
    archive_path = tmp_path / "artifact-project.zip"
    _zip_project(PROJECT, archive_path)
    extract_root = tmp_path / "extracted"
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)
    project = extract_root / PROJECT.name
    runtime = tmp_path / "runtime"

    environment = _python_environment()
    completed = subprocess.run(
        [sys.executable, str(project / "runners" / "dev" / "run.py"), "--state-base-path", str(runtime)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files

    import polars as pl

    result = pl.read_parquet(output_files[0])
    assert result.select("order_id").to_series().to_list() == [1, 2, 3]
    assert result.select("category_group").to_series().to_list() == ["physical", "digital", "physical"]
    assert (runtime / "logs").is_dir()
    # Metadata logging keeps the declared file reference while the runtime
    # record carries the SQL sent to the reader after the source predicate was
    # applied.  These are deliberately two different representations.
    dataflow_logs = sorted((runtime / "logs" / "execution_logs" / "dataflow_run_log").rglob("*.json"))
    assert dataflow_logs
    dataflow_record = json.loads(dataflow_logs[0].read_text(encoding="utf-8").splitlines()[0])
    assert dataflow_record["source_query"] == "artifact:/queries/orders.sql"
    source_action = json.loads(str(dataflow_record["source_action"]))
    assert "WHERE o.amount IS NOT NULL" in source_action["query"]


@pytest.mark.integration
@pytest.mark.spark
def test_local_spark_runner_executes_file_dataflow(tmp_path: Path) -> None:
    """The local Spark runner executes a portable CSV-to-Parquet flow."""
    pytest.importorskip("pyspark")
    pytest.importorskip("polars")
    if shutil.which("java") is None:
        pytest.skip("local Spark runner requires a Java executable")

    project = tmp_path / "spark-project"
    metadata = project / "metadata"
    input_root = project / "data" / "input" / "orders"
    output_root = project / "data" / "output"
    input_root.mkdir(parents=True)
    output_root.mkdir(parents=True)
    (metadata / "metadata.json").parent.mkdir(parents=True)
    (input_root / "orders.csv").write_text(
        "order_id,amount\n1,10\n2,20\n",
        encoding="utf-8",
    )
    (metadata / "metadata.json").write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "csv",
                        "configure": {"base_path": "./data/input"},
                    },
                    {
                        "name": "destination",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": "./data/output"},
                    },
                ],
                "dataflows": [
                    {
                        "name": "spark_orders",
                        "stage": "spark_test",
                        "source": {"connection_name": "source", "table": "orders"},
                        "destination": {
                            "connection_name": "destination",
                            "table": "orders",
                            "load_type": "overwrite",
                        },
                        "transform": {},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(LOCAL_SPARK_RUNNER),
            "--metadata-path",
            str(metadata / "metadata.json"),
            "--working-directory",
            str(project),
            "--state-base-path",
            str(runtime),
            "--stage",
            "spark_test",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output_files = sorted((output_root / "orders").glob("*.parquet"))
    assert output_files

    import polars as pl

    result = pl.read_parquet(output_files[0])
    # Spark CSV readers keep unhinted fields as strings; a schema-hints sample
    # covers explicit numeric casting separately.
    assert result.select("order_id").to_series().to_list() == ["1", "2"]
    assert (runtime / "logs" / "execution_logs").is_dir()


@pytest.mark.integration
def test_artifact_relative_query_requires_artifact_root_in_explicit_mode(tmp_path: Path) -> None:
    """An explicit metadata provider does not silently infer an artifact root."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    _copy_public_tree(PROJECT, project)
    runtime = tmp_path / "runtime"
    # The project runner intentionally uses artifact mode.  A separate local
    # runner with only ``metadata_base_path`` must supply ``artifact_base_path``
    # when the metadata declares an ``artifact:/`` SQL reference; this check
    # protects that explicit boundary from accidental path inference.
    local_runner = subprocess.run(
        [
            sys.executable,
            str(LOCAL_RUNNER),
            "--metadata-base-path",
            str(project / "metadata"),
            "--working-directory",
            str(project),
            "--state-base-path",
            str(runtime),
            "--stage",
            "bronze2silver",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert local_runner.returncode == 1
    assert "requires artifact_base_path" in local_runner.stderr


@pytest.mark.integration
def test_local_artifact_runner_honors_explicit_watermark_root(tmp_path: Path) -> None:
    """An artifact runner passes an explicit watermark root to FileProvider."""
    pytest.importorskip("polars")
    project = tmp_path / INCREMENTAL_PROJECT.name
    _copy_public_tree(INCREMENTAL_PROJECT, project)
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(LOCAL_RUNNER),
            "--artifact-base-path",
            str(project),
            "--watermark-base-path",
            str(runtime / "watermarks"),
            "--working-directory",
            str(project),
            "--stage",
            "ingest2bronze",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert list((runtime / "watermarks").rglob("watermark_value.json"))


@pytest.mark.integration
def test_artifact_project_accepts_inline_sql_without_query_file(tmp_path: Path) -> None:
    """The same project runner can execute an inline SQL source reference."""
    pytest.importorskip("polars")
    project = tmp_path / "inline-sql-project"
    _copy_public_tree(PROJECT, project)
    metadata_path = project / "metadata" / "dataflows" / "orders_query.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["dataflows"][0]["source"]["query"] = (
        "SELECT order_id, amount, category FROM orders "
        "WHERE amount > 0 ORDER BY order_id"
    )
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(project / "runners" / "dev" / "run.py"),
            "--state-base-path",
            str(runtime),
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    import polars as pl

    output = pl.read_parquet(project / "data" / "output" / "orders" / "orders.parquet")
    assert output.select("order_id").to_series().to_list() == [1, 2, 3]


@pytest.mark.integration
def test_downloaded_function_project_runs_from_source_tree(tmp_path: Path) -> None:
    """A project-owned Python source can be imported before Driver startup."""
    pytest.importorskip("polars")
    archive_path = tmp_path / "function-project.zip"
    _zip_project(FUNCTION_PROJECT, archive_path)
    extract_root = tmp_path / "extracted"
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)
    project = extract_root / FUNCTION_PROJECT.name
    runtime = tmp_path / "runtime"

    environment = _python_environment()
    completed = subprocess.run(
        [sys.executable, str(project / "runners" / "dev" / "run.py"), "--state-base-path", str(runtime)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files


@pytest.mark.integration
def test_downloaded_transform_project_runs_one_focused_behavior(tmp_path: Path) -> None:
    """A compact transform project has one dataflow and verifiable values."""
    pytest.importorskip("polars")
    archive_path = tmp_path / "transform-project.zip"
    _zip_project(TRANSFORM_PROJECT, archive_path)
    extract_root = tmp_path / "extracted"
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)
    project = extract_root / TRANSFORM_PROJECT.name
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(project / "runners" / "dev" / "run.py"),
            "--state-base-path",
            str(runtime),
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    import polars as pl

    output = pl.read_parquet(project / "data" / "output" / "orders" / "orders.parquet")
    assert output.select("category").to_series().to_list() == ["hardware", "software", "hardware"]
    assert output.columns[:3] == ["order_id", "category", "amount"]


@pytest.mark.integration
def test_incremental_project_persists_integer_watermark(tmp_path: Path) -> None:
    """The second run only writes the newly sequenced input row."""
    pytest.importorskip("polars")
    archive_path = tmp_path / "incremental-project.zip"
    _zip_project(INCREMENTAL_PROJECT, archive_path)
    extract_root = tmp_path / "extracted"
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)
    project = extract_root / INCREMENTAL_PROJECT.name
    runtime = tmp_path / "runtime"

    environment = _python_environment()
    runner = project / "runners" / "dev" / "run.py"
    command = [sys.executable, str(runner), "--state-base-path", str(runtime)]
    first = subprocess.run(command, cwd=tmp_path, env=environment, check=False, capture_output=True, text=True)
    assert first.returncode == 0, first.stdout + first.stderr
    with (project / "data" / "input" / "orders" / "orders.csv").open(
        "a", encoding="utf-8"
    ) as handle:
        handle.write("3,5.50,3\n")
    second = subprocess.run(command, cwd=tmp_path, env=environment, check=False, capture_output=True, text=True)
    assert second.returncode == 0, second.stdout + second.stderr

    import polars as pl

    output = project / "data" / "output" / "orders"
    result = pl.concat([pl.read_parquet(path) for path in sorted(output.glob("*.parquet"))])
    assert result.select("order_id").to_series().to_list() == [1, 2, 3]
    watermark = next((runtime / "watermarks").rglob("watermark_value.json"))
    assert '"updated_sequence": 3' in watermark.read_text(encoding="utf-8")


@pytest.mark.integration
def test_cli_built_artifact_runner_executes_outside_checkout(tmp_path: Path) -> None:
    """A CLI build contains a runnable environment runner and no repo paths."""
    pytest.importorskip("polars")
    project = tmp_path / PROJECT.name
    _copy_public_tree(PROJECT, project)
    environment = _python_environment()

    built = subprocess.run(
        [
            sys.executable,
            "-m",
            "datacoolie",
            "--project-dir",
            str(project),
            "build",
            "--format",
            "json",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0, built.stdout + built.stderr

    runner = project / ".builds" / "current" / "dev" / "runners" / "run.py"
    assert runner.is_file()
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [sys.executable, str(runner), "--state-base-path", str(runtime)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output = project / ".builds" / "current" / "dev" / "data" / "output" / "orders" / "orders.parquet"
    assert output.is_file()

    import polars as pl

    assert pl.read_parquet(output).select("order_id").to_series().to_list() == [1, 2, 3]


@pytest.mark.integration
def test_cli_built_function_project_runner_imports_packaged_source(tmp_path: Path) -> None:
    """The build-selected ZIP remains importable from a clean environment."""
    pytest.importorskip("polars")
    project = tmp_path / FUNCTION_PROJECT.name
    _copy_public_tree(FUNCTION_PROJECT, project)
    environment = _python_environment()

    built = subprocess.run(
        [
            sys.executable,
            "-m",
            "datacoolie",
            "--project-dir",
            str(project),
            "build",
            "--format",
            "json",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0, built.stdout + built.stderr

    runner = project / ".builds" / "current" / "dev" / "runners" / "run.py"
    assert runner.is_file()
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [sys.executable, str(runner), "--state-base-path", str(runtime)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output = project / ".builds" / "current" / "dev" / "data" / "output" / "orders" / "orders.parquet"
    assert output.is_file()


@pytest.mark.integration
def test_cli_built_transform_project_runner_executes_outside_checkout(tmp_path: Path) -> None:
    """The built runner can create its fixture and run the transform artifact."""
    pytest.importorskip("polars")
    project = tmp_path / TRANSFORM_PROJECT.name
    _copy_public_tree(TRANSFORM_PROJECT, project)
    environment = _python_environment()
    built = subprocess.run(
        [
            sys.executable,
            "-m",
            "datacoolie",
            "--project-dir",
            str(project),
            "build",
            "--format",
            "json",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0, built.stdout + built.stderr
    runner = project / ".builds" / "current" / "dev" / "runners" / "run.py"
    assert runner.is_file()
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [sys.executable, str(runner), "--state-base-path", str(runtime)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    import polars as pl

    output_files = sorted(
        (project / ".builds" / "current" / "dev" / "data" / "output" / "orders").glob("*.parquet")
    )
    assert output_files
    assert pl.read_parquet(output_files[0]).select("category").to_series().to_list() == [
        "hardware",
        "software",
        "hardware",
    ]


@pytest.mark.integration
def test_provider_fixture_hydrates_sqlite_and_loopback_api() -> None:
    """The provider example exercises local startup without a Driver."""
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("httpx")
    completed = subprocess.run(
        [sys.executable, str(PROVIDER_FIXTURE), "--provider", "both"],
        cwd=ROOT,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "provider=sqlite connections=2 dataflows=1" in completed.stdout
    assert "provider=api connections=2 dataflows=1" in completed.stdout


@pytest.mark.integration
def test_provider_fixture_runs_both_local_driver_handoffs(tmp_path: Path) -> None:
    """The opt-in fixture executes one exact local dataflow per provider."""
    pytest.importorskip("polars")
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("httpx")
    work_dir = tmp_path / "provider-execution"
    completed = subprocess.run(
        [
            sys.executable,
            str(PROVIDER_FIXTURE),
            "--provider",
            "both",
            "--run-dataflow",
            "--work-dir",
            str(work_dir),
        ],
        cwd=ROOT,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "provider=sqlite executed=1 connections=2 dataflows=1" in completed.stdout
    assert "provider=api executed=1 connections=2 dataflows=1" in completed.stdout

    import polars as pl

    expected = {"order_id": [1, 2, 3], "amount": [19.99, 29.0, 5.5]}
    for provider_name in ("sqlite", "api"):
        output = work_dir / provider_name / "output" / "orders" / "orders.parquet"
        assert output.is_file(), output
        actual = pl.read_parquet(output).select(list(expected)).sort("order_id")
        assert actual.to_dict(as_series=False) == expected


@pytest.mark.integration
def test_standalone_file_provider_controls_initialization_and_cache(tmp_path: Path) -> None:
    """A provider can be initialized and closed independently of Driver."""
    metadata = tmp_path / "metadata"
    shutil.copytree(PROJECT / "metadata", metadata)
    completed = subprocess.run(
        [sys.executable, str(STANDALONE_PROVIDER), str(metadata), "--no-cache"],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "connections=2 dataflows=1 cache=off" in completed.stdout


@pytest.mark.integration
def test_sql_roots_sample_resolves_folder_qualified_references() -> None:
    """Multiple SQL roots require their deterministic folder prefixes."""
    completed = subprocess.run(
        [sys.executable, str(SQL_ROOTS_SAMPLE)],
        cwd=ROOT,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "sql1=SELECT 1 AS order_id" in completed.stdout
    assert "sql2=SELECT 2 AS order_id" in completed.stdout


@pytest.mark.integration
def test_local_runner_persists_snapshot_job_and_batch_dataflow_logs(tmp_path: Path) -> None:
    """Batch detail logs and the one-line job snapshot remain valid JSON."""
    pytest.importorskip("polars")
    project = tmp_path / INCREMENTAL_PROJECT.name
    _copy_public_tree(INCREMENTAL_PROJECT, project)
    runtime = project / ".runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(LOCAL_RUNNER),
            "--metadata-base-path",
            str(project / "metadata"),
            "--working-directory",
            str(project),
            "--state-base-path",
            str(runtime),
            "--log-base-path",
            str(runtime / "logs"),
            "--log-persistence-mode",
            "batch",
            "--log-flush-interval-seconds",
            "0",
            "--log-flush-batch-bytes",
            "1",
            "--run-attributes-json",
            '{"scheduler_job_id":"example-job-001"}',
            "--stage",
            "ingest2bronze",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

    log_root = runtime / "logs"
    execution_files = sorted((log_root / "execution_logs").rglob("*.json"))
    dataflow_files = [path for path in execution_files if "dataflow_run_log" in path.parts]
    job_files = [path for path in execution_files if "job_run_log" in path.parts]
    system_files = sorted((log_root / "system_logs").rglob("*.json"))
    assert dataflow_files and job_files and system_files

    def records(paths: list[Path]) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        for path in paths:
            for line in path.read_text(encoding="utf-8").splitlines():
                payload = json.loads(line)
                assert isinstance(payload, dict)
                result.append(payload)
        return result

    dataflow_records = records(dataflow_files)
    job_records = records(job_files)
    system_records = records(system_files)
    assert dataflow_records
    assert len(job_records) == 1
    assert all(record["log_schema_version"] == 4 for record in dataflow_records + job_records + system_records)
    assert all(record["_type"] in {"dataflow_run_log", "job_run_log"} for record in dataflow_records + job_records)
    assert all(record["_type"] == "system_log" for record in system_records)
    assert dataflow_records[0]["dataflow_run_id"]
    assert json.loads(str(job_records[0]["run_attributes"])) == {
        "scheduler_job_id": "example-job-001"
    }


@pytest.mark.integration
def test_public_transformer_plugin_runs_through_driver_extension_seam(tmp_path: Path) -> None:
    """The documented entry-point class can be injected through a Driver subclass."""
    pytest.importorskip("polars")
    from datacoolie import transformer_registry
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.orchestration.driver import DataCoolieDriver
    from datacoolie.platforms.local_platform import LocalPlatform

    module_spec = importlib.util.spec_from_file_location("public_pii_masker", PLUGIN_SOURCE)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    transformer_registry.register("public_pii_masker", module.PiiMaskerTransformer)

    project = tmp_path / "plugin-project"
    metadata = project / "metadata"
    input_root = project / "data" / "input"
    metadata.mkdir(parents=True)
    (input_root / "customers").mkdir(parents=True)
    (project / "data" / "output").mkdir(parents=True)
    (input_root / "customers" / "customers.csv").write_text(
        "customer_id,email\n1,alice@example.com\n2,bob@example.com\n",
        encoding="utf-8",
    )
    (metadata / "metadata.json").write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "csv",
                        "configure": {"base_path": "./data/input"},
                    },
                    {
                        "name": "destination",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": "./data/output"},
                    },
                ],
                "dataflows": [
                    {
                        "name": "mask_customers",
                        "stage": "plugin",
                        "source": {"connection_name": "source", "table": "customers"},
                        "transform": {
                            "configure": {"pii_mask": {"columns": ["email"]}}
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "customers",
                            "load_type": "overwrite",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class PluginDriver(DataCoolieDriver):
        def _create_transformer_pipeline(self, dataflow_run_id=None, column_name_mode="lower"):
            pipeline = super()._create_transformer_pipeline(dataflow_run_id, column_name_mode)
            pipeline.add_transformer(
                transformer_registry.get("public_pii_masker", engine=self._engine)
            )
            return pipeline

    import os as _os

    current_directory = Path.cwd()
    _os.chdir(project)
    platform = LocalPlatform()
    try:
        with PluginDriver(
            engine=PolarsEngine(platform=platform),
            platform=platform,
            metadata_provider=FileProvider(
                config_path=str(metadata / "metadata.json"),
                platform=platform,
            ),
            state_base_path=str(project / ".runtime"),
            config=DataCoolieRunConfig(job_id="plugin-example"),
        ) as driver:
            result = driver.run(stage="plugin")
        assert result.failed == 0
        import polars as pl

        output_files = sorted((project / "data" / "output" / "customers").glob("*.parquet"))
        assert output_files
        output = pl.read_parquet(output_files)
        assert output.get_column("email").to_list() == ["***", "***"]
    finally:
        _os.chdir(current_directory)
        transformer_registry.unregister("public_pii_masker")


@pytest.mark.integration
def test_public_transformer_plugin_wheel_discovers_entry_point(tmp_path: Path) -> None:
    """The installed wheel executes the authored tutorial through a real Driver."""
    pytest.importorskip("build")
    pytest.importorskip("polars")
    project = tmp_path / "plugin-package"
    shutil.copytree(PLUGIN_PROJECT, project)
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    built = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(wheel_dir)],
        cwd=project,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0, built.stdout + built.stderr
    wheels = sorted(wheel_dir.glob("*.whl"))
    assert len(wheels) == 1

    site = tmp_path / "site"
    installed = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(site), str(wheels[0])],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert installed.returncode == 0, installed.stdout + installed.stderr
    environment = _python_environment()
    environment["PYTHONPATH"] = str(site) + os.pathsep + environment["PYTHONPATH"]
    discovered = subprocess.run(
        [
            sys.executable,
            "-c",
            "from datacoolie import transformer_registry; "
            "assert 'pii_masker' in transformer_registry.list_plugins(); "
            "import inspect; from pathlib import Path; import pii_masker; "
            "assert Path(inspect.getfile(pii_masker)).parent == Path('site').resolve(); "
            "print('pii_masker: discovered')",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert discovered.returncode == 0, discovered.stdout + discovered.stderr
    assert "pii_masker: discovered" in discovered.stdout

    tutorial = (ROOT / "docs" / "extensions" / "transformer-tutorial.md").read_text(
        encoding="utf-8"
    )
    metadata_blocks = re.findall(r"```json\n(.*?)\n```", tutorial, flags=re.DOTALL)
    runner_blocks = re.findall(r"```python\n(.*?)\n```", tutorial, flags=re.DOTALL)
    assert len(metadata_blocks) == len(runner_blocks) == 1
    # Execute the actual author-facing script and metadata, not another recipe.
    json.loads(metadata_blocks[0])
    (tmp_path / "metadata.json").write_text(metadata_blocks[0], encoding="utf-8")
    (tmp_path / "run.py").write_text(runner_blocks[0], encoding="utf-8")
    for _ in range(2):
        completed = subprocess.run(
            [sys.executable, "run.py"],
            cwd=tmp_path,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        assert "masking, nulls and skipped configuration verified" in completed.stdout

    import polars as pl

    for table, expected in (
        ("masked_customers", ["***", "***", None]),
        ("unmasked_customers", ["alice@example.com", "bob@example.com", None]),
    ):
        output = pl.read_parquet(sorted((tmp_path / "data/output" / table).glob("*.parquet")))
        assert output.height == 3
        assert output.sort("customer_id").get_column("email").to_list() == expected


@pytest.mark.integration
@pytest.mark.parametrize("start,expected", [("1", [1, 2]), ("-10", [1, 2]),
                                            ("+2", [2])],
                         ids=["default-start", "negative-start", "manual-later-start"])
def test_local_replay_runner_processes_integer_range(tmp_path: Path, start, expected) -> None:
    """Replay uses an inclusive lower/exclusive upper integer range."""
    pytest.importorskip("polars")
    project = tmp_path / INCREMENTAL_PROJECT.name
    _copy_public_tree(INCREMENTAL_PROJECT, project)
    runtime = project / ".runtime"
    environment = _python_environment()
    completed = subprocess.run(
        [
            sys.executable,
            str(REPLAY_RUNNER),
            "--metadata-path",
            str(project / "metadata"),
            "--watermark-base-path",
            str(runtime / "watermarks"),
            "--log-base-path",
            str(runtime / "logs"),
            "--working-directory",
            str(project),
            "--start",
            start,
            "--end",
            "4",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files
    import polars as pl

    rows = pl.concat([pl.read_parquet(path) for path in output_files])
    assert rows.select("order_id").to_series().to_list() == expected
    assert not list((runtime / "watermarks").rglob("watermark_value.json"))


@pytest.mark.integration
def test_local_replay_runner_repeats_integer_chunks(tmp_path: Path) -> None:
    """Chunked replay saves observations and reruns the range on repeat."""
    pytest.importorskip("polars")
    project = tmp_path / INCREMENTAL_PROJECT.name
    _copy_public_tree(INCREMENTAL_PROJECT, project)
    runtime = tmp_path / "runtime"
    environment = _python_environment()
    command = [
        sys.executable,
        str(REPLAY_RUNNER),
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
        json.dumps({"step": 2}),
        "--save-watermark",
        "--confirm-save-watermark",
    ]
    first = subprocess.run(
        command,
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stdout + first.stderr
    output_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    assert output_files
    import polars as pl

    first_rows = pl.concat([pl.read_parquet(path) for path in output_files])
    assert sorted(first_rows.select("order_id").to_series().to_list()) == [1, 2]
    watermark = next((runtime / "watermarks").rglob("watermark_value.json"))
    assert '"updated_sequence": 2' in watermark.read_text(encoding="utf-8")

    second = subprocess.run(
        command,
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert second.returncode == 0, second.stdout + second.stderr
    second_files = sorted((project / "data" / "output" / "orders").glob("*.parquet"))
    second_rows = pl.concat([pl.read_parquet(path) for path in second_files])
    assert second_rows.height == first_rows.height * 2


@pytest.mark.integration
def test_operational_runners_require_explicit_mutation_confirmation(tmp_path: Path) -> None:
    """Replay watermark writes and maintenance mutations fail closed."""
    replay = subprocess.run(
        [
            sys.executable,
            str(REPLAY_RUNNER),
            "--metadata-path",
            str(tmp_path / "metadata.json"),
            "--watermark-base-path",
            str(tmp_path / "watermarks"),
            "--log-base-path",
            str(tmp_path / "logs"),
            "--start",
            "1",
            "--end",
            "2",
            "--save-watermark",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert replay.returncode == 2
    assert "confirm-save-watermark" in replay.stderr.lower()

    maintenance = subprocess.run(
        [
            sys.executable,
            str(MAINTENANCE_RUNNER),
            "--metadata-path",
            str(tmp_path / "metadata.json"),
            "--watermark-base-path",
            str(tmp_path / "watermarks"),
            "--log-base-path",
            str(tmp_path / "logs"),
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert maintenance.returncode == 2
    assert "confirm-maintenance" in maintenance.stderr.lower()


@pytest.mark.integration
def test_local_maintenance_runner_compacts_existing_delta_table(tmp_path: Path) -> None:
    """The public maintenance runner executes against an existing Delta table."""
    pytest.importorskip("polars")
    pytest.importorskip("deltalake")

    project = tmp_path / "maintenance-project"
    metadata = project / "metadata"
    (metadata / "dataflows").mkdir(parents=True)
    table_path = project / "data" / "output" / "orders"
    table_path.parent.mkdir(parents=True)
    (metadata / "connections.json").write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "csv",
                        "configure": {"base_path": "./data/input"},
                    },
                    {
                        "name": "destination",
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": "./data/output"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (metadata / "dataflows" / "orders.json").write_text(
        json.dumps(
            {
                "dataflows": [
                    {
                        "name": "orders",
                        "stage": "maintenance",
                        "source": {"connection_name": "source", "table": "orders"},
                        "destination": {
                            "connection_name": "destination",
                            "table": "orders",
                            "load_type": "overwrite",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    import polars as pl

    pl.DataFrame({"order_id": [1, 2], "amount": [10, 20]}).write_delta(
        str(table_path), mode="overwrite"
    )
    untouched = project / "data" / "output" / "untouched"
    pl.DataFrame({"order_id": [99], "amount": [999]}).write_delta(str(untouched), mode="overwrite")
    untouched_before = {path.relative_to(untouched): path.read_bytes()
                        for path in untouched.rglob("*") if path.is_file()}
    runtime = tmp_path / "runtime"
    completed = subprocess.run(
        [
            sys.executable,
            str(MAINTENANCE_RUNNER),
            "--metadata-path",
            str(metadata),
            "--watermark-base-path",
            str(runtime / "watermarks"),
            "--log-base-path",
            str(runtime / "logs"),
            "--working-directory",
            str(project),
            "--connection",
            "destination",
            "--no-cleanup",
            "--confirm-maintenance",
        ],
        cwd=tmp_path,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (table_path / "_delta_log").is_dir()
    maintenance_logs = sorted(
        (runtime / "logs" / "execution_logs" / "dataflow_run_log").rglob("*.json")
    )
    assert maintenance_logs
    record = json.loads(maintenance_logs[0].read_text(encoding="utf-8").splitlines()[0])
    assert record["operation_type"] == "maintenance"
    assert record["status"] == "succeeded"
    assert pl.read_delta(str(table_path)).sort("order_id").to_dicts() == [
        {"order_id": 1, "amount": 10}, {"order_id": 2, "amount": 20}
    ]
    assert {path.relative_to(untouched): path.read_bytes()
            for path in untouched.rglob("*") if path.is_file()} == untouched_before
    assert not list((runtime / "watermarks").rglob("watermark_value.json"))
