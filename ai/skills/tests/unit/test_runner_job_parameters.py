"""Execute complete runner templates through the real driver and job selection.

Only host SDKs, metadata, logging sinks and per-dataflow processing are faked;
configuration validation, driver entry points and distribution stay real.
"""

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import (
    Connection, DataCoolieRunConfig, DataFlow, DataFlowRuntimeInfo, Destination, Source,
)
from datacoolie.orchestration import driver as driver_module


RUNNERS = Path(__file__).resolve().parents[2] / "datacoolie-build" / "templates" / "runners"
TEMPLATES = tuple(sorted(RUNNERS.glob("*.example")))
NOTEBOOKS = tuple(path for path in TEMPLATES if ".ipynb." in path.name)
STAGE = "extract_orders,enrich_customers"


class _Widgets:
    def __init__(self, supplied):
        self.values = dict(supplied)

    def get(self, name):
        return self.values[name]

    def text(self, name, default):
        self.values.setdefault(name, default)


@pytest.fixture
def host(monkeypatch):
    connection = Connection(name="test", format="delta", configure={"base_path": "/data"})
    # Group 1 and 4 belong to shard 1/3; group 0 and 2 must be excluded there.
    # Independent IDs a/d/f belong to shards 1/0/2 respectively (MD5).
    flows = [
        DataFlow(
            dataflow_id=name, group_number=group,
            source=Source(connection=connection, table="source", watermark_columns=["id"]),
            destination=Destination(connection=connection, table=name),
        )
        for name, group in [("g0", 0), ("g1", 1), ("g4", 4), ("g2", 2),
                            ("a", None), ("d", None), ("f", None)]
    ]
    provider = SimpleNamespace(
        get_dataflows=Mock(return_value=flows),
        get_maintenance_dataflows=Mock(return_value=flows),
    )
    state = SimpleNamespace(configs=[], processed=[], provider=provider, flows=flows, glue_options=[])

    def module(name, **attributes):
        fake = ModuleType(name)
        fake.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, fake)

    for platform, class_name in [("local", "LocalPlatform"), ("aws", "AWSPlatform"),
                                 ("databricks", "DatabricksPlatform"), ("fabric", "FabricPlatform")]:
        module(f"datacoolie.platforms.{platform}_platform", **{class_name: Mock(return_value=object())})
    for engine, class_name in [("polars", "PolarsEngine"), ("spark", "SparkEngine")]:
        module(f"datacoolie.engines.{engine}_engine", **{
            class_name: lambda **kwargs: SimpleNamespace(platform=kwargs["platform"]),
        })
    module("datacoolie.metadata.file_provider", FileProvider=Mock(return_value=provider))
    spark = SimpleNamespace(stop=Mock())
    builder = SimpleNamespace(appName=lambda _: SimpleNamespace(getOrCreate=lambda: spark))
    module("pyspark", __path__=[])
    module("pyspark.sql", SparkSession=SimpleNamespace(builder=builder))
    module("pyspark.context", SparkContext=SimpleNamespace(getOrCreate=lambda: object()))
    module("awsglue", __path__=[])
    module("awsglue.context", GlueContext=lambda _: SimpleNamespace(spark_session=spark))

    def resolve(argv, options):
        state.glue_options.append(list(options))
        # Match Glue's required-option behavior: absent requested keys fail.
        return {name: argv[argv.index(f"--{name}") + 1] for name in options}

    module("awsglue.utils", getResolvedOptions=resolve)
    monkeypatch.setattr(driver_module, "create_system_logger", lambda **_: None)
    monkeypatch.setattr(driver_module, "create_etl_logger", lambda **_: None)
    real_init = driver_module.DataCoolieDriver.__init__

    def init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        state.configs.append(self.config)

    def process(self, flow, **kwargs):
        state.processed.append(flow.dataflow_id)
        return DataFlowRuntimeInfo(dataflow_id=flow.dataflow_id, status="succeeded")

    monkeypatch.setattr(driver_module.DataCoolieDriver, "__init__", init)
    for method in ("_process_dataflow", "_process_replay", "_process_maintenance"):
        monkeypatch.setattr(driver_module.DataCoolieDriver, method, process)
    state.spark = spark
    return state


def _execute(path, jobs, host, monkeypatch):
    maintenance = path.name.startswith("maintenance_")
    replay = path.name.startswith("replay_")
    namespace = {"__name__": "__main__", "__file__": str(path), "spark": host.spark}
    if ".ipynb." in path.name:
        supplied = {"STAGE": STAGE, "CONFIRM_MAINTENANCE": "true"}
        if jobs is not None:
            supplied.update(JOB_NUM=str(jobs[0]), JOB_INDEX=str(jobs[1]))
        namespace["dbutils"] = SimpleNamespace(widgets=_Widgets(supplied))
        notebook = json.loads(path.read_text(encoding="utf-8"))
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            exec(compile("".join(cell["source"]), str(path), "exec"), namespace)
            if "parameters" in cell.get("metadata", {}).get("tags", []) and "fabric" in path.name:
                # Fabric injects pipeline overrides immediately after defaults.
                namespace.update(supplied)
        return

    if "glue" in path.name:
        values = {"REGION": "us-east-1", "METADATA_PATH": "/fake/metadata.json",
                  "WATERMARK_BASE_PATH": "/fake/state", "LOG_BASE_PATH": "/fake/logs",
                  "STAGE": STAGE}
        if jobs is not None:
            values.update(JOB_NUM=str(jobs[0]), JOB_INDEX=str(jobs[1]))
    else:
        values = {"metadata-path": "/fake/metadata.json", "watermark-base-path": "/fake/state",
                  "base-log-path": "/fake/logs"}
        if not maintenance:
            values["stage"] = STAGE
        if replay:
            values.update(start="1", end="10")
        if jobs is not None:
            values.update({"job-num": str(jobs[0]), "job-index": str(jobs[1])})
    argv = [str(path), *(item for key, value in values.items() for item in (f"--{key}", value))]
    if maintenance:
        argv.append("--confirm-maintenance")
    monkeypatch.setattr(sys, "argv", argv)
    try:
        exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)
    except SystemExit as exc:
        assert exc.code == 0, f"runner exited before successful execution: {exc.code}"


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.name)
@pytest.mark.parametrize("jobs", [None, (3, 1)], ids=["omitted-defaults", "supplied-shard"])
def test_whole_runner_propagates_job_config_and_executes_only_its_partition(path, jobs, host, monkeypatch):
    assert len(TEMPLATES) == 11
    _execute(path, jobs, host, monkeypatch)
    assert len(host.configs) == 1  # Exactly one framework job, never runner-side fan-out.
    config = host.configs[0]
    assert type(config) is DataCoolieRunConfig
    assert (config.job_num, config.job_index) == (jobs or (1, 0))
    assert type(config.job_num) is int and type(config.job_index) is int
    expected_workers = (
        DataCoolieRunConfig().max_workers
        if path.name in {"replay_databricks_spark.ipynb.example", "maintenance_databricks_spark.ipynb.example"}
        else 4
    )
    assert config.max_workers == expected_workers
    expected = {f.dataflow_id for f in host.flows} if jobs is None else {"g1", "g4", "a"}
    assert set(host.processed) == expected
    assert len(host.processed) == len(expected)
    if path.name.startswith("maintenance_"):
        host.provider.get_maintenance_dataflows.assert_called_once_with(connection=None)
        host.provider.get_dataflows.assert_not_called()
    else:
        host.provider.get_dataflows.assert_called_once_with(
            stage=STAGE, active_only=True, attach_schema_hints=True,
        )
        host.provider.get_maintenance_dataflows.assert_not_called()
    if "glue" in path.name:
        assert len(host.glue_options) == 1
        assert ("JOB_NUM" in host.glue_options[0]) is (jobs is not None)
        assert ("JOB_INDEX" in host.glue_options[0]) is (jobs is not None)


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.name)
@pytest.mark.parametrize("jobs,field", [((0, 0), "job_num"), ((3, -1), "job_index"),
                                        ((3, 3), "job_index")])
def test_whole_runner_delegates_invalid_job_values_to_framework(path, jobs, field, host, monkeypatch):
    with pytest.raises(ConfigurationError, match=f"DataCoolieRunConfig.{field}"):
        _execute(path, jobs, host, monkeypatch)
    assert host.configs == []
    assert host.processed == []
    host.provider.get_dataflows.assert_not_called()
    host.provider.get_maintenance_dataflows.assert_not_called()


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_job_defaults_live_in_tagged_notebook_parameter_cell(path):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells = [cell for cell in notebook["cells"]
             if "parameters" in cell.get("metadata", {}).get("tags", [])]
    assert len(cells) == 1
    namespace = {}
    exec(compile("".join(cells[0]["source"]), str(path), "exec"), namespace)
    assert namespace["JOB_NUM"] == 1 and type(namespace["JOB_NUM"]) is int
    assert namespace["JOB_INDEX"] == 0 and type(namespace["JOB_INDEX"]) is int
