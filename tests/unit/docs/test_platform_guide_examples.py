"""Exercise published smoke guards and reachable scenario failure signaling."""

import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCENARIOS = ROOT / "usecase-sim" / "platforms"
SAMPLES = tuple(sorted(
    path for path in SCENARIOS.rglob("sample_*")
    if path.suffix in {".py", ".ipynb"} and "secrets" not in path.name
))


def _code(path):
    if path.suffix == ".py":
        return path.read_text(encoding="utf-8")
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    return "\n".join(
        line for cell in cells if cell["cell_type"] == "code"
        for line in "".join(cell["source"]).splitlines()
        if not line.startswith(("%", "!"))
    )


@pytest.mark.parametrize("path", SAMPLES, ids=lambda p: p.name)
@pytest.mark.parametrize("failed,pending", [(1, 0), (0, 1), (0, 0)])
def test_scenario_terminal_guard_signals_incomplete_run(path, failed, pending):
    tree = ast.parse(_code(path), filename=str(path))
    guards = [node for node in tree.body if isinstance(node, ast.If)
              and "result.failed" in ast.unparse(node.test)]
    assert len(guards) == 1
    code = compile(ast.Module(body=guards, type_ignores=[]), str(path), "exec")
    namespace = {"result": SimpleNamespace(failed=failed, pending=pending, total=0)}
    if failed or pending:
        with pytest.raises(RuntimeError, match="incomplete"):
            exec(code, namespace)
    else:
        exec(code, namespace)  # No blanket nonempty requirement for generic jobs.


@pytest.mark.parametrize("names", [[], ["wrong_flow"],
                                   ["orders_platform_smoke", "extra"],
                                   ["orders_platform_smoke"]])
def test_documented_smoke_guard_checks_selection_before_running(names):
    prose = (ROOT / "docs/examples/runners.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```python\n(.*?)\n```", prose, re.S)
    code = next(block for block in blocks if block.startswith("selected = driver.load_dataflows"))
    selected = [SimpleNamespace(name=name) for name in names]
    driver = Mock()
    driver.load_dataflows.return_value = selected
    if names == ["orders_platform_smoke"]:
        exec(code, {"driver": driver})
        driver.run.assert_called_once_with(dataflows=selected)
    else:
        with pytest.raises(RuntimeError, match="selection"):
            exec(code, {"driver": driver})
        driver.run.assert_not_called()
    driver.load_dataflows.assert_called_once_with(stage="platform_smoke", active_only=True)


@pytest.mark.parametrize("counts", [(1, 1, 0, 0), (0, 0, 0, 0), (1, 0, 0, 0),
                                    (1, 0, 1, 0), (1, 0, 0, 1)])
def test_documented_smoke_guard_requires_complete_success(counts):
    prose = (ROOT / "docs/examples/runners.md").read_text(encoding="utf-8")
    code = next(block for block in re.findall(r"```python\n(.*?)\n```", prose, re.S)
                if block.startswith("if (result.total"))
    result = SimpleNamespace(**dict(zip(("total", "succeeded", "failed", "pending"), counts)))
    if counts == (1, 1, 0, 0):
        exec(code, {"result": result})
    else:
        with pytest.raises(RuntimeError, match="did not complete"):
            exec(code, {"result": result})


@pytest.mark.parametrize("relative", ["fabric/run_spark.ipynb", "fabric/run_polars.ipynb",
                                      "databricks/run_spark.ipynb", "databricks/replay_spark.ipynb",
                                      "databricks/maintenance_spark.ipynb"])
def test_notebook_config_path_defaults_use_exact_metadata_file(relative, tmp_path):
    """Defaults sent to config_path must resolve a file, not directory discovery."""
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.platforms.local_platform import LocalPlatform

    path = ROOT / "docs/examples/files/runners" / relative
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cell = next(cell for cell in notebook["cells"]
                if "parameters" in cell.get("metadata", {}).get("tags", []))
    namespace = {}
    exec("".join(cell["source"]), namespace)
    declared = namespace["METADATA_PATH"]
    assert declared.endswith("/metadata/metadata.json")
    metadata = tmp_path / "metadata" / Path(declared).name
    metadata.parent.mkdir()
    metadata.write_text('{"connections": [], "dataflows": []}', encoding="utf-8")
    provider = FileProvider(config_path=str(metadata), platform=LocalPlatform())
    provider.initialize()
    assert provider.get_dataflows() == []
    provider.close()
