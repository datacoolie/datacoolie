"""Execute reference examples in isolated local processes."""

from pathlib import Path
import os
import re
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
REFERENCE = ROOT / "docs" / "reference"


def _run(script: str, tmp_path: Path) -> None:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.integration
def test_exact_core_model_example_constructs_hydrated_objects(tmp_path) -> None:
    content = (REFERENCE / "api/core.md").read_text(encoding="utf-8")
    script = re.findall(r"```python\n(.*?)\n```", content, re.DOTALL)[0]
    _run(script + '\nassert source.connection is connection\n', tmp_path)


@pytest.mark.integration
def test_standalone_logging_example_persists_and_releases_capture(tmp_path) -> None:
    content = (REFERENCE / "concepts/logging.md").read_text(encoding="utf-8")
    configuration = content.split("## Configuration\n", 1)[1]
    script = re.findall(r"```python\n(.*?)\n```", configuration, re.DOTALL)[0]
    probe = '''
from pathlib import Path
import json
records = [json.loads(line) for path in Path('.').rglob('*.json')
           for line in path.read_text(encoding='utf-8').splitlines()]
assert records, 'No persisted standalone logs'
assert all(record['log_schema_version'] == 4 for record in records)
assert all(record['log_session_id'] for record in records)
with SystemLogger(LogConfig(), platform):
    pass  # A second session must be able to acquire the process capture owner.
'''
    _run(script + "\n" + probe, tmp_path)
