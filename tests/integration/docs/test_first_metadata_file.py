"""Execute the introductory metadata document and authored runner unchanged."""

from pathlib import Path
import json
import re

import polars as pl
import pytest


DOC = Path(__file__).resolve().parents[3] / "docs/guide/metadata/first-metadata-file.md"


@pytest.mark.integration
def test_first_metadata_tutorial_runs_twice_without_duplicate_business_rows(tmp_path, monkeypatch):
    text = DOC.read_text(encoding="utf-8")
    document_section = text.split("## Step 5", 1)[1].split("## Step 6", 1)[0]
    document = json.loads(re.search(r"```json\n(.*?)\n```", document_section, re.S)[1])
    (tmp_path / "metadata.json").write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    scripts = re.findall(r"```python\n(.*?)\n```", text.split("## Step 6", 1)[1].split("## Adding", 1)[0], re.S)
    assert len(scripts) == 2
    for _ in range(2):
        namespace = {}
        for script in scripts:
            exec(compile(script, str(DOC), "exec"), namespace)
        rows = pl.read_delta(str(tmp_path / "data/output/bronze/sales/orders"))
        assert rows.select("order_id", "amount").sort("order_id").rows() == [("1", "10"), ("2", "20")]
