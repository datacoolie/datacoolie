"""Contract tests for the generated public metadata-schema tree."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HOOK_PATH = ROOT / "docs" / "scripts" / "copy_schema_files.py"


def _load_hook():
    spec = importlib.util.spec_from_file_location("copy_schema_files", HOOK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_schema_hook_generates_byte_identical_latest_alias_and_public_index(
    tmp_path: Path,
) -> None:
    docs_dir = tmp_path / "docs"
    schema_dir = docs_dir / "schema"
    stale = schema_dir / "latest" / "stale.json"
    stale.parent.mkdir(parents=True)
    stale.write_text("{}", encoding="utf-8")

    hook = _load_hook()
    hook.on_pre_build({"docs_dir": str(docs_dir)})

    source_root = ROOT / "src" / "datacoolie" / "project" / "schemas"
    source_index = hook._load_index_tools().build_public_index(source_root)
    source = source_root / source_index["latest"]["source_path"]
    latest = schema_dir / "latest" / "metadata.schema.json"
    index = json.loads((schema_dir / "index.json").read_text(encoding="utf-8"))

    assert latest.read_bytes() == source.read_bytes()
    assert not stale.exists()
    assert index["latest"]["version"] == source_index["latest"]["version"]
    assert index["latest"]["path"] == "latest/metadata.schema.json"
    assert index["latest"]["source_path"] == source_index["latest"]["source_path"]
    assert index["latest"]["sha256"] == index["schemas"][0]["sha256"]
