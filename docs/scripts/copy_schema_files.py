"""Publish project-owned metadata JSON Schemas into docs/schema at build time.

Single source of truth lives in:
  src/datacoolie/project/schemas/

Published at:
  https://datacoolie.github.io/datacoolie/schema/<schema-relative-path>

The generated ``latest`` path is a byte-identical alias of the highest stable
version. It is public authoring/discovery metadata only; the CLI still resolves
the installed framework's bundled version offline.
"""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path


def _load_index_tools():
    script = Path(__file__).resolve().parents[2] / "scripts" / "generate_metadata_schema_index.py"
    spec = importlib.util.spec_from_file_location("datacoolie_schema_index", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load metadata schema index tools: {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def on_pre_build(config) -> None:  # noqa: ANN001
    docs_dir = Path(config["docs_dir"])
    project_schemas = Path(__file__).resolve().parents[2] / "src" / "datacoolie" / "project" / "schemas"
    index_tools = _load_index_tools()
    index_tools.check_index(project_schemas)

    destination_root = docs_dir / "schema"
    expected = {
        path.relative_to(project_schemas).as_posix()
        for path in project_schemas.rglob("*.json")
    }
    public_index = index_tools.build_public_index(project_schemas)
    expected.add(index_tools.LATEST_SCHEMA_PATH)
    if destination_root.exists():
        for stale in destination_root.rglob("*.json"):
            if stale.relative_to(destination_root).as_posix() not in expected:
                stale.unlink()

    for schema_file in project_schemas.rglob("*.json"):
        # Preserve relative paths, including versioned metadata schema directories.
        rel = schema_file.relative_to(project_schemas)
        dest = destination_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(schema_file, dest)

    latest_source = project_schemas / public_index["latest"]["source_path"]
    latest_destination = destination_root / index_tools.LATEST_SCHEMA_PATH
    latest_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(latest_source, latest_destination)
    (destination_root / "index.json").write_bytes(
        index_tools.public_index_bytes(project_schemas)
    )
