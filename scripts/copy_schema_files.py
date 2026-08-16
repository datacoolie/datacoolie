"""Publish DataCoolie build-skill JSON Schemas into docs/schema at build time.

Single source of truth lives in:
  ai/skills/datacoolie-build/schemas/

Published at:
  https://datacoolie.github.io/datacoolie/schema/<schema-relative-path>
"""

from __future__ import annotations

import shutil
from pathlib import Path


def on_pre_build(config) -> None:  # noqa: ANN001
    docs_dir = Path(config["docs_dir"])
    skills_schemas = Path(__file__).resolve().parents[2] / "ai" / "skills" / "datacoolie-build" / "schemas"

    for schema_file in skills_schemas.rglob("*.json"):
        # Preserve relative paths, including versioned metadata schema directories.
        rel = schema_file.relative_to(skills_schemas)
        dest = docs_dir / "schema" / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(schema_file, dest)
