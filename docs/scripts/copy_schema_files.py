"""MkDocs hook: copy JSON Schema files from skills/ into docs/schema/ at build time.

Single source of truth lives in:
  skills/datacoolie-metadata/schemas/<version>/metadata.schema.json

Published at:
  https://datacoolie.github.io/datacoolie/schema/<version>/metadata.schema.json
"""

from __future__ import annotations

import shutil
from pathlib import Path


def on_pre_build(config) -> None:  # noqa: ANN001
    docs_dir = Path(config["docs_dir"])
    skills_schemas = Path(__file__).resolve().parents[2] / "skills" / "datacoolie-metadata" / "schemas"

    for schema_file in skills_schemas.rglob("*.json"):
        # Preserve version folder: schemas/0.1.0/metadata.schema.json → docs/schema/0.1.0/metadata.schema.json
        rel = schema_file.relative_to(skills_schemas)
        dest = docs_dir / "schema" / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(schema_file, dest)
