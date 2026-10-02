"""Generate the authored metadata reference from the selected JSON Schema."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import mkdocs_gen_files

from datacoolie.project.schema import LATEST_SCHEMA_URL, resolve_metadata_schema

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _metadata_reference import render_metadata_reference, verify_latest_reference  # noqa: E402


resolved = resolve_metadata_schema()
public_index = json.loads((SCRIPT_DIR.parent / "schema" / "index.json").read_text(encoding="utf-8"))
verify_latest_reference(
    public_index["latest"],
    version=resolved.descriptor.version,
    public_url=resolved.descriptor.public_url,
    sha256=resolved.descriptor.sha256,
    latest_url=LATEST_SCHEMA_URL,
)
content = render_metadata_reference(
    resolved.document,
    latest_url=LATEST_SCHEMA_URL,
)

with mkdocs_gen_files.open("reference/metadata-schema.md", "w") as fp:
    fp.write(content)
