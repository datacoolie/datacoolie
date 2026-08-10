#!/usr/bin/env python3
"""Render optional project-owned DataCoolie build automation."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path


SCRIPT_NAMES = (
    "_loaders.py",
    "_schema_resolver.py",
    "inspect_capabilities.py",
    "materialize.py",
    "merge.py",
    "requirements.txt",
    "validate.py",
    "validate_build.py",
    "validate_config.py",
)

WRAPPER = '''#!/usr/bin/env python3
"""Project-owned entrypoint for immutable DataCoolie build materialization."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent / "datacoolie_build" / "scripts"
sys.path.insert(0, str(SCRIPTS))
runpy.run_path(str(SCRIPTS / "materialize.py"), run_name="__main__")
'''


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render(workspace: Path, *, force: bool = False) -> Path:
    workspace = workspace.resolve()
    if not (workspace / "config.yaml").is_file():
        raise ValueError(f"Workspace config not found: {workspace / 'config.yaml'}")

    skill_root = Path(__file__).resolve().parent.parent
    automation = workspace / "automation"
    support = automation / "datacoolie_build"
    managed_targets = [automation / "build.py", support / "scripts", support / "schemas"]
    existing = [path for path in managed_targets if path.exists()]
    if existing and not force:
        raise ValueError(
            "Project automation already exists; review it and pass --force to refresh managed files"
        )

    scripts_target = support / "scripts"
    schemas_target = support / "schemas"
    scripts_target.mkdir(parents=True, exist_ok=True)
    schemas_target.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    for name in SCRIPT_NAMES:
        source = skill_root / "scripts" / name
        destination = scripts_target / name
        shutil.copy2(source, destination)
        copied.append(destination)
    for source in sorted((skill_root / "schemas").rglob("*.json")):
        destination = schemas_target / source.relative_to(skill_root / "schemas")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied.append(destination)
    wrapper = automation / "build.py"
    wrapper.write_text(WRAPPER, encoding="utf-8")
    copied.append(wrapper)
    manifest = {
        "schema_version": 1,
        "source": "datacoolie-build",
        "files": [
            {
                "path": path.relative_to(automation).as_posix(),
                "sha256": _sha256(path),
            }
            for path in sorted(copied)
        ],
    }
    manifest_path = automation / "AUTOMATION-MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return automation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        output = render(args.workspace, force=args.force)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"OK: rendered project-owned automation -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
