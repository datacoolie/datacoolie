#!/usr/bin/env python3
"""Render optional project-owned wrappers around the installed DataCoolie CLI."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import warnings


BUILD_WRAPPER = '''#!/usr/bin/env python3
"""Project-owned CI entrypoint; the installed CLI owns all build logic."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


def _cli() -> list[str]:
    return [sys.executable, "-m", "datacoolie"]


def _run(command: list[str]):
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result


def _succeeded(result) -> bool:
    if result.returncode != 0:
        return False
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError):
        return False
    return isinstance(payload, dict) and payload.get("ok") is True


def _exit_code(result) -> int:
    return result.returncode or 1


def main() -> int:
    command = _cli()
    validate = _run(
        [*command, "validate", "--project-dir", str(PROJECT), "--format", "json"],
    )
    if not _succeeded(validate):
        return _exit_code(validate)
    build = _run(
        [*command, "build", "--project-dir", str(PROJECT), "--format", "json", *sys.argv[1:]],
    )
    return build.returncode if _succeeded(build) else _exit_code(build)


if __name__ == "__main__":
    raise SystemExit(main())
'''

VALIDATE_WRAPPER = '''#!/usr/bin/env python3
"""Project-owned CI entrypoint for CLI validation."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


def _cli() -> list[str]:
    return [sys.executable, "-m", "datacoolie"]


def _run(command: list[str]):
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result


def _succeeded(result) -> bool:
    if result.returncode != 0:
        return False
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError):
        return False
    return isinstance(payload, dict) and payload.get("ok") is True


def _exit_code(result) -> int:
    return result.returncode or 1


if __name__ == "__main__":
    result = _run(
        [*_cli(), "validate", "--project-dir", str(PROJECT), "--format", "json", *sys.argv[1:]],
    )
    raise SystemExit(result.returncode if _succeeded(result) else _exit_code(result))
'''


README = """# Project automation\n\nThese wrappers call the installed `dc`/`datacoolie` CLI. They do not vendor\nmetadata merging, validation, packaging, or artifact-manifest code.\n\n- `python automation/validate.py` validates the project.\n- `python automation/build.py` validates and builds every environment.\n\nRelease/upload remains a target-owned workflow. The renderer's legacy\n`--metadata-layout` option is accepted for compatibility and emits a\ndeprecation warning; project configuration and the installed CLI own layout\ndecisions.\n"""


def render(
    workspace: Path, *, force: bool = False, metadata_layout: str | None = None
) -> Path:
    """Create direct CLI wrappers; *metadata_layout* is retained for API compatibility.

    The CLI configuration is authoritative, so a renderer option cannot change
    build semantics. It is accepted only to avoid breaking callers that still
    pass the preview option while they migrate to ``datacoolie.yml``.
    """

    if metadata_layout is not None:
        warnings.warn(
            "metadata_layout is deprecated; use datacoolie.yml and the installed CLI",
            DeprecationWarning,
            stacklevel=2,
        )
    workspace = workspace.expanduser().resolve()
    config = workspace / "datacoolie.yml"
    if not config.is_file():
        raise ValueError(f"Project contract not found: {config}")
    automation = workspace / "automation"
    managed = [
        automation / "build.py",
        automation / "validate.py",
        automation / "README.md",
    ]
    existing = [path for path in managed if path.exists()]
    if existing and not force:
        raise ValueError(
            "Project automation already exists; pass --force to refresh managed files"
        )
    automation.mkdir(parents=True, exist_ok=True)
    for path, content in (
        (automation / "build.py", BUILD_WRAPPER),
        (automation / "validate.py", VALIDATE_WRAPPER),
        (automation / "README.md", README),
    ):
        path.write_text(content, encoding="utf-8", newline="\n")
    return automation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--metadata-layout", choices=("single", "split", "preserve"), default=None
    )
    args = parser.parse_args()
    try:
        output = render(
            args.workspace, force=args.force, metadata_layout=args.metadata_layout
        )
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"OK: rendered project-owned automation -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
