"""package.py — Build datacoolie functions into a deployable artifact.

Detects pyproject.toml (wheel) or __init__.py (zip) and produces a
distributable package for platform deployment.

Usage:
    python scripts/package.py
    python scripts/package.py --format wheel --output ./dist/
    python scripts/package.py --format zip
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def build_wheel(functions_dir: Path, output_dir: Path) -> Path:
    """Build a wheel from functions/ using python -m build."""
    output_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(output_dir), str(functions_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: wheel build failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    wheels = list(output_dir.glob("*.whl"))
    if not wheels:
        print("ERROR: No .whl file produced", file=sys.stderr)
        sys.exit(1)

    print(f"  \u2713 Built wheel: {wheels[0].name}")
    return wheels[0]


def build_zip(functions_dir: Path, output_dir: Path) -> Path:
    """Zip the functions/ directory (excluding __pycache__)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_base = output_dir / "functions"

    # shutil.make_archive wants base name without extension
    archive_path = shutil.make_archive(
        str(archive_base),
        "zip",
        root_dir=functions_dir.parent,
        base_dir=functions_dir.name,
    )
    # Filter __pycache__ by rebuilding
    # shutil.make_archive doesn't support exclusions, so we use a manual approach
    import zipfile

    clean_zip = output_dir / "functions.zip"
    with zipfile.ZipFile(archive_path, "r") as src, zipfile.ZipFile(
        str(clean_zip) + ".tmp", "w", zipfile.ZIP_DEFLATED
    ) as dst:
        for item in src.infolist():
            if "__pycache__" not in item.filename:
                dst.writestr(item, src.read(item.filename))

    Path(archive_path).unlink(missing_ok=True)
    tmp_path = Path(str(clean_zip) + ".tmp")
    tmp_path.rename(clean_zip)

    print(f"  \u2713 Built zip: {clean_zip.name}")
    return clean_zip


def validate_import(functions_dir: Path) -> bool:
    """Quick check that the functions package is importable."""
    import os as _os
    env = _os.environ.copy()
    env["PYTHONPATH"] = str(functions_dir.parent) + _os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-c", "import functions"],
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        print("  \u2713 Import validation passed")
        return True
    print(f"  \u2717 Import validation failed: {result.stderr.strip()}")
    return False


def detect_format(functions_dir: Path) -> str:
    """Auto-detect packaging format."""
    if (functions_dir / "pyproject.toml").is_file():
        return "wheel"
    return "zip"


def main() -> None:
    parser = argparse.ArgumentParser(description="Package datacoolie functions for deployment")
    parser.add_argument(
        "--format",
        choices=("wheel", "zip", "auto"),
        default="auto",
        help="Package format (default: auto-detect)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: .datacoolie/generated/dist/)",
    )
    parser.add_argument(
        "--functions-dir",
        type=Path,
        default=None,
        help="Functions directory (default: ./functions/)",
    )
    args = parser.parse_args()

    project_dir = Path.cwd()
    functions_dir = args.functions_dir or (project_dir / "functions")
    output_dir = args.output or (project_dir / ".datacoolie" / "generated" / "dist")

    if not functions_dir.is_dir():
        print("No functions/ directory found — skipping packaging.")
        sys.exit(0)

    fmt = args.format if args.format != "auto" else detect_format(functions_dir)

    print(f"\nDataCoolie Deploy — Package ({fmt})")
    print("-" * 50)

    if fmt == "wheel":
        if not (functions_dir / "pyproject.toml").is_file():
            print("ERROR: --format wheel requires functions/pyproject.toml", file=sys.stderr)
            sys.exit(1)
        build_wheel(functions_dir, output_dir)
    else:
        if not (functions_dir / "__init__.py").is_file():
            print("ERROR: --format zip requires functions/__init__.py", file=sys.stderr)
            sys.exit(1)
        build_zip(functions_dir, output_dir)

    validate_import(functions_dir)

    print("-" * 50)
    print(f"Package output: {output_dir}\n")


if __name__ == "__main__":
    main()
