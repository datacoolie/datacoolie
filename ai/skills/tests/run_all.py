"""Run tests for maintained Skill scripts; source services are opt-in."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    groups = sorted(path.name for path in (HERE / "unit").iterdir() if path.is_dir()
                    and not path.name.startswith(("_", ".")))
    parser.add_argument("groups", nargs="*", metavar="GROUP",
                        help=f"Select: {', '.join(groups)}; build is an alias for automation")
    parser.add_argument("--integration", action="store_true", help="Also test Discover with Docker sources")
    parser.add_argument("--keep-integration", action="store_true", help="Keep source services after tests")
    args = parser.parse_args()
    args.groups = ["automation" if name == "build" else name for name in args.groups]
    unknown = sorted(set(args.groups) - set(groups))
    if unknown:
        parser.error(f"Unknown script test group(s): {', '.join(unknown)}")
    if args.integration and args.groups and "discover" not in args.groups:
        parser.error("--integration requires the discover group")
    if args.keep_integration and not args.integration:
        parser.error("--keep-integration requires --integration")

    targets = [HERE / "unit" / name for name in dict.fromkeys(args.groups)] if args.groups else [HERE / "unit"]
    if args.integration:
        targets.append(HERE / "integration")
    command = [sys.executable, "-m", "pytest", "-o", "addopts=", *map(str, targets), "-q"]
    if args.keep_integration:
        command.append("--keep-services")
    return subprocess.run(command, cwd=HERE.parents[2], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
