"""Assert that a failed paginated API read committed no target or state."""

from __future__ import annotations

import argparse
from pathlib import Path


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
RUNTIME_DATA_DIR = USECASE_SIM_DIR / ".runtime" / "data"


def _under(path: Path, root: Path, label: str) -> Path:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = (USECASE_SIM_DIR.parent / resolved).resolve()
    else:
        resolved = resolved.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise AssertionError(f"{label} must stay below {root}: {resolved}") from exc
    return resolved


def validate(output: str, state_root: str) -> None:
    output_path = _under(Path(output), RUNTIME_DATA_DIR / "output", "output")
    state_path = _under(Path(state_root), RUNTIME_DATA_DIR, "state_root")
    if output_path.exists() and any(output_path.rglob("*")):
        raise AssertionError(f"Failed API pagination created target files: {output_path}")
    if state_path.exists() and any(state_path.rglob("watermark_value.json")):
        raise AssertionError(f"Failed API pagination created watermark state: {state_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--state-root", required=True)
    args = parser.parse_args()
    validate(args.output, args.state_root)
    print("validated API pagination failure: no target or watermark commit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
