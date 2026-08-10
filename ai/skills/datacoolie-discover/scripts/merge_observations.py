"""Deterministically merge validated per-probe observations into one final CSV."""
from __future__ import annotations

import argparse
from pathlib import Path

from _observation_contract import KEY_FIELDS, atomic_write_observations, read_observations


def merge(inputs: list[Path], output: Path) -> int:
    if not inputs:
        raise ValueError("At least one --input is required")
    rows = []
    for path in inputs:
        rows.extend(read_observations(path))
    rows.sort(key=lambda row: tuple(row[field] for field in KEY_FIELDS) + (row["ordinal"],))
    return atomic_write_observations(output, rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge discovery observation probe outputs.", allow_abbrev=False,
    )
    parser.add_argument("--input", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        count = merge(args.input, args.output)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Merged {count} observation(s) into {args.output}")


if __name__ == "__main__":
    main()
