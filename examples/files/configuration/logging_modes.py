"""Build public LogConfig variants without touching the filesystem."""

from __future__ import annotations

import argparse

from datacoolie.logging import LogConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=".runtime/logs")
    parser.add_argument("--mode", choices=("snapshot", "batch"), default="snapshot")
    parser.add_argument("--flush-interval-seconds", type=float, default=300.0)
    args = parser.parse_args()
    config = LogConfig(
        output_path=args.output_path,
        persistence_mode=args.mode,
        flush_interval_seconds=args.flush_interval_seconds,
    )
    print(
        f"mode={config.persistence_mode} output={config.output_path} "
        f"flush_interval_seconds={config.flush_interval_seconds:g}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
