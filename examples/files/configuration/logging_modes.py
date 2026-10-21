"""Build public LogConfig variants without touching the filesystem."""

from __future__ import annotations

import argparse

from datacoolie.logging import LogConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=".runtime/logs")
    parser.add_argument("--mode", choices=("snapshot", "batch"), default="snapshot")
    parser.add_argument("--flush-interval-seconds", type=float, default=300.0)
    parser.add_argument("--diagnostic-max-bytes", type=int, default=65_536)
    parser.add_argument("--max-record-bytes", type=int, default=1_048_576)
    args = parser.parse_args()
    config = LogConfig(
        output_path=args.output_path,
        persistence_mode=args.mode,
        flush_interval_seconds=args.flush_interval_seconds,
        diagnostic_max_bytes=args.diagnostic_max_bytes,
        max_record_bytes=args.max_record_bytes,
    )
    print(
        f"mode={config.persistence_mode} output={config.output_path} "
        f"flush_interval_seconds={config.flush_interval_seconds:g} "
        f"diagnostic_max_bytes={config.diagnostic_max_bytes} "
        f"max_record_bytes={config.max_record_bytes}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
