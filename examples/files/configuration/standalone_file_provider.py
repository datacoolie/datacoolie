"""Use FileProvider without a Driver session."""

from __future__ import annotations

import argparse

from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata_base_path")
    parser.add_argument("--watermark-base-path")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    # Construction is side-effect free. Initialization is the explicit point
    # at which files are read and the complete metadata scope is validated.
    provider = FileProvider(
        metadata_base_path=args.metadata_base_path,
        platform=LocalPlatform(),
        watermark_base_path=args.watermark_base_path,
        enable_cache=not args.no_cache,
    )
    try:
        provider.initialize()
        print(
            f"connections={len(provider.get_connections())} "
            f"dataflows={len(provider.get_dataflows())} "
            f"cache={'off' if args.no_cache else 'on'}"
        )
    finally:
        provider.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
