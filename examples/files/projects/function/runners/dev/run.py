"""Runnable source-tree entrypoint for the packaged function example."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="ingest2bronze")
    parser.add_argument("--state-base-path", default=".runtime")
    args = parser.parse_args()

    project_root = next(
        candidate
        for candidate in [Path(__file__).resolve(), *Path(__file__).resolve().parents]
        if (candidate / "metadata").is_dir() and (candidate / "functions").is_dir()
    )
    os.chdir(project_root)
    # A source checkout imports ``functions`` from the project root. A built
    # artifact instead supplies the deterministic functions.zip payload.
    packaged = project_root / "functions" / "functions.zip"
    sys.path.insert(0, str(packaged if packaged.is_file() else project_root))

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        artifact_base_path=str(project_root),
        state_base_path=args.state_base_path,
        config=DataCoolieRunConfig(
            job_id="function-project-local",
            allowed_function_prefixes=["functions"],
        ),
    ) as driver:
        result = driver.run(stage=args.stage)
    print(f"completed={result.succeeded} failed={result.failed} total={result.total}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
