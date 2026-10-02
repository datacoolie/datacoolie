"""Run the incremental CSV example once; invoke it again after new input arrives."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="ingest2bronze")
    parser.add_argument("--state-base-path", default=".runtime")
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[2]
    os.chdir(project_root)

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        artifact_base_path=str(project_root),
        state_base_path=args.state_base_path,
        config=DataCoolieRunConfig(job_id="incremental-project-local"),
    ) as driver:
        result = driver.run(stage=args.stage)
    print(f"completed={result.succeeded} failed={result.failed} total={result.total}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
