"""Minimal project-owned runner for a built DataCoolie artifact.

This file is a template, not a framework command. The project chooses its
engine, platform, stage and external job parameters before constructing the
Driver.
"""

from __future__ import annotations

import argparse

from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact", help="Built artifact root or current projection")
    parser.add_argument("--stage", required=True)
    parser.add_argument("--state-base-path", default=".runtime")
    parser.add_argument("--sql-base-path", action="append", default=[])
    parser.add_argument("--job-num", type=int, default=1)
    parser.add_argument("--job-index", type=int, default=0)
    args = parser.parse_args()

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        artifact_base_path=args.artifact,
        state_base_path=args.state_base_path,
        sql_base_path=args.sql_base_path or None,
        config=DataCoolieRunConfig(job_num=args.job_num, job_index=args.job_index),
    ) as driver:
        result = driver.run(stage=args.stage)
        print(f"completed={result.succeeded} failed={result.failed} total={result.total}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
