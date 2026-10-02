"""Runnable artifact-project entrypoint.

The project registers a tiny in-memory fixture before DataCoolie prepares the
SQL dataflow. This is intentionally project code: table registration is not a
framework-side discovery step.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="bronze2silver")
    parser.add_argument("--state-base-path", default=".runtime")
    args = parser.parse_args()

    # Locate the environment root from its metadata/queries pair. This works
    # both from the source project and from ``.builds/current/<env>`` after the
    # CLI copies ``runners/dev`` into the environment artifact.
    project_root = next(
        candidate
        for candidate in [Path(__file__).resolve(), *Path(__file__).resolve().parents]
        if (candidate / "metadata").is_dir() and (candidate / "queries").is_dir()
    )

    # Run from the extracted project root so relative connection paths retain
    # the same meaning after a ZIP download.
    import os

    os.chdir(project_root)
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    engine.register_table(
        "orders",
        pl.DataFrame(
            {
                "order_id": [1, 2, 3],
                "amount": [19.99, 29.00, 5.50],
                "category": ["hardware", "software", "hardware"],
            }
        ),
    )
    engine.register_table(
        "order_categories",
        pl.DataFrame(
            {
                "category": ["hardware", "software"],
                "category_group": ["physical", "digital"],
            }
        ),
    )

    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        artifact_base_path=str(project_root),
        state_base_path=args.state_base_path,
        config=DataCoolieRunConfig(job_id="artifact-project-local"),
    ) as driver:
        result = driver.run(stage=args.stage)
    print(f"completed={result.succeeded} failed={result.failed} total={result.total}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
