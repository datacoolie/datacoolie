"""Run one focused built-in transform example."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


def _project_root() -> Path:
    for candidate in (Path(__file__).resolve(), *Path(__file__).resolve().parents):
        if (candidate / "metadata").is_dir():
            return candidate
    raise FileNotFoundError("Cannot locate the transform project metadata root")


def _ensure_input(root: Path) -> None:
    source = root / "data" / "input" / "orders" / "orders.csv"
    if source.is_file():
        return
    source.parent.mkdir(parents=True, exist_ok=True)
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("order_id", "category", "amount"))
        writer.writerows(((1, " Hardware ", "19.99"), (2, "Software", "29.00"), (3, " HARDWARE", "5.50")))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="transform")
    parser.add_argument("--state-base-path", default=".runtime")
    args = parser.parse_args()

    project_root = _project_root()
    os.chdir(project_root)
    _ensure_input(project_root)
    platform = LocalPlatform()
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        artifact_base_path=str(project_root),
        state_base_path=args.state_base_path,
        config=DataCoolieRunConfig(job_id="transform-project-local"),
    ) as driver:
        result = driver.run(stage=args.stage)
    print(f"completed={result.succeeded} failed={result.failed} total={result.total}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
