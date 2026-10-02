"""Inspect the detached external context carried by a Driver session."""

from __future__ import annotations

import argparse
import json

from datacoolie.core.models.run_config import DataCoolieRunConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--attributes-json",
        type=json.loads,
        default={"factory_pipeline_run_id": "example-001", "trigger": "manual"},
        help="A JSON object supplied by the external scheduler",
    )
    args = parser.parse_args()
    config = DataCoolieRunConfig(run_attributes=args.attributes_json)
    print(json.dumps(config.run_attributes, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
