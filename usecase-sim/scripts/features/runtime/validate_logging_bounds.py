"""Qualify v6 snapshot/batch diagnostics with native Polars and fresh paths."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from datacoolie.core.models.connection import Connection  # noqa: E402
from datacoolie.core.models.dataflow import DataFlow  # noqa: E402
from datacoolie.core.models.destination import Destination  # noqa: E402
from datacoolie.core.models.run_config import DataCoolieRunConfig  # noqa: E402
from datacoolie.core.models.source import Source  # noqa: E402
from datacoolie.engines.polars_engine import PolarsEngine  # noqa: E402
from datacoolie.logging.configuration.config import LogConfig  # noqa: E402
from datacoolie.orchestration.driver import DataCoolieDriver  # noqa: E402
from datacoolie.platforms.local_platform import LocalPlatform  # noqa: E402


def run(root: Path) -> dict:
    if root.exists():
        raise ValueError("Choose a fresh --root; existing evidence is preserved")
    root.mkdir(parents=True)
    receipts = []
    for mode in ("snapshot", "batch"):
        for bounded in (False, True):
            case = f"{mode}_{'bounded' if bounded else 'normal'}"
            case_root = root / case
            engine = PolarsEngine()
            engine.register_table("logging_fixture", pl.DataFrame({"id": [1, 2], "value": ["a", "b"]}))
            source = Connection(name="logging_source", connection_type="lakehouse", format="delta", configure={"base_path": str(case_root / "source")})
            target = Connection(name="logging_target", connection_type="file", format="parquet", configure={"base_path": str(case_root / "output")})
            query = "SELECT * FROM logging_fixture" + (" /*" + "🧭" * 5000 + "*/" if bounded else "")
            flow = DataFlow(name="logging_bounds", source=Source(connection=source, query=query), destination=Destination(connection=target, table="rows", load_type="overwrite"))
            log_root = case_root / "logs"
            with DataCoolieDriver(engine, platform=LocalPlatform(), config=DataCoolieRunConfig(job_id=f"logging-v6-{case}"), state_base_path=str(case_root / "state"), log_base_path=str(log_root), log_config=LogConfig(persistence_mode=mode, diagnostic_max_bytes=256 if bounded else 65536, flush_interval_seconds=0, flush_batch_bytes=1)) as driver:
                result = driver.run(dataflows=[flow])
                assert result.succeeded == 1 and result.failed == 0, result
            rows = [json.loads(line) for path in log_root.rglob("*.json") for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            assert rows and all(row["log_schema_version"] == 6 for row in rows)
            jobs = [row for row in rows if row["_type"] == "job_run_log"]
            flows = [row for row in rows if row["_type"] == "dataflow_run_log"]
            assert len(jobs) == len(flows) == 1
            job, record = jobs[0], flows[0]
            assert job["total_dataflows"] == job["total_succeeded"] == 1
            assert job["log_records_dropped"] == 0
            assert record["status"] == "succeeded" and record["metrics"]["rows_read"] == 2
            assert record["metrics"]["rows_written"] is None
            assert record["source_query"] == query
            if bounded:
                assert record["diagnostic_issues"]
                assert any(issue["field"].endswith("source_action") for issue in record["diagnostic_issues"])
            output = pl.read_parquet(str(case_root / "output" / "rows" / "**" / "*.parquet"))
            assert sorted(output["id"].to_list()) == [1, 2]
            receipts.append({"case": case, "job_id": job["job_id"], "log_session_id": job["log_session_id"], "output_rows": 2, "rows_written_metric": None, "diagnostic_issues": record.get("diagnostic_issues", []), "log_records_dropped": 0})
    receipt = {"schema_version": 6, "engine": "polars", "polars_version": pl.__version__, "cases": receipts}
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.root.resolve()), indent=2))
