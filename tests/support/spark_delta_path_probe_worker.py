"""Native Delta path assertions using container Python; no pytest dependency."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from uuid import uuid4

PRODUCT_ROOT = Path(__file__).resolve().parents[2]


def _hashes(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def _expect_failure(call, exception_type=Exception):
    try:
        call()
    except exception_type as failure:
        return failure
    raise AssertionError(f"Expected {exception_type.__name__}")


def main():
    sys.path.insert(0, str(PRODUCT_ROOT / "usecase-sim"))
    from runner._runner_utils import build_spark_session
    from delta import DeltaTable
    from datacoolie.engines._spark.delta import table_exists_by_path
    from datacoolie.core.exceptions import EngineError

    root = (
        PRODUCT_ROOT
        / "usecase-sim/.runtime/data/behavior_cases"
        / ("delta_probe_spark_" + uuid4().hex)
    )
    root.mkdir(parents=True, exist_ok=False)
    spark = build_spark_session(
        app_name="bounded-delta-path-probe",
        extra_config={"spark.sql.shuffle.partitions": "1", "spark.master": "local[1]"},
    )
    try:
        assert spark.version.startswith("3.5."), spark.version
        target = root / "table"
        relative = target.relative_to(PRODUCT_ROOT).as_posix()
        spark.createDataFrame([(101, "alpha")], "id long, value string").write.format(
            "delta"
        ).save(str(target))
        before = _hashes(target)
        # Read-only same-table diagnosis of the original SQL resolution defect.
        failure = _expect_failure(
            lambda: spark.sql(f"DESCRIBE DETAIL delta.`{relative}`").collect()
        )
        sql_error_class = failure.getErrorClass()
        assert sql_error_class in {
            "DELTA_MISSING_DELTA_TABLE", "TABLE_OR_VIEW_NOT_FOUND"
        }, sql_error_class
        assert (
            spark.sql(f"DESCRIBE DETAIL delta.`{target.as_posix()}`").collect()[0][
                "format"
            ]
            == "delta"
        )
        assert (
            DeltaTable.forPath(spark, relative).detail().collect()[0]["format"]
            == "delta"
        )
        assert table_exists_by_path(spark, None, relative)
        assert table_exists_by_path(spark, None, target.as_posix())
        assert not table_exists_by_path(spark, None, (root / "missing").as_posix())
        empty = root / "empty"
        empty.mkdir()
        assert not table_exists_by_path(spark, None, empty.as_posix())
        occupied = root / "occupied"
        occupied.mkdir()
        (occupied / "data.txt").write_text("occupied")
        _expect_failure(
            lambda: table_exists_by_path(spark, None, occupied.as_posix()), EngineError
        )
        corrupt = root / "corrupt/_delta_log"
        corrupt.mkdir(parents=True)
        (corrupt / "00000000000000000000.json").write_text("not-json")
        _expect_failure(
            lambda: table_exists_by_path(spark, None, corrupt.parent.as_posix())
        )
        assert [
            row.asDict()
            for row in spark.read.format("delta").load(relative).select(
                "id", "value"
            ).collect()
        ] == [{"id": 101, "value": "alpha"}]
        assert _hashes(target) == before, "Read-only probes altered the table"
        observation = {
            "status": "passed",
            "spark_version": spark.version,
            "relative_sql_error_class": sql_error_class,
        }
    finally:
        spark.stop()
    print(json.dumps(observation, sort_keys=True))


if __name__ == "__main__":
    main()
