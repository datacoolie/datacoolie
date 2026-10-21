"""Native Delta path assertions using container Python; no pytest dependency."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
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


def _error_class(error):
    getter = getattr(error, "getErrorClass", None)
    return getter() if callable(getter) else None


def main():
    sys.path.insert(0, str(PRODUCT_ROOT / "usecase-sim"))
    from runner._runner_utils import build_spark_session
    from delta import DeltaTable
    from datacoolie.engines._spark.delta import table_exists_by_path
    from datacoolie.core.exceptions import EngineError
    from datacoolie.platforms import LocalPlatform

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
    platform = LocalPlatform()
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
            DeltaTable.forPath(spark, relative).toDF().schema.simpleString()
            == "struct<id:bigint,value:string>"
        )
        assert table_exists_by_path(spark, platform, relative)
        assert table_exists_by_path(spark, platform, target.as_posix())
        assert not table_exists_by_path(
            spark, platform, (root / "missing").as_posix()
        )
        empty = root / "empty"
        empty.mkdir()
        assert not table_exists_by_path(spark, platform, empty.as_posix())
        occupied = root / "occupied"
        occupied.mkdir()
        (occupied / "data.txt").write_text("occupied")
        _expect_failure(
            lambda: table_exists_by_path(spark, platform, occupied.as_posix()), EngineError
        )
        empty_log = root / "empty-log/_delta_log"
        empty_log.mkdir(parents=True)
        empty_log_failure = _expect_failure(
            lambda: table_exists_by_path(
                spark, platform, empty_log.parent.as_posix()
            )
        )

        corrupt = root / "corrupt"
        shutil.copytree(target, corrupt)
        (corrupt / "_delta_log/00000000000000000001.json").write_text(
            "not valid json\n"
        )
        corrupt_failure = _expect_failure(
            lambda: table_exists_by_path(spark, platform, corrupt.as_posix())
        )

        unsupported = root / "unsupported"
        shutil.copytree(target, unsupported)
        (unsupported / "_delta_log/00000000000000000001.json").write_text(
            json.dumps({"protocol": {"minReaderVersion": 99, "minWriterVersion": 2}})
            + "\n"
        )
        unsupported_failure = _expect_failure(
            lambda: table_exists_by_path(spark, platform, unsupported.as_posix())
        )

        permission = root / "permission"
        permission_observation = {"status": "skipped-root"}
        if hasattr(os, "geteuid") and os.geteuid() != 0:
            shutil.copytree(target, permission)
            try:
                permission.chmod(0)
                permission_failure = _expect_failure(
                    lambda: table_exists_by_path(
                        spark, platform, permission.as_posix()
                    )
                )
                permission_observation = {
                    "status": "propagated",
                    "error": type(permission_failure).__name__,
                    "class": _error_class(permission_failure),
                }
            finally:
                permission.chmod(0o755)

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
            "native_failures": {
                "empty_log": {
                    "error": type(empty_log_failure).__name__,
                    "class": _error_class(empty_log_failure),
                },
                "corrupt": {
                    "error": type(corrupt_failure).__name__,
                    "class": _error_class(corrupt_failure),
                },
                "unsupported": {
                    "error": type(unsupported_failure).__name__,
                    "class": _error_class(unsupported_failure),
                },
            },
            "permission": permission_observation,
        }
    finally:
        spark.stop()
    print(json.dumps(observation, sort_keys=True))


if __name__ == "__main__":
    main()
