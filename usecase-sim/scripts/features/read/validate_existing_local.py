"""Exact native readback for existing local read and transform/partition fixtures."""

from __future__ import annotations
import argparse
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixture_support import compile_existing  # noqa: E402
from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    business_rows,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)

MODES = [
    "csv",
    "json",
    "jsonl",
    "parquet",
    "avro",
    "delta",
    "row_filter",
    "partition_single",
    "partition_multi",
    "combined",
]


def validate(root=None, mode="csv"):
    import polars as pl

    if mode == "row_filter":
        group, flow = "transform", "row_filter__on_computed_col"
    elif mode.startswith("partition_"):
        group, flow = "write_basic", "partition__" + mode.split("_")[1]
    elif mode == "combined":
        group, flow = "workflows", "combined__dedup_schema_addcols_partition"
    else:
        group, flow = "read", "read__" + mode
    case = resolve_case_root(group, root)
    document, identity = compile_existing(case, group, flow)
    engine = PolarsEngine()
    source = document["dataflows"][0]["source"]
    table = source["table"]
    source_format = next(
        c["format"]
        for c in document["connections"]
        if c["name"] == source["connection_name"]
    )
    if mode in ("csv", "json", "jsonl", "parquet", "avro", "delta"):
        rows = [{"id": "1", "value": "Alpha Việt"}, {"id": "2", "value": 'quote, " x'}]
    elif mode == "row_filter":
        rows = [
            {"id": "1", "amount": "99.50"},
            {"id": "2", "amount": "100.00"},
            {"id": "3", "amount": "101.25"},
        ]
    else:
        rows = [
            {
                "order_id": "1",
                "order_date": "2023-12-31",
                "amount": "99.50",
                "modified_at": "2024-01-01T10:00:00",
                "region": "North",
            },
            {
                "order_id": "2",
                "order_date": "2024-01-01",
                "amount": "101.25",
                "modified_at": "2024-01-02T10:00:00",
                "region": "South",
            },
        ]
        if mode == "combined":
            rows.insert(
                0, {**rows[0], "amount": "1.00", "modified_at": "2023-12-01T10:00:00"}
            )
    path = case.input / table
    path.mkdir(parents=True, exist_ok=False)
    frame = pl.DataFrame(rows)
    if source_format == "delta":
        frame.write_delta(str(path))
    else:
        file = path / ("source." + source_format)
        {
            "csv": frame.write_csv,
            "json": frame.write_json,
            "jsonl": frame.write_ndjson,
            "parquet": frame.write_parquet,
            "avro": frame.write_avro,
        }[source_format](file)
    evidence = run_driver(case, stage=identity["runtime_stage"])
    assert_successful_flow(evidence, identity["runtime_flow"])
    target = case.output / document["dataflows"][0]["destination"]["table"]
    fmt = next(
        c["format"]
        for c in document["connections"]
        if c["name"] == document["dataflows"][0]["destination"]["connection_name"]
    )
    if fmt == "delta":
        actual = business_rows(read_delta_rows(engine, target))
    else:
        actual = business_rows(
            pl.concat(
                [pl.read_parquet(p) for p in target.rglob("*.parquet")]
            ).to_dicts()
        )
    if mode == "row_filter":
        expected = [{**rows[2], "id": 3, "amount": 101.25, "amount_double": 101.25}]
    elif mode.startswith("partition_"):
        expected = [
            {
                **r,
                "order_id": int(r["order_id"]),
                "amount": float(r["amount"]),
                "order_date": date.fromisoformat(r["order_date"]),
                "order_year": int(r["order_date"][:4]),
                **({"region_part": r["region"]} if mode == "partition_multi" else {}),
            }
            for r in rows
        ]
    elif mode == "combined":
        expected = [
            {
                **r,
                "order_id": int(r["order_id"]),
                "order_date": date.fromisoformat(r["order_date"]),
                "amount": Decimal(r["amount"]),
                "order_year": int(r["order_date"][:4]),
                "etl_batch": "combined_test",
            }
            for r in rows[1:]
        ]
    else:
        expected = rows
    if mode == "csv":
        expected = [{**r, "id": int(r["id"])} for r in rows]
    assert_exact_rows(actual, expected)
    schema = {
        k: str(v)
        for k, v in (
            engine.read_delta(str(target)).collect_schema()
            if fmt == "delta"
            else pl.read_parquet(next(target.rglob("*.parquet"))).schema
        ).items()
        if not k.startswith("__")
    }
    if mode in ("csv", "json", "jsonl", "parquet", "avro", "delta"):
        assert_schema(
            schema, {"id": "Int64" if mode == "csv" else "String", "value": "String"}
        )
    elif mode == "row_filter":
        assert_schema(
            schema, {"id": "Int64", "amount": "Float64", "amount_double": "Float64"}
        )
    else:
        expected_schema = {
            "order_id": "Int64",
            "order_date": "Date",
            "amount": "Float64",
            "modified_at": "String",
            "region": "String",
            "order_year": "Int32",
        }
        if mode == "combined":
            expected_schema.update(
                amount="Decimal(precision=18, scale=2)", etl_batch="String"
            )
        if mode == "partition_multi":
            expected_schema["region_part"] = "String"
        assert_schema(schema, expected_schema)
    if mode.startswith("partition_") or mode == "combined":
        from deltalake import DeltaTable

        partition_columns = DeltaTable(str(target)).metadata().partition_columns
        expected_partitions = (
            ["region_part", "order_year"]
            if mode == "partition_multi"
            else ["order_year"]
        )
        assert partition_columns == expected_partitions, partition_columns
        layout = [
            str(p.relative_to(target)).replace("\\", "/")
            for p in target.rglob("*.parquet")
        ]
        assert any("order_year=2023/" in p for p in layout) and any(
            "order_year=2024/" in p for p in layout
        )
    else:
        layout = []
    payload = receipt_payload(
        case,
        runs=[evidence],
        script=Path(__file__),
        details={
            "mode": mode,
            "identity_mapping": identity,
            "exact_rows": actual,
            "business_schema": schema,
            "partition_layout": layout,
        },
    )
    payload["profile"]["destination_format"] = fmt
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--mode", choices=MODES, default="csv")
    args = parser.parse_args()
    print(validate(args.root, args.mode))
