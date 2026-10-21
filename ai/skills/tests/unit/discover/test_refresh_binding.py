"""Refresh evidence must cover the source and bind each exact CSV."""

import json

import pytest

from _observation_contract import (
    atomic_write_observations,
    make_observation,
    read_observations,
)
from _probe_status import observation_scope, write_probe_status
from merge_observations import merge


def probe(tmp_path, name, *, status="complete", kind="objects"):
    csv = tmp_path / f"{name}.csv"
    receipt = tmp_path / f"{name}.json"
    rows = [
        make_observation(
            source="erp",
            object_type="table",
            object=name,
            column="id",
            data_type="long",
            native_type="bigint",
        )
    ]
    atomic_write_observations(csv, rows)
    write_probe_status(
        receipt,
        source="erp",
        probe="fixture",
        row_count=1,
        issues=["bounded"] if status == "partial" else [],
        output_path=csv,
        scope=observation_scope(
            rows, kind=kind, filters={"table": name} if kind == "objects" else {}
        ),
    )
    return csv, receipt


def boundary(tmp_path, names):
    path = tmp_path / "coverage.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sources": [
                    {
                        "source": "erp",
                        "evidence": "Read-only owner inventory fixture, revision 1",
                        "objects": [
                            ["erp", "table", "", "", name, ""] for name in names
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_distinct_probes_same_source_refresh_with_inventory(tmp_path):
    pairs = [probe(tmp_path, name) for name in ("orders", "customers")]
    base = tmp_path / "base.csv"
    atomic_write_observations(
        base,
        [
            make_observation(
                source="crm",
                object_type="table",
                object="contacts",
                column="id",
                native_type="bigint",
                data_type="long",
            )
        ],
    )
    output = tmp_path / "candidate.csv"
    assert (
        merge(
            [],
            output,
            probe_inputs=pairs,
            base=base,
            replace_sources=["erp"],
            coverage_input=boundary(tmp_path, ["orders", "customers"]),
        )
        == 3
    )
    assert {row["source"] for row in read_observations(output)} == {"crm", "erp"}


@pytest.mark.parametrize(
    "failure",
    [
        "swap",
        "tamper",
        "filtered",
        "missing_object",
        "duplicate_status",
        "duplicate_csv",
        "legacy",
    ],
)
def test_refresh_rejections_preserve_base(tmp_path, failure):
    first, second = probe(tmp_path, "orders"), probe(tmp_path, "customers")
    base = tmp_path / "base.csv"
    atomic_write_observations(
        base, read_observations(first[0]) + read_observations(second[0])
    )
    original = base.read_bytes()
    pairs = [first, second]
    coverage = boundary(tmp_path, ["orders", "customers"])
    if failure == "swap":
        pairs = [(first[0], second[1]), (second[0], first[1])]
    elif failure == "tamper":
        first[0].write_bytes(first[0].read_bytes() + b"\n")
    elif failure == "filtered":
        coverage = None
    elif failure == "missing_object":
        pairs = [first]
    elif failure == "duplicate_status":
        pairs = [first, (second[0], first[1])]
    elif failure == "duplicate_csv":
        pairs = [first, first]
    else:
        first[1].write_text(
            json.dumps({"source": "erp", "status": "complete", "row_count": 1})
        )
    with pytest.raises(ValueError):
        merge(
            [],
            tmp_path / "candidate.csv",
            probe_inputs=pairs,
            base=base,
            replace_sources=["erp"],
            coverage_input=coverage,
        )
    assert base.read_bytes() == original
    assert not (tmp_path / "candidate.csv").exists()


def test_partial_coverage_requires_exact_acceptance(tmp_path):
    pair = probe(tmp_path, "orders", status="partial")
    base = tmp_path / "base.csv"
    atomic_write_observations(base, read_observations(pair[0]))
    coverage = boundary(tmp_path, ["orders", "customers"])
    with pytest.raises(ValueError, match="explicit acceptance"):
        merge(
            [],
            tmp_path / "candidate.csv",
            probe_inputs=[pair],
            base=base,
            replace_sources=["erp"],
            coverage_input=coverage,
        )
    assert (
        merge(
            [],
            tmp_path / "candidate.csv",
            probe_inputs=[pair],
            base=base,
            replace_sources=["erp"],
            coverage_input=coverage,
            accept_partial_sources=["erp"],
        )
        == 1
    )


def test_candidate_cannot_overwrite_input_or_base(tmp_path):
    pair = probe(tmp_path, "orders", kind="source")
    base = tmp_path / "base.csv"
    atomic_write_observations(base, read_observations(pair[0]))
    for output in (base, pair[0], pair[1]):
        with pytest.raises(ValueError, match="distinct"):
            merge([], output, probe_inputs=[pair], base=base, replace_sources=["erp"])
